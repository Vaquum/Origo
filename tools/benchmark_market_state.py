"""Measure the deployed market state service against PRD-0022's frozen acceptance protocol.

The protocol, its thresholds and the raw-trade reference are fixed in slice #476. On
37.27.112.167 the runbook in ``docs/Developer/Market-state-cube.md`` runs ``client`` in a
consumer container, pipes the generated SQL through ``clickhouse-client``, runs ``backfill`` in
the Dagster container and finally ``verdict``, whose last line is ``PASS`` or
``FAIL <criteria>``. The client needs no ClickHouse credentials: it only calls the service and
reads result files through the cube reader.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.market_state_reader import (
    DEFAULT_URL,
    MarketStateError,
    MarketStateResult,
    open_file,
    query,
    read_table,
)

PROTOCOL_VERSION: Final = 1
STAGES: Final = (('A', 1, 1), ('B', 5, 1), ('C', 3, 2))  # (stage, rounds, concurrent streams)
PAIRS: Final = 3  # stage D: two C11 requests at once, three times
THRESHOLDS: Final = {
    'Q1_p90_seconds': 3.0,
    'Q2_median_seconds': 5.0,
    'Q2_max_seconds': 10.0,
    'R1_rss_peak_bytes': 1024**3,
    'K2_p50_increase_seconds': 2.0,
}
TOLERANCE: Final = (1e-8, 1e-12)  # absolute USDT, relative
NUMERIC_CASES: Final = ('C04', 'C10', 'C11', 'C12')
CONTENTION_SERIES: Final = (
    ('provisional', 'binance_spot_trades'),
    ('provisional', 'binance_spot_trades:mount'),
    ('provisional', 'binance_spot_aggtrades'),
    ('provisional', 'binance_spot_aggtrades:mount'),
    ('provisional', 'binance_perp_aggtrades'),
    ('provisional', 'binance_perp_aggtrades:mount'),
    ('depth', 'depth20_snapshots'),
    ('depth', 'depth200_snapshots'),
)
REPORTED_SERIES: Final = (
    ('provisional', 'binance_perp_trades'),
    ('provisional', 'binance_perp_trades:mount'),
)
QUERY_RECEIPTS: Final = ('market_state_api', 'binance_spot_trades:query')
BASELINE: Final = timedelta(minutes=60)
SETTLING: Final = timedelta(minutes=5)
SERVICE_ROOT: Final = '/opt/origo/market-state'
DAGIT_URL: Final = 'http://127.0.0.1:4000'
FINEST: Final = 'C11'

_T0_US: Final = 1_609_459_200_000_000
_BASE_TIME_US: Final = 56_250_000
_BASE_PRICE: Final = 125
_RESULT_LINE: Final = re.compile(r'market state result (\S+) (.*)$')
_PUBLISHED_LINE: Final = re.compile(r'market state query (\S+) published (.*)$')


def cases(start: datetime) -> dict[str, dict[str, object]]:
    """The frozen corpus; ``start`` is the run start floored to the minute."""
    hour = iso(start - timedelta(hours=1))
    midnight = iso(start.replace(hour=0, minute=0))
    return {
        'C01': {'t1': hour},
        'C02': {'t1': midnight, 'tR': 225, 'pR': 250},
        'C03': {'t1': '2026-09-01T00:00:00Z', 't2': '2026-09-02T00:00:00Z'},
        'C04': {'t1': '2026-09-01T06:00:30Z', 't2': '2026-09-01T17:59:30Z', 'p1': '77062.5', 'p2': 78437.5, 'tR': 450, 'pR': 500},
        'C05': {'t1': '2026-08-01T00:00:00Z', 't2': '2026-09-01T00:00:00Z'},
        'C06': {'t1': '2026-08-01T00:00:00Z', 't2': '2026-09-01T00:00:00Z', 'tR': 900, 'pR': 250},
        'C07': {'t1': '2025-01-01T00:00:00Z', 't2': '2026-01-01T00:00:00Z', 'tR': 900, 'pR': 250},
        'C08': {'t1': '2025-01-01T00:00:00Z', 't2': '2026-01-01T00:00:00Z'},
        'C09': {'t1': '2022-01-01T00:00:00Z', 't2': '2024-01-01T00:00:00Z', 'p1': 15000, 'p2': 50000, 'tR': 225, 'pR': 500},
        'C10': {'t1': '2021-01-01T00:00:00Z', 't2': '2026-09-01T00:00:00Z', 'p1': 60000, 'p2': 120000},
        'C11': {},
        'C12': {'tR': 3600, 'pR': 1000},
        'C13': {'tR': 57600},
        'C14': {'pR': 16000},
        'C15': {'p1': 30000, 'p2': 70000, 'tR': 3600, 'pR': 250},
        'C16': {'t1': '2026-09-01T00:00:00Z', 't2': '2026-09-02T00:00:00Z', 'p1': 500000, 'p2': 600000},
    }


def iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec='microseconds')


# --------------------------------------------------------------------------------------
# client: stages A-D through the real service, then the SQL the host runs
# --------------------------------------------------------------------------------------


class _Samples:
    """``samples.jsonl``, appended by every stream as its requests return."""

    def __init__(self, path: Path) -> None:
        self.path, self.rows, self.lock = path, list[dict[str, object]](), threading.Lock()

    def add(self, sample: dict[str, object]) -> None:
        with self.lock:
            self.rows.append(sample)
            with self.path.open('a') as handle:
                handle.write(json.dumps(sample, sort_keys=True) + '\n')


def client(
    run: Path,
    mount: Path,
    url: str,
    *,
    corpus: Mapping[str, Mapping[str, object]] | None = None,
    stages: Sequence[tuple[str, int, int]] = STAGES,
    pairs: int = PAIRS,
    finest: str = FINEST,
    numeric: Sequence[str] = NUMERIC_CASES,
) -> None:
    """Run the stages, then write ``result_ids.txt``, ``reference.sql`` and ``evidence.sql``."""
    started = datetime.now(UTC)
    minute = started.replace(second=0, microsecond=0)
    chosen = {case: dict(request) for case, request in (cases(minute) if corpus is None else corpus).items()}
    run.mkdir(parents=True, exist_ok=True)
    (run / 'run.json').write_text(json.dumps({
        'protocol_version': PROTOCOL_VERSION, 'started_at': stamp(started), 'cases': chosen,
        'stages': [list(stage) for stage in stages], 'pairs': pairs, 'finest': finest,
        'numeric': list(numeric), 'url': url, 'mount': str(mount),
    }, indent=1, sort_keys=True))
    samples = _Samples(run / 'samples.jsonl')
    order = list(chosen)
    for stage, rounds, streams in stages:
        orders = [order if stream % 2 == 0 else order[::-1] for stream in range(streams)]
        _together([
            (lambda stream=stream: [
                samples.add(_sample(stage, stream, number, case, chosen[case], url, mount))
                for number in range(rounds) for case in orders[stream]
            ])
            for stream in range(streams)
        ])
    for number in range(pairs):
        barrier = threading.Barrier(2)
        _together([
            (lambda stream=stream: (barrier.wait(), samples.add(_sample('D', stream, number, finest, chosen[finest], url, mount))))
            for stream in range(2)
        ])
    last = next(rounds for stage, rounds, _ in stages if stage == 'B') - 1
    checked = [
        sample for sample in samples.rows
        if sample['stage'] == 'B' and sample['round'] == last and sample['case'] in numeric and succeeded(sample)
    ]
    pins = sorted({tuple(pin) for sample in checked for pin in result_metadata(sample, mount, url)['pins']})
    identities = [(str(key), str(revision), str(build)) for key, _, revision, build in pins]
    tag = f'market_state_reference:{run.name}'
    (run / 'reference.sql').write_text(f'SET max_query_size = 67108864;\n{reference_select(identities, tag)};\n')
    ids = sorted(str(sample['result_id']) for sample in samples.rows if sample['result_id'])
    (run / 'result_ids.txt').write_text(''.join(f'{result_id}\n' for result_id in ids))
    (run / 'evidence.sql').write_text(evidence_sql(started, ids, tag))


def _together(work: Sequence[Callable[[], object]]) -> None:
    if len(work) == 1:
        work[0]()
        return
    threads = [threading.Thread(target=task) for task in work]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def _sample(stage: str, stream: int, number: int, case: str, request: Mapping[str, object], url: str, mount: Path) -> dict[str, object]:
    started = datetime.now(UTC)
    began = time.perf_counter()
    sample: dict[str, object] = {
        'stage': stage, 'stream': stream, 'round': number, 'case': case, 'request': dict(request),
        'started_at': stamp(started), 'status': None, 'error': '', 'result_id': '', 'readable': False,
        'read_seconds': None, 'cells': 0, 'bytes': 0, 'cells_path': '', 'summary_path': '',
        'data_cutoff': '', 'canonical_through': '', 'state_token': '',
    }
    try:
        result = ask(request, url)
    except MarketStateError as error:
        return {**sample, 'request_seconds': time.perf_counter() - began, 'ended_at': stamp(datetime.now(UTC)),
                'status': error.status, 'error': json.dumps(dict(error.body), sort_keys=True)}
    except (OSError, ValueError) as error:
        return {**sample, 'request_seconds': time.perf_counter() - began, 'ended_at': stamp(datetime.now(UTC)),
                'error': f'{type(error).__name__}: {error}'}
    seconds = time.perf_counter() - began
    response = result.response
    sample.update({
        'request_seconds': seconds, 'ended_at': stamp(datetime.now(UTC)), 'status': 200,
        'result_id': result.result_id, 'cells': int(str(response['cell_count'])),
        'cells_path': result.cells, 'summary_path': result.summary,
        'data_cutoff': str(response['data_cutoff']), 'canonical_through': str(response['canonical_through']),
        'state_token': str(response['state_token']),
    })
    began = time.perf_counter()
    try:
        cells = read_table(mounted(result.cells, mount), url=url)
        summary = read_table(mounted(result.summary, mount), url=url)
    except (MarketStateError, OSError) as error:
        sample['error'] = f'unreadable: {type(error).__name__}: {error}'
        return sample
    sample['read_seconds'] = time.perf_counter() - began
    sample['readable'] = cells.num_rows == sample['cells'] and summary.num_rows == 1
    sample['bytes'] = sum(os.stat(mounted(path, mount)).st_size for path in (result.cells, result.summary))
    return sample


def ask(request: Mapping[str, object], url: str) -> MarketStateResult:
    """One corpus request through the cube reader, each field passed as the corpus wrote it."""
    def text(name: str) -> str | None:
        value = request.get(name)
        return None if value is None else str(value)

    def price(name: str) -> int | float | str | None:
        value = request.get(name)
        if value is None or isinstance(value, int | float | str):
            return value
        raise TypeError(f'{name} must be a number or a decimal string.')

    def resolution(name: str) -> int | float | None:
        value = request.get(name)
        if value is None or isinstance(value, int | float):
            return value
        raise TypeError(f'{name} must be a number.')

    return query(
        t1=text('t1'), t2=text('t2'), p1=price('p1'), p2=price('p2'),
        tR=resolution('tR'), pR=resolution('pR'), url=url,
    )


def mounted(path: str, mount: Path) -> str:
    """A path the service returned, under the consumer's own mount point."""
    return str(mount) + path.removeprefix(SERVICE_ROOT) if path.startswith(SERVICE_ROOT) else path


def succeeded(sample: Mapping[str, object]) -> bool:
    return sample['status'] == 200 and sample['readable'] is True


def result_metadata(sample: Mapping[str, object], mount: Path, url: str) -> dict[str, object]:
    schema = open_file(mounted(str(sample['summary_path']), mount), url=url).schema
    return json.loads(schema.metadata[b'origo.market_state'])


def reference_select(identities: Iterable[tuple[str, str, str]], tag: str) -> str:
    """The raw-trade reference over exactly the pinned identities, grouped by build and base cell.

    It reads the raw revision tables only, never the cube or a ``_current`` view. Time uses the
    normalized datetime in integer microseconds, price integer cents, and volumes exact
    Decimal128 sums of the stored quote quantities.
    """
    groups: dict[bool, list[str]] = {False: [], True: []}
    for key, revision, build in identities:
        groups['T' in key].append(f"('{key[:10]}', '{revision}', '{build}')")
    parts = []
    for provisional, table in ((False, 'binance_spot_trades_raw_revisions'), (True, 'binance_spot_trades_raw_latest_revisions')):
        if groups[provisional]:
            parts.append(
                f"""SELECT {int(provisional)} AS provisional, build_id,
                toUInt64(intDiv(toUnixTimestamp64Micro(datetime) - {_T0_US}, {_BASE_TIME_US})) AS i,
                toUInt64(intDiv(toUInt64(round(price * 100)), {_BASE_PRICE * 100})) AS j, quote_quantity, is_buyer_maker
                FROM origo.{table}
                WHERE (source_date, revision, build_id) IN (SELECT * FROM values('source_date Date, revision String, build_id UUID', {', '.join(groups[provisional])}))"""
            )
    if not parts:
        raise ValueError('The reference needs at least one pinned identity.')
    return f"""SELECT toUInt8(provisional) AS provisional, toString(build_id) AS build, i, j,
    count() AS trades, countIf(is_buyer_maker = 0) AS taker_trades,
    sum(toDecimal128(quote_quantity, 12)) AS volume, sumIf(toDecimal128(quote_quantity, 12), is_buyer_maker = 0) AS taker_volume
    FROM ({' UNION ALL '.join(parts)})
    GROUP BY provisional, build_id, i, j
    SETTINGS max_threads = 2, max_memory_usage = 8589934592, max_bytes_before_external_group_by = 4294967296,
    max_execution_time = 3600, min_bytes_to_use_direct_io = 1, log_comment = '{tag}'"""


def evidence_sql(started: datetime, result_ids: Sequence[str], tag: str) -> str:
    """Every read the verdict needs from ClickHouse, one JSONEachRow ``section`` per statement."""
    since = (started - BASELINE).astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    begun = started.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    comments = ', '.join(f"'{value}'" for value in (*result_ids, tag))
    series = ', '.join(f"('{feed}', '{name}')" for feed, name in (*CONTENTION_SERIES, *REPORTED_SERIES, QUERY_RECEIPTS))
    return f"""SELECT 'window' AS section, toString(now64(3, 'UTC')) AS ended_at, version() AS server_version FORMAT JSONEachRow;
SYSTEM FLUSH LOGS;
SELECT 'statement' AS section, q.log_comment AS log_comment, toString(q.type) AS type,
    toString(toTimeZone(q.event_time_microseconds, 'UTC')) AS finished_at,
    query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage,
    ProfileEvents['SelectedParts'] AS selected_parts, ProfileEvents['SelectedRanges'] AS selected_ranges,
    ProfileEvents['SelectedMarks'] AS selected_marks, ProfileEvents['SelectedRows'] AS selected_rows,
    ProfileEvents['SelectedBytes'] AS selected_bytes, ProfileEvents['UserTimeMicroseconds'] AS cpu_us,
    ProfileEvents['OSReadBytes'] AS disk_read_bytes,
    ProfileEvents['ExternalAggregationCompressedBytes'] AS spilled_group_by_bytes,
    ProfileEvents['ExternalSortCompressedBytes'] AS spilled_sort_bytes,
    multiIf(query LIKE '%component_hashes%', 'pin', query LIKE '%min(price_index)%', 'extent',
            query LIKE '%sumKahan(volume)%', 'cells', query LIKE '%source_cleanup_log%', 'validate',
            query LIKE '%toDecimal128(quote_quantity%', 'reference', 'other') AS statement,
    Settings AS settings
FROM system.query_log AS q
WHERE q.event_time >= toDateTime('{begun}', 'UTC') AND q.type != 'QueryStart' AND q.log_comment IN ({comments})
FORMAT JSONEachRow;
SELECT 'parts' AS section, table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed_bytes,
    sum(data_uncompressed_bytes) AS uncompressed_bytes, count() AS parts
FROM system.parts WHERE active AND database = 'origo' AND table LIKE 'binance_spot_trades%'
GROUP BY table FORMAT JSONEachRow;
SELECT 'table' AS section, database, name, engine FROM system.tables
WHERE name LIKE '%market_state%' AND engine LIKE '%MergeTree%' FORMAT JSONEachRow;
SELECT 'receipt' AS section, w.feed AS feed, w.series AS series, toString(toTimeZone(w.minute, 'UTC')) AS minute,
    toString(toTimeZone(w.recorded_at, 'UTC')) AS recorded_at, w.status AS status, w.error_code AS error_code, w.rows AS rows
FROM origo.worker_minute_log AS w
WHERE w.recorded_at >= toDateTime64('{since}', 3, 'UTC') AND (w.feed, w.series) IN ({series})
FORMAT JSONEachRow;
"""


# --------------------------------------------------------------------------------------
# backfill: the historical cube upgrade's throughput, from inside the Dagster container
# --------------------------------------------------------------------------------------

UPGRADE_JOB: Final = 'refresh_binance_spot_trades_canonical_source_job'
UPGRADE_WINDOW: Final = ('2026-09-24 16:59:02', '2026-09-25 01:59:55')


def backfill(runs_db: Path, trades_per_day: Callable[[], Mapping[str, int]]) -> dict[str, object]:
    """Rows per summed run second and per wall second of the 2,092-day cube upgrade."""
    connection = sqlite3.connect(f'file:{runs_db}?mode=ro', uri=True)
    try:
        runs = connection.execute(
            'SELECT partition, start_time, end_time FROM runs WHERE pipeline_name = ? AND status = ? '
            'AND create_timestamp >= ? AND create_timestamp <= ? AND partition IS NOT NULL',
            (UPGRADE_JOB, 'SUCCESS', *UPGRADE_WINDOW),
        ).fetchall()
    finally:
        connection.close()
    counts = trades_per_day()
    rows = sum(counts.get(str(partition), 0) for partition, _, _ in runs)
    summed = sum(float(end) - float(start) for _, start, end in runs)
    wall = max(float(end) for _, _, end in runs) - min(float(start) for _, start, _ in runs)
    return {
        'job': UPGRADE_JOB, 'window': list(UPGRADE_WINDOW), 'runs': len(runs), 'rows': rows,
        'days_without_counts': sorted(str(partition) for partition, _, _ in runs if str(partition) not in counts),
        'summed_run_seconds': summed, 'wall_seconds': wall,
        'rows_per_run_second': rows / summed, 'rows_per_wall_second': rows / wall,
    }


def _cube_trades_per_day() -> dict[str, int]:
    connection = make_clickhouse_client(get_clickhouse_settings())
    try:
        rows = connection.execute(
            f"SELECT toString(toDate(toDateTime(intDiv(time_index, 1536) * 86400 + {_T0_US // 1_000_000}, 'UTC'))) AS day, "
            'sum(toUInt64(trade_count)) FROM origo.binance_spot_trades_market_state_revisions '
            "WHERE (partition_key, revision, build_id) IN (SELECT partition_key, revision, build_id "
            "FROM origo.source_current_partitions WHERE source_key = 'binance_spot_trades' AND provisional = 0) "
            'GROUP BY day',
            settings={'max_threads': 2, 'max_execution_time': 120},
        )
    finally:
        connection.disconnect()
    return {str(day): int(count) for day, count in rows}


# --------------------------------------------------------------------------------------
# verdict
# --------------------------------------------------------------------------------------


def nearest_rank(values: Sequence[float], quantile: float) -> float:
    """The nearest-rank percentile: the smallest value with at least ``quantile`` of all at or below it."""
    ordered = sorted(values)
    return ordered[max(math.ceil(quantile * len(ordered)), 1) - 1]


def latency(samples: Sequence[Mapping[str, object]], finest: str = FINEST) -> dict[str, dict[str, object]]:
    """Q1-Q3: every sample counts, and a failed request counts as infinitely slow."""
    def seconds(sample: Mapping[str, object]) -> float:
        return float(str(sample['request_seconds'])) if succeeded(sample) else math.inf

    pooled = [seconds(sample) for sample in samples if sample['stage'] in ('A', 'B', 'C')]
    fine = [seconds(sample) for sample in samples if sample['case'] == finest]
    failures = [sample for sample in samples if not succeeded(sample)]
    p90 = nearest_rank(pooled, 0.9)
    median, slowest = nearest_rank(fine, 0.5), max(fine)
    return {
        'Q1': {'p90_seconds': p90, 'samples': len(pooled), 'limit': THRESHOLDS['Q1_p90_seconds'],
               'passed': p90 <= THRESHOLDS['Q1_p90_seconds']},
        'Q2': {'median_seconds': median, 'max_seconds': slowest, 'samples': len(fine),
               'limits': [THRESHOLDS['Q2_median_seconds'], THRESHOLDS['Q2_max_seconds']],
               'passed': median <= THRESHOLDS['Q2_median_seconds'] and slowest <= THRESHOLDS['Q2_max_seconds']},
        'Q3': {'failures': len(failures), 'passed': not failures},
    }


def protocol_followed(
    samples: Sequence[Mapping[str, object]], corpus: Sequence[str], stages: Sequence[Sequence[object]], pairs: int, finest: str
) -> dict[str, object]:
    """Every case has exactly the samples the stages promise, so no case carries more weight."""
    expected = {case: sum(int(str(rounds)) * int(str(streams)) for _, rounds, streams in stages) for case in corpus}
    counted: dict[str, int] = defaultdict(int)
    paired = 0
    for sample in samples:
        if sample['stage'] == 'D':
            paired += 1
        else:
            counted[str(sample['case'])] += 1
    passed = dict(counted) == expected and paired == 2 * pairs and all(
        sample['case'] == finest for sample in samples if sample['stage'] == 'D'
    )
    return {'expected_per_case': expected, 'counted': dict(counted), 'pair_samples': paired, 'passed': passed}


def check_result(cells: pa.Table, summary: Mapping[str, object], metadata: Mapping[str, object], reference: pa.Table) -> dict[str, object]:
    """N1-N4 for one result against the reference rows of its own pinned builds."""
    absolute, relative = TOLERANCE
    grid = metadata['grid']
    assert isinstance(grid, dict)
    time_exponent, price_exponent = int(grid['time_exponent']), int(grid['price_exponent'])
    pins = metadata['pins']
    assert isinstance(pins, list)
    builds = pa.array(sorted({str(pin[3]) for pin in pins}))
    rows = reference.filter(pc.is_in(pc.cast(reference['build'], pa.string()), value_set=builds))
    first, last = _base_edge(summary['t1']), _base_edge(summary['t2'])
    if summary['p1'] is None or summary['p2'] is None:
        low = high = 0
    else:
        low, high = int(float(str(summary['p1']))) // _BASE_PRICE, int(float(str(summary['p2']))) // _BASE_PRICE
    rows = rows.filter(pc.and_(
        pc.and_(pc.greater_equal(rows['i'], first), pc.less(rows['i'], last)),
        pc.and_(pc.greater_equal(rows['j'], low), pc.less(rows['j'], high)),
    ))
    rolled = pa.table({
        'time_index': _shift(rows['i'], time_exponent),
        'price_index': _shift(rows['j'], price_exponent),
        'trades': rows['trades'], 'taker_trades': rows['taker_trades'],
        'volume': rows['volume'], 'taker_volume': rows['taker_volume'],
    }).group_by(['time_index', 'price_index']).aggregate([
        ('trades', 'sum'), ('taker_trades', 'sum'), ('volume', 'sum'), ('taker_volume', 'sum'),
    ])
    joined = cells.join(rolled, keys=['time_index', 'price_index'], join_type='full outer')
    only_cube = joined.filter(pc.is_null(joined['trades_sum'])).num_rows
    only_reference = joined.filter(pc.is_null(joined['trade_count'])).num_rows
    both = joined.filter(pc.and_(pc.is_valid(joined['trades_sum']), pc.is_valid(joined['trade_count'])))
    counts_equal = both.num_rows == 0 or bool(pc.all(pc.and_(
        pc.equal(both['trade_count'], both['trades_sum']),
        pc.equal(both['taker_buy_trade_count'], both['taker_trades_sum']),
    )).as_py())
    worst = 0.0
    volumes_within = True
    for cube_column, reference_column in (('volume', 'volume_sum'), ('taker_buy_volume', 'taker_volume_sum')):
        exact = pc.cast(both[reference_column], pa.float64())
        difference = pc.abs(pc.subtract(both[cube_column], exact))
        allowed = pc.max_element_wise(pc.multiply(pc.abs(exact), relative), absolute)
        if both.num_rows:
            volumes_within = volumes_within and bool(pc.all(pc.less_equal(difference, allowed)).as_py())
            worst = max(worst, float(pc.max(pc.divide(difference, allowed)).as_py()))
    reference_totals = {
        'trade_count': int(pc.sum(rolled['trades_sum']).as_py() or 0),
        'taker_buy_trade_count': int(pc.sum(rolled['taker_trades_sum']).as_py() or 0),
        'volume': Decimal(pc.sum(rolled['volume_sum']).as_py() or 0),
        'taker_buy_volume': Decimal(pc.sum(rolled['taker_volume_sum']).as_py() or 0),
    }
    totals_within = all(
        abs(float(str(summary[name])) - float(reference_totals[name])) <= max(absolute, relative * abs(float(reference_totals[name])))
        for name in ('volume', 'taker_buy_volume')
    ) and all(summary[name] == reference_totals[name] for name in ('trade_count', 'taker_buy_trade_count'))
    price_resolution = float(str(summary['pR']))
    recomputed = _fsum_summary(cells, price_resolution)
    exact_rows = _exact_row_pocs(rolled, price_resolution)
    return {
        'cells': cells.num_rows, 'reference_cells': rolled.num_rows, 'reference_builds': len(builds),
        'only_in_cube': only_cube, 'only_in_reference': only_reference,
        'N1': only_cube == 0 and only_reference == 0 and counts_equal and all(
            summary[name] == reference_totals[name] for name in ('trade_count', 'taker_buy_trade_count')
        ),
        'N2': volumes_within and totals_within,
        'worst_volume_error_over_tolerance': worst,
        'N3': all(summary[name] == value for name, value in recomputed.items()),
        'N4': summary['poc'] == exact_rows['poc'] and summary['taker_buy_poc'] == exact_rows['taker_buy_poc'],
        'summary': {name: summary[name] for name in ('volume', 'taker_buy_volume', 'trade_count', 'taker_buy_trade_count', 'poc', 'taker_buy_poc')},
        'reference_totals': {name: str(value) for name, value in reference_totals.items()},
        'reference_pocs': exact_rows,
    }


def _shift(values: pa.ChunkedArray, exponent: int) -> pa.ChunkedArray:
    """``floor(index / 2^exponent)`` as UInt64, the cube's column type; 64 or more gives 0."""
    indexes = pc.cast(values, pa.uint64())
    if exponent >= 64:
        return pc.multiply(indexes, pa.scalar(0, pa.uint64()))
    return pc.shift_right(indexes, pa.scalar(exponent, pa.uint64()))


def _base_edge(value: object) -> int:
    moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    delta = moment.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return (micros - _T0_US) // _BASE_TIME_US


def _fsum_summary(cells: pa.Table, price_resolution: float) -> dict[str, object]:
    """The declared reductions recomputed from ``cells.arrow``: ``math.fsum`` totals and row sums."""
    ordered = cells.sort_by([('price_index', 'ascending')])
    rows = ordered['price_index'].to_pylist()
    recomputed: dict[str, object] = {}
    for measure, poc in (('volume', 'poc'), ('taker_buy_volume', 'taker_buy_poc')):
        values = ordered[measure].to_pylist()
        recomputed[measure] = math.fsum(values)
        sums: dict[int, list[float]] = defaultdict(list)
        for row, value in zip(rows, values, strict=True):
            sums[int(row)].append(float(value))
        recomputed[poc] = _poc({row: math.fsum(parts) for row, parts in sums.items()}, price_resolution)
    recomputed['trade_count'] = int(pc.sum(cells['trade_count']).as_py() or 0)
    recomputed['taker_buy_trade_count'] = int(pc.sum(cells['taker_buy_trade_count']).as_py() or 0)
    return recomputed


def _exact_row_pocs(rolled: pa.Table, price_resolution: float) -> dict[str, float | None]:
    pocs: dict[str, float | None] = {}
    for column, name in (('volume_sum', 'poc'), ('taker_volume_sum', 'taker_buy_poc')):
        sums: dict[int, Decimal] = defaultdict(Decimal)
        for row, value in zip(rolled['price_index'].to_pylist(), rolled[column].to_pylist(), strict=True):
            sums[int(row)] += Decimal(value)
        pocs[name] = _poc(sums, price_resolution)
    return pocs


def _poc(sums: Mapping[int, float] | Mapping[int, Decimal], price_resolution: float) -> float | None:
    best = max(sums.values(), default=0)
    if best <= 0:
        return None
    return (min(row for row, total in sums.items() if total == best) + 0.5) * price_resolution


def landing_lags(
    receipts: Sequence[Mapping[str, object]], feed: str, series: str, start: datetime, end: datetime
) -> dict[str, object]:
    """First-OK landing lag of every minute that closed in ``[start, end)``, and FAILED receipts in it."""
    closes: dict[datetime, list[datetime]] = defaultdict(list)
    failed = 0
    for receipt in receipts:
        if receipt['feed'] != feed or receipt['series'] != series:
            continue
        minute = _utc(str(receipt['minute']))
        recorded = _utc(str(receipt['recorded_at']))
        if receipt['status'] == 'OK' and start <= minute + timedelta(minutes=1) < end:
            closes[minute + timedelta(minutes=1)].append(recorded)
        if receipt['status'] == 'FAILED' and start <= recorded < end:
            failed += 1
    expected = []
    close = start.replace(second=0, microsecond=0)
    close = close if close >= start else close + timedelta(minutes=1)
    while close < end:
        expected.append(close)
        close += timedelta(minutes=1)
    lags = [(min(closes[moment]) - moment).total_seconds() for moment in expected if moment in closes]
    return {
        'minutes': len(expected), 'landed': len(lags), 'missing': len(expected) - len(lags), 'failed': failed,
        'p50_seconds': nearest_rank(lags, 0.5) if lags else None,
        'p95_seconds': nearest_rank(lags, 0.95) if lags else None,
        'max_seconds': max(lags) if lags else None,
    }


def contention(receipts: Sequence[Mapping[str, object]], started: datetime, ended: datetime) -> dict[str, object]:
    """K1 and K2 per listed series; the reported series carry no criterion."""
    window_end = ended - SETTLING
    rows: dict[str, object] = {}
    k1 = k2 = True
    for feed, series in (*CONTENTION_SERIES, *REPORTED_SERIES):
        baseline = landing_lags(receipts, feed, series, started - BASELINE, started)
        window = landing_lags(receipts, feed, series, started, window_end)
        judged = (feed, series) in CONTENTION_SERIES
        within = (
            baseline['p50_seconds'] is not None and window['p50_seconds'] is not None
            and float(str(window['p50_seconds'])) <= float(str(baseline['p50_seconds'])) + THRESHOLDS['K2_p50_increase_seconds']
        )
        if judged:
            k1 = k1 and window['missing'] == 0 and window['failed'] == 0 and window['minutes'] > 0
            k2 = k2 and within
        rows[f'{feed}/{series}'] = {'judged': judged, 'baseline': baseline, 'window': window}
    return {'series': rows, 'K1': k1, 'K2': k2, 'window': [stamp(started), stamp(window_end)]}


def phases(lines: Iterable[str]) -> dict[str, dict[str, int]]:
    """The service's per-result phase timings, keyed by result ID."""
    found: dict[str, dict[str, int]] = defaultdict(dict)
    for line in lines:
        for pattern in (_RESULT_LINE, _PUBLISHED_LINE):
            match = pattern.search(line)
            if match:
                found[match.group(1)].update(
                    {key: int(value) for key, value in (field.split('=', 1) for field in match.group(2).split())}
                )
    return dict(found)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace(' ', 'T'))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _host(path: Path) -> dict[str, list[str]]:
    facts: dict[str, list[str]] = defaultdict(list)
    for line in path.read_text().splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            facts[key.strip()].append(value.strip())
    return dict(facts)


def _dagit_materializations(dagit: str, started: datetime, ended: datetime) -> list[dict[str, object]]:
    graphql = (
        '{ assetOrError(assetKey: {path: ["market_state_query_service"]}) { ... on Asset { '
        f'assetMaterializations(afterTimestampMillis: "{int(started.timestamp() * 1000)}", '
        f'beforeTimestampMillis: "{int(ended.timestamp() * 1000)}", limit: 10000) '
        '{ timestamp metadataEntries { label ... on IntMetadataEntry { intValue } } } } } }'
    )
    request = urllib.request.Request(
        dagit + '/graphql', data=json.dumps({'query': graphql}).encode(), headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.load(response)
    return [
        {entry['label']: entry.get('intValue') for entry in event['metadataEntries']}
        for event in body['data']['assetOrError']['assetMaterializations']
    ]


def compression(path: str) -> dict[str, object]:
    """The C11 result re-encoded as zstd and LZ4 IPC, against plain and memory-mapped reads."""
    measured: dict[str, object] = {}
    began = time.perf_counter()
    with pa.OSFile(path) as source:
        table = ipc.open_file(source).read_all()
    plain = time.perf_counter() - began
    began = time.perf_counter()
    with pa.memory_map(path) as source:
        ipc.open_file(source).read_all()
    measured['uncompressed'] = {
        'bytes': os.stat(path).st_size, 'read_seconds': plain, 'memory_mapped_read_seconds': time.perf_counter() - began,
    }
    with tempfile.TemporaryDirectory() as scratch:
        for codec in ('zstd', 'lz4'):
            target = Path(scratch) / f'cells.{codec}.arrow'
            began = time.perf_counter()
            with ipc.new_file(str(target), table.schema, options=ipc.IpcWriteOptions(compression=codec)) as writer:
                writer.write_table(table, max_chunksize=65_536)
            written = time.perf_counter() - began
            began = time.perf_counter()
            with pa.OSFile(str(target)) as source:
                ipc.open_file(source).read_all()
            measured[codec] = {'bytes': target.stat().st_size, 'write_seconds': written, 'read_seconds': time.perf_counter() - began}
    return measured


def verdict(run: Path, mount: Path, url: str, dagit: str) -> int:
    """Judge a run directory; write ``report.json`` and ``report.md``; print the verdict last."""
    meta, samples, evidence = load(run)
    started, ended = _utc(str(meta['started_at'])), _utc(str(evidence['window'][0]['ended_at']))
    finest = [sample for sample in samples if sample['case'] == meta['finest'] and succeeded(sample)]
    report = judge(
        run.name, meta, samples, evidence,
        numeric=numeric_checks(meta, samples, mount, url, _read_reference(run / 'reference.arrow')),
        timings=phases((run / 'service.log').read_text().splitlines()),
        hosts=(_host(run / 'host_before.txt'), _host(run / 'host_after.txt')),
        staging=sorted(os.listdir(mount / 'staging')),
        materializations=_dagit_materializations(dagit, started, ended),
        extras={
            'backfill': json.loads((run / 'backfill.json').read_text()),
            'compression': compression(mounted(str(finest[-1]['cells_path']), mount)) if finest else {},
            'results_volume': _inventory(mount),
        },
    )
    (run / 'report.json').write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    (run / 'report.md').write_text(markdown(report))
    print(report['verdict'])
    return 0 if report['verdict'] == 'PASS' else 1


def load(run: Path) -> tuple[dict[str, object], list[dict[str, object]], dict[str, list[dict[str, object]]]]:
    """``run.json``, the samples and the evidence rows by section."""
    meta = json.loads((run / 'run.json').read_text())
    samples = [json.loads(line) for line in (run / 'samples.jsonl').read_text().splitlines() if line]
    evidence: dict[str, list[dict[str, object]]] = defaultdict(list)
    for line in (run / 'evidence.jsonl').read_text().splitlines():
        if line:
            row = json.loads(line)
            evidence[str(row['section'])].append(row)
    return meta, samples, dict(evidence)


def numeric_checks(
    meta: Mapping[str, object], samples: Sequence[Mapping[str, object]], mount: Path, url: str, reference: pa.Table
) -> dict[str, dict[str, object]]:
    """N1-N4 for the last stage-B result of every numerical case, read through the cube reader."""
    stages = meta['stages']
    numeric = meta['numeric']
    assert isinstance(stages, list) and isinstance(numeric, list)
    last = next(int(rounds) for stage, rounds, _ in stages if stage == 'B') - 1
    checked: dict[str, dict[str, object]] = {}
    for sample in samples:
        if sample['stage'] == 'B' and sample['round'] == last and sample['case'] in numeric and succeeded(sample):
            cells = read_table(mounted(str(sample['cells_path']), mount), url=url)
            summary = read_table(mounted(str(sample['summary_path']), mount), url=url).to_pylist()[0]
            checked[str(sample['case'])] = check_result(cells, summary, result_metadata(sample, mount, url), reference)
    return checked


def judge(
    name: str,
    meta: Mapping[str, object],
    samples: Sequence[Mapping[str, object]],
    evidence: Mapping[str, Sequence[Mapping[str, object]]],
    *,
    numeric: Mapping[str, Mapping[str, object]],
    timings: Mapping[str, Mapping[str, int]],
    hosts: tuple[Mapping[str, Sequence[str]], Mapping[str, Sequence[str]]],
    staging: Sequence[str],
    materializations: Sequence[Mapping[str, object]],
    extras: Mapping[str, object],
) -> dict[str, object]:
    """Every criterion of the frozen protocol over one run's gathered inputs."""
    started = _utc(str(meta['started_at']))
    window = evidence['window'][0]
    ended = _utc(str(window['ended_at']))
    stages, expected_numeric, cases_ = meta['stages'], meta['numeric'], meta['cases']
    assert isinstance(stages, list) and isinstance(expected_numeric, list) and isinstance(cases_, dict)
    finest = str(meta['finest'])
    successes = [sample for sample in samples if succeeded(sample)]
    ids = {str(sample['result_id']) for sample in samples if sample['result_id']}
    criteria: dict[str, dict[str, object]] = {}
    criteria['P0'] = protocol_followed(samples, list(cases_), stages, int(str(meta['pairs'])), finest)
    criteria.update(latency(samples, finest))
    host_after = hosts[1]
    restarts = [key for key in ('clickhouse_started', 'market_state_started') if _utc(host_after[key][0]) > started]
    criteria['V'] = {'restarted_during_run': restarts, 'passed': not restarts}
    for key in ('N1', 'N2', 'N3', 'N4'):
        criteria[key] = {
            'passed': len(numeric) == len(expected_numeric) and all(bool(result[key]) for result in numeric.values()),
            'results': {case: result[key] for case, result in numeric.items()},
        }
    peak = max((timings[result_id].get('rss_peak_bytes', 0) for result_id in ids if result_id in timings), default=0)
    criteria['R1'] = {'rss_peak_bytes': peak, 'limit': THRESHOLDS['R1_rss_peak_bytes'],
                      'passed': 0 < peak <= THRESHOLDS['R1_rss_peak_bytes']}
    statements = evidence.get('statement', [])
    served = [row for row in statements if row['statement'] != 'reference']
    spilled = sum(int(str(row['spilled_group_by_bytes'])) + int(str(row['spilled_sort_bytes'])) for row in served)
    memory = max((int(str(row['memory_usage'])) for row in served), default=0)
    criteria['R2'] = {'statements': len(served), 'spilled_bytes': spilled, 'max_statement_memory_bytes': memory,
                      'passed': bool(served) and spilled == 0 and memory <= 4 * 1024**3}
    storage_full = sum(1 for sample in samples if sample['status'] == 507)
    criteria['R3'] = {'staging': list(staging), 'storage_full': storage_full, 'passed': not staging and not storage_full}
    receipts = evidence.get('receipt', [])
    load = contention(receipts, started, ended)
    criteria['K1'] = {'passed': bool(load['K1'])}
    criteria['K2'] = {'passed': bool(load['K2'])}
    counted = sum(
        int(str(row['rows'])) for row in receipts
        if (row['feed'], row['series']) == QUERY_RECEIPTS and started <= _utc(str(row['recorded_at'])) <= ended
    )
    shown = sum(int(str(entry.get('queries_ok') or 0)) for entry in materializations)
    criteria['O1'] = {'successful_requests': len(successes), 'query_receipt_rows': counted,
                      'dagit_queries_ok': shown, 'materializations': len(materializations),
                      'passed': counted >= len(successes) and shown >= len(successes)}
    failed = [key for key, value in criteria.items() if not value['passed']]
    return {
        'protocol_version': meta['protocol_version'], 'run': name, 'started_at': meta['started_at'],
        'ended_at': stamp(ended), 'server_version': window['server_version'],
        'verdict': 'PASS' if not failed else 'FAIL ' + ' '.join(failed),
        'criteria': criteria,
        'cases': _case_table(samples, timings),
        'numeric': dict(numeric),
        'statements': _statement_table(statements),
        'reference_cost': [row for row in statements if row['statement'] == 'reference'],
        'parts': evidence.get('parts', []), 'tables': evidence.get('table', []),
        'contention': load,
        **extras,
        'host_before': hosts[0], 'host_after': hosts[1],
        'samples': list(samples),
    }


def _read_reference(path: Path) -> pa.Table:
    with pa.OSFile(str(path)) as source:
        table = ipc.open_stream(source).read_all()
    return table.combine_chunks()


def _case_table(samples: Sequence[Mapping[str, object]], timings: Mapping[str, Mapping[str, int]]) -> dict[str, dict[str, object]]:
    table: dict[str, dict[str, object]] = {}
    for case in sorted({str(sample['case']) for sample in samples}):
        mine = [sample for sample in samples if sample['case'] == case]
        done = [sample for sample in mine if succeeded(sample)]
        seconds = [float(str(sample['request_seconds'])) for sample in done]
        reads = [float(str(sample['read_seconds'])) for sample in done]
        phase = [timings[str(sample['result_id'])] for sample in done if str(sample['result_id']) in timings]
        table[case] = {
            'request': mine[0]['request'], 'samples': len(mine), 'failures': len(mine) - len(done),
            'cold_seconds': next((float(str(s['request_seconds'])) for s in done if s['stage'] == 'A'), None),
            'p50_seconds': nearest_rank(seconds, 0.5) if seconds else None,
            'p95_seconds': nearest_rank(seconds, 0.95) if seconds else None,
            'max_seconds': max(seconds) if seconds else None,
            'read_p50_seconds': nearest_rank(reads, 0.5) if reads else None,
            'cells': done[-1]['cells'] if done else None, 'bytes': done[-1]['bytes'] if done else None,
            'phases_p50_ms': {
                name: nearest_rank([float(entry[name]) for entry in phase if name in entry], 0.5)
                for name in ('pin_ms', 'extent_ms', 'sql_ms', 'write_ms', 'validate_ms', 'publish_ms', 'total_ms')
                if any(name in entry for entry in phase)
            },
        }
    return table


def _statement_table(statements: Sequence[Mapping[str, object]]) -> dict[str, dict[str, object]]:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in statements:
        grouped[str(row['statement'])].append(row)
    return {
        name: {
            'count': len(rows),
            'p50_ms': nearest_rank([float(str(row['query_duration_ms'])) for row in rows], 0.5),
            'max_ms': max(float(str(row['query_duration_ms'])) for row in rows),
            'max_read_rows': max(int(str(row['read_rows'])) for row in rows),
            'max_memory_bytes': max(int(str(row['memory_usage'])) for row in rows),
            'max_selected_marks': max(int(str(row['selected_marks'])) for row in rows),
            'spilled_bytes': sum(int(str(row['spilled_group_by_bytes'])) + int(str(row['spilled_sort_bytes'])) for row in rows),
            'settings': dict(rows[-1]['settings']) if isinstance(rows[-1]['settings'], dict) else rows[-1]['settings'],
        }
        for name, rows in sorted(grouped.items())
    }


def _inventory(mount: Path) -> dict[str, object]:
    results = mount / 'results'
    entries = sorted(results.iterdir()) if results.is_dir() else []
    return {
        'results': len(entries),
        'result_bytes': sum(file.stat().st_size for entry in entries for file in entry.iterdir()),
        'staging': sorted(os.listdir(mount / 'staging')),
        'lifecycle_bytes': (mount / 'lifecycle.sqlite').stat().st_size,
    }


def markdown(report: Mapping[str, object]) -> str:
    """The report as posted on #462: every criterion, case, sample and failure."""
    criteria = report['criteria']
    assert isinstance(criteria, dict)
    lines = [
        f"## Market state acceptance run `{report['run']}` (protocol {report['protocol_version']})",
        '',
        f"**Verdict: {report['verdict']}**. Started {report['started_at']}, evidence read {report['ended_at']}, "
        f"ClickHouse {report['server_version']}.",
        '',
        '### Criteria',
        '',
        '| Criterion | Passed | Values |',
        '|---|---|---|',
    ]
    for key, value in criteria.items():
        shown = {name: item for name, item in value.items() if name != 'passed'}
        lines.append(f"| {key} | {'yes' if value['passed'] else '**no**'} | `{json.dumps(shown, sort_keys=True, default=str)}` |")
    lines += ['', '### Cases', '', '| Case | Request | n | Fail | Cold s | p50 s | p95 s | Max s | Read p50 s | Cells | Bytes | Phase p50 ms |', '|---|---|---|---|---|---|---|---|---|---|---|---|']
    cases_ = report['cases']
    assert isinstance(cases_, dict)
    for case, row in cases_.items():
        lines.append(
            f"| {case} | `{json.dumps(row['request'], sort_keys=True)}` | {row['samples']} | {row['failures']} | "
            f"{_fmt(row['cold_seconds'])} | {_fmt(row['p50_seconds'])} | {_fmt(row['p95_seconds'])} | {_fmt(row['max_seconds'])} | "
            f"{_fmt(row['read_p50_seconds'])} | {row['cells']} | {row['bytes']} | `{json.dumps(row['phases_p50_ms'], sort_keys=True)}` |"
        )
    for title, key in (
        ('Numerical checks', 'numeric'), ('Statements', 'statements'), ('Raw reference cost', 'reference_cost'),
        ('Contention', 'contention'), ('Completed backfill', 'backfill'), ('Compression', 'compression'),
        ('Projection and tables', 'parts'), ('Tables named market_state', 'tables'), ('Results volume', 'results_volume'),
        ('Host before', 'host_before'), ('Host after', 'host_after'),
    ):
        lines += ['', f'### {title}', '', '```json', json.dumps(report[key], indent=1, sort_keys=True, default=str), '```']
    lines += ['', '### Samples', '', '| Stage | Stream | Round | Case | Status | Seconds | Read s | Cells | Result | Error |', '|---|---|---|---|---|---|---|---|---|---|']
    samples = report['samples']
    assert isinstance(samples, list)
    for sample in samples:
        lines.append(
            f"| {sample['stage']} | {sample['stream']} | {sample['round']} | {sample['case']} | {sample['status']} | "
            f"{_fmt(sample['request_seconds'])} | {_fmt(sample['read_seconds'])} | {sample['cells']} | "
            f"{str(sample['result_id'])[:8]} | {sample['error']} |"
        )
    return '\n'.join(lines) + '\n'


def _fmt(value: object) -> str:
    return '' if value is None else f'{float(str(value)):.3f}'


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='benchmark_market_state.py', description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    run_client = commands.add_parser('client', help='run stages A-D and write the run directory and its SQL')
    run_client.add_argument('--run', type=Path, required=True)
    run_client.add_argument('--mount', type=Path, default=Path(SERVICE_ROOT))
    run_client.add_argument('--url', default=DEFAULT_URL)
    run_verdict = commands.add_parser('verdict', help='judge the run; print PASS or FAIL <criteria> last')
    run_verdict.add_argument('--run', type=Path, required=True)
    run_verdict.add_argument('--mount', type=Path, default=Path(SERVICE_ROOT))
    run_verdict.add_argument('--url', default=DEFAULT_URL)
    run_verdict.add_argument('--dagit', default=DAGIT_URL)
    run_backfill = commands.add_parser('backfill', help="print the historical cube upgrade's throughput as JSON")
    run_backfill.add_argument('--runs-db', type=Path, default=Path('/opt/dagster-instance/runs.db'))
    arguments = parser.parse_args(argv)
    if arguments.command == 'client':
        client(arguments.run, arguments.mount, arguments.url)
        return 0
    if arguments.command == 'verdict':
        return verdict(arguments.run, arguments.mount, arguments.url, arguments.dagit)
    print(json.dumps(backfill(arguments.runs_db, _cube_trades_per_day), indent=1, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
