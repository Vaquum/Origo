"""Measure the deployed market state service against PRD-0022's frozen acceptance protocol.

The protocol, its criteria and the raw-trade reference are fixed in slice #476, and
``tests/origo_source_native/test_market_state_acceptance.py`` holds this module to them. On
37.27.112.167 the runbook in ``docs/Developer/Market-state-cube.md`` runs ``client`` in a
consumer container, pipes the generated SQL through ``clickhouse-client``, runs ``backfill`` in
the Dagster container, then ``verdict`` for the interim report and, after the recovery and
expiry steps, ``finalize`` for the final one. The client needs no ClickHouse credentials: it
only calls the service and reads result files through the cube reader.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime, timedelta
from datetime import time as clock
from decimal import Decimal
from multiprocessing import get_context
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
STREAM_ORDERS: Final = ('ascending', 'descending')  # stream 0 runs the cases in ID order, stream 1 in reverse
PAIR_CASE: Final = 'C11'
PAIRS: Final = 3  # stage D: two PAIR_CASE requests started together, three times
FINEST: Final = 'C11'
NUMERIC_CASES: Final = ('C04', 'C10', 'C11', 'C12', 'C13', 'C14')
QUANTILES: Final = {'Q1': 0.9, 'median': 0.5, 'K2_window': 0.9, 'K2_baseline': 0.95}
THRESHOLDS: Final = {
    'Q1_p90_seconds': 3.0,
    'Q2_median_seconds': 5.0,
    'Q2_max_seconds': 10.0,
    'R1_rss_peak_bytes': 1024**3,
    'R2_statement_memory_bytes': 2 * 1024**3,
    'K2_p50_increase_seconds': 2.0,
    'K2_p90_over_baseline_p95_seconds': 5.0,
}
TOLERANCE: Final = (1e-8, 1e-12)  # absolute USDT, relative
BASELINE: Final = timedelta(minutes=60)
SETTLING: Final = timedelta(minutes=5)
MIN_BASELINE_MINUTES: Final = 50
MIN_WINDOW_MINUTES: Final = 5
QUIET_HOURS: Final = (clock(0, 0), clock(1, 30))  # the frozen corpus never starts in [00:00, 01:30) UTC
EXPIRY_WAIT: Final = timedelta(hours=24, minutes=2)  # after the run's last read: expiry plus one cleanup tick
CONTENTION_SERIES: Final = (  # (feed, data series, its publication series)
    ('provisional', 'binance_spot_trades', 'binance_spot_trades:mount'),
    ('provisional', 'binance_spot_aggtrades', 'binance_spot_aggtrades:mount'),
    ('provisional', 'binance_perp_aggtrades', 'binance_perp_aggtrades:mount'),
    ('depth', 'depth20_snapshots', None),
    ('depth', 'depth200_snapshots', None),
)
REPORTED_SERIES: Final = (('provisional', 'binance_perp_trades', 'binance_perp_trades:mount'),)
QUERY_RECEIPTS: Final = ('market_state_api', 'binance_spot_trades:query')
PROJECTION_TABLES: Final = ('binance_spot_trades_market_state_latest_revisions', 'binance_spot_trades_market_state_revisions')
REFERENCE_SETTINGS: Final = {
    'max_threads': 2,
    'max_memory_usage': 8 * 1024**3,
    'max_bytes_ratio_before_external_group_by': 0,
    'max_bytes_ratio_before_external_sort': 0,
    'max_execution_time': 3600,
    'min_bytes_to_use_direct_io': 1,
}
CONTAINERS: Final = (
    'clickhouse', 'dagster', 'market-state', 'provisional-worker', 'provisional-binance-spot-aggtrades',
    'provisional-binance-perp-aggtrades', 'provisional-binance-perp-trades', 'depth-worker',
)
SERVICE_ROOT: Final = '/opt/origo/market-state'
DAGIT_URL: Final = 'http://127.0.0.1:4000'
DAGIT_QUERY: Final = (
    '{ assetOrError(assetKey: {path: ["market_state_query_service"]}) { ... on Asset { '
    'assetMaterializations(afterTimestampMillis: "%d", beforeTimestampMillis: "%d", limit: 10000) '
    '{ timestamp metadataEntries { label ... on IntMetadataEntry { intValue } } } } } }'
)
UPGRADE_JOB: Final = 'refresh_binance_spot_trades_canonical_source_job'
UPGRADE_WINDOW: Final = ('2026-09-24 16:59:02', '2026-09-25 01:59:55')
REPORT_PART_CHARS: Final = 60_000

_T0_US: Final = 1_609_459_200_000_000
_BASE_TIME_US: Final = 56_250_000
_BASE_PRICE: Final = 125
_RESULT_LINE: Final = re.compile(r'market state result (\S+) (.*)$')
_PUBLISHED_LINE: Final = re.compile(r'market state query (\S+) published (.*)$')
_OK_COUNT: Final = re.compile(r'(?:^| )ok=(\d+)')
_UUID: Final = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


def cases(start: datetime) -> dict[str, dict[str, object]]:
    """The frozen corpus for a run starting at ``start``, which is floored to the minute."""
    if start.tzinfo is None:
        raise ValueError('The run start must be timezone-aware.')
    minute = start.astimezone(UTC).replace(second=0, microsecond=0)
    hour = iso(minute - timedelta(hours=1))
    midnight = iso(minute.replace(hour=0, minute=0))
    return {
        'C01': {'t1': hour},
        'C02': {'t1': midnight, 'tR': 225, 'pR': 250},
        'C03': {'t1': '2026-09-01T00:00:00Z', 't2': '2026-09-02T00:00:00Z'},
        'C04': {'t1': '2026-09-01T06:00:30Z', 't2': '2026-09-01T17:59:30Z', 'p1': '77062.5', 'p2': 78312.5, 'tR': 450, 'pR': 500},
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


def nearest_rank(values: Sequence[float], quantile: float) -> float:
    """The value at rank ``ceil(quantile * N)`` of the ascending values, counting from 1."""
    if not values:
        raise ValueError('A percentile needs at least one value.')
    ordered = sorted(values)
    return ordered[max(math.ceil(quantile * len(ordered)), 1) - 1]


# --------------------------------------------------------------------------------------
# client: stages A-D through the real service, then the SQL the host runs
# --------------------------------------------------------------------------------------


def client(
    run: Path,
    mount: Path,
    url: str,
    *,
    corpus: Mapping[str, Mapping[str, object]] | None = None,
    stages: Sequence[tuple[str, int, int]] = STAGES,
    pairs: int = PAIRS,
    pair_case: str = PAIR_CASE,
    finest: str = FINEST,
    numeric: Sequence[str] = NUMERIC_CASES,
) -> None:
    """Run stages A-D, then write ``result_ids.txt``, ``reference.sql`` and ``evidence.sql``."""
    started = datetime.now(UTC)
    frozen = corpus is None
    if frozen and QUIET_HOURS[0] <= started.time() < QUIET_HOURS[1]:
        raise SystemExit('The frozen corpus does not start between 00:00 and 01:30 UTC: C02 would cover almost nothing.')
    chosen = {case: dict(request) for case, request in (cases(started) if corpus is None else corpus).items()}
    run.mkdir(parents=True, exist_ok=True)
    (run / 'run.json').write_text(json.dumps({
        'protocol_version': PROTOCOL_VERSION, 'frozen': frozen and _frozen_constants(stages, pairs, pair_case, finest, numeric),
        'started_at': stamp(started), 'cases': chosen, 'stages': [list(stage) for stage in stages],
        'pairs': pairs, 'pair_case': pair_case, 'finest': finest, 'numeric': list(numeric),
        'url': url, 'mount': str(mount),
    }, indent=1, sort_keys=True))
    samples: list[dict[str, object]] = []
    order = list(chosen)
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context('spawn')) as streams:
        for stage, rounds, count in stages:
            orders = [order if STREAM_ORDERS[stream] == 'ascending' else order[::-1] for stream in range(count)]
            if count == 1:
                samples += _stream(stage, 0, rounds, orders[0], chosen, url, str(mount), 0.0)
                continue
            start_at = time.time() + 2.0
            futures = [
                streams.submit(_stream, stage, stream, rounds, orders[stream], chosen, url, str(mount), start_at)
                for stream in range(count)
            ]
            samples += [sample for future in futures for sample in future.result()]
        for number in range(pairs):
            start_at = time.time() + 2.0
            futures = [
                streams.submit(_stream, 'D', stream, 1, [pair_case], chosen, url, str(mount), start_at, number)
                for stream in range(2)
            ]
            samples += [sample for future in futures for sample in future.result()]
    (run / 'samples.jsonl').write_text(''.join(json.dumps(sample, sort_keys=True) + '\n' for sample in samples))
    ids = sorted(str(sample['result_id']) for sample in samples if sample['result_id'])
    (run / 'result_ids.txt').write_text(''.join(f'{result_id}\n' for result_id in ids))
    last = next(rounds for stage, rounds, _ in stages if stage == 'B') - 1
    checked = [
        sample for sample in samples
        if sample['stage'] == 'B' and sample['round'] == last and sample['case'] in numeric and succeeded(sample)
    ]
    pins = sorted({tuple(pin) for sample in checked for pin in _list(result_metadata(sample, mount, url)['pins'])})
    identities = [(str(key), str(revision), str(build)) for key, _, revision, build in pins]
    tag = f'market_state_reference:{run.name}'
    (run / 'evidence.sql').write_text(evidence_sql(started, ids, tag))
    (run / 'reference.sql').write_text(f'SET max_query_size = 67108864;\n{reference_select(identities, tag)};\n')


def _frozen_constants(
    stages: Sequence[tuple[str, int, int]], pairs: int, pair_case: str, finest: str, numeric: Sequence[str]
) -> bool:
    return (tuple(stages), pairs, pair_case, finest, tuple(numeric)) == (STAGES, PAIRS, PAIR_CASE, FINEST, NUMERIC_CASES)


def _stream(
    stage: str,
    stream: int,
    rounds: int,
    order: Sequence[str],
    corpus: Mapping[str, Mapping[str, object]],
    url: str,
    mount: str,
    start_at: float,
    first_round: int = 0,
) -> list[dict[str, object]]:
    """One stream's requests, each sent after the previous one's files were read back."""
    time.sleep(max(start_at - time.time(), 0.0))
    return [
        _sample(stage, stream, first_round + number, case, corpus[case], url, Path(mount))
        for number in range(rounds) for case in order
    ]


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
    sample.update({
        'request_seconds': time.perf_counter() - began, 'ended_at': stamp(datetime.now(UTC)), 'status': 200,
        'result_id': result.result_id, 'cells': int(str(result.response['cell_count'])),
        'cells_path': result.cells, 'summary_path': result.summary,
        'data_cutoff': str(result.response['data_cutoff']), 'canonical_through': str(result.response['canonical_through']),
        'state_token': str(result.response['state_token']),
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


def _statements(sample: Mapping[str, object]) -> set[str]:
    """The statements a published result ran: a result with cells read them, and read its
    time window's price extent first unless both price bounds were supplied."""
    if int(str(sample['cells'])) == 0:
        return {'floor', 'pin', 'validate'}
    request = _mapping(sample['request'])
    automatic = request.get('p1') is None or request.get('p2') is None
    return {'floor', 'pin', 'cells', 'validate'} | ({'extent'} if automatic else set())


def result_metadata(sample: Mapping[str, object], mount: Path, url: str) -> dict[str, object]:
    schema = open_file(mounted(str(sample['summary_path']), mount), url=url).schema
    return json.loads(schema.metadata[b'origo.market_state'])


def _list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f'Expected a list, got {type(value).__name__}.')
    return value


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f'Expected a mapping, got {type(value).__name__}.')
    return value


def reference_select(identities: Iterable[tuple[str, str, str]], tag: str) -> str:
    """The raw-trade reference over exactly the pinned identities, grouped by build and base cell.

    It reads the raw revision tables only, never the cube or a ``_current`` view, and selects the
    pinned ``(source_date, revision, build_id)`` tuples; a build ID belongs to one partition.
    Time uses the normalized datetime in integer microseconds, price integer cents, and volumes
    exact Decimal128(18) sums. It also counts trades whose builder price index would differ from
    the integer-cents one, and trades before the cube's history start: both must be zero. With no
    identities, because every numerical request failed, it reads nothing and returns no rows, so
    the verdict still runs and reports the failures.
    """
    groups: dict[bool, list[str]] = {False: [], True: []}
    for key, revision, build in identities:
        groups['T' in key].append(f"('{key[:10]}', '{revision}', '{build}')")
    parts = []
    for provisional, table in ((False, 'binance_spot_trades_raw_revisions'), (True, 'binance_spot_trades_raw_latest_revisions')):
        if groups[provisional]:
            parts.append(
                f"""SELECT {int(provisional)} AS provisional, build_id, datetime, price, quote_quantity, is_buyer_maker,
                toUInt64(intDiv(toUnixTimestamp64Micro(datetime) - {_T0_US}, {_BASE_TIME_US})) AS i,
                toUInt64(intDiv(toUInt64(round(price * 100)), {_BASE_PRICE * 100})) AS j
                FROM origo.{table}
                WHERE (source_date, revision, build_id) IN (SELECT * FROM values('source_date Date, revision String, build_id UUID', {', '.join(groups[provisional])}))"""
            )
    if not parts:
        parts.append(
            """SELECT 0 AS provisional, build_id, datetime, price, quote_quantity, is_buyer_maker,
                toUInt64(0) AS i, toUInt64(0) AS j FROM origo.binance_spot_trades_raw_revisions WHERE 0"""
        )
    settings = ', '.join(f'{name} = {value}' for name, value in REFERENCE_SETTINGS.items())
    return f"""SELECT toUInt8(provisional) AS provisional, toString(build_id) AS build, i, j,
    count() AS trades, countIf(is_buyer_maker = 0) AS taker_trades,
    sum(toDecimal128(quote_quantity, 18)) AS volume, sumIf(toDecimal128(quote_quantity, 18), is_buyer_maker = 0) AS taker_volume,
    countIf(toUInt64(floor(price / {_BASE_PRICE})) != j) AS price_index_disagreements,
    countIf(toUnixTimestamp64Micro(datetime) < {_T0_US}) AS before_history
    FROM ({' UNION ALL '.join(parts)})
    GROUP BY provisional, build_id, i, j
    SETTINGS {settings}, log_comment = '{tag}'"""


def evidence_sql(started: datetime, result_ids: Sequence[str], tag: str) -> str:
    """Every read the verdict needs from ClickHouse, one JSONEachRow ``section`` per statement.

    Query logs are flushed first, so a missing statement is missing, not late.
    """
    since = (started - BASELINE).astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    begun = started.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    comments = ', '.join(f"'{value}'" for value in (*result_ids, tag))
    names = [(feed, series) for feed, data, mount in (*CONTENTION_SERIES, *REPORTED_SERIES) for series in (data, mount) if series]
    series = ', '.join(f"('{feed}', '{name}')" for feed, name in (*names, QUERY_RECEIPTS))
    return f"""SYSTEM FLUSH LOGS;
SELECT 'window' AS section, toString(now64(6, 'UTC')) AS ended_at, version() AS server_version FORMAT JSONEachRow;
SELECT 'statement' AS section, q.log_comment AS log_comment, toString(q.type) AS type,
    toString(toTimeZone(q.event_time_microseconds, 'UTC')) AS finished_at,
    q.query_duration_ms AS query_duration_ms, q.read_rows AS read_rows, q.read_bytes AS read_bytes,
    q.result_rows AS result_rows, q.result_bytes AS result_bytes, q.memory_usage AS memory_usage,
    q.ProfileEvents['SelectedParts'] AS selected_parts, q.ProfileEvents['SelectedRanges'] AS selected_ranges,
    q.ProfileEvents['SelectedMarks'] AS selected_marks, q.ProfileEvents['SelectedRows'] AS selected_rows,
    q.ProfileEvents['SelectedBytes'] AS selected_bytes, q.ProfileEvents['UserTimeMicroseconds'] AS cpu_us,
    q.ProfileEvents['OSReadBytes'] AS disk_read_bytes,
    q.ProfileEvents['ExternalAggregationWritePart'] + q.ProfileEvents['ExternalSortWritePart']
        + q.ProfileEvents['ExternalProcessingFilesTotal'] AS spill_events,
    multiIf(q.query LIKE '%source_capacity_log%', 'floor', q.query LIKE '%component_hashes%', 'pin',
            q.query LIKE '%min(price_index)%', 'extent', q.query LIKE '%sumKahan(volume)%', 'cells',
            q.query LIKE '%source_cleanup_log%', 'validate', q.query LIKE '%toDecimal128(quote_quantity%', 'reference',
            'other') AS statement,
    q.Settings AS settings
FROM system.query_log AS q
WHERE q.event_time >= toDateTime('{begun}', 'UTC') AND q.type != 'QueryStart' AND q.log_comment IN ({comments})
FORMAT JSONEachRow;
SELECT 'request' AS section, q.log_comment AS log_comment,
    toString(toTimeZone(min(q.event_time_microseconds), 'UTC')) AS first_at
FROM system.query_log AS q
WHERE q.event_time >= toDateTime('{begun}', 'UTC')
    AND match(q.log_comment, '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$')
    AND (q.query LIKE '%source_capacity_log%' OR q.query LIKE '%component_hashes%' OR q.query LIKE '%min(price_index)%'
         OR q.query LIKE '%sumKahan(volume)%' OR q.query LIKE '%source_cleanup_log%')
GROUP BY q.log_comment FORMAT JSONEachRow;
SELECT 'parts' AS section, p.table AS table, sum(p.rows) AS rows, sum(p.data_compressed_bytes) AS compressed_bytes,
    sum(p.data_uncompressed_bytes) AS uncompressed_bytes, count() AS parts
FROM system.parts AS p WHERE p.active AND p.database = 'origo' AND p.table LIKE 'binance_spot_trades%'
GROUP BY p.table FORMAT JSONEachRow;
SELECT 'table' AS section, t.database AS database, t.name AS name, t.engine AS engine
FROM system.tables AS t WHERE t.name LIKE '%market_state%' FORMAT JSONEachRow;
SELECT 'receipt' AS section, w.feed AS feed, w.series AS series, toString(toTimeZone(w.minute, 'UTC')) AS minute,
    toString(toTimeZone(w.recorded_at, 'UTC')) AS recorded_at, w.status AS status, w.error_code AS error_code,
    w.rows AS rows, w.error AS error
FROM origo.worker_minute_log AS w
WHERE w.recorded_at >= toDateTime64('{since}', 3, 'UTC') AND (w.feed, w.series) IN ({series})
FORMAT JSONEachRow;
SELECT 'maintenance' AS section, 'cleanup' AS kind, c.partition_key AS partition_key,
    toString(toTimeZone(c.completed_at, 'UTC')) AS at
FROM origo.source_cleanup_log AS c
WHERE c.source_key = 'binance_spot_trades' AND c.completed_at >= toDateTime64('{since}', 6, 'UTC')
UNION ALL
SELECT 'maintenance' AS section, 'rollout' AS kind, r.activation_group AS partition_key,
    toString(toTimeZone(r.recorded_at, 'UTC')) AS at
FROM origo.source_component_rollout_log AS r
WHERE r.source_key = 'binance_spot_trades' AND r.recorded_at >= toDateTime64('{since}', 6, 'UTC')
FORMAT JSONEachRow;
"""


# --------------------------------------------------------------------------------------
# backfill: the historical cube upgrade's throughput, from inside the Dagster container
# --------------------------------------------------------------------------------------


def backfill(runs_db: Path, trades_per_day: Callable[[], Mapping[str, int]], since: datetime) -> dict[str, object]:
    """Rows per summed run second and per wall second of the 2,092-day cube upgrade, and every
    native backfill run still running or ended after the baseline before ``since`` began; ``judge``
    keeps those that started before the evidence read."""
    connection = sqlite3.connect(f'file:{runs_db}?mode=ro', uri=True)
    try:
        runs = connection.execute(
            'SELECT partition, start_time, end_time FROM runs WHERE pipeline_name = ? AND status = ? '
            'AND create_timestamp >= ? AND create_timestamp <= ? AND partition IS NOT NULL',
            (UPGRADE_JOB, 'SUCCESS', *UPGRADE_WINDOW),
        ).fetchall()
        overlapping = connection.execute(
            'SELECT pipeline_name, backfill_id, partition, start_time, end_time FROM runs '
            "WHERE backfill_id IS NOT NULL AND backfill_id != '' AND (end_time IS NULL OR end_time >= ?)",
            ((since - BASELINE).timestamp(),),
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
        'native_backfills': [
            {'job': job, 'backfill_id': backfill_id, 'partition': partition, 'start_time': start, 'end_time': end}
            for job, backfill_id, partition, start, end in overlapping
        ],
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
# verdict: judge a run directory
# --------------------------------------------------------------------------------------


def latency(samples: Sequence[Mapping[str, object]], finest: str = FINEST) -> dict[str, dict[str, object]]:
    """Q1-Q3: every sample counts, and a failed request counts as infinitely slow."""
    def seconds(sample: Mapping[str, object]) -> float:
        return float(str(sample['request_seconds'])) if succeeded(sample) else math.inf

    pooled = [seconds(sample) for sample in samples if sample['stage'] in ('A', 'B', 'C')]
    fine = [seconds(sample) for sample in samples if sample['case'] == finest]
    failures = [sample for sample in samples if not succeeded(sample)]
    # With no samples at all a quantile is as slow as a failure, so an incomplete run still gets its report.
    p90 = nearest_rank(pooled, QUANTILES['Q1']) if pooled else math.inf
    median, slowest = (nearest_rank(fine, QUANTILES['median']), max(fine)) if fine else (math.inf, math.inf)
    return {
        'Q1': {'p90_seconds': p90, 'rank': math.ceil(QUANTILES['Q1'] * len(pooled)), 'samples': len(pooled),
               'limit': THRESHOLDS['Q1_p90_seconds'], 'passed': p90 <= THRESHOLDS['Q1_p90_seconds']},
        'Q2': {'median_seconds': median, 'max_seconds': slowest, 'samples': len(fine),
               'limits': [THRESHOLDS['Q2_median_seconds'], THRESHOLDS['Q2_max_seconds']],
               'passed': median <= THRESHOLDS['Q2_median_seconds'] and slowest <= THRESHOLDS['Q2_max_seconds']},
        'Q3': {'failures': [_failure(sample) for sample in failures], 'passed': not failures},
    }


def _failure(sample: Mapping[str, object]) -> str:
    return f"{sample['stage']}/{sample['stream']}/{sample['round']}/{sample['case']}: {sample['status']} {sample['error']}"


def expected_samples(
    corpus: Sequence[str], stages: Sequence[Sequence[object]], pairs: int, pair_case: str
) -> Counter[tuple[str, int, int, str]]:
    """The exact (stage, stream, round, case) multiset a complete run holds."""
    expected: Counter[tuple[str, int, int, str]] = Counter()
    for stage, rounds, streams in stages:
        for stream in range(int(str(streams))):
            for number in range(int(str(rounds))):
                for case in corpus:
                    expected[(str(stage), stream, number, case)] += 1
    for number in range(pairs):
        for stream in range(2):
            expected[('D', stream, number, pair_case)] += 1
    return expected


def completeness(
    samples: Sequence[Mapping[str, object]], corpus: Sequence[str], stages: Sequence[Sequence[object]], pairs: int, pair_case: str
) -> dict[str, object]:
    """P0: exactly the promised samples, each with finite, non-negative measurements."""
    seen = Counter((str(s['stage']), int(str(s['stream'])), int(str(s['round'])), str(s['case'])) for s in samples)
    expected = expected_samples(corpus, stages, pairs, pair_case)
    missing, extra = expected - seen, seen - expected
    corrupt = [
        _failure(sample) for sample in samples
        if not _finite(sample.get('request_seconds')) or (succeeded(sample) and not _finite(sample.get('read_seconds')))
    ]
    return {
        'samples': sum(seen.values()), 'expected': sum(expected.values()),
        'missing': sorted('/'.join(map(str, key)) for key in missing.elements()),
        'unexpected': sorted('/'.join(map(str, key)) for key in extra.elements()),
        'corrupt': corrupt, 'passed': not missing and not extra and not corrupt,
    }


def _finite(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def check_result(cells: pa.Table, summary: Mapping[str, object], metadata: Mapping[str, object], reference: pa.Table) -> dict[str, object]:
    """N1-N4 for one result against the reference rows of its own pinned builds."""
    grid = _mapping(metadata['grid'])
    time_exponent, price_exponent = int(str(grid['time_exponent'])), int(str(grid['price_exponent']))
    builds = pa.array(sorted({str(_list(pin)[3]) for pin in _list(metadata['pins'])}), pa.string())
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
    absolute, relative = TOLERANCE
    worst = 0.0
    volumes_within = True
    for cube_column, reference_column in (('volume', 'volume_sum'), ('taker_buy_volume', 'taker_volume_sum')):
        if both.num_rows:
            exact = pc.cast(both[reference_column], pa.float64())
            difference = pc.abs(pc.subtract(both[cube_column], exact))
            allowed = pc.max_element_wise(pc.multiply(pc.abs(exact), relative), absolute)
            volumes_within = volumes_within and bool(pc.all(pc.less_equal(difference, allowed)).as_py())
            worst = max(worst, float(pc.max(pc.divide(difference, allowed)).as_py()))
    exact_totals = {
        'trade_count': int(pc.sum(rolled['trades_sum']).as_py() or 0),
        'taker_buy_trade_count': int(pc.sum(rolled['taker_trades_sum']).as_py() or 0),
        'volume': Decimal(pc.sum(rolled['volume_sum']).as_py() or 0),
        'taker_buy_volume': Decimal(pc.sum(rolled['taker_volume_sum']).as_py() or 0),
    }
    totals_within = all(_within(float(str(summary[name])), exact_totals[name]) for name in ('volume', 'taker_buy_volume'))
    counts_totals = all(summary[name] == exact_totals[name] for name in ('trade_count', 'taker_buy_trade_count'))
    price_resolution = float(str(summary['pR']))
    recomputed = recompute_summary(cells, price_resolution)
    pocs = _exact_pocs(rolled, price_resolution)
    near_ties = []
    for name, (winner, runner_up, margin, upper) in pocs.items():
        if summary[name] != winner:
            near_ties.append({
                'measure': name, 'reference': winner, 'runner_up': runner_up,
                'margin': None if margin is None else str(margin),
                'within_tolerance': margin is not None and upper is not None and summary[name] == runner_up
                and margin <= 2 * Decimal(max(absolute, relative * float(upper))),
            })
    return {
        'cells': cells.num_rows, 'reference_cells': rolled.num_rows, 'reference_builds': len(builds),
        'only_in_cube': only_cube, 'only_in_reference': only_reference,
        'price_index_disagreements': int(pc.sum(rows['price_index_disagreements']).as_py() or 0),
        'before_history': int(pc.sum(rows['before_history']).as_py() or 0),
        'N1': only_cube == 0 and only_reference == 0 and counts_equal and counts_totals,
        'N2': volumes_within and totals_within,
        'worst_volume_error_over_tolerance': worst,
        'N3': all(summary[name] == value for name, value in recomputed.items()),
        'N4': all(bool(tie['within_tolerance']) for tie in near_ties),
        'pocs': {name: {'reference': value[0], 'runner_up': value[1], 'margin': None if value[2] is None else str(value[2])}
                 for name, value in pocs.items()},
        'near_ties': near_ties,
        'summary': {name: summary[name] for name in ('volume', 'taker_buy_volume', 'trade_count', 'taker_buy_trade_count', 'poc', 'taker_buy_poc')},
        'reference_totals': {name: str(value) for name, value in exact_totals.items()},
        'partial': {name: summary[name] for name in ('first_column_partial', 'last_column_partial', 'first_row_partial', 'last_row_partial')},
    }


def _within(value: float, exact: Decimal) -> bool:
    absolute, relative = TOLERANCE
    return abs(Decimal(value) - exact) <= Decimal(max(absolute, relative * abs(float(exact))))


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


def recompute_summary(cells: pa.Table, price_resolution: float) -> dict[str, object]:
    """The four totals and both POCs recomputed from ``cells.arrow`` by the declared reductions.

    Volume totals are ``math.fsum`` over every cell, and row sums ``math.fsum`` over the row's
    cells, used only to find the POCs; the summary carries no row sums.
    """
    ordered = cells.sort_by([('price_index', 'ascending')])
    rows = ordered['price_index'].to_pylist()
    recomputed: dict[str, object] = {}
    for measure, poc in (('volume', 'poc'), ('taker_buy_volume', 'taker_buy_poc')):
        values = ordered[measure].to_pylist()
        recomputed[measure] = math.fsum(values)
        sums: dict[int, list[float]] = defaultdict(list)
        for row, value in zip(rows, values, strict=True):
            sums[int(row)].append(float(value))
        recomputed[poc] = _poc({row: Decimal(math.fsum(parts)) for row, parts in sums.items()}, price_resolution)[0]
    recomputed['trade_count'] = int(pc.sum(cells['trade_count']).as_py() or 0)
    recomputed['taker_buy_trade_count'] = int(pc.sum(cells['taker_buy_trade_count']).as_py() or 0)
    return recomputed


def _exact_pocs(rolled: pa.Table, price_resolution: float) -> dict[str, tuple[float | None, float | None, Decimal | None, Decimal | None]]:
    pocs: dict[str, tuple[float | None, float | None, Decimal | None, Decimal | None]] = {}
    for column, name in (('volume_sum', 'poc'), ('taker_volume_sum', 'taker_buy_poc')):
        sums: dict[int, Decimal] = defaultdict(Decimal)
        for row, value in zip(rolled['price_index'].to_pylist(), rolled[column].to_pylist(), strict=True):
            sums[int(row)] += Decimal(value)
        pocs[name] = _poc(sums, price_resolution)
    return pocs


def _poc(sums: Mapping[int, Decimal], price_resolution: float) -> tuple[float | None, float | None, Decimal | None, Decimal | None]:
    """The winning row's centre (the lower row on equality), the runner-up's, their margin and the winner's sum."""
    ranked = sorted(((total, -row) for row, total in sums.items() if total > 0), reverse=True)
    if not ranked:
        return None, None, None, None
    best = (-ranked[0][1] + 0.5) * price_resolution
    if len(ranked) == 1:
        return best, None, None, ranked[0][0]
    return best, (-ranked[1][1] + 0.5) * price_resolution, ranked[0][0] - ranked[1][0], ranked[0][0]


def landing(
    receipts: Sequence[Mapping[str, object]], feed: str, data: str, mount: str | None, start: datetime, end: datetime,
    observed: datetime,
) -> dict[str, object]:
    """Landing lags of the data minutes that closed in ``[start, end)``, as seen at ``observed``.

    A data minute lands at its first ``OK`` receipt. Its publication lands at the first ``OK``
    receipt of the publication series recorded at or after that: publication receipts carry the
    worker's tick minute, not a data minute, and an unchanged state publishes nothing. Receipts
    recorded after ``observed``, when the evidence was read, do not count.
    """
    first_ok: dict[datetime, datetime] = {}
    published: list[datetime] = []
    failed = 0
    for receipt in receipts:
        if receipt['feed'] != feed:
            continue
        recorded = _utc(str(receipt['recorded_at']))
        if recorded > observed:
            continue
        if receipt['series'] == data and receipt['status'] == 'OK':
            minute = _utc(str(receipt['minute']))
            first_ok[minute] = min(first_ok.get(minute, recorded), recorded)
        elif mount is not None and receipt['series'] == mount and receipt['status'] == 'OK':
            published.append(recorded)
        if receipt['series'] in (data, mount) and receipt['status'] == 'FAILED' and start <= recorded < end:
            failed += 1
    published.sort()
    closes = []
    close = start.replace(second=0, microsecond=0)
    close = close if close >= start else close + timedelta(minutes=1)
    while close < end:
        closes.append(close)
        close += timedelta(minutes=1)
    lags, mount_lags, unpublished = [], [], 0
    for moment in closes:
        landed = first_ok.get(moment - timedelta(minutes=1))
        if landed is None:
            continue
        lags.append((landed - moment).total_seconds())
        if mount is not None:
            after = next((value for value in published if value >= landed), None)
            if after is None:
                unpublished += 1
            else:
                mount_lags.append((after - moment).total_seconds())
    return {
        'minutes': len(closes), 'landed': len(lags), 'missing': len(closes) - len(lags),
        'unpublished': unpublished, 'failed_receipts': failed,
        'lag': _spread(lags), 'publication_lag': _spread(mount_lags) if mount is not None else None,
    }


def _spread(values: Sequence[float]) -> dict[str, float] | None:
    if not values:
        return None
    return {'p50': nearest_rank(values, QUANTILES['median']), 'p90': nearest_rank(values, QUANTILES['K2_window']),
            'p95': nearest_rank(values, QUANTILES['K2_baseline']), 'max': max(values)}


def contention(
    receipts: Sequence[Mapping[str, object]], started: datetime, queried: datetime,
    referenced: tuple[datetime, datetime] | None, observed: datetime,
) -> dict[str, object]:
    """K1 and K2 over the query window against the hour before it, as seen when the evidence was
    read at ``observed``; the reference window is reported."""
    windows = {
        'baseline': (started - BASELINE, started),
        'query': (started, queried + SETTLING),
        **({'reference': referenced} if referenced is not None else {}),
    }
    series: dict[str, object] = {}
    k1 = k2 = True
    for feed, data, mount in (*CONTENTION_SERIES, *REPORTED_SERIES):
        measured = {name: landing(receipts, feed, data, mount, *bounds, observed) for name, bounds in windows.items()}
        judged = (feed, data, mount) in CONTENTION_SERIES
        baseline, window = measured['baseline'], measured['query']
        complete = (
            int(str(baseline['landed'])) >= MIN_BASELINE_MINUTES and int(str(window['minutes'])) >= MIN_WINDOW_MINUTES
            and window['missing'] == 0 and window['unpublished'] == 0
        )
        timely = complete and all(
            _timely(baseline[key], window[key]) for key in ('lag', 'publication_lag') if key == 'lag' or mount is not None
        )
        if judged:
            k1, k2 = k1 and complete, k2 and timely
        series[f'{feed}/{data}'] = {'judged': judged, 'complete': complete, 'timely': timely, **measured}
    return {
        'series': series, 'K1': k1, 'K2': k2, 'observed': stamp(observed),
        'windows': {name: [stamp(a), stamp(b)] for name, (a, b) in windows.items()},
    }


def _timely(baseline: object, window: object) -> bool:
    if not isinstance(baseline, dict) or not isinstance(window, dict):
        return False
    return (
        window['p50'] <= baseline['p50'] + THRESHOLDS['K2_p50_increase_seconds']
        and window['p90'] <= baseline['p95'] + THRESHOLDS['K2_p90_over_baseline_p95_seconds']
    )


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


def published_between(lines: Iterable[str], start: datetime, end: datetime) -> list[str]:
    """Result IDs the service log shows published in ``[start, end]``."""
    return [
        match.group(1) for line in lines
        for match in [_PUBLISHED_LINE.search(line)] if match and start <= _log_time(line) <= end
    ]


def _log_time(line: str) -> datetime:
    """A service log line's time: ``YYYY-MM-DD HH:MM:SS,mmm`` in UTC, as the service logs it."""
    return datetime.strptime(line[:23], '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=UTC)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace(' ', 'T'))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def host_facts(path: Path) -> dict[str, list[str]]:
    facts: dict[str, list[str]] = defaultdict(list)
    for line in path.read_text().splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            facts[key.strip()].append(value.strip())
    return dict(facts)


def dagit_materializations(dagit: str, started: datetime, ended: datetime) -> list[dict[str, object]]:
    """The ``market_state_query_service`` materializations Dagit shows for the window."""
    graphql = DAGIT_QUERY % (int(started.timestamp() * 1000), int(ended.timestamp() * 1000))
    request = urllib.request.Request(
        dagit + '/graphql', data=json.dumps({'query': graphql}).encode(), headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.load(response)
    return [
        {'timestamp': event['timestamp'], **{entry['label']: entry.get('intValue') for entry in event['metadataEntries']}}
        for event in body['data']['assetOrError']['assetMaterializations']
    ]


def compression(path: str) -> dict[str, object]:
    """The finest result re-encoded as zstd and LZ4 IPC in a scratch directory, against plain and memory-mapped reads."""
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


def inventory(mount: Path) -> dict[str, object]:
    results = mount / 'results'
    entries = sorted(results.iterdir()) if results.is_dir() else []
    return {
        'results': len(entries),
        'result_bytes': sum(file.stat().st_size for entry in entries for file in entry.iterdir()),
        'staging': sorted(os.listdir(mount / 'staging')),
        'lifecycle_bytes': (mount / 'lifecycle.sqlite').stat().st_size,
    }


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
    last = next(int(str(_list(stage)[1])) for stage in _list(meta['stages']) if _list(stage)[0] == 'B') - 1
    checked: dict[str, dict[str, object]] = {}
    for sample in samples:
        if sample['stage'] == 'B' and sample['round'] == last and sample['case'] in _list(meta['numeric']) and succeeded(sample):
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
    log: Sequence[str],
    hosts: tuple[Mapping[str, Sequence[str]], Mapping[str, Sequence[str]]],
    deploys: Sequence[Mapping[str, object]],
    staging: Sequence[str],
    materializations: Sequence[Mapping[str, object]],
    extras: Mapping[str, object],
) -> dict[str, object]:
    """Every interim criterion of the frozen protocol over one run's gathered inputs.

    The verdict is ``VOID`` when an external cause is evidenced (a deploy, another consumer,
    maintenance, an unclean baseline), ``INTERIM PASS`` when every criterion holds, and
    ``FAIL <criteria>`` otherwise. ``finalize`` then adds recovery and expiry.
    """
    started = _utc(str(meta['started_at']))
    window = list(evidence.get('window', []))
    ended = _utc(str(window[0]['ended_at'])) if window else None
    corpus, stages = list(_mapping(meta['cases'])), [_list(stage) for stage in _list(meta['stages'])]
    pairs, pair_case, finest = int(str(meta['pairs'])), str(meta['pair_case']), str(meta['finest'])
    successes = [sample for sample in samples if succeeded(sample)]
    ids = [str(sample['result_id']) for sample in successes]
    queried = max((_utc(str(sample['ended_at'])) for sample in samples), default=started)
    timings = phases(log)
    statements = list(evidence.get('statement', []))
    reference_rows = [row for row in statements if row['statement'] == 'reference']
    referenced = None
    if reference_rows:
        finished = _utc(str(reference_rows[-1]['finished_at']))
        referenced = (finished - timedelta(milliseconds=int(str(reference_rows[-1]['query_duration_ms']))), finished)
    before, after = hosts

    voids: list[str] = []
    deployed = [
        deploy for deploy in deploys
        if _utc(str(deploy['createdAt'])) <= (ended or queried) and _utc(str(deploy['updatedAt'])) >= started - BASELINE
    ]
    if deployed:
        voids.append(f"deploy {', '.join(str(deploy['headSha'])[:8] for deploy in deployed)} overlapped the run or its baseline")
    unclean = [
        container for container in CONTAINERS
        if container != 'market-state' and f'{container}_started' in before
        and _utc(before[f'{container}_started'][0]) > started - BASELINE
    ]
    if unclean and not deployed:
        voids.append(f"baseline unclean: {', '.join(unclean)} started within 60 minutes before the run")
    # Every service request, in flight, failed or published, logs statements under its result ID.
    # The run's own requests are its known IDs, and each failure that never learned its ID accounts
    # for at most one unknown ID whose first statement ran while that request was open: a failing
    # run can never pass itself off as a shared one, and a failure that never reached the service
    # cannot hide another consumer. A busy answer is the run's own Q3 failure; a consumer holding
    # the slot shows up here.
    shared_until = queried + SETTLING
    known = {str(sample['result_id']) for sample in samples if sample['result_id']}
    unknown = sorted(
        _utc(str(row['first_at'])) for row in evidence.get('request', [])
        if str(row['log_comment']) not in known and started <= _utc(str(row['first_at'])) <= shared_until
    )
    open_ = sorted(
        (_utc(str(sample['ended_at'])), _utc(str(sample['started_at']))) for sample in samples if not sample['result_id']
    )
    foreign = 0
    for first in unknown:
        request = next((request for request in open_ if request[1] <= first <= request[0]), None)
        if request is None:
            foreign += 1
        else:
            open_.remove(request)
    published = sorted(set(published_between(log, started, shared_until)) - known)
    if foreign or published:
        voids.append(f'another consumer shared the service: {foreign} foreign requests, {len(published)} foreign results')
    maintenance = [row for row in evidence.get('maintenance', []) if started <= _utc(str(row['at'])) <= (ended or queried)]
    if maintenance:
        voids.append(f'maintenance ran during the run: {len(maintenance)} cleanup or rollout records')
    # runs.db is read after the run; only a backfill run that started before the evidence read
    # and was still running when the baseline began overlapped it.
    backfills = [
        run for run in map(_mapping, _list(_mapping(extras.get('backfill', {})).get('native_backfills', [])))
        if run['start_time'] is not None and float(str(run['start_time'])) <= (ended or queried).timestamp()
        and (run['end_time'] is None or float(str(run['end_time'])) >= (started - BASELINE).timestamp())
    ]
    if backfills:
        voids.append(f'native backfill runs overlapped the run or its baseline: {len(backfills)}')

    criteria: dict[str, dict[str, object]] = {}
    frozen = meta.get('frozen') is True and meta.get('protocol_version') == PROTOCOL_VERSION
    criteria['F'] = {'frozen': frozen, 'passed': frozen}
    criteria['P0'] = completeness(samples, corpus, stages, pairs, pair_case)
    missing_sections = [
        section for section in ('window', 'statement', 'request', 'parts', 'table', 'receipt') if not evidence.get(section)
    ]
    unlogged = [result_id for result_id in ids if {'pin_ms', 'total_ms', 'rss_peak_bytes'} - set(timings.get(result_id, {}))]
    unrecorded = [
        str(sample['result_id']) for sample in successes
        if not _statements(sample) <= {str(row['statement']) for row in statements if row['log_comment'] == sample['result_id']}
    ]
    criteria['E0'] = {
        'missing_sections': missing_sections, 'results_without_phase_logs': unlogged,
        'results_without_statements': unrecorded, 'reference_statement': bool(reference_rows),
        'materializations': len(materializations),
        'passed': not missing_sections and not unlogged and not unrecorded and bool(reference_rows) and bool(materializations),
    }
    nonempty = [_failure(sample) for sample in successes if sample['case'] == 'C16' and sample['cells'] != 0]
    c04 = numeric['C04']['partial'] if 'C04' in numeric else None
    criteria['P1'] = {
        'c16_nonempty': nonempty, 'c04_partial': c04,
        'passed': not nonempty and ('C04' not in corpus or c04 == {
            'first_column_partial': True, 'last_column_partial': True, 'first_row_partial': True, 'last_row_partial': True,
        }),
    }
    criteria.update(latency(samples, finest))
    # A container missing from either snapshot has no restart or baseline evidence.
    missing = [container for container in CONTAINERS if any(f'{container}_started' not in facts for facts in hosts)]
    restarted = [
        container for container in CONTAINERS
        if f'{container}_started' in after and _utc(after[f'{container}_started'][0]) > started
    ]
    criteria['S'] = {
        'restarted': restarted, 'missing': missing,
        'restart_counts': {key: value for key, value in after.items() if key.endswith('_restarts')},
        'oom_killed': {key: value for key, value in after.items() if key.endswith('_oom_killed')},
        'passed': not missing and (not restarted or bool(deployed)),
    }
    for key in ('N1', 'N2', 'N3', 'N4'):
        criteria[key] = {
            'passed': len(numeric) == len(_list(meta['numeric'])) and all(bool(result[key]) for result in numeric.values())
            and all(result['price_index_disagreements'] == 0 and result['before_history'] == 0 for result in numeric.values()),
            'results': {case: result[key] for case, result in numeric.items()},
        }
    peak = max((timings[result_id].get('rss_peak_bytes', 0) for result_id in ids if result_id in timings), default=0)
    criteria['R1'] = {'rss_peak_bytes': peak, 'limit': THRESHOLDS['R1_rss_peak_bytes'],
                      'passed': 0 < peak <= THRESHOLDS['R1_rss_peak_bytes']}
    served = [row for row in statements if row['statement'] != 'reference']
    spills = sum(int(str(row['spill_events'])) for row in served)
    memory = max((int(str(row['memory_usage'])) for row in served), default=0)
    criteria['R2'] = {'statements': len(served), 'spill_events': spills, 'max_statement_memory_bytes': memory,
                      'limit': THRESHOLDS['R2_statement_memory_bytes'],
                      'passed': bool(served) and spills == 0 and memory <= THRESHOLDS['R2_statement_memory_bytes']}
    storage_full = sum(1 for sample in samples if sample['status'] == 507)
    sizes = sorted((int(str(sample['bytes'])) for sample in successes), reverse=True)
    criteria['R3'] = {
        'staging': list(staging), 'storage_full': storage_full,
        # Publication renames within the volume, so staging never copies a result: at most the two
        # concurrent queries' results are in staging at once, beside everything already published.
        'retained_result_bytes': sum(sizes), 'peak_staging_bytes': sum(sizes[:2]), 'largest_result_bytes': sizes[0] if sizes else 0,
        'passed': not staging and not storage_full,
    }
    tables = [(str(row['name']), str(row['engine'])) for row in evidence.get('table', [])]
    physical = sorted(table for table, engine in tables if 'MergeTree' in engine)
    criteria['R4'] = {
        'objects': tables, 'physical': physical,
        'passed': physical == list(PROJECTION_TABLES) and all(engine == 'View' for _, engine in tables if 'MergeTree' not in engine),
    }
    load_ = contention(list(evidence.get('receipt', [])), started, queried, referenced, ended or queried)
    criteria['K1'] = {'passed': bool(load_['K1'])}
    criteria['K2'] = {'passed': bool(load_['K2'])}
    # The query window's receipts and materializations count exactly the answers the run received:
    # another consumer inside the window voids the run, and nothing outside it counts.
    answered = sum(1 for sample in samples if sample['status'] == 200)
    receipted = sum(
        int(match.group(1)) for row in evidence.get('receipt', [])
        if (row['feed'], row['series']) == QUERY_RECEIPTS and started <= _utc(str(row['recorded_at'])) <= shared_until
        for match in [_OK_COUNT.search(str(row['error']))] if match
    )
    shown = sum(
        int(str(entry.get('queries_ok') or 0)) for entry in materializations
        if started <= datetime.fromtimestamp(int(str(entry['timestamp'])) / 1000, UTC) <= shared_until
    )
    criteria['O1'] = {'answered_ok': answered, 'receipted_ok': receipted, 'dagit_queries_ok': shown,
                      'passed': receipted == answered == shown}

    failed = [key for key, value in criteria.items() if not value['passed']]
    verdict = f"VOID {'; '.join(voids)}" if voids else 'INTERIM PASS' if not failed else 'FAIL ' + ' '.join(failed)
    cold = not [line for line in log if _PUBLISHED_LINE.search(line) and _log_time(line) < started]
    return {
        'protocol_version': meta['protocol_version'], 'run': name, 'started_at': meta['started_at'],
        'queries_ended_at': stamp(queried), 'evidence_read_at': stamp(ended) if ended else None,
        'server_version': window[0]['server_version'] if window else None,
        'verdict': verdict, 'voids': voids, 'criteria': criteria,
        'stage_a': 'first requests since the service started; ClickHouse caches and the OS page cache as found'
        if cold else 'not cold: the service answered queries before the run',
        'cases': _case_table(samples, timings),
        'stages': _stage_table(samples),
        'numeric': dict(numeric),
        'statements': _statement_table(statements),
        'reference_cost': reference_rows,
        'parts': list(evidence.get('parts', [])), 'tables': list(evidence.get('table', [])),
        'maintenance': list(evidence.get('maintenance', [])),
        'contention': load_,
        'materializations': list(materializations),
        'deploys': list(deploys),
        **extras,
        'host_before': dict(before), 'host_after': dict(after),
        'samples': list(samples),
    }


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
            **_percentiles(seconds, 'seconds'),
            'read_p50_seconds': nearest_rank(reads, QUANTILES['median']) if reads else None,
            'cells': done[-1]['cells'] if done else None, 'bytes': done[-1]['bytes'] if done else None,
            'phases_p50_ms': {
                key: nearest_rank([float(entry[key]) for entry in phase if key in entry], QUANTILES['median'])
                for key in ('pin_ms', 'extent_ms', 'sql_ms', 'write_ms', 'validate_ms', 'publish_ms', 'total_ms')
                if any(key in entry for entry in phase)
            },
        }
    return table


def _percentiles(values: Sequence[float], unit: str) -> dict[str, float | None]:
    return {
        f'p50_{unit}': nearest_rank(values, 0.5) if values else None,
        f'p95_{unit}': nearest_rank(values, 0.95) if values else None,
        f'max_{unit}': max(values) if values else None,
    }


def _stage_table(samples: Sequence[Mapping[str, object]]) -> dict[str, dict[str, float | None]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for sample in samples:
        if succeeded(sample):
            grouped[str(sample['stage'])].append(float(str(sample['request_seconds'])))
            if sample['stage'] in ('A', 'B', 'C'):
                grouped['A-C pooled'].append(float(str(sample['request_seconds'])))
    return {stage: _percentiles(values, 'seconds') for stage, values in sorted(grouped.items())}


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
            'spill_events': sum(int(str(row['spill_events'])) for row in rows),
            'settings': rows[-1]['settings'],
        }
        for name, rows in sorted(grouped.items())
    }


def verdict(run: Path, mount: Path, url: str, dagit: str) -> int:
    """The interim verdict: write ``report.json`` and ``report.md``; print the verdict last."""
    meta, samples, evidence = load(run)
    started = _utc(str(meta['started_at']))
    ended = _utc(str(evidence['window'][0]['ended_at'])) if evidence.get('window') else datetime.now(UTC)
    finest = [sample for sample in samples if sample['case'] == meta['finest'] and succeeded(sample)]
    report = judge(
        run.name, meta, samples, evidence,
        numeric=numeric_checks(meta, samples, mount, url, read_reference(run / 'reference.arrow')),
        log=(run / 'service.log').read_text().splitlines(),
        hosts=(host_facts(run / 'host_before.txt'), host_facts(run / 'host_after.txt')),
        deploys=json.loads((run / 'deploys.json').read_text()),
        staging=sorted(os.listdir(mount / 'staging')),
        materializations=dagit_materializations(dagit, started, ended),
        extras={
            'backfill': json.loads((run / 'backfill.json').read_text()),
            'compression': compression(mounted(str(finest[-1]['cells_path']), mount)) if finest else {},
            'results_volume': inventory(mount),
        },
    )
    write_report(run, 'report', report)
    print(report['verdict'])
    return 0 if report['verdict'] == 'INTERIM PASS' else 1


def lifecycle_facts(root: Path, ids: set[str]) -> dict[str, str]:
    """What the service's lifecycle store and result directory hold for the run's result IDs."""
    connection = sqlite3.connect(f'file:{root / "lifecycle.sqlite"}?mode=ro', uri=True, timeout=30)
    try:
        rows = connection.execute('SELECT result_id FROM results').fetchall()
        accesses = connection.execute('SELECT result_id, max(last_access_ns) FROM files GROUP BY result_id').fetchall()
    finally:
        connection.close()
    last = max((int(ns) for result_id, ns in accesses if result_id in ids), default=0)
    on_disk = {entry.name for entry in (root / 'results').iterdir()} if (root / 'results').is_dir() else set()
    return {
        'checked_at': stamp(datetime.now(UTC)),
        'last_access': stamp(datetime.fromtimestamp(last / 1e9, UTC)) if last else '',
        'results_on_disk': str(len(ids & on_disk)),
        'lifecycle_rows': str(sum(1 for (result_id,) in rows if result_id in ids)),
    }


def read_reference(path: Path) -> pa.Table:
    with pa.OSFile(str(path)) as source:
        table = ipc.open_stream(source).read_all()
    return table.combine_chunks()


def finalize(run: Path) -> int:
    """The final verdict: the interim report with the recovery and expiry steps."""
    report = json.loads((run / 'report.json').read_text())
    recovery, expiry = host_facts(run / 'recovery.txt'), host_facts(run / 'expiry.txt')

    def fact(facts: Mapping[str, Sequence[str]], key: str) -> str:
        return facts[key][0] if key in facts else ''

    interrupted = fact(recovery, 'interrupted')
    performed = bool(_UUID.fullmatch(interrupted))
    criteria = dict(report['criteria'])
    criteria['RC'] = {
        'performed': performed, 'interrupted_result': interrupted, 'client_exit': fact(recovery, 'client_exit'),
        'interrupted_logged': fact(recovery, 'interrupted_logged'), 'staging_after': fact(recovery, 'staging_after'),
        'lifecycle_rows': fact(recovery, 'lifecycle_rows'), 'receipt': fact(recovery, 'receipt'),
        'follow_up_status': fact(recovery, 'follow_up_status'),
        'passed': performed and fact(recovery, 'client_exit') not in ('', '0') and fact(recovery, 'interrupted_logged') == '1'
        and fact(recovery, 'staging_after') == '0' and fact(recovery, 'lifecycle_rows') == '0'
        and fact(recovery, 'receipt').startswith('FAILED EXPORT_INTERRUPTED') and fact(recovery, 'follow_up_status') == '200',
    }
    anchor = host_facts(run / 'expiry_due.txt')
    last, checked = fact(anchor, 'last_access'), fact(expiry, 'checked_at')
    due = stamp(_utc(last) + EXPIRY_WAIT) if last else ''
    criteria['E1'] = {
        'last_access': last, 'due_at': due, 'checked_at': checked, 'results_on_disk': fact(expiry, 'results_on_disk'),
        'lifecycle_rows': fact(expiry, 'lifecycle_rows'),
        'passed': bool(due and checked) and _utc(checked) >= _utc(due)
        and fact(expiry, 'results_on_disk') == '0' and fact(expiry, 'lifecycle_rows') == '0',
    }
    interim = str(report['verdict'])
    failed = [key for key, value in criteria.items() if not value['passed']]
    if interim.startswith('VOID'):
        final = interim
    elif not performed:
        final = 'INCOMPLETE the recovery step missed the in-flight export; repeat it'
    else:
        final = 'PASS' if not failed else 'FAIL ' + ' '.join(failed)
    report.update({'criteria': criteria, 'interim_verdict': interim, 'verdict': final})
    write_report(run, 'report-final', report)
    print(final)
    return 0 if final == 'PASS' else 1


def write_report(run: Path, stem: str, report: Mapping[str, object]) -> None:
    """``<stem>.json``, ``<stem>.md`` and the markdown cut into parts that fit one GitHub comment each."""
    (run / f'{stem}.json').write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    rendered = markdown(report)
    (run / f'{stem}.md').write_text(rendered)
    digest = hashlib.sha256(rendered.encode()).hexdigest()
    parts = report_parts(rendered)
    for number, text in enumerate(parts, start=1):
        (run / f'{stem}.part{number}.md').write_text(f'`{stem}.md` sha256 `{digest}`, part {number} of {len(parts)}\n\n{text}')


def report_parts(text: str) -> list[str]:
    """``text`` cut at line ends into consecutive parts of at most ``REPORT_PART_CHARS``."""
    parts: list[str] = []
    while len(text) > REPORT_PART_CHARS:
        cut = text.rindex('\n', 0, REPORT_PART_CHARS) + 1
        parts.append(text[:cut])
        text = text[cut:]
    return [*parts, text] if text else parts


def markdown(report: Mapping[str, object]) -> str:
    """The report as posted on #462: every criterion, case, sample and failure."""
    lines = [
        f"## Market state acceptance run `{report['run']}` (protocol {report['protocol_version']})",
        '',
        f"**Verdict: {report['verdict']}**. Started {report['started_at']}, queries ended {report['queries_ended_at']}, "
        f"evidence read {report['evidence_read_at']}, ClickHouse {report['server_version']}. Stage A: {report['stage_a']}.",
        '',
        '### Criteria',
        '',
        '| Criterion | Passed | Values |',
        '|---|---|---|',
    ]
    for key, value in _mapping(report['criteria']).items():
        values = _mapping(value)
        shown = {name: item for name, item in values.items() if name != 'passed'}
        lines.append(f"| {key} | {'yes' if values['passed'] else '**no**'} | `{json.dumps(shown, sort_keys=True, default=str)}` |")
    lines += ['', '### Cases', '', '| Case | Request | n | Fail | Cold s | p50 s | p95 s | Max s | Read p50 s | Cells | Bytes | Phase p50 ms |',
              '|---|---|---|---|---|---|---|---|---|---|---|---|']
    for case, entry in _mapping(report['cases']).items():
        row = _mapping(entry)
        lines.append(
            f"| {case} | `{json.dumps(row['request'], sort_keys=True)}` | {row['samples']} | {row['failures']} | "
            f"{_fmt(row['cold_seconds'])} | {_fmt(row['p50_seconds'])} | {_fmt(row['p95_seconds'])} | {_fmt(row['max_seconds'])} | "
            f"{_fmt(row['read_p50_seconds'])} | {row['cells']} | {row['bytes']} | `{json.dumps(row['phases_p50_ms'], sort_keys=True)}` |"
        )
    for title, key in (
        ('Stages', 'stages'), ('Numerical checks', 'numeric'), ('Statements', 'statements'), ('Raw reference cost', 'reference_cost'),
        ('Contention', 'contention'), ('Maintenance records', 'maintenance'), ('Deploys', 'deploys'),
        ('Dagit materializations', 'materializations'), ('Completed backfill', 'backfill'), ('Compression', 'compression'),
        ('Projection and raw tables', 'parts'), ('Tables named market_state', 'tables'), ('Results volume', 'results_volume'),
        ('Host before', 'host_before'), ('Host after', 'host_after'),
    ):
        lines += ['', f'### {title}', '', '```json', json.dumps(report.get(key), indent=1, sort_keys=True, default=str), '```']
    lines += ['', '### Samples', '', '| Stage | Stream | Round | Case | Status | Seconds | Read s | Cells | Result | Error |', '|---|---|---|---|---|---|---|---|---|---|']
    for entry in _list(report['samples']):
        sample = _mapping(entry)
        lines.append(
            f"| {sample['stage']} | {sample['stream']} | {sample['round']} | {sample['case']} | {sample['status']} | "
            f"{_fmt(sample['request_seconds'])} | {_fmt(sample['read_seconds'])} | {sample['cells']} | "
            f"{str(sample['result_id'])[:8]} | {sample['error']} |"
        )
    return '\n'.join(lines) + '\n'


def _fmt(value: object) -> str:
    return '' if value is None else f'{float(str(value)):.3f}'


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='benchmark_market_state.py', description=(__doc__ or '').splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    run_client = commands.add_parser('client', help='run stages A-D and write the run directory and its SQL')
    run_verdict = commands.add_parser('verdict', help='judge the run; print INTERIM PASS, VOID or FAIL <criteria> last')
    for command in (run_client, run_verdict):
        command.add_argument('--run', type=Path, required=True)
        command.add_argument('--mount', type=Path, default=Path(SERVICE_ROOT))
        command.add_argument('--url', default=DEFAULT_URL)
    run_verdict.add_argument('--dagit', default=DAGIT_URL)
    run_finalize = commands.add_parser('finalize', help='add recovery and expiry; print PASS, VOID, INCOMPLETE or FAIL last')
    run_finalize.add_argument('--run', type=Path, required=True)
    run_backfill = commands.add_parser('backfill', help="print the historical cube upgrade's throughput and overlapping backfills as JSON")
    run_backfill.add_argument('--runs-db', type=Path, default=Path('/opt/dagster-instance/runs.db'))
    run_backfill.add_argument('--since', required=True, help="the run's started_at from run.json")
    run_expiry = commands.add_parser('expiry', help='print the lifecycle facts of the result IDs read from stdin')
    run_expiry.add_argument('--root', type=Path, default=Path(SERVICE_ROOT))
    arguments = parser.parse_args(argv)
    if arguments.command == 'client':
        client(arguments.run, arguments.mount, arguments.url)
        return 0
    if arguments.command == 'verdict':
        return verdict(arguments.run, arguments.mount, arguments.url, arguments.dagit)
    if arguments.command == 'finalize':
        return finalize(arguments.run)
    if arguments.command == 'expiry':
        facts = lifecycle_facts(arguments.root, set(sys.stdin.read().split()))
        print(''.join(f'{key}={value}\n' for key, value in facts.items()), end='')
        return 0
    print(json.dumps(backfill(arguments.runs_db, _cube_trades_per_day, _utc(arguments.since)), indent=1, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
