from __future__ import annotations

import hashlib
import importlib
import json
import logging
import math
import sqlite3
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from datetime import time as clock
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from origo.query import market_state
from origo.query.market_state import parse_request
from origo.query.market_state_reader import read_table
from origo.sources.lifecycle import SourceRuntime

from .test_market_state_api import Service, service  # noqa: F401
from .test_market_state_query import MINUTES, cube  # noqa: F401

# pytest puts tests/ on sys.path, where tests/tools shadows the repository's tools/; the
# benchmark, and the stream processes it spawns, import it from the repository root.
sys.path.append(str(Path(__file__).resolve().parents[2]))
bench = importlib.import_module('tools.benchmark_market_state')


def test_frozen_protocol_matches_the_slice() -> None:
    assert bench.PROTOCOL_VERSION == 1
    assert bench.STAGES == (('A', 1, 1), ('B', 5, 1), ('C', 3, 2))
    assert bench.STREAM_ORDERS == ('ascending', 'descending')
    assert (bench.PAIR_CASE, bench.PAIRS, bench.FINEST) == ('C11', 3, 'C11')
    assert bench.NUMERIC_CASES == ('C04', 'C10', 'C11', 'C12', 'C13', 'C14')
    assert bench.QUANTILES == {'Q1': 0.9, 'median': 0.5, 'K2_window': 0.9, 'K2_baseline': 0.95}
    assert bench.THRESHOLDS == {
        'Q1_p90_seconds': 3.0,
        'Q2_median_seconds': 5.0,
        'Q2_max_seconds': 10.0,
        'R1_rss_peak_bytes': 1024**3,
        'R2_statement_memory_bytes': 2 * 1024**3,
        'K2_p50_increase_seconds': 2.0,
        'K2_p90_over_baseline_p95_seconds': 5.0,
    }
    assert bench.TOLERANCE == (1e-8, 1e-12)
    assert (bench.BASELINE, bench.SETTLING, bench.EXPIRY_WAIT) == (
        timedelta(minutes=60), timedelta(minutes=5), timedelta(hours=24, minutes=2),
    )
    assert (bench.MIN_BASELINE_MINUTES, bench.MIN_WINDOW_MINUTES) == (50, 5)
    assert bench.QUIET_HOURS == (clock(0, 0), clock(1, 30))
    assert bench.CONTENTION_SERIES == (
        ('provisional', 'binance_spot_trades', 'binance_spot_trades:mount'),
        ('provisional', 'binance_spot_aggtrades', 'binance_spot_aggtrades:mount'),
        ('provisional', 'binance_perp_aggtrades', 'binance_perp_aggtrades:mount'),
        ('depth', 'depth20_snapshots', None),
        ('depth', 'depth200_snapshots', None),
    )
    assert bench.REPORTED_SERIES == (('provisional', 'binance_perp_trades', 'binance_perp_trades:mount'),)
    assert bench.REFERENCE_SETTINGS == {
        'max_threads': 2, 'max_memory_usage': 8 * 1024**3, 'max_bytes_ratio_before_external_group_by': 0,
        'max_bytes_ratio_before_external_sort': 0, 'max_execution_time': 3600, 'min_bytes_to_use_direct_io': 1,
    }
    assert bench.PROJECTION_TABLES == (
        'binance_spot_trades_market_state_latest_revisions', 'binance_spot_trades_market_state_revisions',
    )
    assert bench.CONTAINERS == (
        'clickhouse', 'dagster', 'market-state', 'provisional-worker', 'provisional-binance-spot-aggtrades',
        'provisional-binance-perp-aggtrades', 'provisional-binance-perp-trades', 'depth-worker',
    )
    assert (bench.UPGRADE_JOB, bench.UPGRADE_WINDOW) == (
        'refresh_binance_spot_trades_canonical_source_job', ('2026-09-24 16:59:02', '2026-09-25 01:59:55'),
    )
    # The generated SQL and the Dagit read are frozen byte for byte.
    identities = [
        ('2021-01-01', 'rev-a', '00000000-0000-0000-0000-000000000001'),
        ('2021-01-02T00:00:00Z', 'rev-b', '00000000-0000-0000-0000-000000000002'),
    ]
    frozen = {
        'reference': bench.reference_select(identities, 'market_state_reference:frozen'),
        'evidence': bench.evidence_sql(
            datetime(2026, 9, 26, 7, 45, tzinfo=UTC), ['00000000-0000-0000-0000-000000000001'], 'market_state_reference:frozen'
        ),
        'dagit': bench.DAGIT_QUERY % (1, 2),
    }
    assert {name: hashlib.sha256(text.encode()).hexdigest() for name, text in frozen.items()} == {
        'reference': '2194d54d1843e246ebc123321970c7b85b3ded22cfc7e0e2a1800de0a6f1507e',
        'evidence': 'b2f95b6b70ec1daf737695a2ab72b81807d83bffa8ea62bdb1a6205dd35e668c',
        'dagit': '0255ad5cb6b14dfe55df090ff65d8afafb74fbc88faff8240f52c0150b0c8ede',
    }
    # The corpus, resolved for an unfloored start: the tail cases floor to the minute.
    corpus = bench.cases(datetime(2026, 9, 26, 7, 45, 56, 789000, tzinfo=UTC))
    assert corpus == {
        'C01': {'t1': '2026-09-26T06:45:00Z'},
        'C02': {'t1': '2026-09-26T00:00:00Z', 'tR': 225, 'pR': 250},
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
    # Types are part of the corpus: C04 sends one price as a decimal string and one as a number.
    assert [type(corpus['C04'][key]) for key in ('p1', 'p2', 'tR')] == [str, float, int]
    assert all(type(value) is int for case in ('C09', 'C10', 'C15', 'C16') for key, value in corpus[case].items() if key[0] == 'p')
    with pytest.raises(ValueError, match='timezone-aware'):
        bench.cases(datetime(2026, 9, 26, 7, 45))
    # Every case is a request the service accepts; every case gets 12 samples in A-C and the
    # finest case 6 more in D, 198 in all.
    requests = {case: parse_request(json.dumps(request).encode()) for case, request in corpus.items()}
    expected = bench.expected_samples(list(corpus), bench.STAGES, bench.PAIRS, bench.PAIR_CASE)
    assert sum(expected.values()) == 198 and Counter(case for _, _, _, case in expected.elements())['C11'] == 18
    # C04 rounds both price bounds up from a midpoint and cuts all four edges: at 450 s x 500
    # USDT no rounded edge is a multiple of 2^3 columns or 2^2 rows.
    c04 = requests['C04']
    assert c04.t1 is not None and c04.t2 is not None and c04.p1 is not None and c04.p2 is not None
    assert (c04.p1 / 125, c04.p2 / 125) == (Decimal('616.5'), Decimal('626.5'))
    columns = (market_state._time_edge(c04.t1), market_state._time_edge(c04.t2))
    rows = (market_state._price_edge(c04.p1), market_state._price_edge(c04.p2))
    assert rows == (617, 627) and all(edge % 2**3 for edge in columns) and all(edge % 2**2 for edge in rows)
    # C10 is half open: it ends where 2026-09-01 begins, so 2026-08-31 is its last day.
    assert market_state._time_edge(datetime(2026, 9, 1, tzinfo=UTC)) % 1536 == 0


def _sample(case: str, stage: str, seconds: float, *, stream: int = 0, number: int = 0, status: int = 200,
            readable: bool = True, read: float | None = 0.1) -> dict[str, object]:
    return {'case': case, 'stage': stage, 'stream': stream, 'round': number, 'request_seconds': seconds,
            'read_seconds': read, 'status': status, 'readable': readable, 'error': ''}


def test_verdict_counts_failures_and_applies_nearest_rank() -> None:
    assert bench.nearest_rank([3.0, 1.0, 2.0, 4.0], 0.5) == 2.0
    assert bench.nearest_rank([1.0, 1.0, 1.0, 2.0], 0.5) == 1.0
    assert bench.nearest_rank([7.0], 0.9) == 7.0
    with pytest.raises(ValueError):
        bench.nearest_rank([], 0.5)
    # 192 pooled samples: p90 is the 173rd. With 19 failures it is the largest finite sample;
    # a 20th failure puts +inf on rank 173.
    fast = [_sample('C02', 'B', 0.5) for _ in range(173)]
    nineteen = bench.latency([*fast, *[_sample('C02', 'B', 0.1, status=503) for _ in range(19)]], finest='C02')
    assert (nineteen['Q1']['rank'], nineteen['Q1']['p90_seconds'], nineteen['Q1']['passed']) == (173, 0.5, True)
    assert len(nineteen['Q3']['failures']) == 19 and nineteen['Q3']['passed'] is False
    twenty = bench.latency([*fast[:172], *[_sample('C02', 'C', 0.2, readable=False) for _ in range(20)]], finest='C02')
    assert (twenty['Q1']['rank'], twenty['Q1']['p90_seconds'], twenty['Q1']['passed']) == (173, math.inf, False)
    # 18 finest samples: the median is the 9th; stage D counts only toward Q2.
    finest = [_sample('C11', stage, 4.0) for stage in 'ABBBBBCCCCCC'] + [_sample('C11', 'D', 11.0) for _ in range(6)]
    judged = bench.latency([*finest, *[_sample('C02', 'B', 0.5) for _ in range(12)]])
    assert (judged['Q2']['samples'], judged['Q2']['median_seconds'], judged['Q2']['max_seconds']) == (18, 4.0, 11.0)
    assert judged['Q2']['passed'] is False and judged['Q1']['samples'] == 24 and judged['Q1']['p90_seconds'] == 4.0
    # P0 accepts exactly the promised samples and nothing else.
    stages = [['A', 1, 1], ['C', 1, 2]]
    whole = [
        _sample(case, stage, 1.0, stream=stream)
        for case in ('C01', 'C02') for stage, streams in (('A', 1), ('C', 2)) for stream in range(streams)
    ] + [_sample('C02', 'D', 1.0, stream=stream) for stream in range(2)]
    assert bench.completeness(whole, ['C01', 'C02'], stages, 1, 'C02')['passed'] is True
    missing = bench.completeness(whole[1:], ['C01', 'C02'], stages, 1, 'C02')
    assert missing['passed'] is False and missing['missing'] == ['A/0/0/C01']
    duplicate = bench.completeness([*whole, whole[0]], ['C01', 'C02'], stages, 1, 'C02')
    assert duplicate['passed'] is False and duplicate['unexpected'] == ['A/0/0/C01']
    stray = bench.completeness([*whole[:-1], _sample('C01', 'D', 1.0, stream=1)], ['C01', 'C02'], stages, 1, 'C02')
    assert stray['missing'] == ['D/1/0/C02'] and stray['unexpected'] == ['D/1/0/C01']
    for corrupt in (math.nan, math.inf, -0.5):
        broken = [{**whole[0], 'request_seconds': corrupt}, *whole[1:]]
        assert bench.completeness(broken, ['C01', 'C02'], stages, 1, 'C02')['corrupt']
    unread = [{**whole[0], 'read_seconds': None}, *whole[1:]]
    assert bench.completeness(unread, ['C01', 'C02'], stages, 1, 'C02')['passed'] is False


def _arrow(statement: str) -> pa.Table:
    client = market_state._connect()
    try:
        raw = client.raw_query(statement, settings={}, fmt='ArrowStream', external_data=None)
    finally:
        client.close()
    return ipc.open_stream(pa.BufferReader(raw)).read_all()


def _without_direct_io(sql: str) -> str:
    # The test ClickHouse keeps its data on tmpfs, which has no direct I/O.
    return sql.replace('min_bytes_to_use_direct_io = 1', 'min_bytes_to_use_direct_io = 0')


_CORPUS: dict[str, dict[str, object]] = {
    # Rounded explicit bounds cutting edge columns and rows; both prices round up from a midpoint.
    'T1': {'t1': '2021-01-01T00:57:11.25Z', 't2': '2021-01-01T01:00:00Z', 'p1': '28812.5', 'p2': 29062.5, 'tR': 225, 'pR': 250},
    # All coverage, the canonical day and the provisional minutes, at base resolution.
    'T2': {},
    # The provisional tail only, coarse on both axes.
    'T3': {'t1': '2021-01-02T00:00:00Z', 'tR': 112.5, 'pR': 500},
    # Independent exponents: coarse time with base price, and base time with coarse price.
    'T4': {'tR': 57600},
    'T5': {'pR': 16000},
}


def _run(service: Service, cube: SourceRuntime, run: Path, stages: tuple[tuple[str, int, int], ...]) -> list[dict[str, Any]]:
    for minute in MINUTES:
        cube.build(minute, provisional=True)
    bench.client(run, service.store.root, service.url, corpus=_CORPUS, stages=stages, pairs=1, pair_case='T2',
                 finest='T2', numeric=tuple(_CORPUS))
    return [json.loads(line) for line in (run / 'samples.jsonl').read_text().splitlines()]


def test_reference_agrees_with_real_results_and_catches_a_mismatch(
    service: Service, cube: SourceRuntime, tmp_path: Path
) -> None:
    run = tmp_path / 'run'
    stages = (('A', 1, 1), ('B', 1, 1), ('C', 1, 2))
    samples = _run(service, cube, run, stages)
    assert len(samples) == 5 * 4 + 2 and all(bench.succeeded(sample) for sample in samples)
    assert bench.completeness(samples, list(_CORPUS), [list(stage) for stage in stages], 1, 'T2')['passed']
    assert sorted((run / 'result_ids.txt').read_text().split()) == sorted(sample['result_id'] for sample in samples)
    setting, statement = (run / 'reference.sql').read_text().split(';\n', 1)
    assert setting == 'SET max_query_size = 67108864' and 'binance_spot_trades_raw_latest_revisions' in statement
    reference = _arrow(_without_direct_io(statement.rstrip().removesuffix(';')))
    checked: dict[str, dict[str, Any]] = {}
    tables: dict[str, tuple[pa.Table, dict[str, Any], dict[str, Any]]] = {}
    for sample in samples:
        if sample['stage'] != 'B':
            continue
        cells = read_table(sample['cells_path'], url=service.url)
        summary = read_table(sample['summary_path'], url=service.url).to_pylist()[0]
        metadata = bench.result_metadata(sample, service.store.root, service.url)
        tables[sample['case']] = (cells, summary, metadata)
        checked[sample['case']] = bench.check_result(cells, summary, metadata, reference)
    for case, result in checked.items():
        assert (result['N1'], result['N2'], result['N3'], result['N4']) == (True, True, True, True), (case, result)
        assert result['only_in_cube'] == result['only_in_reference'] == 0 and result['cells'] == result['reference_cells'] > 0
        assert result['price_index_disagreements'] == result['before_history'] == 0 and result['near_ties'] == []
    # 2,366 captured trades of 2021-01-01 and 3,243 in the first three minutes of 2021-01-02.
    assert checked['T2']['summary']['trade_count'] == checked['T4']['summary']['trade_count'] == 2366 + 3243
    assert checked['T3']['summary']['trade_count'] == 3243
    assert (checked['T1']['reference_builds'], checked['T2']['reference_builds'], checked['T3']['reference_builds']) == (1, 4, 3)
    assert checked['T1']['partial'] == {
        'first_column_partial': True, 'last_column_partial': False, 'first_row_partial': True, 'last_row_partial': True,
    }
    # Judged against another real result's reference, a result fails N1: its builds are not there.
    cells, summary, metadata = tables['T1']
    tail = [(str(key), str(revision), str(build)) for key, _, revision, build in tables['T3'][2]['pins']]
    other = _arrow(_without_direct_io(bench.reference_select(tail, 'test')))
    wrong = bench.check_result(cells, summary, metadata, other)
    assert wrong['N1'] is False and wrong['only_in_cube'] == cells.num_rows and wrong['reference_cells'] == 0
    # A summary that does not belong to the cells fails N3, with its own reference intact.
    assert bench.check_result(cells, tables['T2'][1], metadata, reference)['N3'] is False
    # With no identities, because every numerical request failed, the reference still runs and is
    # empty, so the verdict reports those failures instead of the run stopping.
    empty = _arrow(_without_direct_io(bench.reference_select([], 'test')))
    assert empty.num_rows == 0 and empty.schema.names == reference.schema.names
    assert bench.check_result(cells, summary, metadata, empty)['N1'] is False


def _receipt(series: str, minute: datetime, recorded: datetime, status: str = 'OK', feed: str = 'provisional') -> dict[str, object]:
    return {
        'feed': feed, 'series': series, 'minute': minute.strftime('%Y-%m-%d %H:%M:%S'),
        'recorded_at': recorded.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], 'status': status, 'error_code': '', 'rows': 1, 'error': '',
    }


def _receipts(started: datetime, end: datetime, lag: float = 20.0, window_lag: float = 21.0) -> list[dict[str, object]]:
    """Receipts shaped as the workers write them: data receipts carry their data minute, while
    publication receipts carry the worker's tick minute and are written inside that tick."""
    rows: list[dict[str, object]] = []
    minute = started - bench.BASELINE - timedelta(minutes=1)
    while minute < end:
        close = minute + timedelta(minutes=1)
        delay = lag if close < started else window_lag
        for feed, data, mount in (*bench.CONTENTION_SERIES, *bench.REPORTED_SERIES):
            rows.append(_receipt(data, minute, close + timedelta(seconds=delay - 2), 'STARTED', feed))
            rows.append(_receipt(data, minute, close + timedelta(seconds=delay), 'OK', feed))
            if mount is not None:
                tick = close + timedelta(seconds=delay + 5)
                rows.append(_receipt(mount, tick.replace(second=0, microsecond=0), tick, 'OK', feed))
        minute = close
    return rows


def test_contention_compares_first_ok_landing_lags() -> None:
    started = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    queried = started + timedelta(minutes=6)
    end = queried + timedelta(minutes=12)
    receipts = _receipts(started, end)
    load = bench.contention(receipts, started, queried, None, end)
    assert load['K1'] is True and load['K2'] is True
    # Only receipts recorded by the time the evidence was read count: a window minute that
    # landed after that is missing.
    late_minute = (started + timedelta(minutes=4)).strftime('%Y-%m-%d %H:%M:%S')
    landed_late = [
        {**row, 'recorded_at': (end + timedelta(seconds=30)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}
        if row['series'] == 'binance_spot_trades' and row['minute'] == late_minute else row
        for row in receipts
    ]
    assert bench.contention(landed_late, started, queried, None, end)['K1'] is False
    spot = load['series']['provisional/binance_spot_trades']
    assert (spot['baseline']['landed'], spot['baseline']['lag']['p50']) == (60, 20.0)
    assert (spot['query']['minutes'], spot['query']['landed'], spot['query']['lag']['p50']) == (11, 11, 21.0)
    # Publication lag runs from the data minute's close to the first OK publication after it
    # landed, whatever tick minute that publication receipt carries.
    assert spot['query']['publication_lag']['p50'] == 26.0
    # A missing data minute, or one never published afterwards, fails K1.
    gap = (started + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
    missing = [row for row in receipts if not (row['series'] == 'binance_spot_trades' and row['minute'] == gap)]
    assert bench.contention(missing, started, queried, None, end)['K1'] is False
    unpublished = [
        row for row in receipts
        if not (row['series'] == 'binance_spot_trades:mount' and str(row['recorded_at']) >= started.strftime('%Y-%m-%d %H:%M:%S'))
    ]
    assert bench.contention(unpublished, started, queried, None, end)['K1'] is False
    # A median 3 s above the baseline, or a p90 beyond its p95 plus 5 s, fails K2; 1.9 s does not.
    assert bench.contention(_receipts(started, end, window_lag=23.0), started, queried, None, end)['K2'] is False
    assert bench.contention(_receipts(started, end, window_lag=21.9), started, queried, None, end)['K2'] is True
    # Too little baseline is an incomplete measurement, not a pass.
    short = [row for row in receipts if str(row['minute']) >= (started - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')]
    assert bench.contention(short, started, queried, None, end)['K1'] is False
    # The reported perp trades series never decides the verdict.
    without_perp = [row for row in receipts if not str(row['series']).startswith('binance_perp_trades')]
    assert bench.contention(without_perp, started, queried, None, end)['K1'] is True


def _evidence(sql: str) -> str:
    client = market_state._connect()
    try:
        return ''.join(
            client.raw_query(statement, settings={}, fmt=None, external_data=None).decode()
            for statement in (part.strip() for part in sql.split(';\n')) if statement
        )
    finally:
        client.close()


def test_verdict_judges_a_real_run_directory(
    service: Service, cube: SourceRuntime, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    run = tmp_path / 'run'
    stages = (('A', 1, 1), ('B', 2, 1), ('C', 1, 2))
    with caplog.at_level(logging.INFO):
        samples = _run(service, cube, run, stages)
        service.api.tick(datetime.now(UTC))
    ticked = str(int(datetime.now(UTC).timestamp() * 1000))
    # The service logs through logging; the runbook captures the same lines with docker logs.
    log = [
        f'{datetime.fromtimestamp(record.created, UTC):%Y-%m-%d %H:%M:%S},{int(record.msecs):03d} INFO {record.name} {record.getMessage()}'
        for record in caplog.records
    ]
    statement = (run / 'reference.sql').read_text().split(';\n', 1)[1].rstrip().removesuffix(';')
    reference = _arrow(_without_direct_io(statement))
    (run / 'evidence.jsonl').write_text(_evidence((run / 'evidence.sql').read_text()))
    meta, loaded, evidence = bench.load(run)
    assert loaded == samples and set(evidence) >= {'window', 'statement', 'request', 'parts', 'table', 'receipt'}
    assert {row['log_comment'] for row in evidence['request']} == {sample['result_id'] for sample in samples}
    started = datetime.fromisoformat(str(meta['started_at']))
    # Host facts as the runbook records them: every container started long before the run.
    hosts = (
        {f'{name}_started': ['2021-01-01T00:00:00Z'] for name in bench.CONTAINERS},
        {f'{name}_started': ['2021-01-01T00:00:00Z'] for name in bench.CONTAINERS},
    )
    inputs: dict[str, Any] = {
        'numeric': bench.numeric_checks(meta, samples, service.store.root, service.url, reference),
        'log': log, 'hosts': hosts, 'deploys': [],
        'staging': sorted(path.name for path in (service.store.root / 'staging').iterdir()),
        # As dagit_materializations reads them: the event time in milliseconds beside the metadata.
        'materializations': [
            {'timestamp': ticked, **metadata} for asset, metadata in service.reporter.calls if asset == 'market_state_query_service'
        ],
        'extras': {
            'backfill': {},
            'compression': bench.compression(next(s['cells_path'] for s in reversed(samples) if s['case'] == 'T2')),
            'results_volume': bench.inventory(service.store.root),
        },
    }
    report = bench.judge(run.name, meta, samples, evidence, **inputs)
    passed = {key: value['passed'] for key, value in report['criteria'].items()}
    # A custom corpus is not the frozen protocol, and this ClickHouse runs no live feeds; every
    # other criterion holds on this real run.
    assert passed == {
        'F': False, 'P0': True, 'E0': True, 'P1': True, 'Q1': True, 'Q2': True, 'Q3': True, 'S': True,
        'N1': True, 'N2': True, 'N3': True, 'N4': True, 'R1': passed['R1'], 'R2': True, 'R3': True, 'R4': True,
        'K1': False, 'K2': False, 'O1': True,
    }
    assert report['voids'] == [] and str(report['verdict']).startswith('FAIL F')
    assert report['criteria']['R1']['rss_peak_bytes'] > 0 and report['criteria']['O1']['answered_ok'] == len(samples)
    assert [row['statement'] for row in report['reference_cost']] == ['reference']
    assert set(report['compression']) == {'uncompressed', 'zstd', 'lz4'} and report['results_volume']['staging'] == []
    assert str(report['stage_a']).startswith('first requests since the service started')
    # Missing, duplicated or foreign evidence changes the verdict; nothing missing becomes a pass.
    later = [(started + timedelta(seconds=1)).isoformat()]
    settled = max(datetime.fromisoformat(str(sample['ended_at'])) for sample in samples) + bench.SETTLING + timedelta(seconds=1)
    query_receipt = next(row for row in evidence['receipt'] if (row['feed'], row['series']) == bench.QUERY_RECEIPTS)
    busy = {
        **samples[-1], 'status': 503, 'result_id': '', 'readable': False, 'read_seconds': None, 'cells': 0,
        'error': '{"error": "busy"}',
    }
    changed: dict[str, tuple[str, list[dict[str, Any]], dict[str, Any], dict[str, Any]]] = {
        'sample dropped': ('P0', samples[1:], evidence, {}),
        'sample duplicated': ('P0', [*samples, samples[0]], evidence, {}),
        'statements missing': ('E0', samples, {**evidence, 'statement': []}, {}),
        'receipts missing': ('O1', samples, {**evidence, 'receipt': []}, {}),
        'receipts after the window': ('O1', samples, {
            **evidence, 'receipt': [{**row, 'recorded_at': settled.isoformat()} for row in evidence['receipt']],
        }, {}),
        'receipt of another request': ('O1', samples, {**evidence, 'receipt': [*evidence['receipt'], {**query_receipt, 'error': 'ok=1'}]}, {}),
        'materializations after the window': ('O1', samples, evidence, {
            'materializations': [{**entry, 'timestamp': str(int(settled.timestamp() * 1000))} for entry in inputs['materializations']],
        }),
        'busy answer': ('Q3', [*samples, busy], evidence, {}),
        'host snapshot missing a container': ('S', samples, evidence, {
            'hosts': (hosts[0], {key: value for key, value in hosts[1].items() if not key.startswith('market-state_')}),
        }),
        'phase log missing': ('E0', samples, evidence, {'log': [line for line in log if 'market state result' not in line]}),
        'cells statement missing': ('E0', samples, {
            **evidence, 'statement': [
                row for row in evidence['statement'] if not (row['statement'] == 'cells' and row['log_comment'] == samples[0]['result_id'])
            ],
        }, {}),
        # T2 has automatic price bounds, so it read its price extent.
        'extent statement missing': ('E0', samples, {
            **evidence, 'statement': [
                row for row in evidence['statement']
                if not (row['statement'] == 'extent' and row['log_comment'] == next(s['result_id'] for s in samples if s['case'] == 'T2'))
            ],
        }, {}),
        # C16's shape: a result with no occupied cell still read cells over its non-empty price interval.
        'cells statement of an empty result missing': ('E0', [{**samples[0], 'cells': 0}, *samples[1:]], {
            **evidence, 'statement': [
                row for row in evidence['statement'] if not (row['statement'] == 'cells' and row['log_comment'] == samples[0]['result_id'])
            ],
        }, {}),
        'finest case missing': ('Q2', [sample for sample in samples if sample['case'] != meta['finest']], evidence, {}),
        'materializations missing': ('E0', samples, evidence, {'materializations': []}),
        'numerical result missing': ('N1', samples, evidence, {'numeric': {k: v for k, v in inputs['numeric'].items() if k != 'T3'}}),
        'service restarted': ('S', samples, evidence, {'hosts': (hosts[0], {**hosts[1], 'market-state_started': later})}),
    }
    for name, (criterion, altered_samples, altered_evidence, altered_inputs) in changed.items():
        altered = bench.judge(run.name, meta, altered_samples, altered_evidence, **{**inputs, **altered_inputs})
        assert altered['criteria'][criterion]['passed'] is False, name
    # External causes void the run instead of passing or failing it.
    foreign = [*log, f'{started + timedelta(seconds=1):%Y-%m-%d %H:%M:%S},000 INFO origo.workers.market_state_api '
               'market state query 11111111-2222-3333-4444-555555555555 published publish_ms=1 total_ms=2 rss_peak_bytes=3']
    assert str(bench.judge(run.name, meta, samples, evidence, **{**inputs, 'log': foreign})['verdict']).startswith('VOID another consumer')
    in_flight = {'section': 'request', 'log_comment': '11111111-2222-3333-4444-666666666666', 'first_at': (started + timedelta(seconds=2)).isoformat()}
    shared = {**evidence, 'request': [*evidence['request'], in_flight]}
    assert str(bench.judge(run.name, meta, samples, shared, **inputs)['verdict']).startswith(
        'VOID another consumer shared the service: 1 foreign requests, 0 foreign results'
    )
    # The run's own failed request, which never learned its result ID, accounts for that ID: the
    # run fails instead of voiding, so a failing service cannot hide behind a shared window.
    failed = {
        **samples[0], 'status': 500, 'result_id': '', 'readable': False, 'read_seconds': None, 'cells': 0,
        'error': '{"error": "export_failed"}', 'ended_at': (started + timedelta(seconds=3)).isoformat(),
    }
    own = bench.judge(run.name, meta, [*samples, failed], shared, **inputs)
    assert own['voids'] == [] and own['criteria']['Q3']['passed'] is False
    # A failure open only after that ID's first statement, such as a refused connection, cannot
    # account for it; nor does the run's own busy answer void anything.
    unreached = {
        **failed, 'status': None, 'error': 'ConnectionRefusedError: [Errno 111] Connection refused',
        'started_at': (started + timedelta(seconds=4)).isoformat(), 'ended_at': (started + timedelta(seconds=4)).isoformat(),
    }
    assert str(bench.judge(run.name, meta, [*samples, unreached], shared, **inputs)['verdict']).startswith(
        'VOID another consumer shared the service: 1 foreign requests'
    )
    assert bench.judge(run.name, meta, [*samples, busy], evidence, **inputs)['voids'] == []
    deploy = {'createdAt': started.isoformat(), 'updatedAt': (started + timedelta(minutes=3)).isoformat(), 'headSha': 'abcdef0123456789'}
    assert str(bench.judge(run.name, meta, samples, evidence, **{**inputs, 'deploys': [deploy]})['verdict']).startswith('VOID deploy abcdef01')
    recent = ({**hosts[0], 'provisional-worker_started': [(started - timedelta(minutes=30)).isoformat()]}, hosts[1])
    assert str(bench.judge(run.name, meta, samples, evidence, **{**inputs, 'hosts': recent})['verdict']).startswith('VOID baseline unclean')
    running = {
        'job': 'backfill_binance_spot_trades_source_job', 'backfill_id': 'abcd', 'partition': '2026-09-20',
        'start_time': (started - timedelta(minutes=30)).timestamp(), 'end_time': None,
    }
    backfilled = {**inputs['extras'], 'backfill': {'native_backfills': [running]}}
    assert str(bench.judge(run.name, meta, samples, evidence, **{**inputs, 'extras': backfilled})['verdict']).startswith('VOID native backfill')
    # Backfill runs that started after the evidence read, ended before the baseline or never started did not overlap.
    read = datetime.fromisoformat(str(evidence['window'][0]['ended_at'])).replace(tzinfo=UTC)
    apart = {**inputs['extras'], 'backfill': {'native_backfills': [
        {**running, 'start_time': (read + timedelta(minutes=1)).timestamp()},
        {**running, 'start_time': (started - timedelta(hours=3)).timestamp(), 'end_time': (started - timedelta(minutes=61)).timestamp()},
        {**running, 'start_time': None},
    ]}}
    assert bench.judge(run.name, meta, samples, evidence, **{**inputs, 'extras': apart})['voids'] == []
    # The lifecycle facts the expiry step records, from the service's own store.
    facts = bench.lifecycle_facts(service.store.root, {sample['result_id'] for sample in samples})
    assert facts['results_on_disk'] == facts['lifecycle_rows'] == str(len(samples)) and facts['last_access']
    rendered = bench.markdown(report)
    parts = bench.report_parts(rendered)
    assert ''.join(parts) == rendered and all(len(part) <= bench.REPORT_PART_CHARS for part in parts)
    assert '**Verdict: FAIL F' in rendered and rendered.count('| B | 0 |') == 2 * len(_CORPUS)


def test_backfill_reads_the_run_store(tmp_path: Path) -> None:
    # Dagster's run store columns as production has them; the rows are run metadata, not market data.
    runs_db = tmp_path / 'runs.db'
    connection = sqlite3.connect(runs_db)
    connection.execute(
        'CREATE TABLE runs (id INTEGER, run_id TEXT, snapshot_id TEXT, pipeline_name TEXT, mode TEXT, status TEXT, '
        'run_body TEXT, partition TEXT, partition_set TEXT, create_timestamp TEXT, update_timestamp TEXT, '
        'start_time REAL, end_time REAL, backfill_id TEXT)'
    )
    since = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    rows = [
        (bench.UPGRADE_JOB, 'SUCCESS', '2021-01-01', '2026-09-24 17:00:00', 1000.0, 1007.0, None),
        (bench.UPGRADE_JOB, 'SUCCESS', '2021-01-02', '2026-09-24 17:01:00', 1060.0, 1066.0, None),
        (bench.UPGRADE_JOB, 'FAILURE', '2021-01-03', '2026-09-24 17:02:00', 1120.0, 1121.0, None),
        ('backfill_binance_spot_trades_source_job', 'STARTED', '2026-09-20', '2026-09-26 09:30:00',
         (since - timedelta(minutes=30)).timestamp(), None, 'abcd'),
        ('backfill_binance_spot_trades_source_job', 'SUCCESS', '2026-09-19', '2026-09-25 09:30:00',
         (since - timedelta(days=1)).timestamp(), (since - timedelta(days=1, minutes=-5)).timestamp(), 'efgh'),
    ]
    connection.executemany(
        "INSERT INTO runs (pipeline_name, status, partition, create_timestamp, start_time, end_time, backfill_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    connection.commit()
    connection.close()
    measured = bench.backfill(runs_db, lambda: {'2021-01-01': 700, '2021-01-02': 600}, since)
    assert (measured['runs'], measured['rows'], measured['summed_run_seconds'], measured['wall_seconds']) == (2, 1300, 13.0, 66.0)
    assert (measured['rows_per_run_second'], measured['rows_per_wall_second']) == (100.0, 1300 / 66)
    assert [run['backfill_id'] for run in measured['native_backfills']] == ['abcd']
