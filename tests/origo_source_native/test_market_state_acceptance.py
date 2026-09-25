from __future__ import annotations

import json
import logging
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from origo.query import market_state
from origo.query.market_state import parse_request
from origo.query.market_state_reader import read_table
from origo.sources.lifecycle import SourceRuntime
from tools import benchmark_market_state as bench

from .test_market_state_api import Service, service  # noqa: F401
from .test_market_state_query import MINUTES, cube  # noqa: F401


def test_frozen_protocol_matches_the_slice() -> None:
    start = datetime(2026, 9, 26, 7, 45, tzinfo=UTC)
    assert bench.PROTOCOL_VERSION == 1
    assert bench.cases(start) == {
        'C01': {'t1': '2026-09-26T06:45:00Z'},
        'C02': {'t1': '2026-09-26T00:00:00Z', 'tR': 225, 'pR': 250},
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
    assert bench.STAGES == (('A', 1, 1), ('B', 5, 1), ('C', 3, 2))
    assert bench.PAIRS == 3
    assert bench.THRESHOLDS == {
        'Q1_p90_seconds': 3.0,
        'Q2_median_seconds': 5.0,
        'Q2_max_seconds': 10.0,
        'R1_rss_peak_bytes': 1024**3,
        'K2_p50_increase_seconds': 2.0,
    }
    assert bench.TOLERANCE == (1e-8, 1e-12)
    assert bench.NUMERIC_CASES == ('C04', 'C10', 'C11', 'C12')
    assert bench.CONTENTION_SERIES == (
        ('provisional', 'binance_spot_trades'),
        ('provisional', 'binance_spot_trades:mount'),
        ('provisional', 'binance_spot_aggtrades'),
        ('provisional', 'binance_spot_aggtrades:mount'),
        ('provisional', 'binance_perp_aggtrades'),
        ('provisional', 'binance_perp_aggtrades:mount'),
        ('depth', 'depth20_snapshots'),
        ('depth', 'depth200_snapshots'),
    )
    # Every case is a request the service accepts, and every case gets 12 samples in A-C.
    for request in bench.cases(start).values():
        parse_request(json.dumps(request).encode())
    assert sum(rounds * streams for _, rounds, streams in bench.STAGES) == 12


def _sample(case: str, stage: str, seconds: float, *, status: int = 200, readable: bool = True) -> dict[str, object]:
    return {'case': case, 'stage': stage, 'request_seconds': seconds, 'status': status, 'readable': readable}


def test_verdict_counts_failures_and_applies_nearest_rank() -> None:
    assert bench.nearest_rank([3.0, 1.0, 2.0, 4.0], 0.5) == 2.0
    assert bench.nearest_rank([float(value) for value in range(1, 11)], 0.9) == 9.0
    assert bench.nearest_rank([float(value) for value in range(1, 12)], 0.9) == 10.0
    assert bench.nearest_rank([1.0, math.inf], 0.9) == math.inf
    # One failure in ten samples is the slowest sample, not a dropped one: p90 holds, Q3 fails.
    fast = [_sample('C02', 'B', 0.5) for _ in range(9)]
    one = bench.latency([*fast, _sample('C02', 'B', 0.1, status=503)], finest='C02')
    assert (one['Q1']['p90_seconds'], one['Q1']['passed'], one['Q3']['failures'], one['Q3']['passed']) == (0.5, True, 1, False)
    # Two failures in ten push p90 to infinity; an unreadable result fails like an error.
    two = bench.latency([*fast[:8], _sample('C02', 'B', 0.1, status=503), _sample('C02', 'C', 0.2, readable=False)], finest='C02')
    assert two['Q1']['p90_seconds'] == math.inf and not two['Q1']['passed']
    # Stage D counts only toward the finest case; its tail cannot hide in the pooled percentile.
    finest = [_sample('C11', stage, 4.0) for stage in 'ABBBBBCCCCCC'] + [_sample('C11', 'D', 11.0)]
    judged = bench.latency([*finest, *[_sample('C02', 'B', 0.5) for _ in range(12)]])
    assert (judged['Q2']['median_seconds'], judged['Q2']['max_seconds'], judged['Q2']['passed']) == (4.0, 11.0, False)
    assert judged['Q1']['samples'] == 24 and judged['Q1']['p90_seconds'] == 4.0 and judged['Q1']['passed'] is False
    # The protocol check refuses a run whose cases carry unequal weight.
    stages = [['A', 1, 1], ['B', 1, 1]]
    whole = [_sample(case, stage, 1.0) for case in ('C01', 'C02') for stage in 'AB'] + [_sample('C02', 'D', 1.0) for _ in range(2)]
    assert bench.protocol_followed(whole, ['C01', 'C02'], stages, 1, 'C02')['passed'] is True
    assert bench.protocol_followed(whole[1:], ['C01', 'C02'], stages, 1, 'C02')['passed'] is False


def _arrow(statement: str) -> pa.Table:
    client = market_state._connect()
    try:
        raw = client.raw_query(statement, settings={}, fmt='ArrowStream', external_data=None)
    finally:
        client.close()
    return ipc.open_stream(pa.BufferReader(raw)).read_all()


def test_reference_agrees_with_real_results_and_catches_a_mismatch(
    service: Service, cube: SourceRuntime, tmp_path: Path
) -> None:
    for minute in MINUTES:
        cube.build(minute, provisional=True)
    corpus: dict[str, dict[str, object]] = {
        # Rounded explicit bounds with partial edge columns and rows.
        'T1': {'t1': '2021-01-01T00:57:11.25Z', 't2': '2021-01-01T01:00:00Z', 'p1': '28812.5', 'p2': 29160, 'tR': 225, 'pR': 250},
        # All coverage, canonical day and provisional minutes, at base.
        'T2': {},
        # The provisional tail only, coarse.
        'T3': {'t1': '2021-01-02T00:00:00Z', 'tR': 112.5, 'pR': 500},
    }
    run = tmp_path / 'run'
    stages = (('A', 1, 1), ('B', 1, 1), ('C', 1, 2))
    bench.client(run, service.store.root, service.url, corpus=corpus, stages=stages, pairs=1, finest='T2', numeric=('T1', 'T2', 'T3'))
    samples: list[dict[str, Any]] = [json.loads(line) for line in (run / 'samples.jsonl').read_text().splitlines()]
    assert len(samples) == 3 * 4 + 2 and all(bench.succeeded(sample) for sample in samples)
    assert bench.protocol_followed(samples, list(corpus), [list(stage) for stage in stages], 1, 'T2')['passed']
    assert sorted((run / 'result_ids.txt').read_text().split()) == sorted(sample['result_id'] for sample in samples)
    assert 'binance_spot_trades_raw_latest_revisions' in (run / 'reference.sql').read_text()
    # The reference runs as generated, against the raw revisions the service's results pinned.
    sql = (run / 'reference.sql').read_text()
    setting, statement = sql.split(';\n', 1)
    assert setting == 'SET max_query_size = 67108864'
    # The test ClickHouse keeps its data on tmpfs, which has no direct I/O.
    reference = _arrow(statement.rstrip().removesuffix(';').replace('min_bytes_to_use_direct_io = 1', 'min_bytes_to_use_direct_io = 0'))
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
        assert result['only_in_cube'] == result['only_in_reference'] == 0 and result['cells'] == result['reference_cells']
    # 2,366 captured trades of 2021-01-01 and 3,243 in the first three minutes of 2021-01-02.
    assert checked['T2']['summary']['trade_count'] == 2366 + 3243
    assert checked['T3']['summary']['trade_count'] == 3243
    assert checked['T1']['reference_builds'] == 1 and checked['T3']['reference_builds'] == 3
    # A result judged against another real result's reference fails: its builds are not there.
    cells, summary, metadata = tables['T1']
    tail = [(str(key), str(revision), str(build)) for key, _, revision, build in tables['T3'][2]['pins']]
    other = _arrow(bench.reference_select(tail, 'test').replace('min_bytes_to_use_direct_io = 1', 'min_bytes_to_use_direct_io = 0'))
    wrong = bench.check_result(cells, summary, metadata, other)
    assert wrong['N1'] is False and wrong['only_in_cube'] == cells.num_rows and wrong['reference_cells'] == 0


def _receipt(series: str, minute: datetime, lag: float, status: str = 'OK', feed: str = 'provisional') -> dict[str, object]:
    return {
        'feed': feed, 'series': series, 'minute': minute.strftime('%Y-%m-%d %H:%M:%S'),
        'recorded_at': (minute + timedelta(minutes=1, seconds=lag)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'status': status, 'error_code': '', 'rows': 1,
    }


def test_contention_compares_first_ok_landing_lags() -> None:
    started = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    ended = started + timedelta(minutes=20)
    receipts: list[dict[str, object]] = []
    for feed, series in (*bench.CONTENTION_SERIES, *bench.REPORTED_SERIES):
        for offset in range(-61, 20):
            minute = started + timedelta(minutes=offset)
            receipts.append(_receipt(series, minute, 50.0, 'STARTED', feed))
            receipts.append(_receipt(series, minute, 55.0 if offset < 0 else 56.0, 'OK', feed))
            # A later retry of the same minute does not change its first-OK landing lag.
            receipts.append(_receipt(series, minute, 400.0, 'OK', feed))
    load = bench.contention(receipts, started, ended)
    assert load['K1'] is True and load['K2'] is True
    spot = load['series']['provisional/binance_spot_trades']
    assert spot['baseline']['minutes'] == 60 and spot['baseline']['p50_seconds'] == 55.0
    # Minutes closing inside the 15 minutes before the 5-minute settling margin.
    assert (spot['window']['minutes'], spot['window']['landed'], spot['window']['p50_seconds']) == (15, 15, 56.0)
    # A missing minute or a FAILED receipt on a listed series fails K1.
    gap = started + timedelta(minutes=3)
    missing = [row for row in receipts if not (row['series'] == 'binance_spot_trades' and row['minute'] == gap.strftime('%Y-%m-%d %H:%M:%S'))]
    assert bench.contention(missing, started, ended)['K1'] is False
    failed = [*receipts, _receipt('depth20_snapshots', started + timedelta(minutes=2), 1.0, 'FAILED', 'depth')]
    assert bench.contention(failed, started, ended)['K1'] is False
    # A median more than 2 s above the baseline fails K2.
    slower = [
        {**row, 'recorded_at': (datetime.fromisoformat(str(row['recorded_at'])) + timedelta(seconds=3)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}
        if row['series'] == 'binance_spot_aggtrades' and str(row['minute']) >= started.strftime('%Y-%m-%d %H:%M:%S') else row
        for row in receipts
    ]
    assert bench.contention(slower, started, ended)['K2'] is False
    # The reported perp trades series never decides the verdict.
    without_perp = [row for row in receipts if row['series'] != 'binance_perp_trades']
    assert bench.contention(without_perp, started, ended)['K1'] is True


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
    for minute in MINUTES:
        cube.build(minute, provisional=True)
    corpus: dict[str, dict[str, object]] = {'T1': {'t1': '2021-01-01T00:57:11.25Z', 'tR': 225}, 'T2': {}}
    run = tmp_path / 'run'
    stages = (('A', 1, 1), ('B', 2, 1), ('C', 1, 2))
    with caplog.at_level(logging.INFO):
        bench.client(run, service.store.root, service.url, corpus=corpus, stages=stages, pairs=1, finest='T2', numeric=('T1', 'T2'))
    service.api.tick(datetime.now(UTC))
    statement = (run / 'reference.sql').read_text().split(';\n', 1)[1].rstrip().removesuffix(';')
    reference = _arrow(statement.replace('min_bytes_to_use_direct_io = 1', 'min_bytes_to_use_direct_io = 0'))
    (run / 'evidence.jsonl').write_text(_evidence((run / 'evidence.sql').read_text()))
    meta, samples, evidence = bench.load(run)
    assert set(evidence) == {'window', 'statement', 'parts', 'table', 'receipt'}
    assert {row['name'] for row in evidence['table']} >= {
        'binance_spot_trades_market_state_revisions', 'binance_spot_trades_market_state_latest_revisions'
    }
    report = bench.judge(
        run.name, meta, samples, evidence,
        numeric=bench.numeric_checks(meta, samples, service.store.root, service.url, reference),
        timings=bench.phases(record.getMessage() for record in caplog.records),
        # The host facts the runbook records: both containers started before the run.
        hosts=({}, {'clickhouse_started': ['2021-01-01T00:00:00Z'], 'market_state_started': ['2021-01-01T00:00:00Z']}),
        staging=sorted(path.name for path in (service.store.root / 'staging').iterdir()),
        materializations=[metadata for asset, metadata in service.reporter.calls if asset == 'market_state_query_service'],
        extras={
            'backfill': {},
            'compression': bench.compression(next(s['cells_path'] for s in reversed(samples) if s['case'] == 'T2')),
            'results_volume': bench._inventory(service.store.root),
        },
    )
    criteria = report['criteria']
    assert isinstance(criteria, dict)
    passed = {key: value['passed'] for key, value in criteria.items()}
    # This ClickHouse runs no live feeds, so only the contention criteria have nothing to judge.
    assert passed == {
        'P0': True, 'Q1': True, 'Q2': True, 'Q3': True, 'V': True, 'N1': True, 'N2': True, 'N3': True, 'N4': True,
        'R1': passed['R1'], 'R2': True, 'R3': True, 'K1': False, 'K2': False, 'O1': True,
    }
    assert report['verdict'] == ('FAIL K1 K2' if passed['R1'] else 'FAIL R1 K1 K2')
    assert criteria['R1']['rss_peak_bytes'] > 0 and criteria['R2']['statements'] == 4 * len(samples)
    assert criteria['O1']['successful_requests'] == len(samples) == 2 * 5 + 2
    assert [row['statement'] for row in report['reference_cost']] == ['reference']
    cases_ = report['cases']
    assert isinstance(cases_, dict) and set(cases_) == {'T1', 'T2'}
    assert all(set(row['phases_p50_ms']) >= {'pin_ms', 'sql_ms', 'write_ms', 'total_ms'} for row in cases_.values())
    compressed = report['compression']
    assert isinstance(compressed, dict) and set(compressed) == {'uncompressed', 'zstd', 'lz4'}
    volume = report['results_volume']
    assert isinstance(volume, dict) and volume['results'] >= len(samples) and volume['staging'] == []
    rendered = bench.markdown(report)
    assert '**Verdict: FAIL' in rendered and rendered.count('| B | 0 |') == 2 * len(corpus)
