from __future__ import annotations

import json
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import cast
from uuid import uuid4

import pytest

from origo import law
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters.binance_spot_rest import BinanceSpotProvisional
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Client, Partition, Revision, Row
from origo.sources.hashing import content_hash
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES, archive_response

START = datetime(2025, 1, 1, tzinfo=UTC)
SOURCE = 'binance_spot_trades'
CAPTURED = tuple(daily.spot_csv_rows(
    (ARCHIVES / 'BTCUSDT-trades-2025-01-01.csv').read_bytes(),
    daily.BinanceSpotDaily().partition('2025-01-01'),
))


class CapturedMinutes(BinanceSpotProvisional):
    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        rows = tuple(row for row in CAPTURED if partition.start <= cast(datetime, row[-1]) < partition.end)
        assert rows and partition.end <= START + timedelta(minutes=10)
        digest = content_hash(rows, schema_version=1)
        return Revision(digest, digest, '{}', len(rows), lambda: iter(rows))


@dataclass
class LawCase:
    client: Client
    runtime: SourceRuntime

    def minute(self, offset: int) -> None:
        self.runtime.build((START + timedelta(minutes=offset)).strftime('%Y-%m-%dT%H:%M:%SZ'), provisional=True)

    def report(self, now: datetime) -> law.LawReport:
        return law.evaluate(self.client, 'origo', now)


def _case(tmp_path: Path, anchor: datetime | None) -> LawCase:
    client = make_clickhouse_client(get_clickhouse_settings())
    spec = replace(BINANCE_SPOT_TRADES_SPEC, provisional=CapturedMinutes())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4()))
    runtime.setup(anchor=anchor)
    return LawCase(client, runtime)


@pytest.fixture()
def law_case(origo_test_env: dict[str, str], tmp_path: Path) -> Iterator[LawCase]:
    case = _case(tmp_path, START)
    try:
        yield case
    finally:
        case.client.disconnect()


@pytest.fixture()
def canonical_case(origo_test_env: dict[str, str], tmp_path: Path,
                   monkeypatch: pytest.MonkeyPatch) -> Iterator[LawCase]:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    case = _case(tmp_path, None)
    try:
        yield case
    finally:
        case.client.disconnect()


@pytest.fixture()
def real_law_report(law_case: LawCase) -> law.LawReport:
    law_case.minute(0)
    return law_case.report(START + timedelta(minutes=1, seconds=5))


def predicate(report: law.LawReport, key: law.LawPredicate, source: str = SOURCE) -> law.PredicateReport:
    return next(feed for feed in report['feeds'] if feed['source_key'] == source)['predicates'][key]


def test_law_inventory_status_and_schema_contract(real_law_report: law.LawReport,
                                                  law_case: LawCase,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    report = real_law_report
    assert report['schema_version'] == 1 and report['application_version']
    assert report['inventory'] == list(law.LAW_INVENTORY)
    assert {feed['source_key'] for feed in report['feeds']} == set(law.LAW_INVENTORY)
    assert report['sampling_slot'] == '2025-01-01T00:01:00+00:00'
    assert predicate(report, 'R1')['status'] == 'PASS'
    assert report['status'] == 'FAIL'  # Other registered readers are genuinely empty.
    assert json.loads(json.dumps(report)) == report
    monkeypatch.setattr(law, 'SOURCE_REGISTRY', ())
    missing = law_case.report(START)
    assert predicate(missing, 'inventory')['status'] == 'UNKNOWN'
    added = replace(BINANCE_SPOT_TRADES_SPEC, key='unmapped_live_source')
    monkeypatch.setattr(law, 'SOURCE_REGISTRY', (*law.SOURCE_REGISTRY, added))
    assert predicate(law_case.report(START), 'inventory', added.key)['reason'] == 'inventory_uncovered'


def test_real_reader_gap_ignores_later_stored_minutes(law_case: LawCase) -> None:
    law_case.minute(0)
    law_case.minute(2)
    assert law_case.client.execute('SELECT count() FROM origo.binance_spot_trades_raw_latest_revisions') == [(3726,)]
    assert law_case.client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [(2631,)]
    now = START + timedelta(minutes=4, seconds=1)
    report = law_case.report(now)
    r1 = predicate(report, 'R1')
    assert r1['status'] == 'FAIL' and r1['reason'] == 'reader_stale'
    assert r1['evidence']['reader_end'] == (START + timedelta(minutes=1)).isoformat()
    assert r1['evidence']['row_count'] == 2631
    for component in law.PROVISIONAL_COMPONENTS:
        observation = next(p for p in report['projections'] if p['id'] == f'{SOURCE}:{component}')
        assert observation['status'] == 'STALE'
        assert observation['data_through'] == r1['evidence']['reader_end']
        assert str(observation['evidence_id']).startswith(str(r1['evidence']['build_id']) + ':')
    law_case.minute(1)
    repaired = law_case.report(now)
    assert predicate(repaired, 'R1')['status'] == 'PASS'
    for component in law.PROVISIONAL_COMPONENTS:
        observation = next(p for p in repaired['projections'] if p['id'] == f'{SOURCE}:{component}')
        assert observation['status'] == 'CURRENT'
        assert observation['data_through'] == (START + timedelta(minutes=3)).isoformat()


def test_real_canonical_hole_remains_failed_with_fresh_reader_tail(canonical_case: LawCase) -> None:
    canonical_case.runtime.build('2017-08-17')
    canonical_case.runtime.build('2020-01-01')
    report = canonical_case.report(datetime(2020, 1, 2, 0, 0, 5, tzinfo=UTC))
    assert predicate(report, 'R1')['status'] == 'PASS'
    c2 = predicate(report, 'C2')
    assert c2['status'] == 'FAIL'
    assert c2['evidence']['first_invalid_day'] == '2017-08-18'
    assert c2['evidence']['valid_days'] == 1
    assert predicate(report, 'C1')['status'] == 'PASS'


@pytest.mark.parametrize('component', ['time', 'dollar', 'volume', 'tick', 'imbalance', 'aligned'])
def test_canonical_law_rejects_empty_required_component_proofs(canonical_case: LawCase, component: str) -> None:
    canonical_case.runtime.build('2017-08-17')
    yesterday = datetime(2017, 8, 18, 5, tzinfo=UTC)
    older = yesterday + timedelta(days=1)
    assert predicate(canonical_case.report(yesterday), 'C1')['status'] == 'PASS'
    assert predicate(canonical_case.report(older), 'C2')['status'] == 'PASS'
    canonical_case.client.execute(
        'ALTER TABLE origo.source_component_log UPDATE row_count=0 WHERE component=%(component)s',
        {'component': component}, settings={'mutations_sync': 2},
    )
    c1 = predicate(canonical_case.report(yesterday), 'C1')
    assert c1['status'] == 'FAIL' and c1['reason'] == 'component_proof_empty'
    assert c1['evidence']['empty_component'] == component
    assert c1['evidence']['raw_proof_rows'] == 3427
    c2 = predicate(canonical_case.report(older), 'C2')
    assert c2['status'] == 'FAIL' and c2['reason'] == 'component_proof_empty'
    assert c2['evidence']['first_invalid_day'] == '2017-08-17'


@pytest.mark.parametrize('damage', ['duplicate', 'inactive_identity', 'wrong_hash', 'missing_component'])
def test_active_proofs_require_identity_hashes_and_distinct_components(law_case: LawCase, damage: str) -> None:
    law_case.minute(0)
    table = 'origo.source_component_log'
    assert predicate(law_case.report(START + timedelta(minutes=1)), 'R1')['status'] == 'PASS'
    if damage == 'duplicate':
        law_case.client.execute(f"INSERT INTO {table} SELECT * FROM {table} WHERE component='raw_latest'")
    elif damage == 'inactive_identity':
        inactive = uuid4()
        law_case.client.execute(f"INSERT INTO {table} SELECT * REPLACE (%(id)s AS build_id) FROM {table} WHERE component='raw_latest'", {'id': inactive})
        law_case.client.execute(f"ALTER TABLE {table} DELETE WHERE component='raw_latest' AND build_id!=%(id)s", {'id': inactive}, settings={'mutations_sync': 2})
    elif damage == 'wrong_hash':
        law_case.client.execute(f"ALTER TABLE {table} UPDATE content_hash='corrupted-proof' WHERE component='raw_latest'",
                               settings={'mutations_sync': 2})
    else:
        law_case.client.execute(f"ALTER TABLE {table} DELETE WHERE component='time_latest'", settings={'mutations_sync': 2})
    assert predicate(law_case.report(START + timedelta(minutes=1)), 'R1')['status'] == 'UNKNOWN'


def test_r1_counts_selected_minute_rows_from_active_build(law_case: LawCase) -> None:
    law_case.minute(0)
    table = 'origo.binance_spot_trades_raw_latest_revisions'
    # Duplicate only the unchanged captured rows under an inactive build identity.
    law_case.client.execute(f'INSERT INTO {table} SELECT * REPLACE (%(id)s AS build_id) FROM {table}', {'id': uuid4()})
    report = law_case.report(START + timedelta(minutes=1, seconds=5))
    r1 = predicate(report, 'R1')
    assert r1['status'] == 'PASS' and r1['evidence']['row_count'] == 2631
    law_case.client.execute(f'ALTER TABLE {table} DELETE WHERE build_id=%(id)s',
        {'id': r1['evidence']['build_id']}, settings={'mutations_sync': 2})
    empty = law_case.report(START + timedelta(minutes=1))
    assert predicate(empty, 'R1')['reason'] == 'reader_minute_empty'
    assert next(p for p in empty['projections'] if p['id'] == f'{SOURCE}:raw_latest')['status'] == 'FAILED'


def test_r1_budgets_use_latest_fully_covered_closed_minute(law_case: LawCase) -> None:
    law_case.minute(0)
    now = START + timedelta(minutes=4)
    r1 = predicate(law_case.report(now), 'R1')
    assert r1['status'] == 'PASS'
    assert r1['evidence']['age_seconds'] == 180 and r1['evidence']['budget_seconds'] == 180
    assert r1['evidence']['selected_minute'] == START.isoformat()
    assert predicate(law_case.report(now + timedelta(seconds=1)), 'R1')['status'] == 'FAIL'
    assert law.R1_PERP_BUDGET_SECONDS == 300
    # An active but not-yet-closed minute is not selected as an empty current minute.
    assert predicate(law_case.report(START + timedelta(seconds=59)), 'R1')['reason'] == 'reader_empty'


def test_c1_deadline_and_early_arrival_are_independent_of_r1(canonical_case: LawCase) -> None:
    before = datetime(2017, 8, 18, 4, 29, 59, tzinfo=UTC)
    assert predicate(canonical_case.report(before), 'C1')['status'] == 'NOT_DUE'
    assert predicate(canonical_case.report(before + timedelta(seconds=1)), 'C1')['status'] == 'FAIL'
    canonical_case.runtime.build('2017-08-17')
    report = canonical_case.report(before)
    assert predicate(report, 'C1')['status'] == 'PASS'
    assert predicate(report, 'C1')['evidence']['raw_proof_rows'] == 3427
    assert predicate(report, 'R1')['status'] == 'FAIL'
    perp = 'binance_perp_trades'
    assert predicate(canonical_case.report(before.replace(hour=10)), 'C1', perp)['status'] == 'NOT_DUE'
    assert predicate(canonical_case.report(before.replace(hour=10, minute=30)), 'C1', perp)['status'] == 'FAIL'


def test_c2_checks_frozen_older_calendar_throughout_day(canonical_case: LawCase) -> None:
    canonical_case.runtime.build('2017-08-17')
    now = datetime(2017, 8, 19, tzinfo=UTC)
    for stamp in (now, now.replace(hour=23, minute=59)):
        c2 = predicate(canonical_case.report(stamp), 'C2')
        assert c2['status'] == 'PASS'
        assert c2['evidence']['expected_days'] == c2['evidence']['valid_days'] == 1
    waiting = canonical_case.report(now)
    assert next(p for p in waiting['projections'] if p['id'] == f'{SOURCE}:time')['status'] == 'WAITING'
    later = canonical_case.report(now + timedelta(days=1))
    assert predicate(later, 'C2')['evidence']['first_invalid_day'] == '2017-08-18'
    assert predicate(later, 'C1')['status'] == 'NOT_DUE'


@pytest.fixture()
def captured_depth_minute(law_case: LawCase) -> tuple[str, datetime]:
    from origo.assets import create_binance_spot_depth200_1m_table_origo as projection
    from origo.assets import create_binance_spot_depth200_snapshots_table_origo as raw
    from origo.assets import refresh_binance_spot_depth200_1m_origo as refresh
    from origo.assets import sync_binance_spot_depth200_snapshots_to_origo as sync

    settings = get_clickhouse_settings()
    raw._create_snapshots_table(law_case.client, settings)
    projection._create_depth200_1m_table(law_case.client, settings)
    captured = Path(__file__).resolve().parents[1] / 'fixtures/binance/spot/depth200/history'
    rows = [sync._parse_snapshot_line(line) for line in captured.read_text().splitlines() if line.strip()]
    law_case.client.execute(f'INSERT INTO origo.{raw.SNAPSHOTS_TABLE_NAME} VALUES', rows)
    minute = rows[0][0].replace(tzinfo=UTC, second=0, microsecond=0)
    assert refresh.refresh_minute(law_case.client, 'origo', minute) == 1
    return projection.DEPTH200_1M_TABLE_NAME, minute


def test_depth_counts_distinct_closed_slots(
    law_case: LawCase, captured_depth_minute: tuple[str, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.law_catalog import build_catalog, gate_evaluation

    source, minute = captured_depth_minute
    due = minute + timedelta(minutes=1, seconds=law.D1_DELIVERY_GRACE_SECONDS)
    law_case.client.execute(f'INSERT INTO origo.{source} SELECT * FROM origo.{source}')
    report = law_case.report(due)
    assert predicate(report, 'D1', source)['evidence']['missing_slots'] == 1439
    assert predicate(law_case.report(due - timedelta(microseconds=1)), 'D1', source)['evidence']['missing_slots'] == 1440
    # Exercise the tolerance boundary using a smaller test window over the same real minute.
    monkeypatch.setattr(law, 'D1_EXPECTED_SLOTS', 3)
    before = law_case.report(due)
    before_catalog = build_catalog('')
    assert predicate(before, 'D1', source)['status'] == 'PASS'
    assert predicate(law_case.report(due + timedelta(minutes=3)), 'D1', source)['status'] == 'FAIL'
    assert predicate(before, 'D1', source)['evidence']['max_missing'] == 2
    monkeypatch.setattr(law, 'D1_MAX_MISSING_SLOTS', 3)
    after = law_case.report(due + timedelta(minutes=1))
    assert predicate(after, 'D1', source)['evidence']['max_missing'] == 3
    from origo.workers import law_page as page

    for record, catalog in ((before, before_catalog), (after, build_catalog(''))):
        record['catalog_version'] = catalog['version']
        descriptors = {gate['id']: gate for gate in catalog['gates']}
        for feed in record['feeds']:
            for name, result in feed['predicates'].items():
                identity = f"law.{name}:{feed['source_key']}"
                record['gates'].append(gate_evaluation(
                    descriptors[identity], evidence_id=f"{identity}:{record['sampling_slot']}",
                    evaluated_at=record['evaluation_start'],
                    outcome='EXPECTED_WAIT' if result['status'] == 'NOT_DUE' else result['status'],
                    evidence=result['evidence'], reason=result['reason'],
                ))
        record['gates'].append(gate_evaluation(
            descriptors['law.inventory'], evidence_id=record['sampling_slot'] + ':inventory',
            evaluated_at=record['evaluation_start'], outcome='PASS',
            evidence={'live': len(record['inventory'])}, reason='all_live_sources_evaluated',
        ))
    briefs = [page._sample_brief(page._decode(json.dumps(record).encode())) for record in (before, after)]
    assert all(brief['policy'] is not None for brief in briefs)
    assert briefs[0]['policy'] != briefs[1]['policy']
    # Protocol-only PASS envelopes test the window; actual incomplete-system reports remain FAIL.
    assert before['status'] == after['status'] == 'FAIL'
    window = page.consecutive_window([{**brief, 'status': 'PASS'} for brief in briefs], str(briefs[-1]['slot']))
    assert len(window) == 1


def test_depth_delivery_grace_excludes_inflight_slot_until_due(
    law_case: LawCase, captured_depth_minute: tuple[str, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, minute = captured_depth_minute
    monkeypatch.setattr(law, 'D1_EXPECTED_SLOTS', 1)
    monkeypatch.setattr(law, 'D1_MAX_MISSING_SLOTS', 0)
    due = minute + timedelta(minutes=1, seconds=law.D1_DELIVERY_GRACE_SECONDS)
    # The captured minute is present; the next closed minute has not been delivered.
    for now in (due, due + timedelta(seconds=59, microseconds=999999)):
        result = predicate(law_case.report(now), 'D1', source)
        assert result['status'] == 'PASS' and result['evidence']['missing_slots'] == 0
        assert result['evidence']['newest_minute'] == minute.isoformat()
        assert result['evidence']['window_end'] == (minute + timedelta(minutes=1)).isoformat()
        assert result['evidence']['delivery_grace_seconds'] == 60
    result = predicate(law_case.report(due + timedelta(minutes=1)), 'D1', source)
    assert result['status'] == 'FAIL' and result['evidence']['missing_slots'] == 1


class RecordingClient:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.calls: list[tuple[str, Mapping[str, object]]] = []
        self.marker = 'law_test_' + uuid4().hex

    def execute(self, query: str, params: object | None = None,
                settings: Mapping[str, object] | None = None) -> list[Row]:
        assert settings is not None
        self.calls.append((query, settings))
        return self.client.execute(query + f' /* {self.marker} */', params, settings=settings)

    def disconnect(self) -> None:
        self.client.disconnect()


def test_real_reader_queries_obey_cost_and_transport_bounds(law_case: LawCase,
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    law_case.minute(0)
    client = RecordingClient(law_case.client)
    started = time.monotonic()
    report = law.evaluate(client, 'origo', START + timedelta(minutes=1))
    assert predicate(report, 'R1')['status'] == 'PASS'
    assert time.monotonic() - started < 20
    assert all(s['max_memory_usage'] == 536870912 and s['max_threads'] == 1
               and 0 < cast(float, s['max_execution_time']) <= 5 for _, s in client.calls)
    raw_queries = [q for q, _ in client.calls if '_revisions' in q]
    assert raw_queries and all('source_date=%(date)s' in q and 'build_id=%(build)s' in q for q in raw_queries)
    assert all(q.lstrip().startswith(('SELECT', 'WITH')) for q, _ in client.calls)
    law_case.client.execute('SYSTEM FLUSH LOGS')
    costs = law_case.client.execute(
        "SELECT read_rows, memory_usage, query_duration_ms FROM system.query_log "
        "WHERE type='QueryFinish' AND position(query, %(marker)s)>0", {'marker': client.marker})
    assert costs and any(int(str(row[0])) > 0 for row in costs)
    assert all(int(str(row[1])) <= 536870912 and int(str(row[2])) < 5000 for row in costs)

    from clickhouse_driver.errors import ServerException

    from origo.workers.monitor import LawClient

    reader = LawClient(get_clickhouse_settings())
    actual = law.evaluate(reader, 'origo', START + timedelta(minutes=1))
    assert predicate(actual, 'R1')['status'] == 'PASS'
    assert reader.execute("SELECT getSetting('readonly'), getSetting('max_memory_usage'), getSetting('max_threads')", settings=law.LAW_QUERY_SETTINGS) == [(1, 536870912, 1)]
    with pytest.raises(ServerException, match=r'readonly|read-only|READONLY'):
        reader.execute('CREATE TABLE origo.forbidden_law_write (x UInt8) ENGINE=Memory', settings=law.LAW_QUERY_SETTINGS)
    for setting, value in (('max_memory_usage', 536870913), ('max_execution_time', 6), ('max_threads', 2), ('max_execution_time', 0), ('max_memory_usage', 0)):
        with pytest.raises(ServerException):
            reader.client.execute('SELECT 1', settings={**law.LAW_QUERY_SETTINGS, setting: value})
    assert reader.execute("SELECT getSetting('max_memory_usage')", settings={**law.LAW_QUERY_SETTINGS, 'max_memory_usage': 268435456}) == [(268435456,)]
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        reader.execute('SELECT sleep(0.5)', settings={**law.LAW_QUERY_SETTINGS, 'max_execution_time': 0.1})
    assert time.monotonic() - started < 1
    while law_case.client.execute("SELECT count() FROM system.processes WHERE user='law_reader'") != [(0,)]:
        assert time.monotonic() - started < 1
        time.sleep(0.01)
    assert reader.execute('SELECT 1', settings=law.LAW_QUERY_SETTINGS) == [(1,)]
    baseline_start = time.monotonic()
    law_case.minute(1)
    baseline_seconds = time.monotonic() - baseline_start
    ingestion_started = Event()

    def ingest() -> tuple[float, float]:
        ingestion_client = make_clickhouse_client(get_clickhouse_settings())
        runtime = SourceRuntime(
            law_case.runtime.spec, SourceStore(ingestion_client, 'origo', law_case.runtime.spec),
            law_case.runtime.lock_root, str(uuid4()),
        )
        start = time.monotonic()
        ingestion_started.set()
        try:
            runtime.build((START + timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ'), provisional=True)
            return start, time.monotonic()
        finally:
            ingestion_client.disconnect()

    concurrent_reader = RecordingClient(reader)
    with ThreadPoolExecutor(max_workers=1) as pool:
        built = pool.submit(ingest)
        assert ingestion_started.wait(5)
        observation_start = time.monotonic()
        observed = law.evaluate(concurrent_reader, 'origo', START + timedelta(minutes=3, seconds=5))
        observation_end = time.monotonic()
        ingestion_start, ingestion_end = built.result(timeout=10)
    assert observation_start < ingestion_end and ingestion_start < observation_end
    assert predicate(observed, 'R1')['status'] == 'PASS'
    assert law_case.client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [(4999,)]
    law_case.client.execute('SYSTEM FLUSH LOGS')
    concurrent_costs = law_case.client.execute(
        "SELECT read_rows, memory_usage, query_duration_ms FROM system.query_log "
        "WHERE type='QueryFinish' AND position(query, %(marker)s)>0", {'marker': concurrent_reader.marker})
    assert concurrent_costs and all(int(str(row[1])) <= 536870912 and int(str(row[2])) < 5000 for row in concurrent_costs)
    Path('/tmp/origo-law-cost-evidence.json').write_text(json.dumps({
        'environment': 'local Docker ClickHouse; unchanged captured spot rows; no production calls',
        'recorded_at': datetime.now(UTC).isoformat(),
        'baseline': {'minute': '2025-01-01T00:01:00Z', 'raw_rows': 1273, 'ingestion_seconds': baseline_seconds},
        'observer_on': {'minute': '2025-01-01T00:02:00Z', 'raw_rows': 1095,
                        'ingestion_seconds': ingestion_end - ingestion_start,
                        'evaluation_seconds': observation_end - observation_start,
                        'overlap_seconds': min(observation_end, ingestion_end) - max(observation_start, ingestion_start)},
        'query_fields': ['read_rows', 'memory_bytes', 'duration_ms'],
        'initial_query_costs': costs, 'concurrent_query_costs': concurrent_costs,
        'caveat': 'Adjacent minutes have different row counts; these timings establish bounded overlap and successful ingestion, not a throughput ratio.',
    }, indent=2) + '\n')
    # Deadline exhaustion forbids every subsequent database call.
    monkeypatch.setattr(law, 'LAW_EVALUATION_TIMEOUT_SECONDS', 0)
    client.calls.clear()
    timed = law.evaluate(client, 'origo', START)
    assert not client.calls and timed['status'] == 'UNKNOWN'
    assert all(p['reason'] == 'evaluation_timeout' for f in timed['feeds'] for p in f['predicates'].values())


def test_missing_evidence_is_unknown_and_errors_are_sanitized(law_case: LawCase) -> None:
    report = law.evaluate(law_case.client, 'missing_database', START)
    assert report['status'] == 'UNKNOWN'
    public = json.dumps(report)
    assert 'missing_database' not in public and 'SELECT' not in public and 'Traceback' not in public
    assert all(p['status'] == 'UNKNOWN' for feed in report['feeds'] for p in feed['predicates'].values())


def test_projection_status_requires_its_own_current_evidence(law_case: LawCase) -> None:
    law_case.minute(0)
    now = START + timedelta(minutes=1)
    before = law_case.report(now)
    assert next(p for p in before['projections'] if p['id'] == f'{SOURCE}:time_latest')['status'] == 'CURRENT'
    law_case.client.execute("ALTER TABLE origo.source_component_log DELETE WHERE component='time_latest'", settings={'mutations_sync': 2})
    projections = {p['id']: p for p in law_case.report(now)['projections']}
    assert projections[f'{SOURCE}:time_latest']['status'] == 'UNKNOWN'
    assert projections[f'{SOURCE}:raw_latest']['reason'] == 'validated_activation'
    assert projections[f'{SOURCE}:time']['status'] == 'UNKNOWN'


def test_new_depth_declaration_requires_explicit_law_coverage(
    law_case: LawCase, monkeypatch: pytest.MonkeyPatch,
) -> None:
    unsupported = replace(law.DEPTH_SPECS[0], projection_table_name='new_depth_1m')
    monkeypatch.setattr(law, 'DEPTH_SPECS', (*law.DEPTH_SPECS, unsupported))
    report = law_case.report(START)
    assert 'new_depth_1m' in report['inventory']
    feed = next(feed for feed in report['feeds'] if feed['source_key'] == 'new_depth_1m')
    assert feed['predicates']['inventory']['status'] == 'UNKNOWN'
    assert report['status'] != 'PASS'
