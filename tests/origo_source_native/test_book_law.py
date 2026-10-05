from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from origo import law
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.book_spool import Market
from origo.sources.adapters.book_vendor import CryptoHFTBookHourly, hour_partition
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.contracts import Partition, Revision
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.law_catalog import build_catalog

from .test_book_sources import book_runtime as book_runtime, _keys

from .test_book_vendor import (
    REAL_HOUR,
    original_vendor_archive as original_vendor_archive,
    original_vendor_revision as original_vendor_revision,
)


class RecordedVendorHour(CryptoHFTBookHourly):
    def __init__(self, market: str, revision: Revision) -> None:
        super().__init__(cast(Market, market))
        object.__setattr__(self, 'recorded_revision', revision)

    recorded_revision: Revision

    def discover(self, partition: Partition) -> str:
        assert partition.key == REAL_HOUR
        return self.recorded_revision.key

    def fetch(self, partition: Partition) -> Revision:
        assert partition.key == REAL_HOUR
        return self.recorded_revision

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        assert partition.key == REAL_HOUR and revision.key == self.recorded_revision.key


@pytest.fixture()
def authoritative_book_runtime(
    original_vendor_revision: tuple[str, Revision], origo_test_env: dict[str, str], tmp_path: Path
) -> Iterator[SourceRuntime]:
    market, revision = original_vendor_revision
    declared = BINANCE_SPOT_BOOK_SPEC if market == 'spot' else BINANCE_PERP_BOOK_SPEC
    spec = replace(declared, canonical=RecordedVendorHour(market, revision))
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        yield runtime
    finally:
        client.disconnect()


def _predicate(report: law.LawReport, source: str, key: law.LawPredicate) -> law.PredicateReport:
    return next(feed for feed in report['feeds'] if feed['source_key'] == source)['predicates'][key]


def test_canary_books_have_the_same_reader_law_contract(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    now = hour_partition(REAL_HOUR).end + timedelta(seconds=30)
    empty = law.evaluate(runtime.store.client, 'origo', now)
    assert _predicate(empty, runtime.spec.key, 'R1')['reason'] == 'reader_empty'
    assert _predicate(empty, runtime.spec.key, 'C1')['status'] == 'NOT_DUE'
    runtime.build(REAL_HOUR)
    report = law.evaluate(runtime.store.client, 'origo', now)
    assert runtime.spec.rollout_stage.value == 'CANARY'
    assert {
        key
        for key in next(f for f in report['feeds'] if f['source_key'] == runtime.spec.key)[
            'predicates'
        ]
    } == {'R1', 'C1', 'C2'}
    r1 = _predicate(report, runtime.spec.key, 'R1')
    assert r1['status'] == 'PASS' and r1['reason'] == 'reader_current'
    assert [r1['evidence']['rows_' + key] for key in BOOK_COMPONENT_KEYS] == [600, 60, 1, 1]
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'PASS'
    c2 = _predicate(report, runtime.spec.key, 'C2')
    assert (
        c2['status'] == 'FAIL'
        and c2['evidence']['expected_hours'] == int((hour_partition(REAL_HOUR).start - runtime.spec.partitions.start) / timedelta(hours=1))
        and c2['evidence']['missing_hours'] == c2['evidence']['expected_hours']
    )
    projections = [p for p in report['projections'] if p['id'].startswith(runtime.spec.key + ':')]
    assert len(projections) == 8
    assert all(
        p['status'] == 'CURRENT' and p['evidence_id']
        for p in projections
        if not p['id'].endswith('_latest')
    )
    assert all(p['status'] == 'UNKNOWN' for p in projections if p['id'].endswith('_latest'))


def test_hourly_authority_deadline_and_history_are_independent_of_reader_liveness(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    now = hour_partition(REAL_HOUR).end + timedelta(minutes=21)
    report = law.evaluate(runtime.store.client, 'origo', now)
    c1 = _predicate(report, runtime.spec.key, 'C1')
    assert c1['status'] == 'FAIL' and c1['reason'] == 'canonical_hour_missing'
    assert c1['evidence']['hour'] == REAL_HOUR
    assert c1['evidence']['deadline'] == '2026-10-04T10:20:00+00:00'
    assert _predicate(report, runtime.spec.key, 'C2')['status'] == 'FAIL'


def test_hourly_authority_uses_exact_source_component_proofs(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    record = runtime.build(REAL_HOUR)
    runtime.store.execute(
        "ALTER TABLE origo.source_component_log DELETE WHERE source_key=%(source)s AND component='depth200_1m' AND build_id=%(build)s SETTINGS mutations_sync=1",
        {'source': runtime.spec.key, 'build': record.build_id},
    )
    report = law.evaluate(
        runtime.store.client, 'origo', record.partition.end + timedelta(seconds=30)
    )
    assert _predicate(report, runtime.spec.key, 'R1')['status'] == 'UNKNOWN'
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'UNKNOWN'


def test_book_sources_are_required_hourly_reader_laws() -> None:
    catalog = build_catalog('6ac86db9fbe11e53e0fa3fb4470fc7fd8f78bb70')
    gates = {g['id']: g for g in catalog['gates']}
    for source in ('binance_spot_book', 'binance_perp_book'):
        for predicate in ('R1', 'C1', 'C2'):
            key = f'law.{predicate}:{source}'
            assert key in catalog['law_gate_ids'] and gates[key]['cadence'] == 'periodic'
        assert gates[f'law.C1:{source}']['thresholds'] == {
            'delivery_grace_seconds': 1200,
            'activation_grace_seconds': 300,
            'availability_max_age_seconds': 1200,
            'canonical_interval': 'hour',
        }


@pytest.mark.parametrize('key', ['2026-10-03', '2026-10-04', '2026-10-05'])
def test_hourly_audit_preserves_obsolete_requests_and_retries_hours(
    authoritative_book_runtime: SourceRuntime,
    key: str,
) -> None:
    runtime = authoritative_book_runtime
    requested = datetime.now(UTC)
    runtime.store.execute(
        'INSERT INTO origo.source_discovery_log VALUES',
        [(runtime.spec.key, key, requested - timedelta(minutes=1)),
         (runtime.spec.key, REAL_HOUR, requested)],
    )
    runtime.failures.record(
        operation='discovery', partition=key, error_code='BOOK_MINUTES_MISSING', scope='PARTITION'
    )
    assert runtime.audit() == (REAL_HOUR,)
    assert runtime.audit() == (REAL_HOUR,)
    assert runtime.store.execute(
        'SELECT event_type FROM origo.source_failure_log WHERE source_key=%(source)s AND partition_key=%(key)s ORDER BY event_time',
        {'source': runtime.spec.key, 'key': key},
    ) == [('FAILED',), ('RECOVERED',)]
    assert runtime.store.execute(
        'SELECT partition_key FROM origo.source_discovery_log ORDER BY partition_key'
    ) == [(value,) for value in sorted((key, REAL_HOUR))]
    assert runtime.store.records(canonical_only=True) == ()


def test_hourly_schedule_has_an_activation_window_before_c1_deadline(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    partition = hour_partition(REAL_HOUR)
    launch = partition.end + timedelta(
        minutes=int(runtime.spec.orchestration.canonical_cron.split()[0])
    )
    assert runtime.spec.canonical.candidate(launch) == partition
    pending = law.evaluate(runtime.store.client, 'origo', launch)
    c1 = _predicate(pending, runtime.spec.key, 'C1')
    assert c1['status'] == 'NOT_DUE'
    deadline = datetime.fromisoformat(str(c1['evidence']['deadline']))
    assert deadline - launch == timedelta(minutes=5)
    processing = launch + timedelta(minutes=1)
    assert law._c1([], runtime.spec.key, processing)['status'] == 'NOT_DUE'
    assert law._c1([], runtime.spec.key, deadline)['status'] == 'FAIL'
    runtime.build(REAL_HOUR)
    ready = law.evaluate(runtime.store.client, 'origo', processing)
    assert _predicate(ready, runtime.spec.key, 'C1')['status'] == 'PASS'


def test_native_hourly_backfill_orders_completion_after_canonical_build() -> None:
    from dagster import AssetKey, Definitions
    from origo.sources.bundle import build_source_bundle

    for spec in (BINANCE_SPOT_BOOK_SPEC, BINANCE_PERP_BOOK_SPEC):
        bundle = build_source_bundle(spec)
        defs = Definitions(
            assets=bundle.assets,
            jobs=bundle.jobs,
            schedules=bundle.schedules,
            sensors=bundle.sensors,
        )
        job = defs.get_job_def(f'backfill_{spec.key}_source_job')
        reconcile = job.asset_layer.asset_graph.get(AssetKey(f'reconcile_{spec.key}_source_origo'))
        assert AssetKey(f'build_{spec.key}_canonical_revision_origo') in reconcile.parent_keys


def test_fresh_book_minute_does_not_hide_a_provisional_gap(book_runtime: SourceRuntime) -> None:
    runtime = book_runtime
    record = runtime.build(_keys(runtime)[-1], provisional=True)
    now = record.partition.end + timedelta(seconds=30)
    query = law._Queries(runtime.store.client)
    edges = law._edge(query, 'origo', runtime.spec.key, now)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(record.build_id))
    result = law._r1(query, 'origo', runtime.spec, now, edges, proofs)
    assert result['status'] == 'FAIL' and result['reason'] == 'reader_book_minutes_missing'
    assert result['evidence']['age_seconds'] == 30
    assert result['evidence']['covered_minutes'] == 1
    missing = result['evidence']['missing_minutes']
    assert isinstance(missing, int) and missing > 0
    assert [result['evidence']['rows_' + key] for key in BOOK_COMPONENT_KEYS] == [600, 60, 1, 1]


def test_book_tail_proofs_include_distinct_minute_builds(book_runtime: SourceRuntime) -> None:
    runtime = book_runtime
    keys = _keys(runtime)
    first = runtime.build(keys[0], provisional=True)
    last = runtime.build(keys[-1], provisional=True)
    assert first.build_id != last.build_id
    query = law._Queries(runtime.store.client)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(last.build_id), first.partition.start)
    assert {proof.build for proof in proofs if proof.provisional} == {str(first.build_id), str(last.build_id)}
    now = last.partition.end + timedelta(seconds=30)
    result = law._r1(query, 'origo', runtime.spec, now, law._edge(query, 'origo', runtime.spec.key, now), proofs)
    assert result['evidence']['covered_minutes'] == 2


def test_hourly_authority_replaces_overlapping_provisional_book_rows(
    authoritative_book_runtime: SourceRuntime,
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.adapters.book_vendor import replay_hour

    runtime = authoritative_book_runtime
    market, body, _ = original_vendor_archive
    archive = tmp_path / 'original.parquet'
    archive.write_bytes(body)
    spool = tmp_path / 'spool'
    partition = hour_partition(REAL_HOUR)
    replay_hour(archive, market, partition, spool)
    monkeypatch.setenv('ORIGO_BOOK_SPOOL_ROOT', str(spool))
    minute = runtime.build(partition.start.strftime('%Y-%m-%dT%H:%M:%SZ'), provisional=True)
    prefix = 'origo.' + runtime.spec.names.prefix
    assert runtime.store.execute(f'SELECT count() FROM {prefix}_depth20_current') == [(600,)]
    assert runtime.store.execute(f'SELECT count() FROM {prefix}_depth20_latest_current') == [(600,)]
    canonical = runtime.build(REAL_HOUR)
    assert runtime.store.records() == (canonical,)
    assert runtime.store.execute(f'SELECT count() FROM {prefix}_depth20_current') == [(36000,)]
    assert runtime.store.execute(f'SELECT count() FROM {prefix}_depth20_latest_current') == [(0,)]
    assert runtime.store.execute(
        f'SELECT count() FROM {prefix}_depth20_latest_revisions WHERE build_id=%(build)s',
        {'build': minute.build_id},
    ) == [(600,)]
    assert runtime.build(REAL_HOUR) == canonical


def test_provider_delay_and_activation_use_native_evidence(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources import lifecycle
    from origo.sources.contracts import ArchiveNotPublishedYet

    runtime = authoritative_book_runtime
    partition = hour_partition(REAL_HOUR)
    now = [partition.end + timedelta(minutes=30)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return now[0]

    monkeypatch.setattr(lifecycle, 'datetime', Clock)
    available = [False]
    original = RecordedVendorHour.discover

    def discover(self: RecordedVendorHour, requested: Partition) -> str:
        if not available[0]:
            raise ArchiveNotPublishedYet('Actual vendor response controlled as unavailable.')
        return original(self, requested)

    monkeypatch.setattr(RecordedVendorHour, 'discover', discover)
    with pytest.raises(ArchiveNotPublishedYet):
        runtime.discover(partition)
    assert runtime.store.execute("SELECT count() FROM origo.source_failure_log WHERE event_type='FAILED'") == [(0,)]
    waiting = _predicate(law.evaluate(runtime.store.client, 'origo', now[0] + timedelta(minutes=2)), runtime.spec.key, 'C1')
    assert waiting['status'] == 'NOT_DUE' and waiting['reason'] == 'archive_not_published'
    assert waiting['evidence']['provider_available'] is False
    stale = _predicate(law.evaluate(runtime.store.client, 'origo', now[0] + timedelta(minutes=21)), runtime.spec.key, 'C1')
    assert stale['status'] == 'UNKNOWN' and stale['reason'] == 'archive_availability_stale'
    available[0] = True
    now[0] = partition.end + timedelta(minutes=45)
    runtime.discover(partition)
    pending = _predicate(law.evaluate(runtime.store.client, 'origo', now[0] + timedelta(minutes=1)), runtime.spec.key, 'C1')
    assert pending['status'] == 'NOT_DUE' and pending['reason'] == 'archive_activation_pending'
    assert pending['evidence']['deadline'] == (now[0] + timedelta(minutes=5)).isoformat()
    late = _predicate(law.evaluate(runtime.store.client, 'origo', now[0] + timedelta(minutes=5)), runtime.spec.key, 'C1')
    assert late['status'] == 'FAIL' and late['reason'] == 'canonical_hour_missing'
    now[0] += timedelta(minutes=6)
    runtime.build(REAL_HOUR)
    report = law.evaluate(runtime.store.client, 'origo', now[0])
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'PASS'
    assert _predicate(report, runtime.spec.key, 'R1')['status'] == 'FAIL'
    assert _predicate(report, runtime.spec.key, 'C2')['status'] == 'FAIL'


def test_hourly_audit_admits_latest_hour_before_old_pending_requests(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources import lifecycle
    from origo.sources.contracts import SourceError

    runtime = authoritative_book_runtime
    now = hour_partition(REAL_HOUR).end + timedelta(minutes=30)

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return now

    monkeypatch.setattr(lifecycle, 'datetime', Clock)
    keys = [f'2025-06-29T{hour:02d}Z' for hour in range(6)] + [REAL_HOUR]
    runtime.store.execute('INSERT INTO origo.source_discovery_log VALUES',
                          [(runtime.spec.key, key, now - timedelta(days=1)) for key in keys])
    visited: list[str] = []
    original = RecordedVendorHour.discover

    def discover(self: RecordedVendorHour, partition: Partition) -> str:
        visited.append(partition.key)
        if partition.key != REAL_HOUR:
            raise SourceError('PROVIDER_HTTP_404', 'Controlled missing historical provider file.')
        return original(self, partition)

    monkeypatch.setattr(RecordedVendorHour, 'discover', discover)
    assert runtime.audit() == (REAL_HOUR,)
    assert visited[0] == REAL_HOUR and len(visited) == 5


def test_declared_history_migration_preserves_existing_generation(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    record = runtime.build(REAL_HOUR)
    previous = runtime.spec.partitions.previous_starts[0]
    runtime.store.execute(
        "ALTER TABLE origo.source_anchor_log UPDATE anchor=%(anchor)s WHERE source_key=%(source)s SETTINGS mutations_sync=1",
        {'anchor': previous, 'source': runtime.spec.key},
    )
    runtime.setup()
    runtime.setup()
    assert runtime.store.record(record.partition) == record
    assert runtime.store.anchor() == runtime.spec.partitions.start
    evidence = runtime.store.execute(
        "SELECT evidence_json FROM origo.source_observation_log WHERE source_key=%(source)s AND partition_key=''",
        {'source': runtime.spec.key},
    )
    import json

    assert len(evidence) == 1
    assert json.loads(str(evidence[0][0])) == {
        'previous_anchor': previous.isoformat(), 'anchor': runtime.spec.partitions.start.isoformat(),
    }
    runtime.store.execute(
        "ALTER TABLE origo.source_anchor_log UPDATE anchor=%(anchor)s WHERE source_key=%(source)s SETTINGS mutations_sync=1",
        {'anchor': previous - timedelta(hours=1), 'source': runtime.spec.key},
    )
    with pytest.raises(RuntimeError, match='immutable'):
        runtime.setup()
    assert runtime.store.record(record.partition) == record


def test_book_reconciliation_uses_native_hourly_status(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster import DagsterInstance, Definitions, HourlyPartitionsDefinition, build_sensor_context
    from origo.sources.bundle import build_source_bundle

    runtime = authoritative_book_runtime
    record = runtime.build(REAL_HOUR)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
    bundle = build_source_bundle(runtime.spec)
    definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors, schedules=bundle.schedules)
    seen: list[object] = []
    original = DagsterInstance.get_status_by_partition

    def status(self: DagsterInstance, asset_key: object, partition_keys: object, partitions_def: object) -> object:
        seen.append(partitions_def)
        return original(self, asset_key, partition_keys, partitions_def)

    monkeypatch.setattr(DagsterInstance, 'get_status_by_partition', status)
    sensor = next(sensor for sensor in bundle.sensors if sensor.name == f'{runtime.spec.key}_reconciliation_sensor')
    with DagsterInstance.ephemeral() as instance:
        context = build_sensor_context(instance=instance, repository_def=definitions.get_repository_def())
        tick = sensor.evaluate_tick(context)
    assert len(seen) == 1 and isinstance(seen[0], HourlyPartitionsDefinition)
    assert seen[0].start == runtime.spec.partitions.start
    assert any(request.partition_key == record.partition.key for request in tick.run_requests)
