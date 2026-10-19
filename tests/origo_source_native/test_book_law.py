from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
import shutil
from typing import cast
from uuid import uuid4

import pytest

from origo import law
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.book_spool import Market
from origo.sources.adapters.book_vendor import CryptoHFTBookHourly, hour_partition
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.contracts import Partition, Revision, SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.law_catalog import build_catalog

from .test_book_history import (
    OriginalArchive, _transport, rejection_runtime as rejection_runtime,
    original_historical_archives as original_historical_archives,
)
from .test_book_sources import book_runtime as book_runtime, _keys

from .test_book_vendor import (
    REAL_HOUR,
    original_vendor_archive as original_vendor_archive,
    original_vendor_revision as original_vendor_revision,
    original_vendor_preparation as original_vendor_preparation,
    original_vendor_spool as original_vendor_spool,
    original_vendor_following_archive as original_vendor_following_archive,
    vendor_session_root as vendor_session_root,
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
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]],
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = authoritative_book_runtime
    partition = hour_partition(REAL_HOUR)
    now = partition.end + timedelta(seconds=30)
    empty = law.evaluate(runtime.store.client, 'origo', now)
    assert _predicate(empty, runtime.spec.key, 'R1')['reason'] == 'reader_empty'
    assert _predicate(empty, runtime.spec.key, 'C1')['status'] == 'NOT_DUE'
    overdue = law.evaluate(runtime.store.client, 'origo', partition.end + timedelta(hours=1, minutes=21))
    c1 = _predicate(overdue, runtime.spec.key, 'C1')
    assert c1['status'] == 'FAIL' and c1['reason'] == 'canonical_hour_missing'
    assert c1['evidence']['hour'] == REAL_HOUR
    assert c1['evidence']['deadline'] == '2026-10-04T11:20:00+00:00'
    assert c1['evidence']['following_hour'] == '2026-10-04T10Z'
    assert _predicate(overdue, runtime.spec.key, 'C2')['status'] == 'FAIL'
    # Reference the unchanged :20 delivery fallback, independently of schedule ticks.
    launch = partition.end + timedelta(hours=1, minutes=15)
    assert runtime.spec.canonical.candidate(launch) == partition
    pending = _predicate(law.evaluate(runtime.store.client, 'origo', launch), runtime.spec.key, 'C1')
    assert pending['status'] == 'NOT_DUE'
    deadline = datetime.fromisoformat(str(pending['evidence']['deadline']))
    assert deadline - launch == timedelta(minutes=5)
    processing = launch + timedelta(minutes=1)
    assert law._c1([], runtime.spec.key, processing)['status'] == 'NOT_DUE'
    assert law._c1([], runtime.spec.key, deadline)['status'] == 'FAIL'
    with monkeypatch.context() as patch:
        _assert_provider_delay_and_activation_use_native_evidence(runtime, patch)
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
    matured = law.evaluate(runtime.store.client, 'origo', now + timedelta(hours=1))
    assert _predicate(matured, runtime.spec.key, 'C1')['status'] == 'PASS'
    c2 = _predicate(matured, runtime.spec.key, 'C2')
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
    assert _predicate(law.evaluate(runtime.store.client, 'origo', processing), runtime.spec.key, 'C1')['status'] == 'PASS'
    _assert_book_reconciliation_uses_native_hourly_status(runtime, monkeypatch)
    with monkeypatch.context() as patch:
        _assert_real_canonical_discovery_and_admission(
            runtime, original_historical_archives,
            original_vendor_archive, original_vendor_following_archive, patch,
        )
    _assert_hourly_authority_uses_exact_source_component_proofs(runtime)
    _assert_declared_history_migration_preserves_existing_generation(runtime)


def _assert_real_canonical_discovery_and_admission(
    retained: SourceRuntime,
    originals: dict[Market, tuple[OriginalArchive, ...]],
    modern: tuple[Market, bytes, dict[str, str]],
    following: tuple[Market, bytes, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster import DagsterInstance, DagsterRunStatus, Definitions, RunRequest, build_schedule_context
    from dagster._core.remote_origin import RemoteJobOrigin
    from dagster._core.errors import ScheduleExecutionError
    from origo.sources import lifecycle
    from origo.sources.adapters import book_vendor as vendor
    from origo.sources.bundle import build_source_bundle
    from origo.sources.contracts import BuildContext
    from .test_revisioned_source_framework_backfill import _repository_origin

    assert isinstance(retained.spec.canonical, CryptoHFTBookHourly)
    market = retained.spec.canonical.market
    adapter = CryptoHFTBookHourly(market)
    spec = replace(retained.spec, canonical=adapter)
    runtime = replace(retained, spec=spec, store=SourceStore(retained.store.client, 'origo', spec))
    archives = originals[market]
    partition = archives[1].partition
    hour = partition.start + timedelta(hours=2)
    now = [hour + timedelta(minutes=31)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return now[0]

    monkeypatch.setattr(vendor, 'datetime', Clock)
    monkeypatch.setattr(lifecycle, 'datetime', Clock)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
    _, rewritten = _transport(archives, market, monkeypatch)
    transport, replay = vendor.get_response, vendor.replay_hour
    available, rejected = [False], [False]
    counts = {'metadata': 0, 'payload': 0, 'replay': 0}

    def response(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> vendor.Response:
        counts['metadata' if headers else 'payload'] += 1
        assert isinstance(params, dict)
        if not available[0] and params['file'] == vendor._file(market, partition):
            raise SourceError('PROVIDER_HTTP_404', 'Recorded transport: archive unavailable.')
        return transport(url, params=params, headers=headers, weight=weight)

    def replayed(path: Path, market: Market, partition: Partition, root: Path, *,
                 preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> Revision:
        counts['replay'] += 1
        if rejected[0]:
            raise SourceError('BOOK_EVENT_STALE', 'Replay verdict fault on unchanged original vendor bytes.')
        return replay(path, market, partition, root, preludes=preludes, following=following)

    monkeypatch.setattr(vendor, 'get_response', response)
    monkeypatch.setattr(vendor, 'replay_hour', replayed)
    bundle = build_source_bundle(spec)
    definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs, schedules=bundle.schedules)
    schedule = next(s for s in bundle.schedules if s.name.endswith('_canonical_schedule'))
    canonical = next(j for j in bundle.jobs if j.name == f'refresh_{spec.key}_canonical_source_job')
    backfill = next(j for j in bundle.jobs if j.name == f'backfill_{spec.key}_source_job')
    home = runtime.lock_root.parent / 'cadence-dagster'
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        def tick(scheduled: datetime) -> tuple[list[RunRequest], str | None]:
            with build_schedule_context(instance=instance, scheduled_execution_time=scheduled,
                                        repository_def=definitions.get_repository_def()) as context:
                result = schedule.evaluate_tick(context)
                return list(result.run_requests), result.skip_message

        params = {'source': spec.key, 'partition': partition.key}

        def observations() -> list[tuple[object, ...]]:
            return runtime.store.execute(
                'SELECT complete,observed_at FROM origo.source_observation_log '
                'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY observed_at', params,
            )

        # Real adapter404 is waiting only while this is the current candidate.
        assert tick(now[0])[0] == []
        assert observations() == [(0, now[0])]
        assert runtime.store.execute(
            "SELECT count() FROM origo.source_failure_log WHERE source_key=%(source)s "
            "AND partition_key=%(partition)s AND operation='discovery'", params,
        ) == [(0,)]

        # Audit can observe first; the offset request must keep that original deadline.
        available[0] = True
        now[0] = hour + timedelta(minutes=35, seconds=10)
        standalone = runtime.discover(partition)
        first_available = now[0]
        now[0] = hour + timedelta(minutes=36)
        first = tick(now[0])[0][0]
        assert first.partition_key == partition.key and standalone in str(first.run_key)
        if market == 'spot':
            # This original spot triplet has insufficient proven depth; never make it a positive fixture.
            with pytest.raises(SourceError) as failure:
                adapter.fetch(partition)
            assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
        else:
            adapter.fetch(partition)
        fetched_revision = adapter.discover(partition)
        assert fetched_revision != standalone

        # Real fetched prelude evidence changes the key while native work is outstanding.
        for job, status, partition_tags in (
            (canonical, DagsterRunStatus.QUEUED, {'dagster/partition': partition.key}),
            (canonical, DagsterRunStatus.STARTING, {'dagster/partition': partition.key}),
            (backfill, DagsterRunStatus.STARTED, {
                'dagster/asset_partition_range_start': partition.key,
                'dagster/asset_partition_range_end': partition.key,
            }),
        ):
            running = instance.create_run_for_job(
                job, run_config=first.run_config,
                tags={**first.tags, **partition_tags}, status=status,
                remote_job_origin=RemoteJobOrigin(_repository_origin(), job.name),
            )
            before = dict(counts)
            now[0] = hour + timedelta(minutes=41)
            assert tick(now[0])[0] == []
            assert counts['metadata'] > before['metadata']
            assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
            assert observations()[-1] == (1, now[0])
            c1 = _predicate(law.evaluate(runtime.store.client, 'origo', now[0]), spec.key, 'C1')
            assert c1['status'] == 'FAIL'
            assert c1['evidence']['deadline'] == (first_available + timedelta(minutes=5)).isoformat()
            instance.report_run_canceled(running)

        if market == 'spot':
            with pytest.raises(SourceError) as failure:
                runtime.build(partition.key)
            assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
            failures = runtime.store.execute(
                'SELECT event_id,event_type FROM origo.source_failure_log '
                'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY event_time,event_id', params,
            )
            before = dict(counts)
            now[0] = hour + timedelta(minutes=46)
            assert tick(now[0])[0] == []
            assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
            assert runtime.store.execute(
                'SELECT event_id,event_type FROM origo.source_failure_log '
                'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY event_time,event_id', params,
            ) == failures
            with pytest.raises(SourceError):
                runtime.build(partition.key)
            assert counts['replay'] > before['replay']  # Explicit retry may extend its prelude.

            # The positive spot case uses its genuinely accepted October archives through the real adapter.
            modern_partition = hour_partition(REAL_HOUR)
            visited, _ = _transport((
                OriginalArchive(modern_partition, modern[1], modern[2]),
                OriginalArchive(hour_partition('2026-10-04T10Z'), following[1], following[2]),
            ), market, monkeypatch)
            modern_spec = replace(retained.spec, canonical=CryptoHFTBookHourly(market))
            modern_bundle = build_source_bundle(modern_spec)
            modern_definitions = Definitions(assets=modern_bundle.assets, jobs=modern_bundle.jobs,
                                             schedules=modern_bundle.schedules)
            modern_schedule = next(s for s in modern_bundle.schedules if s.name.endswith('_canonical_schedule'))
            now[0] = modern_partition.start + timedelta(hours=2, minutes=56)
            accepted = retained.store.record(modern_partition)
            assert accepted is not None and len(accepted.component_hashes) == 4
            with build_schedule_context(instance=instance, scheduled_execution_time=now[0],
                                        repository_def=modern_definitions.get_repository_def()) as context:
                result = modern_schedule.evaluate_tick(context)
            assert not result.run_requests
            assert result.skip_message == 'This book revision already has complete active component evidence.'
            assert retained.store.record(modern_partition) == accepted and visited
            return

        # A post-fetch component failure must keep its native attempt budget across key enrichment.
        failed = instance.create_run_for_job(
            canonical, run_config=first.run_config,
            tags={**first.tags, 'dagster/partition': partition.key}, status=DagsterRunStatus.STARTED,
            remote_job_origin=RemoteJobOrigin(_repository_origin(), canonical.name),
        )

        def component_failure(context: BuildContext) -> None:
            raise OSError('Controlled component failure after real prelude fetch.')

        fault_spec = replace(spec, components=(
            replace(spec.components[0], build=component_failure), *spec.components[1:],
        ))
        fault_runtime = replace(runtime, spec=fault_spec,
                                store=SourceStore(runtime.store.client, 'origo', fault_spec), run_id=failed.run_id)
        with pytest.raises(OSError, match='Controlled component failure'):
            fault_runtime.build(partition.key)
        instance.report_run_failed(failed)
        before = dict(counts)
        requests, reason = tick(now[0])
        assert not requests and reason == 'Source retry delay has not elapsed.'
        assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
        with monkeypatch.context() as patch:
            def invalid_inventory(self: SourceStore, selected: Partition):
                raise SourceError('RETAINED_CONTENT_INVALID', 'Controlled invalid retained inventory.')

            patch.setattr(SourceStore, 'record', invalid_inventory)
            requests, reason = tick(now[0])
            assert not requests and reason == 'Source retry delay has not elapsed.'
            assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
        # An actual changed provider identity remains a new event, independent of that retry delay.
        rewritten[vendor._file(market, partition)] = 'changed-original-current-after-component-failure'
        expected_revision = adapter.discover(partition)
        assert expected_revision != fetched_revision
        assert tick(now[0])[0]

        # Complete rejection skips preserve the failure and never download/replay again.
        rejected[0] = True
        with pytest.raises(SourceError, match='Replay verdict fault'):
            runtime.build(partition.key)
        failures = runtime.store.execute(
            'SELECT event_id,event_type FROM origo.source_failure_log '
            'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY event_time,event_id', params,
        )
        before = dict(counts)
        now[0] = hour + timedelta(minutes=46)
        assert tick(now[0])[0] == []
        assert counts['metadata'] > before['metadata']
        assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
        assert runtime.store.execute(
            'SELECT event_id,event_type FROM origo.source_failure_log '
            'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY event_time,event_id', params,
        ) == failures
        with pytest.raises(SourceError, match='Replay verdict fault'):
            runtime.build(partition.key)
        assert counts['replay'] == before['replay'] + 1
        monkeypatch.setattr(vendor, '_REJECTION_POLICY_VERSION', vendor._REJECTION_POLICY_VERSION + 1)
        now[0] = hour + timedelta(minutes=51)
        assert tick(now[0])[0]

        # A successful native request keeps its pre-fetch identity; active evidence deduplicates it.
        rejected[0] = False
        vendor._dependency_path(market, partition).unlink()
        accepted_request = tick(now[0])[0][0]
        record = runtime.build(partition.key)
        assert len(record.component_hashes) == 4
        assert record.revision == adapter.discover(partition)
        assert record.revision != expected_revision
        instance.create_run_for_job(canonical, run_config=accepted_request.run_config,
                                    tags={**accepted_request.tags, 'dagster/partition': partition.key},
                                    status=DagsterRunStatus.SUCCESS)
        before = dict(counts)
        now[0] = hour + timedelta(minutes=56)
        requests, reason = tick(now[0])
        assert not requests and reason == 'This book revision already has complete active component evidence.'
        assert runtime.store.record(partition) == record and observations()[-1] == (1, now[0])
        assert counts['metadata'] > before['metadata']
        assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
        proofs = runtime.store.execute(
            'SELECT component,row_count,content_hash FROM origo.source_component_log '
            'WHERE source_key=%(source)s AND build_id=%(build)s', {**params, 'build': record.build_id},
        )
        assert {key: count for key, count, _ in proofs} == {
            'depth20': 36000, 'depth200': 3600, 'depth20_1m': 60, 'depth200_1m': 60,
        }
        assert dict(record.component_hashes) == {key: digest for key, _, digest in proofs}
        component_params = {**params, 'build': record.build_id}
        saved = runtime.store.execute(
            "SELECT * FROM origo.source_component_log WHERE source_key=%(source)s "
            "AND build_id=%(build)s AND component='depth200_1m'", component_params,
        )
        runtime.store.execute(
            "ALTER TABLE origo.source_component_log DELETE WHERE source_key=%(source)s "
            "AND build_id=%(build)s AND component='depth200_1m' SETTINGS mutations_sync=1", component_params,
        )
        assert tick(now[0])[1] != 'This book revision already has complete active component evidence.'
        assert _predicate(law.evaluate(runtime.store.client, 'origo', now[0]), spec.key, 'C1')['status'] == 'UNKNOWN'
        assert runtime.store.record(partition) == record
        runtime.store.execute('INSERT INTO origo.source_component_log VALUES', saved)
        assert tick(now[0])[0] == []
        assert (counts['payload'], counts['replay']) == (before['payload'], before['replay'])
        import json

        runtime.store.execute(
            "ALTER TABLE origo.source_activation_log UPDATE component_hashes='[]' "
            'WHERE source_key=%(source)s AND build_id=%(build)s SETTINGS mutations_sync=1', component_params,
        )
        assert tick(now[0])[0]
        runtime.store.execute(
            'ALTER TABLE origo.source_activation_log UPDATE component_hashes=%(hashes)s '
            'WHERE source_key=%(source)s AND build_id=%(build)s SETTINGS mutations_sync=1',
            {**component_params, 'hashes': json.dumps(record.component_hashes)},
        )
        assert tick(now[0])[0] == []

        # A delayed final tick does not hide a real discovery failure after candidate rollover.
        available[0] = False
        now[0] = hour + timedelta(hours=1)
        with pytest.raises(ScheduleExecutionError) as delayed_failure:
            tick(hour + timedelta(minutes=56))
        assert isinstance(delayed_failure.value.__cause__, SourceError)
        assert delayed_failure.value.__cause__.code == 'PROVIDER_HTTP_404'
        assert runtime.store.execute(
            "SELECT error_code FROM origo.source_failure_log WHERE source_key=%(source)s "
            "AND partition_key=%(partition)s AND operation='discovery' AND event_type='FAILED'", params,
        ) == [('PROVIDER_HTTP_404',)]


def _assert_hourly_authority_uses_exact_source_component_proofs(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    record = runtime.store.record(hour_partition(REAL_HOUR))
    assert record is not None
    now = record.partition.end + timedelta(hours=1, seconds=30)
    before = law.evaluate(runtime.store.client, 'origo', now)
    reader_now = record.partition.end + timedelta(seconds=30)
    assert _predicate(law.evaluate(runtime.store.client, 'origo', reader_now), runtime.spec.key, 'R1')['status'] == 'PASS'
    assert _predicate(before, runtime.spec.key, 'C1')['status'] == 'PASS'
    runtime.store.execute(
        "ALTER TABLE origo.source_component_log DELETE WHERE source_key=%(source)s AND component='depth200_1m' AND build_id=%(build)s SETTINGS mutations_sync=1",
        {'source': runtime.spec.key, 'build': record.build_id},
    )
    report = law.evaluate(runtime.store.client, 'origo', now)
    assert _predicate(law.evaluate(runtime.store.client, 'origo', reader_now), runtime.spec.key, 'R1')['status'] == 'UNKNOWN'
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
            'required_following_hours': 1,
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
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_spool: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.adapters.book_vendor import replay_hour

    runtime = authoritative_book_runtime
    market, body, _ = original_vendor_archive
    spool = tmp_path / 'spool'
    partition = hour_partition(REAL_HOUR)
    following = tmp_path / 'following.parquet'
    following.write_bytes(original_vendor_following_archive[1])
    # Share the verified immutable replay; mutable test storage stays private.
    shutil.copytree(original_vendor_spool, spool)
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
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    truncated = tmp_path / 'truncated.parquet'
    original = pq.ParquetFile(pa.BufferReader(body))
    with pq.ParquetWriter(truncated, original.schema_arrow, compression='zstd') as writer:
        for batch in original.iter_batches(batch_size=8192):
            table = pa.Table.from_batches([batch])
            writer.write_table(table.filter(pc.less(table['event_time'], int(partition.end.timestamp() * 1000) - 1000)))

    def changed(self: RecordedVendorHour, requested: Partition) -> str:
        assert requested.key == REAL_HOUR
        return 'tail-removal-test:' + self.recorded_revision.key

    def incomplete(self: RecordedVendorHour, requested: Partition) -> Revision:
        return replay_hour(truncated, market, requested, tmp_path / 'invalid-grid', following=(following,))

    monkeypatch.setattr(RecordedVendorHour, 'discover', changed)
    monkeypatch.setattr(RecordedVendorHour, 'fetch', incomplete)
    with pytest.raises(SourceError) as failure:
        runtime.build(REAL_HOUR)
    assert failure.value.code == 'BOOK_SEQUENCE_GAP'
    assert runtime.store.records() == (canonical,)
    assert runtime.store.execute(
        f'SELECT count() FROM {prefix}_depth20_latest_revisions WHERE build_id=%(build)s',
        {'build': minute.build_id},
    ) == [(600,)]


def _assert_provider_delay_and_activation_use_native_evidence(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources import lifecycle
    from origo.sources.contracts import ArchiveNotPublishedYet

    runtime = authoritative_book_runtime
    partition = hour_partition(REAL_HOUR)
    now = [partition.end + timedelta(hours=1, minutes=30)]

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
    now[0] = partition.end + timedelta(hours=1, minutes=45)
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
    now = hour_partition(REAL_HOUR).end + timedelta(hours=1, minutes=30)

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


def test_pending_historical_failures_do_not_starve_later_valid_hour(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = authoritative_book_runtime
    now = datetime.now(UTC)
    keys = [(hour_partition(REAL_HOUR).start - timedelta(hours=offset)).strftime('%Y-%m-%dT%HZ')
            for offset in range(5, 0, -1)] + [REAL_HOUR]
    runtime.store.execute('INSERT INTO origo.source_discovery_log VALUES',
                          [(runtime.spec.key, key, now - timedelta(minutes=6 - index))
                           for index, key in enumerate(keys)])
    original = RecordedVendorHour.discover

    def discover(self: RecordedVendorHour, partition: Partition) -> str:
        if partition.key != REAL_HOUR:
            raise SourceError('PROVIDER_HTTP_404', 'Controlled persistent historical gap.')
        return original(self, partition)

    monkeypatch.setattr(RecordedVendorHour, 'discover', discover)
    assert runtime.audit() == ()
    assert runtime.audit() == (REAL_HOUR,)
    record = runtime.build(REAL_HOUR)
    assert record.partition.key == REAL_HOUR
    assert runtime.store.execute(
        "SELECT partition_key,argMax(event_type,event_time) FROM origo.source_failure_log WHERE source_key=%(source)s AND partition_key IN %(keys)s GROUP BY partition_key ORDER BY partition_key",
        {'source': runtime.spec.key, 'keys': keys[:-1]},
    ) == [(key, 'FAILED') for key in sorted(keys[:-1])]


def _assert_declared_history_migration_preserves_existing_generation(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    record = runtime.store.record(hour_partition(REAL_HOUR))
    assert record is not None
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


def _assert_book_reconciliation_uses_native_hourly_status(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster import DagsterInstance, Definitions, HourlyPartitionsDefinition, build_sensor_context
    from origo.sources.bundle import build_source_bundle

    runtime = authoritative_book_runtime
    record = runtime.store.record(hour_partition(REAL_HOUR))
    assert record is not None
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


def test_prior_book_authority_is_unverified_until_closing_input_rebuild(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.contracts import ArchiveNotPublishedYet

    runtime = authoritative_book_runtime
    adapter = cast(RecordedVendorHour, runtime.spec.canonical)
    current = adapter.recorded_revision
    original = replace(current, key=current.key.replace('cryptohftdata-v2:', 'cryptohftdata-v1:', 1))
    object.__setattr__(adapter, 'recorded_revision', original)
    record = runtime.build(REAL_HOUR)
    now = record.partition.end + timedelta(hours=1, minutes=21)
    report = law.evaluate(runtime.store.client, 'origo', now)
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'UNKNOWN'
    assert _predicate(report, runtime.spec.key, 'C1')['reason'] == 'book_closing_input_unverified'

    original_discover = RecordedVendorHour.discover
    def waiting(self: RecordedVendorHour, partition: Partition) -> str:
        raise ArchiveNotPublishedYet('Closing file not published during policy upgrade.')
    monkeypatch.setattr(RecordedVendorHour, 'discover', waiting)
    assert runtime.audit() == ()
    assert runtime.store.record(record.partition) == record
    assert runtime.store.execute("SELECT count() FROM origo.source_failure_log WHERE event_type='FAILED'") == [(0,)]
    monkeypatch.setattr(RecordedVendorHour, 'discover', original_discover)
    object.__setattr__(adapter, 'recorded_revision', current)
    assert REAL_HOUR in runtime.audit()
    rebuilt = runtime.build(REAL_HOUR)
    assert rebuilt.generation == record.generation + 1
    assert _predicate(law.evaluate(runtime.store.client, 'origo', now), runtime.spec.key, 'C1')['status'] == 'PASS'


def test_rejected_book_revision_is_not_replayed_by_audit(
    rejection_runtime: SourceRuntime,
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    from origo.sources.adapters import book_vendor as vendor
    from origo.sources.bundle import SourceRunConfig, _execute_operation

    runtime = rejection_runtime
    market, body, headers = original_vendor_archive
    partition = hour_partition(REAL_HOUR)
    after = hour_partition('2026-10-04T10Z')
    visited, rewritten = _transport((OriginalArchive(partition, body, headers),
                                    OriginalArchive(after, original_vendor_following_archive[1],
                                                    original_vendor_following_archive[2])), market, monkeypatch)
    transport = vendor.get_response
    counts = {'metadata': 0, 'payload': 0, 'replay': 0}

    def counted(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> vendor.Response:
        counts['metadata' if headers else 'payload'] += 1
        return transport(url, params=params, headers=headers, weight=weight)

    monkeypatch.setattr(vendor, 'get_response', counted)
    real_replay = vendor.replay_hour

    def rejected(*args: object, **kwargs: object) -> Revision:
        counts['replay'] += 1
        raise SourceError('BOOK_EVENT_STALE', 'Deliberate replay verdict fault; vendor bytes unchanged.')

    monkeypatch.setattr(vendor, 'replay_hour', rejected)
    runtime.discover(partition)
    with pytest.raises(ExceptionGroup):
        _execute_operation(runtime, 'audit', SourceRunConfig())
    runtime.failures.record(operation='audit', error_code='BOOK_EVENT_STALE', scope='NONE', partition=REAL_HOUR)
    assert runtime.store.record(partition) is None
    assert counts['payload'] == 2 and counts['replay'] == 1
    original_failures = runtime.store.execute('SELECT event_id,event_type FROM origo.source_failure_log ORDER BY event_time,event_id')
    caplog.set_level('INFO')

    def selected_skip() -> None:
        nonlocal runtime
        runtime = replace(runtime, run_id=str(uuid4()), spec=replace(runtime.spec, canonical=CryptoHFTBookHourly(market)))
        before = dict(counts)
        observations = runtime.store.execute('SELECT count() FROM origo.source_observation_log WHERE partition_key=%(key)s', {'key': REAL_HOUR})
        assert _execute_operation(runtime, 'audit', SourceRunConfig()) == {'corrected_partitions': []}
        assert counts['payload'] == before['payload'] and counts['replay'] == before['replay']
        assert counts['metadata'] > before['metadata']
        assert runtime.store.execute('SELECT count() FROM origo.source_observation_log WHERE partition_key=%(key)s', {'key': REAL_HOUR})[0][0] > observations[0][0]
        assert runtime.store.execute('SELECT event_id,event_type FROM origo.source_failure_log ORDER BY event_time,event_id') == original_failures
        assert any('phase=audit_rejected_unchanged' in record.message and REAL_HOUR in record.message
                   and runtime.spec.key in record.message for record in caplog.records)

    selected_skip()
    selected_skip()
    assert runtime.store.record(partition) is None
    # Five controlled old rejected candidates rotate without starving this real hour.
    old = [(partition.start - timedelta(hours=offset)).strftime('%Y-%m-%dT%HZ') for offset in range(6, 1, -1)]
    now = datetime.now(UTC)
    runtime.store.execute('INSERT INTO origo.source_discovery_log VALUES',
                          [(runtime.spec.key, key, now - timedelta(days=1)) for key in old])
    discover, rejected_revision = CryptoHFTBookHourly.discover, CryptoHFTBookHourly.rejected_revision

    def control_discover(self: CryptoHFTBookHourly, requested: Partition) -> str:
        return 'controlled-rejected-candidate:' + requested.key if requested.key in old else discover(self, requested)

    def control_rejected(self: CryptoHFTBookHourly, requested: Partition, revision: str) -> bool:
        return requested.key in old or rejected_revision(self, requested, revision)

    with monkeypatch.context() as control:
        control.setattr(CryptoHFTBookHourly, 'discover', control_discover)
        control.setattr(CryptoHFTBookHourly, 'rejected_revision', control_rejected)
        monkeypatch.setattr(vendor, 'replay_hour', real_replay)
        vendor._dependency_path(market, partition).unlink()
        assert _execute_operation(runtime, 'audit', SourceRunConfig()) == {'corrected_partitions': []}
        assert {str(row[0]) for row in runtime.store.execute('SELECT DISTINCT partition_key FROM origo.source_observation_log WHERE partition_key IN %(keys)s', {'keys': old})} == set(old)
        assert _execute_operation(runtime, 'audit', SourceRunConfig()) == {'corrected_partitions': [REAL_HOUR]}
    canonical = runtime.store.record(partition)
    assert canonical is not None and len(canonical.component_hashes) == 4
    # Remove the controlled requests; no fabricated market records were built.
    runtime.store.execute('ALTER TABLE origo.source_discovery_log DELETE WHERE partition_key IN %(keys)s SETTINGS mutations_sync=1', {'keys': old})
    before_coverage = runtime.store.execute('SELECT count() FROM origo.' + runtime.spec.names.prefix + '_depth20_current')
    assert before_coverage == [(36000,)]
    monkeypatch.setattr(vendor, 'replay_hour', rejected)
    rewritten[vendor._file(market, partition)] = 'controlled-replacement-etag'
    with pytest.raises(ExceptionGroup):
        _execute_operation(runtime, 'audit', SourceRunConfig())
    original_failures = runtime.store.execute('SELECT event_id,event_type FROM origo.source_failure_log ORDER BY event_time,event_id')
    before = dict(counts)
    result = _execute_operation(runtime, 'repair', SourceRunConfig(partition_key=REAL_HOUR))
    assert result['generation'] == canonical.generation
    assert counts['payload'] == before['payload'] and counts['replay'] == before['replay']
    assert runtime.store.execute('SELECT event_id,event_type FROM origo.source_failure_log ORDER BY event_time,event_id') == original_failures
    selected_skip()
    selected_skip()
    assert runtime.store.record(partition) == canonical
    assert runtime.store.execute('SELECT count() FROM origo.' + runtime.spec.names.prefix + '_depth20_current') == before_coverage
    assert runtime.store.execute("SELECT error_code FROM origo.source_failure_log WHERE operation='canonical' GROUP BY failure_key,error_code HAVING argMax(event_type,event_time)='FAILED'") == [('BOOK_EVENT_STALE',)]
    assert visited
    monkeypatch.setattr(vendor, '_REPLAY_POLICY_VERSION', vendor._REPLAY_POLICY_VERSION + 1)
    official = runtime.spec.canonical.discover(partition)
    assert official != canonical.revision
    assert not runtime.spec.canonical.rejected_revision(partition, official)
    before = dict(counts)
    result = _execute_operation(runtime, 'repair', SourceRunConfig(partition_key=REAL_HOUR))
    assert result['generation'] == canonical.generation
    assert counts['payload'] == before['payload'] and counts['replay'] == before['replay']
    assert runtime.store.execute('SELECT event_id,event_type FROM origo.source_failure_log ORDER BY event_time,event_id') == original_failures
    monkeypatch.setattr(vendor, 'replay_hour', real_replay)
    _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=REAL_HOUR, automatic_capacity=True))
    rebuilt = runtime.store.record(partition)
    assert rebuilt is not None and rebuilt.revision == official
    assert rebuilt.generation == canonical.generation + 1 and len(rebuilt.component_hashes) == 4
    assert counts['payload'] > before['payload']
    assert runtime.store.execute("SELECT error_code FROM origo.source_failure_log WHERE operation='canonical' GROUP BY failure_key,error_code HAVING argMax(event_type,event_time)='FAILED'") == []
