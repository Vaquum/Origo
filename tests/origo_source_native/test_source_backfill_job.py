import hashlib
import json
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    DailyPartitionsDefinition,
    Definitions,
    build_sensor_context,
)

from dagster._core.definitions.sensor_definition import SensorExecutionData

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import capacity
from origo.sources.adapters import binance_daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import ConsumerSpec, Snapshot, SnapshotReader, SourceBundle
from origo.sources.prepare import prepare_source
from origo.sources.profiles import spot_consumers
from origo.sources.profiles.formulas.spot_series import SPECS as SPOT_SERIES_SPECS
from origo.sources.storage import SourceStore

from .acceptance_cases import SPOT_CASE
from .test_binance_daily_source_adapter import archive_response

DAY = '2017-08-17'
ASSET = 'build_binance_spot_trades_canonical_revision_origo'


class FakeHfApi:
    """Records the Hugging Face calls a publication makes; nothing leaves the host."""

    calls: list[tuple[str, dict[str, object]]] = []

    def __init__(self, token: str) -> None:
        assert token

    def create_repo(self, **kwargs: object) -> None:
        self.calls.append(('create_repo', kwargs))

    def upload_folder(self, **kwargs: object) -> None:
        self.calls.append(('upload_folder', kwargs))


@pytest.fixture
def ready_job(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SourceStore, DagsterInstance, SourceBundle]]:
    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'files'))
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),)
    )
    monkeypatch.setattr(
        capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7)
    )
    spec = replace(
        BINANCE_SPOT_TRADES_SPEC,
        orchestration=replace(BINANCE_SPOT_TRADES_SPEC.orchestration, retry_count=0),
    )
    client = make_clickhouse_client(get_clickhouse_settings())
    (tmp_path / 'dagster').mkdir()
    try:
        with DagsterInstance.local_temp(
            str(tmp_path / 'dagster'),
            overrides={'python_logs': {'managed_python_loggers': [''], 'python_log_level': 'INFO'}},
        ) as instance:
            yield SourceStore(client, 'origo', spec), instance, build_source_bundle(spec)
    finally:
        client.disconnect()


def _selection(end: str = DAY) -> dict[str, str]:
    return {'dagster/asset_partition_range_start': DAY, 'dagster/asset_partition_range_end': end}


def test_one_job_prepares_verifies_and_publishes_all_files(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, instance, bundle = ready_job
    assert {component.key for component in store.spec.components} == {
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
    }
    assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface'}
    assert store.execute('EXISTS TABLE origo.source_activation_log') == [(0,)]
    assert instance.all_instigator_state() == []
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))

    def no_copy(self: SourceStore, component: str, snapshot: Snapshot) -> list[tuple[object, ...]]:
        raise AssertionError(
            'Publication must query pinned projections without copying historical rows into Python.'
        )

    monkeypatch.setattr(SourceStore, 'rows', no_copy)
    result = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert len(instance.all_instigator_state()) == len(bundle.sensors) + len(bundle.schedules)
    assert store.canonical_ready()
    assert store.execute('SELECT min(successful) FROM origo.source_capacity_log') == [(1,)]
    token = store.snapshot().token
    for consumer in store.spec.consumers:
        root = tmp_path / 'files' / store.spec.key / consumer.key
        manifest = json.loads((root / 'latest.json').read_text())
        assert manifest['state_token'] == token
        # The public series start in 2020; the fixture day publishes an empty, current state.
        assert manifest['files'] == []
        assert manifest.get('month_tokens', {}) == {} and manifest.get('uploads', []) == []
        for file in manifest['files']:
            path = root / 'versions' / manifest['version'] / file['path']
            assert hashlib.sha256(path.read_bytes()).hexdigest() == file['sha256']
        sensor = next(
            (
                sensor
                for sensor in bundle.sensors
                if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
            ),
            None,
        )
        # A consumer that pins provisional rows is published by the provisional worker,
        # not by a sensor; canonical-only consumers keep theirs.
        assert (sensor is not None) == consumer.canonical_only
        if sensor is None:
            continue
        with build_sensor_context(
            instance=instance,
            definitions=Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors),
        ) as context:
            assert sensor.evaluate_tick(context).run_requests == []
    # Re-deployment restores code-owned sensor state without losing its cursor.
    state = instance.all_instigator_state()[0]
    from dagster._core.scheduler.instigation import InstigatorStatus

    instance.update_instigator_state(state.with_status(InstigatorStatus.STOPPED))
    with pytest.raises(RuntimeError, match='sensor or schedule is not prepared'):
        prepare_source(store.spec, instance, check=True)
    prepare_source(store.spec, instance)
    prepare_source(store.spec, instance, check=True)
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert store.snapshot().token == token


def test_file_failure_fails_job_and_retry_keeps_verified_generation(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, _bundle = ready_job
    original = store.spec.consumers[1]
    attempts = 0

    def interrupted(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError('Interrupted publication')
        original.publish(reader, snapshot, destination)

    spec = replace(
        store.spec,
        consumers=(
            store.spec.consumers[0],
            ConsumerSpec(original.key, interrupted),
            *store.spec.consumers[2:],
        ),
    )
    job = next(job for job in build_source_bundle(spec).jobs if job.name.startswith('backfill_'))
    failed = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert not failed.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert not (tmp_path / 'files' / spec.key / 'huggingface' / 'latest.json').exists()
    manifest = tmp_path / 'files' / spec.key / 'mount' / 'latest.json'
    published = manifest.read_bytes()
    assert (
        instance.get_latest_materialization_event(AssetKey(f'publish_{spec.key}_mount')) is not None
    )
    before = store.snapshot()
    retried = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert retried.success
    assert store.snapshot() == before
    assert manifest.read_bytes() == published
    assert instance.get_run_by_id(failed.run_id).status.value == 'FAILURE'


def test_unavailable_day_blocks_publication_and_preserves_completed_day(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, bundle = ready_job
    job = Definitions(
        assets=bundle.assets, jobs=bundle.jobs
    ).resolve_implicit_global_asset_job_def()
    assert job.execute_in_process(
        instance=instance, partition_key=DAY, asset_selection=[AssetKey(ASSET)]
    ).success
    result = job.execute_in_process(
        instance=instance,
        partition_key='2017-08-18',
        asset_selection=[AssetKey(ASSET)],
        raise_on_error=False,
    )
    assert not result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert store.canonical_ready()
    assert list((tmp_path / 'files').rglob('latest.json')) == []
    statuses = instance.get_status_by_partition(
        AssetKey(ASSET),
        [DAY, '2017-08-18'],
        DailyPartitionsDefinition(start_date=DAY, timezone='UTC'),
    )
    assert statuses is not None
    assert statuses[DAY].value == 'MATERIALIZED'
    assert statuses['2017-08-18'].value == 'FAILED'
    for sensor in bundle.sensors:
        if any(
            sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
            for consumer in store.spec.consumers
        ):
            with build_sensor_context(
                instance=instance,
                definitions=Definitions(
                    assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors
                ),
            ) as context:
                assert sensor.evaluate_tick(context).run_requests == []


def test_period_defaults_and_boundaries_are_shared_across_sources() -> None:
    from dagster._core.definitions.partitions.context import partition_loading_context

    for spec in (
        BINANCE_SPOT_TRADES_SPEC,
        replace(BINANCE_SPOT_TRADES_SPEC, key='another_registered_source'),
    ):
        job = next(
            job for job in build_source_bundle(spec).jobs if job.name.startswith('backfill_')
        )
        assert job.is_asset_job
        assert isinstance(job.partitions_def, DailyPartitionsDefinition)
        assert job.backfill_policy.max_partitions_per_run == 1
        assert job.partitions_def.get_first_partition_key() == DAY
        with partition_loading_context(effective_dt=datetime(2020, 1, 3, tzinfo=UTC)):
            assert job.partitions_def.get_last_partition_key() == '2020-01-02'
        assert job.get_run_config_for_partition_key(DAY) == {}
        assert job.name == f'backfill_{spec.key}_source_job'
        from dagster import validate_run_config

        for operation in build_source_bundle(spec).jobs:
            validate_run_config(operation, {})
            if operation.name.startswith(('repair_', 'certify_')):
                assert isinstance(operation.partitions_def, DailyPartitionsDefinition)


def test_storage_change_remeasures_capacity(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    assert job.execute_in_process(instance=instance, tags=_selection()).success
    previous = store.snapshot()
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('replacement-volume', tmp_path),)
    )
    assert job.execute_in_process(instance=instance, tags=_selection()).success
    assert store.snapshot() == previous
    assert store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
    assert (
        store.execute(
            "SELECT countIf(successful) FROM origo.source_capacity_log WHERE volume_id='replacement-volume'"
        )[0][0]
        > 0
    )


def test_preparation_applies_rollout_state_without_manual_switches(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], monkeypatch: pytest.MonkeyPatch
) -> None:
    from dagster import DefaultScheduleStatus, DefaultSensorStatus

    from origo.sources import prepare
    from origo.sources.contracts import RolloutStage

    store, instance, _bundle = ready_job
    for stage in (
        RolloutStage.CANARY,
        RolloutStage.LIVE,
        RolloutStage.CANARY,
        RolloutStage.DORMANT,
    ):
        spec = replace(store.spec, rollout_stage=stage)
        if stage == RolloutStage.DORMANT:

            def forbidden() -> None:
                raise AssertionError('Dormant preparation must not open an external client.')

            monkeypatch.setattr(prepare, 'get_clickhouse_settings', forbidden)
        prepare_source(spec, instance)
        prepare_source(spec, instance, check=True)
        generated = build_source_bundle(spec)
        states = {
            state.instigator_name: state.status.value for state in instance.all_instigator_state()
        }
        for sensor in generated.sensors:
            expected = (
                'RUNNING' if sensor.default_status == DefaultSensorStatus.RUNNING else 'STOPPED'
            )
            assert states.get(sensor.name, 'STOPPED') == expected
        for schedule in generated.schedules:
            expected = (
                'RUNNING' if schedule.default_status == DefaultScheduleStatus.RUNNING else 'STOPPED'
            )
            assert states.get(schedule.name, 'STOPPED') == expected


def test_deployment_preparation_failures_are_dagster_runs(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], monkeypatch: pytest.MonkeyPatch
) -> None:
    from origo.sources import bootstrap
    from origo.sources.contracts import RevisionedSourceSpec

    _store, instance, _bundle = ready_job
    first = bootstrap.prepare_revisioned_sources_job.execute_in_process(instance=instance)
    assert first.success

    def unavailable(spec: RevisionedSourceSpec, instance: DagsterInstance) -> None:
        raise OSError('Source preparation storage unavailable')

    monkeypatch.setattr(bootstrap, 'prepare_source', unavailable)
    result = bootstrap.prepare_revisioned_sources_job.execute_in_process(
        instance=instance, raise_on_error=False
    )
    assert not result.success
    assert instance.get_run_by_id(result.run_id).status.value == 'FAILURE'
    errors = [
        event.dagster_event.step_failure_data.error
        for event in instance.all_logs(result.run_id)
        if event.dagster_event and event.dagster_event.is_step_failure
    ]
    assert any(
        error is not None and 'Source preparation storage unavailable' in error.to_string()
        for error in errors
    )


def test_projection_runs_retire_without_losing_source_history_or_receipts(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from dagster import DagsterRun, DagsterRunStatus

    from origo.maintenance.roles import run_role
    from origo.maintenance.run_storage import OrigoSqliteRunStorage
    from origo.maintenance.source_receipts import preserve_source_receipt, source_reference_reason

    store, instance, bundle = ready_job
    prepare_source(store.spec, instance)
    storage = OrigoSqliteRunStorage.from_local(str(tmp_path / 'run-policy'))
    try:
        for job in bundle.jobs:
            if not (job.name.startswith('publish_') or job.name.startswith('backfill_')):
                continue
            run = DagsterRun(job_name=job.name, status=DagsterRunStatus.SUCCESS, tags=job.tags)
            storage.add_run(run)
            if job.name.startswith('publish_'):
                assert run_role(run) == 'projection'
                assert not run.tags.get('origo_source_key')
                assert source_reference_reason(run) == ''
                identity = f'{store.spec.key}:consumer:{job.name}:verified-test-receipt'
                tagged = run.with_tags(
                    {**run.tags, 'origo_source_event': identity, 'origo_source_attempt': '0'}
                )
                preserve_source_receipt(tagged)
                assert store.run_receipt(identity) == (0, 'SUCCESS', run.run_id)
                storage.delete_run(run.run_id)
                assert not storage.has_run(run.run_id)
            else:
                assert run_role(run) == 'source'
                with pytest.raises(RuntimeError, match='cannot be retired'):
                    storage.delete_run(run.run_id)
                assert storage.has_run(run.run_id)
    finally:
        storage.dispose()


def test_new_verified_data_automatically_requests_every_canonical_only_consumer(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, bundle = ready_job
    backfill = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    assert backfill.execute_in_process(instance=instance, tags=_selection()).success
    previous_token = store.snapshot().token
    canonical = next(
        job for job in bundle.jobs if job.name == f'refresh_{store.spec.key}_canonical_source_job'
    )
    assert canonical.execute_in_process(instance=instance, partition_key='2020-01-01').success
    assert store.canonical_ready()
    assert store.snapshot().token != previous_token
    definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors)
    canonical_only = [consumer for consumer in store.spec.consumers if consumer.canonical_only]
    assert [consumer.key for consumer in canonical_only] == ['huggingface']
    for consumer in canonical_only:
        sensor = next(
            sensor
            for sensor in bundle.sensors
            if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
        )
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            requests = sensor.evaluate_tick(context).run_requests
        assert len(requests) == 1
        request = requests[0]
        assert request.tags['origo_source_state_token'] == store.snapshot().token
        publisher = next(
            job for job in bundle.jobs if job.name == f'publish_{store.spec.key}_{consumer.key}_job'
        )
        assert publisher.execute_in_process(
            instance=instance, run_config=request.run_config, tags=request.tags
        ).success
        manifest = json.loads(
            (tmp_path / 'files' / store.spec.key / consumer.key / 'latest.json').read_text()
        )
        assert manifest['state_token'] == store.snapshot().token
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            assert sensor.evaluate_tick(context).run_requests == []


def test_publication_follows_canonical_state_across_provisional_refreshes(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from origo.sources.contracts import Partition, StateRecord
    from origo.sources.lifecycle import SourceRuntime

    store, instance, bundle = ready_job
    SourceRuntime(store.spec, store, tmp_path / 'locks', 'test-setup').setup()
    generations = iter(range(1, 100))

    def provisional_refresh() -> None:
        # Scheduling state only: one more partial-day generation after the canonical day.
        start = datetime(2017, 8, 18, tzinfo=UTC)
        partition = Partition(
            start.strftime('%Y-%m-%dT%H:%M:%SZ'), start, start + timedelta(minutes=1), True
        )
        store.insert_activation(
            StateRecord(partition, next(generations), 'provisional', uuid4(), ()),
            'provisional-refresh',
        )

    def overlapping(
        publish: Callable[[SnapshotReader, Snapshot, str], None],
    ) -> Callable[[SnapshotReader, Snapshot, str], None]:
        def render(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
            provisional_refresh()
            publish(reader, snapshot, destination)

        return render

    provisional_refresh()
    spec = replace(
        store.spec,
        consumers=tuple(
            replace(consumer, publish=overlapping(consumer.publish))
            for consumer in store.spec.consumers
        ),
    )
    source = build_source_bundle(spec)
    backfill = next(job for job in source.jobs if job.name.startswith('backfill_'))
    assert backfill.execute_in_process(instance=instance, tags=_selection()).success
    canonical = store.snapshot(canonical_only=True).token
    assert store.snapshot().token != canonical
    for consumer in spec.consumers:
        manifest = json.loads(
            (tmp_path / 'files' / store.spec.key / consumer.key / 'latest.json').read_text()
        )
        assert manifest['state_token'] == canonical
        if consumer.canonical_only:
            assert manifest['pinned_token'] == canonical
        else:
            assert manifest['pinned_token'] not in (canonical, store.snapshot().token)
    definitions = Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors)
    sensors = {
        consumer.key: next(
            sensor
            for sensor in source.sensors
            if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
        )
        for consumer in spec.consumers
        if consumer.canonical_only
    }
    assert set(sensors) == {'huggingface'}
    # The consumer that pins provisional rows is the provisional worker's: it publishes
    # when the pinned state changed and a native backfill does not own publication.
    from origo.workers.dagster_reader import DagsterReader
    from origo.workers.provisional import ProvisionalFeed
    from origo.workers.receipts import ensure_monitoring_tables
    from origo.workers.report import Reporter

    ensure_monitoring_tables(store.client, 'origo')

    class _NoBackfill:
        def backfill_owns_publication(self, source_key: str) -> bool:
            return False

    worker = ProvisionalFeed(
        [spec],
        publication_root=tmp_path / 'files',
        reporter=cast(Reporter, object()),
        dagster=cast(DagsterReader, _NoBackfill()),
    )
    provisional_refresh()
    for consumer in sensors:
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            assert sensors[consumer].evaluate_tick(context).run_requests == []
    manifest_path = tmp_path / 'files' / spec.key / 'mount' / 'latest.json'
    published, unpublished = worker._publish(store, spec, datetime.now(UTC))
    assert (published, unpublished) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == canonical
    # The overlapping renderer refreshed the provisional state during the render, so the
    # files pin a state behind the current one and the next publication follows it; once
    # the state stops moving, the worker publishes nothing.
    assert pinned['pinned_token'] not in (canonical, store.snapshot().token)
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == canonical
    assert pinned['pinned_token'] == store.snapshot().token
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([], [])
    canonical_job = next(
        job for job in source.jobs if job.name == f'refresh_{store.spec.key}_canonical_source_job'
    )
    assert canonical_job.execute_in_process(instance=instance, partition_key='2020-01-01').success
    advanced = store.snapshot(canonical_only=True).token
    assert advanced != canonical
    for consumer in sensors:
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            requests = sensors[consumer].evaluate_tick(context).run_requests
        assert len(requests) == 1
        assert requests[0].tags['origo_source_state_token'] == advanced
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == advanced
    assert pinned['pinned_token'] == store.snapshot().token


def test_canonical_day_retires_failed_provisional_intervals_inside_it(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from uuid import uuid4

    from origo.sources.failures import FailureLog
    from origo.sources.lifecycle import SourceRuntime

    store, _instance, _bundle = ready_job
    runtime = SourceRuntime(store.spec, store, tmp_path / 'locks', 'test-setup')
    runtime.setup()
    injected = FailureLog(store, tmp_path / 'locks', str(uuid4()))
    inside = f'{DAY}T10:15:00Z'
    injected.record(
        operation='component',
        scope='PARTITION',
        partition=inside,
        component='raw_latest',
        error_code='COMPONENT_CONTENT_INVALID',
    )
    injected.record(
        operation='provisional', scope='PARTITION', partition=inside, error_code='RUN_FAILED'
    )
    outside = '2017-08-18T00:03:00Z'
    injected.record(
        operation='provisional', scope='PARTITION', partition=outside, error_code='RUN_FAILED'
    )
    runtime.build(DAY)
    assert store.execute(
        'SELECT partition_key, argMax(event_type, event_time), argMax(details_json, event_time) '
        'FROM origo.source_failure_log GROUP BY failure_key, partition_key ORDER BY partition_key',
    ) == [
        (inside, 'RECOVERED', '{"reason": "superseded by the canonical day"}'),
        (inside, 'RECOVERED', '{"reason": "superseded by the canonical day"}'),
        (outside, 'FAILED', '{}'),
    ]


def test_native_backfill_reserves_source_before_worker_starts(tmp_path: Path) -> None:
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill

    from origo.sources.prepare import backfill_active, backfill_owns_publication

    spec = BINANCE_SPOT_TRADES_SPEC
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        pending = PartitionBackfill(
            backfill_id='native-selection',
            status=BulkActionStatus.REQUESTED,
            from_failure=False,
            tags={},
            backfill_timestamp=1.0,
            asset_selection=[AssetKey(ASSET)],
        )
        instance.add_backfill(pending)
        assert backfill_active(instance, spec)
        assert backfill_owns_publication(instance, spec)
        assert not backfill_active(instance, replace(spec, key='another_source'))
        instance.update_backfill(pending.with_status(BulkActionStatus.FAILED))
        assert not backfill_active(instance, spec)
        assert backfill_owns_publication(instance, spec)
        instance.update_backfill(pending.with_status(BulkActionStatus.COMPLETED_SUCCESS))
        assert not backfill_owns_publication(instance, spec)


@pytest.mark.parametrize('status', ['FAILED', 'COMPLETED_FAILED', 'CANCELED'])
def test_native_backfill_failure_holds_publication_after_last_range_succeeds(
    tmp_path: Path, status: str
) -> None:
    from dagster import DagsterRunStatus
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._core.storage.tags import BACKFILL_ID_TAG

    from origo.sources.prepare import backfill_active, backfill_owns_publication

    spec = BINANCE_SPOT_TRADES_SPEC
    bundle = build_source_bundle(spec)
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        parent = PartitionBackfill(
            backfill_id='separate-gaps',
            status=BulkActionStatus[status],
            from_failure=False,
            tags={},
            backfill_timestamp=1.0,
            asset_selection=[AssetKey(ASSET)],
        )
        instance.add_backfill(parent)
        for day, run_status in (
            ('2017-08-18', DagsterRunStatus.FAILURE),
            ('2020-01-01', DagsterRunStatus.SUCCESS),
        ):
            instance.create_run_for_job(
                job,
                status=run_status,
                tags={
                    BACKFILL_ID_TAG: parent.backfill_id,
                    'dagster/asset_partition_range_start': day,
                    'dagster/asset_partition_range_end': day,
                },
            )
        assert not backfill_active(instance, spec)
        assert backfill_owns_publication(instance, spec)
        definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors)
        for consumer in (item for item in spec.consumers if item.canonical_only):
            sensor = next(
                s for s in bundle.sensors if s.name == f'{spec.key}_{consumer.key}_sensor'
            )
            with build_sensor_context(instance=instance, definitions=definitions) as context:
                tick = sensor.evaluate_tick(context)
                assert tick.run_requests == []
                assert tick.skip_message is not None
                assert 'owns publication' in tick.skip_message
        # A later completed native selection can supersede the failed selection.
        instance.add_backfill(
            parent._replace(
                backfill_id='retried-gaps',
                status=BulkActionStatus.COMPLETED_SUCCESS,
                backfill_timestamp=datetime.now(UTC).timestamp() + 1,
            )
        )
        assert not backfill_owns_publication(instance, spec)


def test_native_job_backfill_waits_for_own_selected_generations(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._core.storage.tags import BACKFILL_ID_TAG

    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    # An earlier verified generation must not satisfy a new backfill's queued day.
    assert job.execute_in_process(instance=instance, partition_key='2020-01-01').success
    root = tmp_path / 'files' / store.spec.key
    previous = {
        consumer.key: (root / consumer.key / 'latest.json').read_bytes()
        for consumer in store.spec.consumers
    }
    parent = PartitionBackfill(
        backfill_id='real-native-job',
        status=BulkActionStatus.REQUESTED,
        from_failure=False,
        tags={},
        backfill_timestamp=datetime.now(UTC).timestamp(),
        asset_selection=list(job.asset_layer.executable_asset_keys),
        partition_names=[DAY, '2020-01-01'],
    )
    assert not parent.is_asset_backfill
    instance.add_backfill(parent)
    first = job.execute_in_process(
        instance=instance,
        partition_key=DAY,
        tags={BACKFILL_ID_TAG: parent.backfill_id},
    )
    assert first.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY, '2020-01-01'}
    assert any(event.is_step_skipped for event in first.all_events)
    for consumer in store.spec.consumers:
        assert (root / consumer.key / 'latest.json').read_bytes() == previous[consumer.key]
    last = job.execute_in_process(
        instance=instance,
        partition_key='2020-01-01',
        tags={BACKFILL_ID_TAG: parent.backfill_id},
    )
    assert last.success
    for consumer in store.spec.consumers:
        assert (
            json.loads((root / consumer.key / 'latest.json').read_text())['state_token']
            == store.snapshot().token
        )
    assert store.execute('SELECT uniqExact(partition_key) FROM origo.source_backfill_log') == [(2,)]


def test_backfill_worker_loss_in_file_step_has_consumer_scope(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], monkeypatch: pytest.MonkeyPatch
) -> None:
    from dagster import build_run_status_sensor_context

    from origo.sources import bundle as implementation
    from origo.sources.bundle import SourceRunConfig
    from origo.sources.lifecycle import SourceRuntime

    store, instance, source = ready_job
    job = next(job for job in source.jobs if job.name.startswith('backfill_'))
    original = implementation._execute_operation

    def crash(runtime: SourceRuntime, operation: str, config: SourceRunConfig) -> dict[str, object]:
        if operation == 'consumer_huggingface':
            raise RuntimeError('Worker failed before publisher dispatch')
        return original(runtime, operation, config)

    with monkeypatch.context() as patch:
        patch.setattr(implementation, '_execute_operation', crash)
        result = job.execute_in_process(instance=instance, partition_key=DAY, raise_on_error=False)
    assert not result.success
    observer = next(sensor for sensor in source.sensors if sensor.name.endswith('_failure_sensor'))
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    with build_run_status_sensor_context(
        sensor_name=observer.name,
        dagster_instance=instance,
        dagster_run=run,
        dagster_event=next(
            e for e in result.all_events if e.event_type_value == 'PIPELINE_FAILURE'
        ),
    ) as context:
        observer(context)
    assert store.execute(
        "SELECT operation,blocking_scope,partition_key,consumer FROM origo.source_failure_log WHERE error_code='RUN_FAILED'"
    ) == [('consumer', 'CONSUMER', None, 'huggingface')]
    assert job.execute_in_process(instance=instance, partition_key=DAY).success


def test_retired_failed_publication_keeps_retry_delay_and_attempt_limit(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
) -> None:
    store, instance, _ = ready_job
    spec = replace(
        store.spec,
        orchestration=replace(store.spec.orchestration, retry_count=1, retry_delay=3600),
    )
    bundle = build_source_bundle(spec)
    canonical = next(
        job for job in bundle.jobs if job.name == f'refresh_{spec.key}_canonical_source_job'
    )
    assert canonical.execute_in_process(instance=instance, partition_key=DAY).success

    def evaluate(current: SourceBundle) -> SensorExecutionData:
        sensor = next(s for s in current.sensors if s.name == f'{spec.key}_huggingface_sensor')
        definitions = Definitions(assets=current.assets, jobs=current.jobs, sensors=current.sensors)
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            return sensor.evaluate_tick(context)

    request = evaluate(bundle).run_requests[0]
    publisher = next(
        job for job in bundle.jobs if job.name == f'publish_{spec.key}_huggingface_job'
    )
    run = instance.create_run_for_job(publisher, tags=request.tags)
    instance.report_run_failed(run, message='Retry policy fault injection.')
    identity = request.tags['origo_source_event']
    store.record_run_receipt(identity, 0, 'FAILURE', run.run_id)
    instance.delete_run(run.run_id)

    delayed = evaluate(bundle)
    assert delayed.run_requests == []
    assert delayed.skip_message == 'Source retry delay has not elapsed.'
    elapsed = build_source_bundle(
        replace(spec, orchestration=replace(spec.orchestration, retry_delay=0))
    )
    retry = evaluate(elapsed).run_requests[0]
    assert retry.tags['origo_source_attempt'] == '1'
    retry_run = instance.create_run_for_job(publisher, tags=retry.tags)
    instance.report_run_failed(retry_run, message='Retry budget fault injection.')
    store.record_run_receipt(identity, 1, 'FAILURE', retry_run.run_id)
    instance.delete_run(retry_run.run_id)
    exhausted = evaluate(elapsed)
    assert exhausted.run_requests == []
    assert exhausted.skip_message.startswith('Automatic source attempts exhausted;')


def test_source_inventory_is_explicit() -> None:
    assert SPOT_CASE.inventory == (
        'time_1m',
        'time_15m',
        'time_30m',
        'time_1h',
        'time_2h',
        'time_4h',
        'dollar_1M',
        'dollar_15M',
        'dollar_30M',
        'dollar_60M',
        'dollar_120M',
        'dollar_240M',
    )
    assert tuple(series.name for series in SPOT_SERIES_SPECS) == SPOT_CASE.inventory
