from dataclasses import replace
from datetime import UTC, datetime
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

from origo.sources import capacity
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import SourceBundle
from origo.sources.prepare import prepare_source
from origo.sources.storage import SourceStore

from .test_backfill_job import ready_job as ready_job

DAY = '2017-08-17'
ASSET = 'build_binance_spot_trades_canonical_revision_origo'


def _selection(end: str = DAY) -> dict[str, str]:
    return {'dagster/asset_partition_range_start': DAY, 'dagster/asset_partition_range_end': end}


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
        assert not backfill_owns_publication(instance, spec)
        instance.update_backfill(pending.with_status(BulkActionStatus.COMPLETED_SUCCESS))
        assert not backfill_owns_publication(instance, spec)


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
