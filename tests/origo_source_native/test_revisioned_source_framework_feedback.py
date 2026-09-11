from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    DagsterRunStatus,
    Definitions,
    RunRequest,
    build_run_status_sensor_context,
    build_sensor_context,
)

from origo.sources import bundle, capacity, dagit
from origo.sources.bundle import SourceRunConfig, build_source_bundle
from origo.sources.contracts import RevisionedSourceSpec, SourceError, StateRecord
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.spot_parity import verify_spot_legacy
from origo.sources.storage import SourceStore, StorageError

from . import test_revisioned_source_framework_backfill as backfill

backfill_env = backfill.backfill_env
BackfillEnv = backfill.BackfillEnv
DAY, ASSET = backfill.DAY, backfill.ASSET
_real_volumes = capacity._volumes
_real_sample = capacity._Volume.sample


def _requests(env: BackfillEnv, cursor: str | None = None) -> tuple[list[RunRequest], str]:
    _, instance, source = env
    sensor = next(s for s in source.sensors if s.name.endswith('_reconciliation_sensor'))
    with build_sensor_context(
        instance=instance,
        cursor=cursor,
        definitions=Definitions(
            assets=source.assets,
            jobs=source.jobs,
            sensors=source.sensors,
        ),
    ) as context:
        tick = sensor.evaluate_tick(context)
        assert tick.cursor is not None
        return tick.run_requests, tick.cursor


def _observe_failure(env: BackfillEnv, result: backfill.ExecuteInProcessResult) -> None:
    _, instance, source = env
    sensor = next(s for s in source.sensors if s.name.endswith('_failure_sensor'))
    with build_run_status_sensor_context(
        sensor_name=sensor.name,
        dagster_instance=instance,
        dagster_run=instance.get_run_by_id(result.run_id),
        dagster_event=next(
            e for e in result.all_events if e.event_type_value == 'PIPELINE_FAILURE'
        ),
    ) as context:
        sensor(context)


def test_native_completion_does_not_enqueue_another_verification(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, instance, source = backfill_env
    original = SourceRuntime.verify
    cursors = []

    def during_run(self: SourceRuntime, key: str) -> tuple[StateRecord, dict[str, object]]:
        requests, cursor = _requests(backfill_env)
        assert all(r.partition_key is None for r in requests)
        cursors.append(cursor)
        return original(self, key)

    monkeypatch.setattr(SourceRuntime, 'verify', during_run)
    asset = next(a for a in source.assets if a.key == AssetKey(ASSET))
    job = Definitions(assets=[asset]).get_implicit_global_asset_job_def()
    result = job.execute_in_process(
        instance=instance,
        asset_selection=[asset.key],
        tags={'dagster/asset_partition_range_start': DAY, 'dagster/asset_partition_range_end': DAY},
        run_config={'ops': {ASSET: {'config': {'capacity_probe': True}}}},
    )
    assert result.success
    for _ in range(3):
        requests, cursor = _requests(backfill_env, cursors[-1])
        cursors.append(cursor)
        assert all(r.partition_key is None for r in requests)
        assert len(cursor) < 100 and 'known' not in cursor
    assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]


@pytest.mark.parametrize('canceled', [False, True])
def test_transient_verification_and_cancellation_retry_without_poisoning_ingestion(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
    canceled: bool,
) -> None:
    runtime, instance, source = backfill_env
    assert backfill._run(backfill_env, probe=True).success
    record = runtime.store.records(canonical_only=True)[0]
    runtime.rollback(record, operator='test', reason='Test reconciliation recovery')
    request = next(r for r in _requests(backfill_env)[0] if r.partition_key == DAY)
    job = next(j for j in source.jobs if j.name == request.job_name)
    if canceled:
        run = instance.create_run_for_job(
            job,
            status=DagsterRunStatus.STARTED,
            tags={**request.tags, 'dagster/partition': DAY},
            run_config=request.run_config,
        )
        instance.report_run_canceled(run)
        run_id = run.run_id
    else:

        def unavailable(
            spec: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
        ) -> dict[str, object]:
            raise ConnectionResetError('Injected transient connection reset before verification')

        with monkeypatch.context() as patch:
            patch.setattr(bundle, 'execute_source', unavailable)
            result = job.execute_in_process(
                instance=instance,
                partition_key=DAY,
                tags=request.tags,
                run_config=request.run_config,
                raise_on_error=False,
            )
        assert not result.success
        _observe_failure(backfill_env, result)
        run_id = result.run_id
        assert runtime.store.execute(
            'SELECT operation FROM origo.source_failure_log WHERE dagster_run_id=%(run)s',
            {'run': run_id},
        ) == [('verification',)]
    assert not instance.get_run_by_id(run_id).tags.get('origo_source_verdict')
    assert all(r.partition_key is None for r in _requests(backfill_env)[0])
    now = datetime.now(UTC) + timedelta(seconds=61)

    class Clock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return now

    monkeypatch.setattr(dagit, 'datetime', Clock)
    retry = next(r for r in _requests(backfill_env)[0] if r.partition_key == DAY)
    assert retry.tags['origo_source_retry_attempt'] == '2'
    # Dagit re-execute inherits this same reconcile-only config.
    inherited = instance.get_run_by_id(run_id).run_config
    assert job.execute_in_process(
        instance=instance, partition_key=DAY, run_config=inherited, tags=retry.tags
    ).success
    assert backfill._status(backfill_env) == 'MATERIALIZED'
    assert all(r.partition_key is None for r in _requests(backfill_env)[0])
    assert (
        runtime.store.execute(
            'SELECT failure_key FROM origo.source_failure_log GROUP BY failure_key '
            "HAVING argMax(event_type, event_time)='FAILED'"
        )
        == []
    )


@pytest.mark.parametrize(
    ('error', 'attempts'),
    [
        (ValueError('Invalid source configuration'), 1),
        (RuntimeError('Invalid component content'), 1),
        (SourceError('SOURCE_LOCK_BUSY', 'Source lock is held'), 1),
        (SourceError('CAPACITY_SAMPLER_STUCK', 'Sampler did not terminate'), 1),
        (SourceError('RETAINED_CONTENT_INVALID', 'Retained content differs'), 1),
        (SourceError('PROVIDER_HTTP_403', 'Provider rejected the request'), 1),
        (SourceError('PROVIDER_HTTP_503', 'Provider unavailable'), 3),
        (SourceError('OFFICIAL_REVISION_CHANGED', 'Republished while building'), 3),
        (SourceError('PARITY_REVISION_CHANGED', 'Republished before comparison'), 3),
    ],
)
def test_actual_retry_policy_only_retries_explicit_transient_errors(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    attempts: int,
) -> None:
    runtime, instance, _ = backfill_env
    spec = replace(
        runtime.spec,
        orchestration=replace(runtime.spec.orchestration, retry_count=2, retry_delay=0),
    )
    source = build_source_bundle(spec)
    calls = []

    def fail(
        spec: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
    ) -> dict[str, object]:
        calls.append(run_id)
        raise error

    monkeypatch.setattr(bundle, 'execute_source', fail)
    result = backfill._run((runtime, instance, source))
    assert not result.success and len(calls) == attempts
    if isinstance(error, SourceError) and error.code in (
        'SOURCE_LOCK_BUSY',
        'CAPACITY_SAMPLER_STUCK',
    ):
        assert not instance.get_run_by_id(result.run_id).tags.get('origo_source_verdict')
    assert sum(e.event_type_value == 'STEP_UP_FOR_RETRY' for e in result.all_events) == attempts - 1
    calls.clear()
    # Config rejection is inside the same terminal boundary, before source I/O.
    job = next(j for j in source.jobs if j.name.startswith('backfill_'))
    result = job.execute_in_process(
        instance=instance,
        partition_key=DAY,
        raise_on_error=False,
        run_config={'ops': {ASSET: {'config': {'partition_key': '2020-01-01'}}}},
    )
    assert not result.success and not calls
    assert not any(e.event_type_value == 'STEP_UP_FOR_RETRY' for e in result.all_events)


def test_repair_preserves_storage_failure_without_rebuilding(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, _ = backfill_env
    record = runtime.build(DAY)
    original = SourceStore.execute

    def fail(
        self: SourceStore, query: str, params: object | None = None
    ) -> list[tuple[object, ...]]:
        if 'SELECT row_count, content_hash' in query:
            raise StorageError('Injected unavailable storage')
        return original(self, query, params)

    monkeypatch.setattr(SourceStore, 'execute', fail)
    with pytest.raises(StorageError, match='unavailable'):
        runtime.repair(DAY)
    assert runtime.store.records(canonical_only=True) == (record,)
    assert runtime.store.execute('SELECT count() FROM origo.source_build_log') == [(1,)]
    assert runtime.store.execute(
        "SELECT count() FROM origo.source_failure_log WHERE error_code='RETAINED_CONTENT_INVALID'"
    ) == [(0,)]


def test_cleanup_failures_preserve_primary_verdict(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, instance, _ = backfill_env
    assert backfill._run(backfill_env, probe=True).success

    def mismatch(self: SourceRuntime, key: str) -> tuple[StateRecord, dict[str, object]]:
        raise SourceError('LEGACY_PARITY_MISMATCH', 'Injected parity failure')

    def failed_sampling(self: capacity.CapacityMonitor, *, successful: bool) -> None:
        self.stop.set()
        self.thread.join(timeout=5)
        raise OSError('Injected capacity persistence failure')

    with monkeypatch.context() as patch:
        patch.setattr(SourceRuntime, 'verify', mismatch)
        patch.setattr(capacity.CapacityMonitor, 'finish', failed_sampling)
        result = backfill._run(backfill_env)
    assert not result.success
    assert (
        instance.get_run_by_id(result.run_id).tags['origo_source_verdict']
        == 'LEGACY_PARITY_MISMATCH'
    )
    messages = '\n'.join(e.user_message for e in instance.all_logs(result.run_id))
    assert (
        'Cleanup failed: capacity measurement' in messages and 'LEGACY_PARITY_MISMATCH' in messages
    )

    class DropFailure:
        def execute(
            self, query: str, params: object | None = None, settings: object | None = None
        ) -> list[tuple[object, ...]]:
            if query.startswith('DROP DATABASE'):
                raise OSError('Injected reference cleanup failure')
            return runtime.store.client.execute(query, params, settings)

        def disconnect(self) -> None:
            raise AssertionError('Verifier must not disconnect the shared client')

    record = runtime.store.records(canonical_only=True)[0]
    corrupt = replace(
        record, component_hashes=tuple((key, '0' * 64) for key, _ in record.component_hashes)
    )
    with pytest.raises(SourceError) as error:
        verify_spot_legacy(DropFailure(), runtime.store.database, corrupt)
    assert error.value.code == 'LEGACY_PARITY_MISMATCH'
    assert 'legacy comparison database also failed' in ' '.join(error.value.__notes__)
    runtime.cleanup_verification(dry_run=False)


def test_health_checks_report_failures_without_creating_run_storm(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, instance, source = backfill_env
    runtime.failures.record(
        operation='discovery', scope='PARTITION', partition=DAY, error_code='PROVIDER_HTTP_503'
    )
    job = next(j for j in source.jobs if j.name == 'reconcile_binance_spot_trades_source_origo_job')
    requests, _ = _requests(backfill_env)
    assert len(requests) == 1 and requests[0].job_name == job.name
    result = job.execute_in_process(instance=instance)
    assert result.success and not result.get_asset_check_evaluations()[0].passed
    now = datetime.now(UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return now

    monkeypatch.setattr(dagit, 'datetime', Clock)
    for _ in range(59):
        now += timedelta(minutes=1)
        assert _requests(backfill_env)[0] == []
    assert runtime.store.execute('SELECT count() FROM origo.source_failure_log') == [(1,)]
    now += timedelta(minutes=2)
    assert len(_requests(backfill_env)[0]) == 1
    runtime.failures.recover(operation='discovery', partition=DAY)
    recovered = job.execute_in_process(instance=instance)
    assert recovered.success and recovered.get_asset_check_evaluations()[0].passed


def test_real_capacity_volume_admission_preconditions(
    backfill_env: BackfillEnv,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, _ = backfill_env
    server = runtime.store.execute('SELECT serverUUID()')[0][0]
    data = tmp_path / 'clickhouse-volume'
    data.mkdir()
    (data / 'uuid').write_text(str(server))
    monkeypatch.setenv('ORIGO_SOURCE_CLICKHOUSE_VOLUME_PATH', str(data))
    monkeypatch.setenv('ORIGO_SOURCE_DAGSTER_VOLUME_PATH', str(tmp_path / 'dagster'))
    volumes = _real_volumes(runtime)
    assert len(volumes) == 3
    assert all(
        str(server) in v.identity and str(v.path.stat().st_dev) in v.identity for v in volumes
    )
    assert _real_sample(volumes[0])[0] > 0
    (data / 'uuid').write_text('00000000-0000-0000-0000-000000000000')
    with pytest.raises(SourceError) as error:
        _real_volumes(runtime)
    assert error.value.code == 'CAPACITY_VOLUME_MISMATCH'
    (data / 'uuid').unlink()
    with pytest.raises(FileNotFoundError):
        _real_volumes(runtime)


def test_cleanup_failure_after_success_is_not_hidden_by_an_outer_exception_handler() -> None:
    from origo.sources.cleanup import preserve_primary_failure

    def fail_cleanup() -> None:
        raise OSError('Cleanup failed after successful work')

    try:
        raise ValueError('An already handled error outside source execution')
    except ValueError:
        with pytest.raises(OSError, match='successful work'):
            with preserve_primary_failure('test cleanup', fail_cleanup):
                assert True
