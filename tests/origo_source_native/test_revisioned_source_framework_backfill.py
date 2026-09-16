from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    ExecuteInProcessResult,
    RunRequest,
)
from dagster._core.remote_origin import RemoteRepositoryOrigin

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import capacity
from origo.sources.adapters import binance_daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import BuildContext, RolloutStage, SourceBundle
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response

ASSET = 'build_binance_spot_trades_canonical_revision_origo'
DAY = '2017-08-17'
BackfillEnv = tuple[SourceRuntime, DagsterInstance, SourceBundle]


def _repository_origin() -> RemoteRepositoryOrigin:
    from dagster._core.remote_origin import (
        ManagedGrpcPythonEnvCodeLocationOrigin,
    )
    from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

    return RemoteRepositoryOrigin(
        ManagedGrpcPythonEnvCodeLocationOrigin(
            LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'
        ),
        '__repository__',
    )


def _start_monitors(instance: DagsterInstance) -> None:
    from dagster._core.remote_origin import RemoteInstigatorOrigin
    from dagster._core.scheduler.instigation import (
        InstigatorState,
        InstigatorStatus,
        InstigatorType,
        SensorInstigatorData,
    )

    for role in ('reconciliation', 'failure'):
        instance.add_instigator_state(
            InstigatorState(
                RemoteInstigatorOrigin(_repository_origin(), f'binance_spot_trades_{role}_sensor'),
                InstigatorType.SENSOR,
                InstigatorStatus.RUNNING,
                SensorInstigatorData(),
            )
        )


@pytest.fixture
def backfill_env(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SourceRuntime, DagsterInstance, SourceBundle]]:
    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
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
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    runtime.setup()
    (tmp_path / 'dagster').mkdir()
    try:
        with DagsterInstance.local_temp(
            str(tmp_path / 'dagster'),
            overrides={'python_logs': {'managed_python_loggers': [''], 'python_log_level': 'INFO'}},
        ) as instance:
            _start_monitors(instance)
            yield runtime, instance, build_source_bundle(spec)
    finally:
        client.disconnect()


def _run(
    environment: BackfillEnv, *, day: str = DAY, reconcile: bool = False, probe: bool = False
) -> ExecuteInProcessResult:
    _runtime, instance, source = environment
    job = next(
        job for job in source.jobs if job.name == 'refresh_binance_spot_trades_canonical_source_job'
    )
    return job.execute_in_process(
        instance=instance,
        partition_key=day,
        raise_on_error=False,
        run_config={
            'ops': {ASSET: {'config': {'capacity_probe': probe, 'reconcile_only': reconcile}}}
        },
    )


def test_native_dagit_backfill_uses_daily_partitions_and_one_run_per_day() -> None:
    assert BINANCE_SPOT_TRADES_SPEC.rollout_stage == RolloutStage.CANARY
    source = build_source_bundle(BINANCE_SPOT_TRADES_SPEC)
    asset = next(asset for asset in source.assets if asset.key == AssetKey(ASSET))
    assert isinstance(asset.partitions_def, DailyPartitionsDefinition)
    assert asset.partitions_def.get_first_partition_key() == DAY
    from dagster._core.definitions.partitions.context import partition_loading_context

    with partition_loading_context(effective_dt=datetime(2020, 1, 3, tzinfo=UTC)):
        assert asset.partitions_def.get_last_partition_key() == '2020-01-02'
    assert asset.backfill_policy.max_partitions_per_run == 1
    assert all(
        schedule.default_status == DefaultScheduleStatus.RUNNING for schedule in source.schedules
    )
    assert all(sensor.default_status == DefaultSensorStatus.RUNNING for sensor in source.sensors)
    assert 'binance_spot_trades_reconciliation_sensor' in {sensor.name for sensor in source.sensors}
    assert (
        next(
            job
            for job in source.jobs
            if job.name == 'refresh_binance_spot_trades_canonical_source_job'
        ).partitions_def
        == asset.partitions_def
    )


def test_backfill_compares_real_archive_with_legacy_and_records_proof(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections import Counter

    from origo.sources.contracts import ComponentSpec, Partition, SourceError

    runtime, instance, _source = backfill_env
    archives = []
    retained_reads = Counter()
    validate = SourceStore.validate_component

    def counted_archive(url: str) -> binance_daily.Response:
        if url.endswith('.zip'):
            archives.append(url)
        return archive_response(url)

    def counted_validation(
        self: SourceStore,
        component: ComponentSpec,
        table: str,
        partition: Partition,
        *,
        predicate: str = '1',
        params: object | None = None,
        legacy_hash: bool = False,
    ) -> tuple[int, str]:
        if table.endswith('_revisions'):
            retained_reads[component.key] += 1
        return validate(
            self,
            component,
            table,
            partition,
            predicate=predicate,
            params=params,
            legacy_hash=legacy_hash,
        )

    with monkeypatch.context() as patch:
        patch.setattr(binance_daily, 'get_response', counted_archive)
        patch.setattr(SourceStore, 'validate_component', counted_validation)
        result = _run(backfill_env, day='2020-01-01', probe=True)
    assert len(archives) == 1
    assert dict(retained_reads) == {
        key: 2 for key in ('raw', 'time', 'dollar', 'volume', 'tick', 'imbalance', 'aligned')
    }
    assert result.success
    rows = runtime.store.execute(
        'SELECT partition_key, checks_json, dagster_run_id FROM origo.source_parity_log'
    )
    assert len(rows) == 1
    assert rows[0][0] == '2020-01-01' and rows[0][2] == result.run_id
    checks = json.loads(rows[0][1])
    assert set(checks) == {'raw', 'time', 'dollar', 'volume', 'tick', 'imbalance', 'aligned'}
    assert checks['raw']['row_count'] == 194010
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {'2020-01-01'}
    from dagster._core.definitions.automation_tick_evaluation_context import (
        build_run_requests_with_backfill_policies,
    )
    from dagster._core.definitions.events import AssetKeyPartitionKey

    asset = next(asset for asset in _source.assets if asset.key == AssetKey(ASSET))
    definitions = Definitions(assets=[asset])
    requests = build_run_requests_with_backfill_policies(
        [AssetKeyPartitionKey(asset.key, key) for key in (DAY, '2020-01-01')],
        definitions.resolve_asset_graph(),
        instance,
    )
    assert len(requests) == 2
    native_job = definitions.get_implicit_global_asset_job_def()
    for request in requests:
        assert request.partition_key is None
        assert (
            request.tags['dagster/asset_partition_range_start']
            == request.tags['dagster/asset_partition_range_end']
        )
        assert native_job.execute_in_process(
            instance=instance, tags=request.tags, asset_selection=request.asset_selection
        ).success
    assert instance.get_materialized_partitions(asset.key) == {DAY, '2020-01-01'}

    messages = '\n'.join(entry.user_message for entry in instance.all_logs(result.run_id))
    assert 'phase=legacy_comparison' in messages and 'phase=verified' in messages

    # Duplicate an actual archive row exactly at the seek-page boundary.
    record = next(
        r for r in runtime.store.records(canonical_only=True) if r.partition.key == '2020-01-01'
    )
    runtime.store.execute(
        'INSERT INTO origo.binance_spot_trades_raw_revisions SELECT * FROM '
        'origo.binance_spot_trades_raw_revisions WHERE build_id=%(build)s '
        'ORDER BY datetime, trade_id LIMIT 1 OFFSET 49999',
        {'build': record.build_id},
    )
    raw = next(c for c in runtime.spec.components if c.key == 'raw')
    with pytest.raises(SourceError, match='duplicate'):
        runtime.store.validate_component(
            raw,
            runtime.store.component_table('raw'),
            record.partition,
            predicate='build_id=%(build)s',
            params={'build': record.build_id},
        )


@pytest.mark.parametrize('kind', ['implicit', 'refresh_range'])
def test_reconciliation_restores_committed_state_after_worker_loss(
    backfill_env: BackfillEnv,
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster import build_run_status_sensor_context

    runtime, instance, source = backfill_env
    asset = next(asset for asset in source.assets if asset.key == AssetKey(ASSET))
    native_job = (
        Definitions(assets=[asset]).get_implicit_global_asset_job_def()
        if kind == 'implicit'
        else next(
            job
            for job in source.jobs
            if job.name == 'refresh_binance_spot_trades_canonical_source_job'
        )
    )
    from origo.sources import bundle
    from origo.sources.contracts import RevisionedSourceSpec

    original = bundle.execute_source
    with monkeypatch.context() as patch:

        def worker_lost(
            spec: RevisionedSourceSpec,
            operation: str,
            config: bundle.SourceRunConfig,
            *,
            run_id: str,
        ) -> dict[str, object]:
            original(spec, operation, config, run_id=run_id)
            raise OSError(
                'Injected worker loss after committed data and proof, before Dagit materialization'
            )

        patch.setattr(bundle, 'execute_source', worker_lost)
        failed = native_job.execute_in_process(
            instance=instance,
            raise_on_error=False,
            asset_selection=[asset.key],
            tags={
                'dagster/asset_partition_range_start': DAY,
                'dagster/asset_partition_range_end': DAY,
            },
        )
    assert not failed.success
    record = runtime.store.records(canonical_only=True)[0]
    assert instance.get_materialized_partitions(asset.key) == set()
    observer = next(sensor for sensor in source.sensors if sensor.name.endswith('_failure_sensor'))
    run = instance.get_run_by_id(failed.run_id)
    with build_run_status_sensor_context(
        sensor_name=observer.name,
        dagster_event=next(
            event for event in failed.all_events if event.event_type_value == 'PIPELINE_FAILURE'
        ),
        dagster_instance=instance,
        dagster_run=run,
    ) as context:
        observer(context)
    assert runtime.store.execute(
        "SELECT operation, partition_key, dagster_run_id FROM origo.source_failure_log WHERE error_code='RUN_FAILED'"
    ) == [('canonical', DAY, failed.run_id)]
    result = _run(backfill_env, reconcile=True)
    assert result.success and _status(backfill_env) == 'MATERIALIZED'
    assert runtime.store.records(canonical_only=True) == (record,)
    assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
    assert runtime.store.execute(
        "SELECT argMax(event_type, event_time) FROM origo.source_failure_log WHERE error_code='RUN_FAILED'"
    ) == [('RECOVERED',)]


def _status(environment: BackfillEnv, day: str = DAY) -> str | None:
    _runtime, instance, source = environment
    asset = next(asset for asset in source.assets if asset.key == AssetKey(ASSET))
    status = instance.get_status_by_partition(asset.key, [day], asset.partitions_def)[day]
    return status.value if status else None


def test_parity_mismatch_fails_partition_and_is_logged(backfill_env: BackfillEnv) -> None:
    runtime, instance, _source = backfill_env
    original = next(component for component in runtime.spec.components if component.key == 'time')

    def missing_bar(context: BuildContext) -> None:
        original.build(context)
        # Fault injection deletes a real computed bar; no invented market row is added.
        context.client.execute(
            f'ALTER TABLE {context.table("time")} DELETE WHERE datetime IN '
            f'(SELECT min(datetime) FROM {context.table("time")})',
            settings={'mutations_sync': 2},
        )

    spec = replace(
        runtime.spec,
        components=tuple(
            replace(component, build=missing_bar) if component.key == 'time' else component
            for component in runtime.spec.components
        ),
    )
    environment = (runtime, instance, build_source_bundle(spec))
    result = _run(environment, probe=True)
    assert not result.success
    assert _status(environment) == 'FAILED'
    assert runtime.store.execute('SELECT count() FROM origo.source_parity_log') == [(0,)]
    assert runtime.store.execute(
        "SELECT count() FROM origo.source_failure_log WHERE error_code='LEGACY_PARITY_MISMATCH'"
    ) == [(1,)]
    messages = '\n'.join(entry.user_message for entry in instance.all_logs(result.run_id))
    assert 'LEGACY_PARITY_MISMATCH' in messages and 'component=time' in messages
    assert runtime.store.execute('SELECT countIf(successful) FROM origo.source_capacity_log') == [
        (0,)
    ]
    # Retry through the normal Dagit job after restoring the correct implementation.
    assert _run(backfill_env, probe=True).success
    assert _status(backfill_env) == 'MATERIALIZED'


def test_reconciliation_marks_corrupt_missing_and_changed_generations(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, instance, _source = backfill_env
    assert _run(backfill_env, probe=True).success
    first = runtime.store.records(canonical_only=True)[0]
    assert _status(backfill_env) == 'MATERIALIZED'
    runtime.store.execute(
        'ALTER TABLE origo.binance_spot_trades_raw_revisions DELETE WHERE trade_id=0 SETTINGS mutations_sync=2'
    )
    failed = _run(backfill_env, reconcile=True)
    assert not failed.success and _status(backfill_env) == 'FAILED'
    repaired = runtime.repair(DAY)
    assert repaired.generation > first.generation
    assert _run(backfill_env, reconcile=True).success
    assert _status(backfill_env) == 'MATERIALIZED'
    runtime.store.execute(
        "ALTER TABLE origo.source_activation_log DELETE WHERE partition_key='2017-08-17' SETTINGS mutations_sync=2"
    )
    assert not _run(backfill_env, reconcile=True).success
    assert _status(backfill_env) == 'FAILED'
    restored = runtime.build(DAY)
    assert _run(backfill_env, reconcile=True).success
    assert _status(backfill_env) == 'MATERIALIZED'
    events = instance.all_logs(failed.run_id)
    assert any('incomplete' in entry.user_message for entry in events)
    latest = instance.get_latest_materialization_event(AssetKey(ASSET))
    state = latest.asset_materialization.metadata['source_state'].value
    assert state['build_id'] == str(restored.build_id)
    assert state['generation'] == restored.generation

    import hashlib
    import io
    import zipfile

    from .test_binance_daily_source_adapter import ARCHIVES

    # Fault injection repackages unmodified official CSV bytes; no market rows are invented.
    packaged = io.BytesIO()
    with zipfile.ZipFile(packaged, 'w', compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(
            f'BTCUSDT-trades-{DAY}.csv', (ARCHIVES / f'BTCUSDT-trades-{DAY}.csv').read_bytes()
        )
    body = packaged.getvalue()
    checksum = hashlib.sha256(body).hexdigest()

    def correction(url: str) -> binance_daily.Response:
        payload = (
            f'{checksum}  BTCUSDT-trades-{DAY}.zip'.encode() if url.endswith('CHECKSUM') else body
        )
        return binance_daily.Response(payload, {}, 200)

    monkeypatch.setattr(binance_daily, 'get_response', correction)
    corrected = runtime.build(DAY)
    assert corrected.revision != restored.revision
    assert _run(backfill_env, reconcile=True).success
    latest = instance.get_latest_materialization_event(AssetKey(ASSET))
    assert latest.asset_materialization.metadata['source_state'].value['revision'] == checksum
    runtime.rollback(
        restored, operator='test', reason='Exercise quarantined older revision', quarantine=True
    )
    assert not _run(backfill_env, reconcile=True).success
    assert _status(backfill_env) == 'FAILED'
    runtime.build(DAY)
    assert _run(backfill_env, reconcile=True).success
    rolled_back = runtime.rollback(
        corrected, operator='test', reason='Verify retained activation visibility'
    )
    assert rolled_back.generation > corrected.generation
    assert _run(backfill_env, reconcile=True).success
    latest = instance.get_latest_materialization_event(AssetKey(ASSET))
    state = latest.asset_materialization.metadata['source_state'].value
    assert state['build_id'] == str(rolled_back.build_id)
    assert state['generation'] == rolled_back.generation
    assert instance.get_run_by_id(failed.run_id).status.value == 'FAILURE'


def test_source_failures_and_recoveries_flow_into_dagit_logs(backfill_env: BackfillEnv) -> None:
    runtime, instance, source = backfill_env
    origin = runtime.run_id
    runtime.failures.record(
        operation='discovery', error_code='PROVIDER_HTTP_503', scope='PARTITION', partition=DAY
    )
    health = next(
        job for job in source.jobs if job.name == 'reconcile_binance_spot_trades_source_origo_job'
    )
    failed = health.execute_in_process(instance=instance, raise_on_error=False)
    assert failed.success
    assert not failed.get_asset_check_evaluations()[0].passed
    messages = '\n'.join(entry.user_message for entry in instance.all_logs(failed.run_id))
    assert f'origin_run={origin}' in messages
    assert 'PROVIDER_HTTP_503' in messages and 'event=FAILED' in messages
    runtime.failures.recover(operation='discovery', partition=DAY)
    recovered = health.execute_in_process(instance=instance, raise_on_error=False)
    assert recovered.success
    messages = '\n'.join(entry.user_message for entry in instance.all_logs(recovered.run_id))
    assert 'PROVIDER_HTTP_503' in messages and 'event=RECOVERED' in messages
    assert runtime.store.execute(
        "SELECT count() FROM origo.source_run_log WHERE status='LOGGED'"
    ) == [(2,)]


def test_capacity_probe_and_limits_gate_historical_work(
    backfill_env: BackfillEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, instance, _source = backfill_env
    attempted = []

    def no_provider(url: str) -> binance_daily.Response:
        attempted.append(url)
        raise AssertionError('Capacity-blocked work contacted its provider.')

    monkeypatch.setattr(binance_daily, 'get_response', no_provider)
    with monkeypatch.context() as patch:
        patch.setattr(capacity._Volume, 'sample', lambda self: (10**12, 10**11, 10**8, 9 * 10**7))
        assert not _run(backfill_env).success
    assert not attempted
    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    assert _run(backfill_env, probe=True).success
    assert runtime.store.execute(
        'SELECT min(working_set_bytes)>0 FROM origo.source_capacity_log'
    ) == [(1,)]
    monkeypatch.setattr(binance_daily, 'get_response', no_provider)
    for metrics in [(10**12, 10**11, 10**8, 9 * 10**7), (10**12, 9 * 10**11, 10**8, 10**6)]:
        monkeypatch.setattr(capacity._Volume, 'sample', lambda self: metrics)
        result = _run(backfill_env)
        assert not result.success and not attempted
        assert any(
            'CAPACITY_RESERVE_BREACHED' in entry.user_message
            for entry in instance.all_logs(result.run_id)
        )


def test_backfill_resume_skips_verified_generations_and_retries_failed_days(
    backfill_env: BackfillEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, instance, source = backfill_env
    assert _run(backfill_env, probe=True).success
    first = runtime.store.records(canonical_only=True)[0]
    proof_count = runtime.store.execute('SELECT count() FROM origo.source_parity_log')
    from origo.sources.contracts import SourceError

    def unavailable(url: str) -> binance_daily.Response:
        raise SourceError('PROVIDER_HTTP_503', 'Provider returned HTTP 503.')

    monkeypatch.setattr(binance_daily, 'get_response', unavailable)
    assert not _run(backfill_env).success
    assert not _run(backfill_env, reconcile=True).success
    assert _status(backfill_env) == 'FAILED'

    def checksum_only(url: str) -> binance_daily.Response:
        assert url.endswith('.CHECKSUM'), (
            'Verified generations must not download the archive again.'
        )
        return archive_response(url)

    monkeypatch.setattr(binance_daily, 'get_response', checksum_only)
    assert _run(backfill_env).success
    assert runtime.store.records(canonical_only=True) == (first,)
    assert runtime.store.execute('SELECT count() FROM origo.source_parity_log') == proof_count
    from dagster import build_sensor_context

    sensor = next(
        sensor for sensor in source.sensors if sensor.name.endswith('_reconciliation_sensor')
    )
    with build_sensor_context(
        instance=instance,
        definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
    ) as context:
        requests = sensor.evaluate_tick(context).run_requests
    assert all(request.partition_key is None for request in requests)
    from datetime import timedelta, tzinfo

    from origo.sources import dagit

    class TickClock:
        @staticmethod
        def now(zone: tzinfo) -> datetime:
            return datetime.now(zone) + timedelta(days=1)

    monkeypatch.setattr(dagit, 'datetime', TickClock)
    with build_sensor_context(
        instance=instance,
        definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
    ) as context:
        requests = sensor.evaluate_tick(context).run_requests
    assert any(request.partition_key == DAY for request in requests)
    request = next(request for request in requests if request.partition_key == DAY)
    assert request.run_config['ops'][ASSET]['config']['reconcile_only'] is True
    job = next(job for job in source.jobs if job.name == request.job_name)
    from dagster import DagsterRunStatus
    from dagster._core.remote_origin import RemoteJobOrigin

    queued = instance.create_run_for_job(
        job,
        status=DagsterRunStatus.QUEUED,
        tags=request.tags,
        remote_job_origin=RemoteJobOrigin(_repository_origin(), job.name),
    )
    with build_sensor_context(
        instance=instance,
        definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
    ) as context:
        followup = sensor.evaluate_tick(context).run_requests
    assert len(followup) == 1 and followup[0].partition_key is None
    instance.report_run_canceled(queued)
    assert job.execute_in_process(
        instance=instance, partition_key=request.partition_key, run_config=request.run_config
    ).success
    assert _status(backfill_env) == 'MATERIALIZED'


def test_native_backfill_queue_bounds_parallel_canonical_runs(tmp_path: Path) -> None:
    from dagster import DagsterRunStatus, RunsFilter
    from dagster._core.instance.config import PoolGranularity
    from dagster._core.op_concurrency_limits_counter import GlobalOpConcurrencyLimitsCounter
    from dagster._core.remote_origin import RemoteJobOrigin

    source = build_source_bundle(BINANCE_SPOT_TRADES_SPEC)
    job = next(
        job for job in source.jobs if job.name.startswith('refresh_') and 'canonical' in job.name
    )
    root = tmp_path / 'queue'
    root.mkdir()
    with DagsterInstance.local_temp(
        str(root), overrides={'concurrency': {'pools': {'default_limit': 1, 'granularity': 'run'}}}
    ) as instance:
        first = instance.create_run_for_job(
            job, status=DagsterRunStatus.STARTED, tags={'dagster/partition': DAY}
        )
        second = instance.create_run_for_job(
            job,
            status=DagsterRunStatus.QUEUED,
            tags={'dagster/partition': '2020-01-01'},
            remote_job_origin=RemoteJobOrigin(_repository_origin(), job.name),
        )
        pool = 'binance_spot_trades_canonical'
        assert first.run_op_concurrency.all_pools == {pool}
        instance.event_log_storage.set_concurrency_slots(pool, 2)
        records = instance.get_run_records(RunsFilter(run_ids=[first.run_id]))
        counter = GlobalOpConcurrencyLimitsCounter(
            instance,
            [second],
            records,
            {pool},
            instance.event_log_storage.get_pool_limits(),
            pool_granularity=PoolGranularity.RUN,
        )
        assert not counter.is_blocked(second)
        instance.event_log_storage.set_concurrency_slots(pool, 1)
        limited = GlobalOpConcurrencyLimitsCounter(
            instance,
            [second],
            records,
            {pool},
            instance.event_log_storage.get_pool_limits(),
            pool_granularity=PoolGranularity.RUN,
        )
        assert limited.is_blocked(second)
        released = GlobalOpConcurrencyLimitsCounter(
            instance,
            [second],
            [],
            {pool},
            instance.event_log_storage.get_pool_limits(),
            pool_granularity=PoolGranularity.RUN,
        )
        assert not released.is_blocked(second)


def test_canonical_job_fails_before_provider_io_when_preparation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from origo.sources import bundle, prepare
    from origo.sources.contracts import RevisionedSourceSpec, SourceError

    def unavailable(spec: RevisionedSourceSpec, instance: DagsterInstance) -> None:
        raise SourceError('PREPARATION_UNAVAILABLE', 'Source preparation unavailable.')

    monkeypatch.setattr(prepare, 'prepare_source', unavailable)

    def forbidden() -> None:
        raise AssertionError('Unmonitored backfill must not construct external settings.')

    monkeypatch.setattr(bundle, 'get_clickhouse_settings', forbidden)
    source = build_source_bundle(BINANCE_SPOT_TRADES_SPEC)
    job = next(
        job for job in source.jobs if job.name == 'refresh_binance_spot_trades_canonical_source_job'
    )
    root = tmp_path / 'instance'
    root.mkdir()
    with DagsterInstance.local_temp(str(root)) as instance:
        result = job.execute_in_process(instance=instance, partition_key=DAY, raise_on_error=False)
        assert not result.success
        assert any(
            'PREPARATION_UNAVAILABLE' in event.user_message
            for event in instance.all_logs(result.run_id)
        )


def test_system_logging_survives_database_failure(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import logging
    import sys

    from origo.sources import dagit
    from origo.sources.contracts import Row

    _runtime, instance, source = backfill_env
    health = next(
        job for job in source.jobs if job.name == 'reconcile_binance_spot_trades_source_origo_job'
    )
    original = dagit.observe_source

    def emit_system_logs(runtime: SourceRuntime) -> dict[str, object]:
        logging.getLogger('clickhouse_driver.test').warning('Injected driver warning reaches Dagit')
        print('Injected source stdout reaches compute logs', flush=True)
        print('Injected source stderr reaches compute logs', file=sys.stderr, flush=True)
        return original(runtime)

    monkeypatch.setattr(dagit, 'observe_source', emit_system_logs)
    result = health.execute_in_process(instance=instance, raise_on_error=False)
    assert result.success
    assert any(
        'Injected driver warning reaches Dagit' in entry.user_message
        for entry in instance.all_logs(result.run_id)
    )
    captured = next(
        event.logs_captured_data
        for event in result.all_events
        if event.event_type_value == 'LOGS_CAPTURED'
    )
    log_key = instance.compute_log_manager.build_log_key_for_run(result.run_id, captured.file_key)
    data = instance.compute_log_manager.get_log_data(log_key)
    assert b'Injected source stdout reaches compute logs' in data.stdout
    assert b'Injected source stderr reaches compute logs' in data.stderr

    def unavailable(_self: SourceStore, query: str, params: object | None = None) -> list[Row]:
        raise OSError('Injected database outage')

    def failed_persistence(runtime: SourceRuntime) -> dict[str, object]:
        patch.setattr(SourceStore, 'execute', unavailable)
        runtime.failures.record(
            operation='cleanup', error_code='INJECTED_DATABASE_OUTAGE', scope='NONE', partition=DAY
        )
        raise AssertionError('Failure persistence unexpectedly succeeded')

    with monkeypatch.context() as patch:
        patch.setattr(dagit, 'observe_source', failed_persistence)
        failed = health.execute_in_process(instance=instance, raise_on_error=False)
    assert not failed.success
    messages = '\n'.join(entry.user_message for entry in instance.all_logs(failed.run_id))
    assert 'INJECTED_DATABASE_OUTAGE' in messages and 'event=FAILED' in messages
    assert f'partition={DAY}' in messages and 'operation=cleanup' in messages
    assert 'OSError' in messages
    assert health.execute_in_process(instance=instance, raise_on_error=False).success


def test_reconciliation_failures_do_not_starve_other_partitions() -> None:
    from datetime import timedelta

    from origo.sources.dagit import _reconciliation_selection

    # Partition scheduling state only: no market rows are generated.
    keys = [
        (datetime.fromisoformat(DAY) + timedelta(days=index)).date().isoformat()
        for index in range(12)
    ]
    urgent = keys[:8]
    selected = [_reconciliation_selection(keys, urgent, tick) for tick in range(len(keys))]
    assert all(len(batch) <= 5 for batch in selected)
    assert set().union(*(set(batch) for batch in selected)) == set(keys)
    assert set().union(*(set(batch) for batch in selected[:2])) >= set(urgent)


@pytest.mark.parametrize(
    'error_code',
    [
        'LEGACY_PARITY_MISMATCH',
        'RETAINED_CONTENT_INVALID',
        'ACTIVE_PARTITION_MISSING',
        'INGESTION_FAILURE_UNRESOLVED',
        'CAPACITY_MEASUREMENT_REQUIRED',
    ],
)
def test_failed_automatic_verification_waits_for_operator_or_state_change(
    backfill_env: BackfillEnv,
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
) -> None:
    from datetime import timedelta, tzinfo

    from dagster import build_sensor_context

    from origo.sources import dagit
    from origo.sources.contracts import Client, SourceError, StateRecord

    runtime, instance, original_source = backfill_env
    assert _run(backfill_env, probe=True).success
    record = runtime.store.records(canonical_only=True)[0]
    runtime.store.execute(
        'ALTER TABLE origo.source_parity_log DELETE WHERE 1 SETTINGS mutations_sync=2'
    )
    attempts = []

    def fail_verification(client: Client, database: str, record: StateRecord) -> dict[str, object]:
        attempts.append(record)
        raise SourceError(error_code, 'Injected verification failure on retained real market data')

    source = build_source_bundle(replace(runtime.spec, verify=fail_verification))
    sensor = next(
        sensor for sensor in source.sensors if sensor.name.endswith('_reconciliation_sensor')
    )
    now = datetime.now(UTC) + timedelta(days=1)

    class TickClock:
        @staticmethod
        def now(zone: tzinfo) -> datetime:
            return now.astimezone(zone)

    monkeypatch.setattr(dagit, 'datetime', TickClock)
    cursor = None

    def requests() -> list[RunRequest]:
        nonlocal cursor
        with build_sensor_context(
            instance=instance,
            cursor=cursor,
            definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
        ) as context:
            result = sensor.evaluate_tick(context)
            cursor = result.cursor
            return result.run_requests

    request = next(request for request in requests() if request.partition_key == DAY)
    job = next(job for job in source.jobs if job.name == request.job_name)
    failed = job.execute_in_process(
        instance=instance,
        partition_key=DAY,
        run_config=request.run_config,
        tags=request.tags,
        raise_on_error=False,
    )
    assert not failed.success and _status(backfill_env) == 'FAILED'
    assert len(attempts) == 1
    failure_count = runtime.store.execute('SELECT count() FROM origo.source_failure_log')
    run_count = len(instance.get_runs())
    for _ in range(10):
        now += timedelta(minutes=1)
        followup = requests()
        assert len(followup) == 1 and followup[0].partition_key is None
    assert len(attempts) == 1
    assert runtime.store.execute('SELECT count() FROM origo.source_failure_log') == failure_count
    assert len(instance.get_runs()) == run_count
    assert _status(backfill_env) == 'FAILED'

    advanced = runtime.rollback(record, operator='test', reason='Test state-change admission')
    now += timedelta(minutes=1)
    request = next(request for request in requests() if request.partition_key == DAY)
    assert request.tags['origo_source_authority'].endswith(f':{advanced.generation}')
    assert not job.execute_in_process(
        instance=instance,
        partition_key=DAY,
        run_config=request.run_config,
        tags=request.tags,
        raise_on_error=False,
    ).success
    assert len(attempts) == 2
    now += timedelta(minutes=1)
    assert all(request.partition_key is None for request in requests())

    # An explicit successful operator verification resumes checks without changing generation.
    assert _run((runtime, instance, original_source), reconcile=True).success
    assert runtime.store.records(canonical_only=True)[0] == advanced
    now += timedelta(minutes=1)
    assert any(request.partition_key == DAY for request in requests())
    assert instance.get_run_by_id(failed.run_id).status.value == 'FAILURE'


@pytest.mark.parametrize('kind', ['implicit', 'backfill_alias', 'canonical', 'refresh_range'])
def test_reconciliation_waits_for_native_partition_runs(
    backfill_env: BackfillEnv,
    kind: str,
) -> None:
    from dagster import DagsterRunStatus, build_sensor_context
    from dagster._core.remote_origin import RemoteJobOrigin

    runtime, instance, source = backfill_env
    runtime.build(DAY)
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == set()
    asset = next(asset for asset in source.assets if asset.key == AssetKey(ASSET))
    if kind == 'implicit':
        job = Definitions(assets=[asset]).get_implicit_global_asset_job_def()
    else:
        name = (
            'backfill_binance_spot_trades_source_job'
            if kind == 'backfill_alias'
            else 'refresh_binance_spot_trades_canonical_source_job'
        )
        job = next(job for job in source.jobs if job.name == name)
    tags = (
        {'dagster/partition': DAY}
        if kind == 'canonical'
        else {'dagster/asset_partition_range_start': DAY, 'dagster/asset_partition_range_end': DAY}
    )
    sensor = next(
        sensor for sensor in source.sensors if sensor.name.endswith('_reconciliation_sensor')
    )

    def requests() -> list[RunRequest]:
        with build_sensor_context(
            instance=instance,
            definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
        ) as context:
            return sensor.evaluate_tick(context).run_requests

    # Other source jobs with the same date tag do not own this canonical partition.
    health = next(
        job for job in source.jobs if job.name == 'reconcile_binance_spot_trades_source_origo_job'
    )
    instance.create_run_for_job(health, status=DagsterRunStatus.STARTED, tags=tags)
    assert any(request.partition_key == DAY for request in requests())
    for status in (DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED):
        run = instance.create_run_for_job(
            job,
            status=status,
            tags=tags,
            asset_selection={asset.key} if kind == 'implicit' else None,
            remote_job_origin=RemoteJobOrigin(_repository_origin(), job.name),
        )
        followup = requests()
        assert all(request.partition_key is None for request in followup)
        instance.report_run_failed(run)
        assert any(request.partition_key == DAY for request in requests())
