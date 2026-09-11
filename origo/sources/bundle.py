import hashlib
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast
from uuid import NAMESPACE_URL, uuid5

import dagster
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetsDefinition,
    BackfillPolicy,
    Config,
    DagsterRunStatus,
    DailyPartitionsDefinition,
    DataVersion,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    Failure,
    JobDefinition,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    RunsFilter,
    RunStatusSensorContext,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    get_dagster_logger,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .archive import archive_session
from .capacity import CapacityMonitor
from .cleanup import preserve_primary_failure
from .contracts import (
    RevisionedSourceSpec,
    RolloutStage,
    SourceBundle,
    SourceError,
    failure_code,
    failure_message,
    retryable_source_error,
)
from .lifecycle import SourceRuntime
from .locking import source_lock
from .storage import SourceStore

# Dagster's multiple-output inference recognizes the unspecialized result class.
if TYPE_CHECKING:
    _SourceResult = MaterializeResult[None]
else:
    _SourceResult = MaterializeResult


class _JobFactory(Protocol):
    def __call__(
        self, name: str, *, selection: list[str], tags: dict[str, str]
    ) -> UnresolvedAssetJobDefinition: ...


class _ScheduleFactory(Protocol):
    def __call__(
        self,
        *,
        name: str,
        job: JobDefinition,
        cron_schedule: str,
        execution_timezone: str,
        default_status: DefaultScheduleStatus,
    ) -> Callable[
        [Callable[[ScheduleEvaluationContext], RunRequest | SkipReason | list[RunRequest]]],
        ScheduleDefinition,
    ]: ...


class _SensorFactory(Protocol):
    def __call__(
        self, *, name: str, job: JobDefinition, default_status: DefaultSensorStatus
    ) -> Callable[
        [Callable[[SensorEvaluationContext], RunRequest | SkipReason]], SensorDefinition
    ]: ...


class _FailureSensorFactory(Protocol):
    def __call__(
        self, *, name: str, default_status: DefaultSensorStatus
    ) -> Callable[[Callable[[RunStatusSensorContext], None]], SensorDefinition]: ...


_define_job = cast(_JobFactory, getattr(dagster, 'define_asset_job'))
_schedule = cast(_ScheduleFactory, getattr(dagster, 'schedule'))
_sensor = cast(_SensorFactory, getattr(dagster, 'sensor'))
_failure_sensor = cast(_FailureSensorFactory, getattr(dagster, 'run_failure_sensor'))


class SourceRunConfig(Config):
    partition_key: str = ''
    destination: str = ''
    anchor: str = ''
    dry_run: bool = True
    reconcile_only: bool = False
    capacity_probe: bool = False


def execute_source(
    spec: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
) -> dict[str, object]:
    logger = get_dagster_logger('origo.sources')
    logger.info(
        'source=%s partition=%s operation=%s phase=started run=%s',
        spec.key,
        config.partition_key,
        operation,
        run_id,
    )
    spec.require_enabled(operation)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    runtime = SourceRuntime(
        spec,
        SourceStore(client, settings.database, spec),
        Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
        run_id,
    )
    with preserve_primary_failure('disconnect', client.disconnect):
        try:
            with archive_session():
                result = _execute_operation(runtime, operation, config)
            failure_operation, _scope, partition, consumer = _failure_context(
                operation, config.partition_key
            )
            if not (operation == 'canonical' and config.reconcile_only):
                runtime.failures.recover(
                    operation=failure_operation, partition=partition, consumer=consumer
                )
            logger.info(
                'source=%s partition=%s operation=%s phase=completed run=%s',
                spec.key,
                config.partition_key,
                operation,
                run_id,
            )
            return result
        except Exception as error:
            logger.error(
                'source=%s partition=%s operation=%s code=%s message=%s run=%s',
                spec.key,
                config.partition_key,
                operation,
                failure_code(error),
                failure_message(error),
                run_id,
            )
            raise


def _config_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError('Source job config must contain mapping objects.')
    return cast(dict[str, object], value)


def _failure_context(operation: str, key: str) -> tuple[str, str, str | None, str | None]:
    if operation.startswith('consumer_'):
        return 'consumer', 'CONSUMER', None, operation.removeprefix('consumer_')
    if operation in ('audit', 'cleanup', 'reconcile'):
        return operation, 'NONE', None, None
    if operation == 'setup':
        return operation, 'SOURCE', None, None
    return 'certification' if operation == 'certify' else operation, 'PARTITION', key or None, None


def _execute_operation(
    runtime: SourceRuntime, operation: str, config: SourceRunConfig
) -> dict[str, object]:
    spec = runtime.spec
    if operation == 'setup':
        runtime.setup(anchor=datetime.fromisoformat(config.anchor) if config.anchor else None)
        return {'source_key': spec.key, 'status': 'ready'}
    if operation == 'canonical':
        if not config.partition_key:
            raise ValueError('Source execution requires an explicit partition key.')
        runtime.cleanup_verification(dry_run=False)
        proof = runtime.store.execute(
            f"""SELECT count() FROM {runtime.store.table('source_active_partitions')} a
            INNER JOIN {runtime.store.table('source_parity_log')} p
            USING (source_key, partition_key, revision, build_id, generation)
            WHERE source_key=%(source)s AND partition_key=%(partition)s AND NOT provisional""",
            {'source': spec.key, 'partition': config.partition_key},
        )[0][0]
        if config.capacity_probe and proof:
            raise SourceError(
                'CAPACITY_PROBE_ALREADY_VERIFIED',
                'Select an unverified day for measurement; use normal backfill configuration to retry a verified day.',
            )
        capacity: CapacityMonitor | None = None
        active = any(
            record.partition.key == config.partition_key
            for record in runtime.store.records(canonical_only=True)
        )
        if not config.reconcile_only or (active and not proof):
            try:
                capacity = CapacityMonitor(runtime, probe=config.capacity_probe)
                capacity.check()
            except Exception as error:
                runtime.failures.record(
                    operation='capacity',
                    scope='SOURCE',
                    error_code=failure_code(error),
                    message=failure_message(error),
                )
                raise
        successful = False
        if capacity is not None:
            capacity.start_sampling()

        def finish_capacity() -> None:
            if capacity is not None:
                try:
                    capacity.finish(successful=successful)
                except Exception as error:
                    runtime.failures.record(
                        operation='capacity',
                        scope='SOURCE',
                        error_code=failure_code(error),
                        message=failure_message(error),
                    )
                    raise

        with preserve_primary_failure('capacity measurement', finish_capacity):
            if not config.reconcile_only:
                try:
                    runtime.build(config.partition_key)
                except SourceError as error:
                    if error.code != 'RETAINED_CONTENT_INVALID':
                        raise
                    runtime.repair(config.partition_key)
                if runtime.parity_failed(config.partition_key):
                    runtime.repair(config.partition_key)
            record, checks = runtime.verify(config.partition_key)
            successful = True
        return {
            'partition_key': record.partition.key,
            'revision': record.revision,
            'build_id': str(record.build_id),
            'generation': record.generation,
            'verified_at': datetime.now(UTC).isoformat(),
            'legacy_parity': checks,
        }
    if operation in ('provisional', 'repair'):
        if not config.partition_key:
            raise ValueError('Source execution requires an explicit partition key.')
        record = (
            runtime.repair(config.partition_key)
            if operation == 'repair'
            else runtime.build(config.partition_key, provisional=operation == 'provisional')
        )
        return {
            'partition_key': record.partition.key,
            'revision': record.revision,
            'build_id': str(record.build_id),
            'generation': record.generation,
        }
    if operation == 'cleanup':
        return {
            'verification_databases': list(runtime.cleanup_verification(dry_run=config.dry_run)),
            'build_ids': list(runtime.cleanup(dry_run=config.dry_run)),
            'dry_run': config.dry_run,
        }
    if operation == 'audit':
        changed = runtime.audit()
        failures: list[Exception] = []
        for key in changed:
            try:
                runtime.build(key)
            except (OSError, ValueError, RuntimeError) as error:
                failures.append(error)
        if failures:
            raise ExceptionGroup('Some audited partitions could not be corrected.', failures)
        return {'corrected_partitions': list(changed)}
    if operation == 'reconcile':
        from .dagit import observe_source

        return observe_source(runtime)
    if operation == 'certify':
        return {'state_token': runtime.certify(config.partition_key, review_state='PENDING').token}
    if operation.startswith('consumer_'):
        consumer = operation.removeprefix('consumer_')
        destination = config.destination or f'/opt/origo/shadow/{spec.key}/{consumer}'
        return {'state_token': runtime.publish(consumer, destination).token}
    raise ValueError(f'Unknown source operation: {operation}')


def _source_asset(spec: RevisionedSourceSpec, operation: str, name: str) -> AssetsDefinition:
    @asset(
        name=name,
        group_name=spec.key,
        check_specs=[AssetCheckSpec(name='source_health', asset=name)]
        if operation == 'reconcile'
        else None,
        partitions_def=DailyPartitionsDefinition(
            start_date=spec.partitions.first_day.isoformat(), timezone='UTC'
        )
        if operation == 'canonical'
        else None,
        backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1)
        if operation == 'canonical'
        else None,
        pool=f'{spec.key}_heavy'
        if operation in ('canonical', 'repair', 'cleanup', 'audit', 'certify')
        else None,
        retry_policy=RetryPolicy(
            max_retries=spec.orchestration.retry_count, delay=spec.orchestration.retry_delay
        )
        if operation == 'canonical'
        else None,
    )
    def execute(context: AssetExecutionContext, config: SourceRunConfig) -> _SourceResult:
        try:
            if operation == 'canonical':
                key = context.partition_key
                if config.partition_key and config.partition_key != key:
                    raise ValueError('Run config date must match the selected Dagit partition.')
                if config.capacity_probe and context.run.tags.get('dagster/backfill'):
                    raise ValueError(
                        'A capacity probe must be one explicitly launched day, not a range backfill.'
                    )
                config = SourceRunConfig(
                    partition_key=key,
                    reconcile_only=config.reconcile_only,
                    capacity_probe=config.capacity_probe,
                )
            if operation in ('canonical', 'repair', 'audit'):
                required = {f'{spec.key}_reconciliation_sensor', f'{spec.key}_failure_sensor'}
                running = {
                    state.instigator_name
                    for state in context.instance.all_instigator_state()
                    if state.status.value == 'RUNNING'
                }
                if not required <= running:
                    raise SourceError(
                        'DAGIT_MONITORING_REQUIRED',
                        'Start the source reconciliation and failure sensors in Dagit before backfilling.',
                    )
            result = execute_source(spec, operation, config, run_id=context.run.run_id)
        except Exception as error:
            context.log.error(
                'source=%s partition=%s operation=%s code=%s message=%s',
                spec.key,
                config.partition_key,
                operation,
                failure_code(error),
                failure_message(error),
            )
            if operation == 'canonical':
                # A data/configuration verdict holds automatic verification; worker and
                # transport failures are retried by the sensor after releasing the pool.
                verdict = (
                    isinstance(error, SourceError)
                    and not retryable_source_error(error)
                    and error.code not in ('SOURCE_LOCK_BUSY', 'CAPACITY_SAMPLER_STUCK')
                ) or isinstance(error, (ValueError, TypeError))
                context.instance.add_run_tags(
                    context.run.run_id,
                    {
                        'origo_source_verdict': failure_code(error) if verdict else '',
                    },
                )
            if operation == 'canonical' and (
                config.reconcile_only or not retryable_source_error(error)
            ):
                raise Failure(
                    description=f'{failure_code(error)}: {failure_message(error)}',
                    allow_retries=False,
                ) from error
            raise
        if operation == 'canonical':
            version = hashlib.sha256(
                str((result['revision'], result['build_id'], result['generation'])).encode()
            ).hexdigest()
            return MaterializeResult(
                value=None,
                metadata={
                    'source_state': MetadataValue.json(result),
                    'source_result': MetadataValue.json(result),
                },
                data_version=DataVersion(version),
            )
        return MaterializeResult(
            value=None,
            metadata={'source_result': MetadataValue.json(result)},
            check_results=[
                AssetCheckResult(
                    passed=result['healthy'] is True,
                    check_name='source_health',
                    metadata={
                        'unresolved_failures': MetadataValue.int(
                            int(str(result['unresolved_failures']))
                        )
                    },
                )
            ]
            if operation == 'reconcile'
            else [],
        )

    return execute


def build_source_bundle(spec: RevisionedSourceSpec) -> SourceBundle:
    names = {
        'setup': f'create_{spec.key}_source_origo',
        'canonical': f'build_{spec.key}_canonical_revision_origo',
        'provisional': f'sync_{spec.key}_provisional_origo',
        'repair': f'repair_{spec.key}_source_origo',
        'cleanup': f'cleanup_{spec.key}_revisions_origo',
        'audit': f'audit_{spec.key}_revisions_origo',
        'certify': f'certify_{spec.key}_source_origo',
        'reconcile': f'reconcile_{spec.key}_source_origo',
    }
    if spec.provisional is None:
        del names['provisional']
    names.update(
        {
            f'consumer_{consumer.key}': f'publish_{spec.key}_{consumer.key}'
            for consumer in spec.consumers
        }
    )
    assets = tuple(_source_asset(spec, operation, name) for operation, name in names.items())
    job_names = {operation: f'{name}_job' for operation, name in names.items()}
    job_names['canonical'] = f'refresh_{spec.key}_canonical_source_job'
    if spec.provisional is not None:
        job_names['provisional'] = f'refresh_{spec.key}_provisional_source_job'
    job_names['audit'] = f'audit_{spec.key}_source_job'
    unresolved = tuple(
        _define_job(
            job_names[operation],
            selection=[name],
            tags={
                'origo_source_key': spec.key,
                'origo_source_operation': operation,
            },
        )
        for operation, name in names.items()
    )
    unresolved += (
        _define_job(
            f'backfill_{spec.key}_source_job',
            selection=[names['canonical']],
            tags={'origo_source_key': spec.key, 'origo_source_operation': 'canonical'},
        ),
    )
    definitions = Definitions(assets=assets, jobs=unresolved)
    jobs = tuple(definitions.resolve_job_def(job.name) for job in unresolved)

    def request(
        operation: str,
        key: str,
        context: ScheduleEvaluationContext | SensorEvaluationContext,
        revision: str = '',
    ) -> RunRequest | SkipReason:
        spec.require_enabled('request')
        consumer_key = (
            operation.removeprefix('consumer_') if operation.startswith('consumer_') else None
        )
        identity = (
            f'{spec.key}:consumer:{consumer_key}:{key}'
            if consumer_key
            else f'{spec.key}:{operation}:{key}' + (f':{revision}' if revision else '')
        )
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        runtime = SourceRuntime(
            spec,
            SourceStore(client, settings.database, spec),
            Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
            '',
        )
        try:
            runtime.require_shared_mount()
            with source_lock(
                runtime.lock_root,
                spec.key,
                'request_' + hashlib.sha256(identity.encode()).hexdigest(),
            ):
                runs = context.instance.get_runs(
                    filters=RunsFilter(tags={'origo_source_event': identity}), limit=1
                )
                attempt = 0
                if runs:
                    latest = runs[0]
                    attempt = int(latest.tags['origo_source_attempt'])
                    event_id = uuid5(
                        NAMESPACE_URL, f'{identity}:{attempt}:{latest.run_id}:{latest.status.value}'
                    )
                    exists = runtime.store.execute(
                        f'SELECT event_id FROM {runtime.store.table("source_run_log")} WHERE event_id=%(event)s',
                        {'event': event_id},
                    )
                    if not exists:
                        runtime.store.execute(
                            f'INSERT INTO {runtime.store.table("source_run_log")} VALUES',
                            [
                                (
                                    event_id,
                                    spec.key,
                                    identity,
                                    attempt,
                                    latest.status.value,
                                    latest.run_id,
                                    datetime.now(UTC),
                                )
                            ],
                        )
                    if latest.status not in (DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED):
                        return SkipReason(
                            'Source event already succeeded or has a nonterminal run.'
                        )
                    attempt += 1
                event_id = uuid5(NAMESPACE_URL, f'{identity}:{attempt}:REQUESTED')
                exists = runtime.store.execute(
                    f'SELECT event_id FROM {runtime.store.table("source_run_log")} WHERE event_id=%(event)s',
                    {'event': event_id},
                )
                if not exists:
                    runtime.store.execute(
                        f'INSERT INTO {runtime.store.table("source_run_log")} VALUES',
                        [
                            (
                                event_id,
                                spec.key,
                                identity,
                                attempt,
                                'REQUESTED',
                                '',
                                datetime.now(UTC),
                            )
                        ],
                    )
                run_key = identity if consumer_key and attempt == 0 else f'{identity}:{attempt}'
                return RunRequest(
                    run_key=run_key,
                    partition_key=key if operation == 'canonical' else None,
                    run_config={
                        'ops': {
                            names[operation]: {
                                'config': {'partition_key': '' if consumer_key else key}
                            }
                        }
                    },
                    tags={
                        'origo_source_key': spec.key,
                        'origo_source_operation': operation,
                        **(
                            {'origo_source_state_token': key}
                            if consumer_key
                            else {'origo_source_partition': key}
                        ),
                        'origo_source_event': identity,
                        'origo_source_attempt': str(attempt),
                        **({'origo_source_consumer': consumer_key} if consumer_key else {}),
                    },
                )
        finally:
            client.disconnect()

    @_schedule(
        name=f'{spec.key}_canonical_schedule',
        job=definitions.resolve_job_def(job_names['canonical']),
        cron_schedule=spec.orchestration.canonical_cron,
        execution_timezone='UTC',
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def canonical(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        if spec.rollout_stage == RolloutStage.DORMANT:
            return SkipReason(f'{spec.key} is DORMANT.')
        now = context.scheduled_execution_time or datetime.now(UTC)
        partition = spec.canonical.candidate(now)
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                f'discovery:{now.isoformat()}',
            )
            revision = runtime.discover(partition)
        finally:
            client.disconnect()
        return request('canonical', partition.key, context, revision)

    @_schedule(
        name=f'{spec.key}_audit_schedule',
        job=definitions.resolve_job_def(job_names['audit']),
        cron_schedule=spec.orchestration.audit_cron,
        execution_timezone='UTC',
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def audit(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        if spec.rollout_stage == RolloutStage.DORMANT:
            return SkipReason(f'{spec.key} is DORMANT.')
        tick = context.scheduled_execution_time or datetime.now(UTC)
        return RunRequest(run_key=f'{spec.key}:audit:{tick.isoformat()}')

    schedules: list[ScheduleDefinition] = [canonical, audit]
    if spec.provisional is not None:

        @_schedule(
            name=f'{spec.key}_provisional_schedule',
            job=definitions.resolve_job_def(job_names['provisional']),
            cron_schedule=spec.orchestration.provisional_cron,
            execution_timezone='UTC',
            default_status=DefaultScheduleStatus.STOPPED,
        )
        def provisional(
            context: ScheduleEvaluationContext,
        ) -> RunRequest | SkipReason | list[RunRequest]:
            if spec.rollout_stage == RolloutStage.DORMANT:
                return SkipReason(f'{spec.key} is DORMANT.')
            adapter = spec.provisional
            if adapter is None:
                raise ValueError('No provisional adapter is declared.')
            now = context.scheduled_execution_time or datetime.now(UTC)
            settings = get_clickhouse_settings()
            client = make_clickhouse_client(settings)
            try:
                store = SourceStore(client, settings.database, spec)
                candidates = adapter.candidates(now, store.anchor(), store.active_intervals())
            finally:
                client.disconnect()
            requests: list[RunRequest] = []
            for partition in candidates:
                proposed = request('provisional', partition.key, context)
                if isinstance(proposed, RunRequest):
                    requests.append(proposed)
            return requests or SkipReason('No eligible closed intervals require work.')

        schedules.append(provisional)

    sensors: list[SensorDefinition] = []
    for consumer in spec.consumers:

        def make_consumer_sensor(consumer_key: str, canonical_only: bool) -> SensorDefinition:
            @_sensor(
                name=f'{spec.key}_{consumer_key}_sensor',
                job=definitions.resolve_job_def(job_names['consumer_' + consumer_key]),
                default_status=DefaultSensorStatus.STOPPED,
            )
            def publish(context: SensorEvaluationContext) -> RunRequest | SkipReason:
                if spec.rollout_stage == RolloutStage.DORMANT:
                    return SkipReason(f'{spec.key} is DORMANT.')
                settings = get_clickhouse_settings()
                client = make_clickhouse_client(settings)
                try:
                    snapshot = SourceStore(client, settings.database, spec).snapshot(
                        canonical_only=canonical_only
                    )
                    if not snapshot.records:
                        return SkipReason('Source has no eligible active partitions.')
                    return request('consumer_' + consumer_key, snapshot.token, context)
                finally:
                    client.disconnect()

            return publish

        sensors.append(make_consumer_sensor(consumer.key, consumer.canonical_only))

    @_failure_sensor(
        name=f'{spec.key}_failure_sensor',
        default_status=DefaultSensorStatus.STOPPED,
    )
    def failure(context: RunStatusSensorContext) -> None:
        operation = next(
            (op for op, job_name in job_names.items() if job_name == context.dagster_run.job_name),
            None,
        )
        if context.dagster_run.job_name == f'backfill_{spec.key}_source_job':
            operation = 'canonical'
        if operation is None:
            selected = context.dagster_run.asset_selection or set()
            operation = next(
                (op for op, name in names.items() if dagster.AssetKey(name) in selected), None
            )
        if operation is None:
            return
        spec.require_enabled('observe_failure')
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                context.dagster_run.run_id,
            )
            recorded = runtime.store.execute(
                f'SELECT count() FROM {runtime.store.table("source_failure_log")} '
                "WHERE source_key=%(source)s AND dagster_run_id=%(run)s AND event_type='FAILED'",
                {'source': spec.key, 'run': context.dagster_run.run_id},
            )
            if recorded[0][0]:
                return
            value: object = context.dagster_run.run_config
            for key in ('ops', names[operation], 'config'):
                value = _config_mapping(value).get(key, {})
            tags = context.dagster_run.tags
            partition_key = (
                _config_mapping(value).get('partition_key')
                or tags.get('dagster/partition')
                or tags.get('dagster/asset_partition_range_start', '')
            )
            if tags.get('dagster/asset_partition_range_end', partition_key) != partition_key:
                raise ValueError('Source backfill worker must contain exactly one day.')
            if not isinstance(partition_key, str):
                raise TypeError('Source partition key must be a string.')
            observed_operation = (
                'verification'
                if operation == 'canonical'
                and (
                    _config_mapping(value).get('reconcile_only') is True
                    or tags.get('origo_source_reconciliation') == 'true'
                )
                else operation
            )
            failure_operation, scope, partition, consumer = _failure_context(
                observed_operation, partition_key
            )
            runtime.failures.record(
                operation=failure_operation,
                error_code='RUN_FAILED',
                scope=scope,
                partition=partition,
                consumer=consumer,
            )
        finally:
            client.disconnect()

    from .dagit import build_reconciliation_sensor

    sensors.append(
        build_reconciliation_sensor(
            spec,
            definitions.resolve_job_def(job_names['canonical']),
            definitions.resolve_job_def(job_names['reconcile']),
            names['canonical'],
        )
    )
    sensors.append(failure)
    return SourceBundle(assets, jobs, tuple(schedules), tuple(sensors))
