import hashlib
import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast
from uuid import NAMESPACE_URL, uuid5

import dagster
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetDep,
    AssetExecutionContext,
    AssetMaterialization,
    AssetSpec,
    AssetsDefinition,
    FreshnessPolicy,
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

from origo.workers.runtime import LIVE_FEED_FRESHNESS_WINDOW
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .archive import archive_session
from .capacity import CapacityMonitor
from .cleanup import preserve_primary_failure
from .contracts import (
    ArchiveNotPublishedYet,
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
    automatic_capacity: bool = False
    allow_full_history: bool = False


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
        active = any(
            record.partition.key == config.partition_key
            for record in runtime.store.records(canonical_only=True)
        )
        if config.capacity_probe and active:
            raise SourceError(
                'CAPACITY_PROBE_ALREADY_ACTIVE',
                'Select a day that is not active for measurement; use normal backfill configuration to retry an active day.',
            )
        capacity: CapacityMonitor | None = None
        if not config.reconcile_only:
            try:
                capacity = CapacityMonitor(
                    runtime, probe=None if config.automatic_capacity else config.capacity_probe
                )
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
            record = runtime.reconcile(config.partition_key)
            successful = True
        return {
            'partition_key': record.partition.key,
            'revision': record.revision,
            'build_id': str(record.build_id),
            'generation': record.generation,
            'reconciled_at': datetime.now(UTC).isoformat(),
        }
    if operation == 'provisional' and not config.partition_key:
        adapter = spec.provisional
        if adapter is None:
            raise ValueError('No provisional adapter is declared.')
        candidates = adapter.candidates(
            datetime.now(UTC), runtime.store.anchor(), runtime.store.active_intervals()
        )
        for partition in candidates:
            runtime.build(partition.key, provisional=True)
        return {'refreshed_partitions': [partition.key for partition in candidates]}
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
    if operation == 'complete':
        from .dagit import observe_source
        from .prepare import publication_current

        for consumer in spec.consumers:
            snapshot = runtime.store.snapshot(canonical_only=True)
            if not publication_current(spec, consumer.key, snapshot.token):
                raise SourceError(
                    'GENERATION_CHANGED', 'Declared files do not match the current source state.'
                )
        runtime.failures.recover(operation='backfill')
        state = observe_source(runtime)
        if state['healthy'] is not True:
            raise SourceError('SOURCE_HEALTH_BLOCKED', 'Backfill has unresolved source failures.')
        return state
    if operation == 'reconcile':
        from .dagit import observe_source

        return observe_source(runtime)
    if operation == 'certify':
        return {'state_token': runtime.certify(config.partition_key, review_state='PENDING').token}
    if operation.startswith('consumer_'):
        from .prepare import publication_current

        consumer = operation.removeprefix('consumer_')
        if not runtime.store.canonical_ready():
            raise SourceError(
                'SOURCE_NOT_READY',
                'Canonical state has incomplete evidence or unresolved partition failures.',
            )
        definition = next(item for item in spec.consumers if item.key == consumer)
        snapshot = runtime.store.snapshot(canonical_only=definition.canonical_only)
        if publication_current(
            spec, consumer, snapshot.token, pinned=not definition.canonical_only
        ):
            runtime.failures.recover(operation='consumer', consumer=consumer)
            return {'state_token': runtime.store.snapshot(canonical_only=True).token}

        destination = config.destination or str(
            Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))
            / spec.key
            / consumer
        )
        return {
            'state_token': runtime.publish(
                consumer, destination, allow_full=config.allow_full_history
            ).token
        }
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
        if operation in ('canonical', 'repair', 'certify')
        else None,
        output_required=operation != 'canonical',
        backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1)
        if operation == 'canonical'
        else None,
        deps=(
            [AssetDep(f'build_{spec.key}_canonical_revision_origo')]
            if operation.startswith('consumer_')
            else [AssetDep(f'publish_{spec.key}_{c.key}') for c in spec.consumers]
            if operation == 'reconcile'
            else None
        ),
        pool=f'{spec.key}_canonical'
        if operation == 'canonical'
        else f'{spec.key}_heavy'
        if operation in ('repair', 'cleanup', 'audit', 'certify')
        else f'{spec.key}_{operation}'
        if operation.startswith('consumer_')
        else None,
        retry_policy=RetryPolicy(
            max_retries=spec.orchestration.retry_count, delay=spec.orchestration.retry_delay
        )
        if operation == 'canonical'
        else None,
    )
    def execute(context: AssetExecutionContext, config: SourceRunConfig) -> Iterator[_SourceResult]:
        if operation == 'canonical' and (
            context.job_def.name != f'refresh_{spec.key}_canonical_source_job'
            or context.has_partition_key_range
        ):
            from .backfill import execute_partition_backfill, publication_ready

            def run_day(day: str) -> dict[str, object]:
                return execute_source(
                    spec,
                    'canonical',
                    SourceRunConfig(partition_key=day, automatic_capacity=True),
                    run_id=context.run.run_id,
                )

            result = execute_partition_backfill(spec, context, run_day)
            if not publication_ready(spec, context, result):
                from dagster._core.definitions.data_version import DATA_VERSION_TAG

                context.log_event(
                    AssetMaterialization(
                        asset_key=name,
                        partition=str(result['partition_key']),
                        metadata={
                            'source_state': MetadataValue.json(result),
                            'source_result': MetadataValue.json(result),
                        },
                        tags={
                            DATA_VERSION_TAG: hashlib.sha256(
                                str(
                                    (result['revision'], result['build_id'], result['generation'])
                                ).encode()
                            ).hexdigest()
                        },
                    )
                )
                return
            yield MaterializeResult(
                value=None,
                metadata={
                    'source_state': MetadataValue.json(result),
                    'source_result': MetadataValue.json(result),
                },
                data_version=DataVersion(
                    hashlib.sha256(
                        str((result['revision'], result['build_id'], result['generation'])).encode()
                    ).hexdigest()
                ),
            )
            return
        try:
            spec.require_enabled(operation)
            from .prepare import prepare_source

            prepare_source(spec, context.instance)
            if operation in ('canonical', 'repair', 'certify'):
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
                    automatic_capacity=not config.reconcile_only,
                )
            selected_operation = (
                'complete'
                if operation == 'reconcile'
                and context.job_def.name == f'backfill_{spec.key}_source_job'
                else operation
            )
            result = execute_source(spec, selected_operation, config, run_id=context.run.run_id)
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
                # A data/configuration verdict holds automatic reconciliation; worker and
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
                        'origo_source_verdict_run': context.run.run_id,
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
            yield MaterializeResult(
                value=None,
                metadata={
                    'source_state': MetadataValue.json(result),
                    'source_result': MetadataValue.json(result),
                },
                data_version=DataVersion(version),
            )
        else:
            yield MaterializeResult(
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


def live_feed_asset(spec: RevisionedSourceSpec) -> str:
    """The external asset the provisional worker materializes each tick for ``spec``."""
    return f'{spec.key}_provisional_feed'


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
    if spec.provisional is not None:
        assets += (
            AssetsDefinition(
                specs=[
                    AssetSpec(
                        live_feed_asset(spec),
                        group_name=spec.key,
                        description='The provisional worker materializes this every tick; '
                        'its freshness policy is the feed\'s liveness in Dagit.',
                        freshness_policy=None
                        if spec.rollout_stage == RolloutStage.DORMANT
                        else FreshnessPolicy.time_window(fail_window=LIVE_FEED_FRESHNESS_WINDOW),
                    )
                ]
            ),
        )
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
                (
                    'origo_projection_source_key'
                    if operation.startswith('consumer_')
                    else 'origo_source_key'
                ): spec.key,
                'origo_source_operation': operation,
            },
        )
        for operation, name in names.items()
    )
    unresolved += (
        _define_job(
            f'backfill_{spec.key}_source_job',
            selection=[
                names['canonical'],
                *(names['consumer_' + c.key] for c in spec.consumers),
                names['reconcile'],
            ],
            tags={
                'origo_source_key': spec.key,
                'origo_source_operation': 'backfill',
                'dagster/max_runtime': '93600',
            },
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
                    if attempt > spec.orchestration.retry_count:
                        return SkipReason('Automatic source attempts exhausted; use native retry after fixing the failure.')
                    records = context.instance.get_run_records(RunsFilter(run_ids=[latest.run_id]), limit=1)
                    ended = records[0].end_time if records else None
                    if ended is not None and datetime.now(UTC).timestamp() - ended < spec.orchestration.retry_delay:
                        return SkipReason('Source retry delay has not elapsed.')
                else:
                    receipt = runtime.store.run_receipt(identity)
                    if receipt is not None:
                        previous_attempt, status, receipt_run = receipt
                        if status == 'SUCCESS':
                            return SkipReason('Source event has a durable successful-run receipt.')
                        if status not in ('FAILURE', 'CANCELED'):
                            raise RuntimeError(
                                f'Retired source event has nonterminal receipt: {status}'
                            )
                        attempt = previous_attempt + 1
                        if attempt > spec.orchestration.retry_count:
                            return SkipReason('Automatic source attempts exhausted; use native retry after fixing the failure.')
                        terminal_event = uuid5(
                            NAMESPACE_URL, f'{identity}:{previous_attempt}:{receipt_run}:{status}'
                        )
                        recent = runtime.store.execute(
                            f'SELECT event_id FROM {runtime.store.table("source_run_log")} '
                            'WHERE event_id=%(event)s AND '
                            'recorded_at > now64(6) - toIntervalSecond(%(delay)s) LIMIT 1',
                            {'event': terminal_event, 'delay': spec.orchestration.retry_delay},
                        )
                        if recent:
                            return SkipReason('Source retry delay has not elapsed.')
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
                        (
                            'origo_projection_source_key' if consumer_key else 'origo_source_key'
                        ): spec.key,
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
        default_status=DefaultScheduleStatus.RUNNING
        if spec.rollout_stage != RolloutStage.DORMANT
        else DefaultScheduleStatus.STOPPED,
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
        except ArchiveNotPublishedYet:
            return SkipReason(f'{spec.key} {partition.key} is not published yet.')
        finally:
            client.disconnect()
        return request('canonical', partition.key, context, revision)

    @_schedule(
        name=f'{spec.key}_audit_schedule',
        job=definitions.resolve_job_def(job_names['audit']),
        cron_schedule=spec.orchestration.audit_cron,
        execution_timezone='UTC',
        default_status=DefaultScheduleStatus.RUNNING
        if spec.rollout_stage != RolloutStage.DORMANT
        else DefaultScheduleStatus.STOPPED,
    )
    def audit(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        if spec.rollout_stage == RolloutStage.DORMANT:
            return SkipReason(f'{spec.key} is DORMANT.')
        from origo.orchestration.policy import has_outstanding

        if has_outstanding(context.instance, job_names['audit']):
            return SkipReason('Source audit already has outstanding work.')
        tick = context.scheduled_execution_time or datetime.now(UTC)
        return RunRequest(run_key=f'{spec.key}:audit:{tick.isoformat()}')

    schedules: list[ScheduleDefinition] = [canonical, audit]
    # Provisional tails and the consumers that pin them run in origo.workers.provisional every
    # minute; the bundle declares the live feed asset that worker materializes each tick, with
    # the freshness policy the daemon evaluates, and keeps a sensor only for canonical-only
    # consumers.

    sensors: list[SensorDefinition] = []
    for consumer in (item for item in spec.consumers if item.canonical_only):

        def make_consumer_sensor(consumer_key: str, canonical_only: bool) -> SensorDefinition:
            @_sensor(
                name=f'{spec.key}_{consumer_key}_sensor',
                job=definitions.resolve_job_def(job_names['consumer_' + consumer_key]),
                default_status=DefaultSensorStatus.STOPPED
                if spec.rollout_stage == RolloutStage.DORMANT
                else DefaultSensorStatus.RUNNING,
            )
            def publish(context: SensorEvaluationContext) -> RunRequest | SkipReason:
                if spec.rollout_stage == RolloutStage.DORMANT:
                    return SkipReason(f'{spec.key} is DORMANT.')
                from origo.orchestration.policy import has_outstanding

                from .prepare import backfill_owns_publication, publication_current

                if backfill_owns_publication(context.instance, spec):
                    return SkipReason(
                        'The backfill job owns publication until its selected period completes.'
                    )
                if has_outstanding(context.instance, job_names['consumer_' + consumer_key]):
                    return SkipReason('A publication run for this consumer is still outstanding.')
                settings = get_clickhouse_settings()
                client = make_clickhouse_client(settings)
                try:
                    store = SourceStore(client, settings.database, spec)
                    if not store.snapshot(canonical_only=True).records:
                        return SkipReason('Source has no eligible active partitions.')
                    if not store.canonical_ready():
                        return SkipReason(
                            'Canonical state has incomplete evidence or unresolved failures.'
                        )
                    snapshot = store.snapshot(canonical_only=canonical_only)
                    if publication_current(
                        spec, consumer_key, snapshot.token, pinned=not canonical_only
                    ):
                        return SkipReason(
                            'Declared files already publish the current source state.'
                        )
                    return request('consumer_' + consumer_key, snapshot.token, context)
                finally:
                    client.disconnect()

            return publish

        sensors.append(make_consumer_sensor(consumer.key, consumer.canonical_only))

    @_failure_sensor(
        name=f'{spec.key}_failure_sensor',
        default_status=DefaultSensorStatus.STOPPED
        if spec.rollout_stage == RolloutStage.DORMANT
        else DefaultSensorStatus.RUNNING,
    )
    def failure(context: RunStatusSensorContext) -> None:
        operation = next(
            (op for op, job_name in job_names.items() if job_name == context.dagster_run.job_name),
            None,
        )
        from .prepare import is_source_backfill

        is_backfill = is_source_backfill(context.dagster_run, spec)
        selected = context.dagster_run.asset_selection or set()
        if is_backfill:
            failed_steps = {
                event.step_key for event in context.for_run_failure().get_step_failure_events()
            }
            operation = next(
                (op for op, name in names.items() if name in failed_steps),
                'canonical',
            )
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
            if is_backfill and operation == 'canonical':
                tags = context.dagster_run.tags
                key = (
                    tags.get('origo_source_partition')
                    if tags.get('origo_source_phase') == 'canonical'
                    else None
                )
                runtime.failures.record(
                    operation='canonical' if key else 'backfill',
                    error_code='RUN_FAILED',
                    scope='PARTITION' if key else 'NONE',
                    partition=key,
                )
                return
            value: object = context.dagster_run.run_config
            for key in ('ops', names[operation], 'config'):
                value = _config_mapping(value).get(key, {})
            tags = context.dagster_run.tags
            selected_key = tags.get('dagster/partition') or tags.get(
                'dagster/asset_partition_range_start', ''
            )
            configured_key = _config_mapping(value).get('partition_key') or ''
            partition_key = (
                selected_key or configured_key
                if operation == 'canonical'
                else configured_key or selected_key
            )
            if tags.get('dagster/asset_partition_range_end', partition_key) != partition_key:
                raise ValueError('Source backfill worker must contain exactly one day.')
            if not isinstance(partition_key, str):
                raise TypeError('Source partition key must be a string.')
            observed_operation = (
                'integrity'
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
