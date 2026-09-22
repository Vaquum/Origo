import argparse
import os
import time
from collections.abc import Iterator
from pathlib import Path

from dagster import AssetKey, DagsterInstance, DagsterRunStatus, RunsFilter, get_dagster_logger
from dagster._core.execution.backfill import BulkActionsFilter, BulkActionStatus, PartitionBackfill
from dagster._core.remote_origin import (
    ManagedGrpcPythonEnvCodeLocationOrigin,
    RemoteInstigatorOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    ScheduleInstigatorData,
    SensorInstigatorData,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .cleanup import preserve_primary_failure
from .contracts import RevisionedSourceSpec, RolloutStage
from .lifecycle import SourceRuntime
from .publication import publication_current as publication_current
from .storage import SourceStore


def configure_source_pool(spec: RevisionedSourceSpec, instance: DagsterInstance) -> None:
    # Run before Dagster captures logs: SQLite index initialization logs through
    # Alembic while holding a non-reentrant lock in the upstream storage class.
    if spec.rollout_stage != RolloutStage.DORMANT:
        instance.event_log_storage.set_concurrency_slots(
            f'{spec.key}_canonical', spec.orchestration.canonical_concurrency
        )


def prepare_source(
    spec: RevisionedSourceSpec, instance: DagsterInstance, *, check: bool = False
) -> None:
    enabled = spec.rollout_stage != RolloutStage.DORMANT
    if enabled:
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        with preserve_primary_failure('disconnect', client.disconnect):
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                'deployment',
            )
            if check:
                runtime.require_shared_mount()
            else:
                runtime.setup()
        root = Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))
        if check:
            if not root.is_dir():
                raise RuntimeError('Source publication volume is not prepared.')
        else:
            root.mkdir(parents=True, exist_ok=True)
    repository = RemoteRepositoryOrigin(
        ManagedGrpcPythonEnvCodeLocationOrigin(
            LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'
        ),
        '__repository__',
    )
    names = [f'{spec.key}_{role}_sensor' for role in ('reconciliation', 'failure')]
    names.extend(
        f'{spec.key}_{consumer.key}_sensor' for consumer in spec.consumers if consumer.canonical_only or consumer.key == 'mount'
    )
    managed: list[
        tuple[str, InstigatorType, InstigatorStatus, SensorInstigatorData | ScheduleInstigatorData]
    ] = [
        (
            name,
            InstigatorType.SENSOR,
            InstigatorStatus.RUNNING if enabled else InstigatorStatus.STOPPED,
            SensorInstigatorData(),
        )
        for name in names
    ]
    schedules = [
        ('canonical', spec.orchestration.canonical_cron),
        ('audit', spec.orchestration.audit_cron),
    ]
    managed.extend(
        (
            f'{spec.key}_{role}_schedule',
            InstigatorType.SCHEDULE,
            InstigatorStatus.RUNNING if enabled else InstigatorStatus.STOPPED,
            ScheduleInstigatorData(cron, start_timestamp=time.time()),
        )
        for role, cron in schedules
    )
    for name, kind, desired, data in managed:
        origin = RemoteInstigatorOrigin(repository, name)
        previous = instance.get_instigator_state(origin.get_id(), origin.get_selector().get_id())
        if check:
            actual = previous.status if previous else InstigatorStatus.STOPPED
            if actual != desired:
                raise RuntimeError(f'Managed source sensor or schedule is not prepared: {name}')
        elif previous is None and desired == InstigatorStatus.RUNNING:
            instance.add_instigator_state(InstigatorState(origin, kind, desired, data))
        elif previous is not None and previous.status != desired:
            instance.update_instigator_state(previous.with_status(desired))
    get_dagster_logger('origo.sources').info(
        'source=%s phase=prepared stage=%s', spec.key, spec.rollout_stage
    )


def is_source_backfill(run: DagsterRun, spec: RevisionedSourceSpec) -> bool:
    canonical_job = f'refresh_{spec.key}_canonical_source_job'
    return run.job_name == f'backfill_{spec.key}_source_job' or (
        (
            run.job_name == canonical_job
            or AssetKey(f'build_{spec.key}_canonical_revision_origo') in (run.asset_selection or ())
        )
        and run.tags.get('origo_source_reconciliation') != 'true'
        and (
            run.job_name != canonical_job
            or 'dagster/asset_partition_range_start' in run.tags
            or run.tags.get('origo_source_operation') == 'backfill'
        )
    )


def _native_backfills(
    instance: DagsterInstance,
    spec: RevisionedSourceSpec,
    statuses: list[BulkActionStatus] | None = None,
) -> Iterator[PartitionBackfill]:
    key = AssetKey(f'build_{spec.key}_canonical_revision_origo')
    cursor = None
    while True:
        page = instance.get_backfills(
            filters=BulkActionsFilter(statuses=statuses), cursor=cursor, limit=25
        )
        for backfill in page:
            if key in (backfill.asset_selection or ()):
                yield backfill
        if len(page) < 25:
            return
        cursor = page[-1].backfill_id


def backfill_active(instance: DagsterInstance, spec: RevisionedSourceSpec) -> bool:
    if (
        next(
            _native_backfills(
                instance,
                spec,
                [BulkActionStatus.REQUESTED, BulkActionStatus.CANCELING, BulkActionStatus.FAILING],
            ),
            None,
        )
        is not None
    ):
        return True
    return any(
        is_source_backfill(run, spec)
        for run in instance.get_runs(
            RunsFilter(
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.CANCELING,
                ]
            )
        )
    )


def backfill_owns_publication(instance: DagsterInstance, spec: RevisionedSourceSpec) -> bool:
    """Whether a backfill in flight holds publication: only an active selection does.

    A terminal verdict never holds publication. The consumers that call this gate
    check canonical readiness next, so a failed backfill that left the canonical
    state partial still cannot publish; a failed backfill over a healthy canonical
    state must not wedge publication forever.
    """
    return backfill_active(instance, spec)


def main() -> None:
    from .registry import SOURCE_REGISTRY

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    with DagsterInstance.get() as instance:
        for spec in SOURCE_REGISTRY:
            if not args.check:
                configure_source_pool(spec, instance)
            prepare_source(spec, instance, check=args.check)


if __name__ == '__main__':
    main()
