import argparse
import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast

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
from .storage import SourceStore


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
    names.extend(f'{spec.key}_{consumer.key}_sensor' for consumer in spec.consumers)
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
    if spec.provisional is not None:
        schedules.append(('provisional', spec.orchestration.provisional_cron))
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
    return run.job_name == f'backfill_{spec.key}_source_job' or (
        AssetKey(f'build_{spec.key}_canonical_revision_origo') in (run.asset_selection or ())
        and run.tags.get('origo_source_reconciliation') != 'true'
        and run.job_name != f'refresh_{spec.key}_canonical_source_job'
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
    if backfill_active(instance, spec):
        return True
    records = [
        record
        for filters in (
            RunsFilter(job_name=f'backfill_{spec.key}_source_job'),
            RunsFilter(tags={'origo_source_key': spec.key, 'origo_source_operation': 'backfill'}),
        )
        for record in instance.get_run_records(filters, limit=1)
    ]
    latest = max(records, key=lambda record: record.create_timestamp, default=None)
    native = next(_native_backfills(instance, spec), None)
    if native is not None and (
        latest is None or native.backfill_timestamp > latest.create_timestamp.timestamp()
    ):
        return native.status not in (BulkActionStatus.COMPLETED_SUCCESS, BulkActionStatus.COMPLETED)
    return latest is not None and latest.dagster_run.status != DagsterRunStatus.SUCCESS


def publication_current(
    spec: RevisionedSourceSpec, consumer: str, token: str, *, root: Path | None = None
) -> bool:
    path = (
        (root or Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')))
        / spec.key
        / consumer
        / 'latest.json'
    )
    if not path.exists():
        return False
    manifest: object = json.loads(path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError('Publication manifest must be an object.')
    data = cast(dict[str, object], manifest)
    if data.get('state_token') != token:
        return False
    files = data.get('files')
    version = data.get('version')
    if not isinstance(files, list) or not isinstance(version, str):
        raise ValueError('Publication manifest lacks file/version evidence.')
    for entry in cast(list[object], files):
        if not isinstance(entry, dict):
            raise ValueError('Publication file evidence must be an object.')
        relative = cast(dict[str, object], entry).get('path')
        if not isinstance(relative, str):
            raise ValueError('Publication file evidence requires a path.')
        if not (path.parent / 'versions' / version / relative).is_file():
            return False
    return True


def main() -> None:
    from .registry import SOURCE_REGISTRY

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    with DagsterInstance.get() as instance:
        for spec in SOURCE_REGISTRY:
            prepare_source(spec, instance, check=args.check)


if __name__ == '__main__':
    main()
