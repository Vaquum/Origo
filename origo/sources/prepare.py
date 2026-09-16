import argparse
import json
import os
from pathlib import Path
from typing import cast

from dagster import DagsterInstance, DagsterRunStatus, RunsFilter, get_dagster_logger
from dagster._core.remote_origin import (
    ManagedGrpcPythonEnvCodeLocationOrigin,
    RemoteInstigatorOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    SensorInstigatorData,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .cleanup import preserve_primary_failure
from .contracts import RevisionedSourceSpec, RolloutStage
from .lifecycle import SourceRuntime
from .storage import SourceStore


def prepare_source(
    spec: RevisionedSourceSpec, instance: DagsterInstance, *, check: bool = False
) -> None:
    if spec.rollout_stage == RolloutStage.DORMANT:
        return
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
    for name in names:
        origin = RemoteInstigatorOrigin(repository, name)
        previous = instance.get_instigator_state(origin.get_id(), origin.get_selector().get_id())
        if check:
            if previous is None or previous.status != InstigatorStatus.RUNNING:
                raise RuntimeError(f'Managed source sensor is not prepared: {name}')
        elif previous is None:
            instance.add_instigator_state(
                InstigatorState(
                    origin, InstigatorType.SENSOR, InstigatorStatus.RUNNING, SensorInstigatorData()
                )
            )
        elif previous.status != InstigatorStatus.RUNNING:
            instance.update_instigator_state(previous.with_status(InstigatorStatus.RUNNING))
    get_dagster_logger('origo.sources').info('source=%s phase=prepared sensors=running', spec.key)


def backfill_active(instance: DagsterInstance, spec: RevisionedSourceSpec) -> bool:
    return bool(
        instance.get_runs(
            RunsFilter(
                job_name=f'backfill_{spec.key}_source_job',
                statuses=[
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.CANCELING,
                ],
            ),
            limit=1,
        )
    )


def publication_current(spec: RevisionedSourceSpec, consumer: str, token: str) -> bool:
    path = (
        Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))
        / spec.key
        / consumer
        / 'latest.json'
    )
    if not path.exists():
        return False
    manifest: object = json.loads(path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError('Publication manifest must be an object.')
    return cast(dict[str, object], manifest).get('state_token') == token


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
