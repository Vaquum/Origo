"""Deployment recovery, recorded as a Dagster run. No market data is deleted."""

import argparse
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

from dagster import (
    Config,
    DagsterInstance,
    DagsterRunStatus,
    OpExecutionContext,
    RunsFilter,
    in_process_executor,
    op,
)
from dagster._core.storage.tags import GRPC_INFO_TAG
from pydantic import Field

from origo.sources.backfill import source_job

from .policy import (
    ACTIVE,
    CLAIM_TAG,
    IDENTITY_TAG,
    REDUNDANT_TAG,
    WORKER_TAG,
    admission_lock,
    execution_tags,
)


def recover_queue(instance: DagsterInstance) -> dict[str, int]:
    kept: set[str] = set()
    counts = {'classified': 0, 'redundant_canceled': 0, 'unique_queued': 0}
    with admission_lock(instance):
        runs = instance.get_runs(
            RunsFilter(statuses=[DagsterRunStatus.QUEUED, *ACTIVE]), ascending=True
        )
        for run in runs:
            tags = execution_tags(run)
            instance.add_run_tags(run.run_id, tags)
            counts['classified'] += 1
            current = instance.get_run_by_id(run.run_id)
            if (
                current is None
                or current.status != DagsterRunStatus.QUEUED
                or current.tags.get(CLAIM_TAG) == current.run_id
            ):
                continue
            identity = tags[IDENTITY_TAG]
            if identity in kept:
                # The native dequeuer rechecks status before launch; never cancel
                # a worker merely because its earlier snapshot was QUEUED.
                instance.add_run_tags(run.run_id, {REDUNDANT_TAG: run.run_id})
                instance.report_run_canceled(
                    run,
                    message='Recovery removed a redundant queued copy; unique work is retained.',
                )
                counts['redundant_canceled'] += 1
            else:
                kept.add(identity)
                counts['unique_queued'] += 1
    return counts


def recover_retired_workers(
    instance: DagsterInstance, retired_workers: set[str], legacy_before: float = 0
) -> int:
    recovered = 0
    for record in instance.get_run_records(RunsFilter(statuses=ACTIVE)):
        run = record.dagster_run
        retired = run.tags.get(WORKER_TAG) in retired_workers
        if not retired and legacy_before and WORKER_TAG not in run.tags:
            address: object = json.loads(run.tags.get(GRPC_INFO_TAG, '{}'))
            retired = (
                isinstance(address, Mapping)
                and cast(Mapping[str, object], address).get('host') == 'localhost'
                and isinstance(cast(Mapping[str, object], address).get('socket'), str)
                and (record.start_time or record.create_timestamp.timestamp()) < legacy_before
            )
        if retired:
            instance.report_run_failed(
                run, message='Deployment confirmed the owning worker container has retired.'
            )
            instance.event_log_storage.free_concurrency_slots_for_run(run.run_id)
            recovered += 1
    return recovered


class RecoveryConfig(Config):
    retired_workers: list[str] = Field(default_factory=list)
    # Supplied only after deployment confirms BOTH prior app containers stopped.
    legacy_before: float = 0


@op
def recover_orchestration(context: OpExecutionContext, config: RecoveryConfig) -> None:
    workers = recover_retired_workers(
        context.instance, set(config.retired_workers), config.legacy_before
    )
    queue = recover_queue(context.instance)
    context.log.info('orchestration_recovery retired_workers=%s queue=%s', workers, queue)
    context.add_output_metadata({'retired_worker_runs': workers, **queue})


@source_job(
    name='recover_orchestration_job',
    executor_def=in_process_executor,
    description='Reconcile confirmed retired workers and redundant queued requests.',
    tags={'origo_source_operation': 'orchestration_recovery'},
)
def recover_orchestration_job() -> None:
    recover_orchestration()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--retired-worker', action='append', default=[])
    parser.add_argument('--legacy-before', type=float, default=0)
    args = parser.parse_args()
    with DagsterInstance.get() as instance:
        result = recover_orchestration_job.execute_in_process(
            instance=instance,
            run_config={
                'ops': {
                    'recover_orchestration': {
                        'config': {
                            'retired_workers': args.retired_worker,
                            'legacy_before': args.legacy_before,
                        }
                    }
                }
            },
            tags={'origo/recovery_time': datetime.now(UTC).isoformat()},
            raise_on_error=False,
        )
        if not result.success:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
