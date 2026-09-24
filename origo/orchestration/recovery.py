"""Deployment recovery: reconcile before any captured op exists, then record the counts as a
Dagster run. No market data is deleted."""

import argparse
import json
import signal
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from types import FrameType
from typing import cast

from dagster import (
    Config,
    DagsterInstance,
    DagsterRun,
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
    SHARE_TAG,
    WORKER_TAG,
    admission_lock,
    bulk_order,
    bulk_share,
    execution_tags,
    reconcile_stale_concurrency_claims,
)


def outstanding_runs(instance: DagsterInstance) -> Iterator[DagsterRun]:
    cursor: str | None = None
    while True:
        batch = instance.get_runs(
            RunsFilter(statuses=[DagsterRunStatus.QUEUED, *ACTIVE]),
            ascending=True,
            cursor=cursor,
            limit=1000,
        )
        if not batch:
            return
        cursor = batch[-1].run_id
        yield from batch


def recover_queue(instance: DagsterInstance) -> dict[str, int]:
    kept: set[str] = set()
    counts = {'classified': 0, 'redundant_canceled': 0, 'unique_queued': 0}
    # Deploy restarts kill pooled steps mid-flight; only terminal runs are freed, so this
    # cannot race admission and runs outside the lock.
    counts['stale_slots_freed'] = reconcile_stale_concurrency_claims(instance)
    # A bulk run keeps the place admission gave it; runs admitted before shares existed
    # take places in their share's queue order, oldest first.
    places: Counter[str] = Counter()
    with admission_lock(instance):
        for run in outstanding_runs(instance):
            share = bulk_share(run)
            order = 0
            if share:
                # A place kept from admission or an interrupted recovery also moves the share's
                # next place past it, so a restart never repeats early places.
                order = bulk_order(run) if SHARE_TAG in run.tags else places[share]
                places[share] = max(places[share], order + 1)
            tags = execution_tags(run, order)
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
    # Counts computed in main() before the run exists; the op only records them.
    retired_worker_runs: int = 0
    queue_counts: dict[str, int] = Field(default_factory=dict)


@op
def recover_orchestration(context: OpExecutionContext, config: RecoveryConfig) -> None:
    # No instance.report_* call may run inside a captured op: reporting on a run whose
    # event shard is not yet initialized logs through Alembic under the storage lock,
    # Dagster captures that log into the same storage, and startup deadlocks.
    context.log.info(
        'orchestration_recovery retired_workers=%s queue=%s',
        config.retired_worker_runs,
        config.queue_counts,
    )
    context.add_output_metadata(
        {'retired_worker_runs': config.retired_worker_runs, **config.queue_counts}
    )


@source_job(
    name='recover_orchestration_job',
    executor_def=in_process_executor,
    description='Reconcile confirmed retired workers and redundant queued requests.',
    tags={'origo_source_operation': 'orchestration_recovery'},
)
def recover_orchestration_job() -> None:
    recover_orchestration()


class Deadline:
    """SIGALRM guard that names the phase in progress when a startup or recovery pass stalls."""

    owner = 'Recovery'
    phase = 'startup'

    @classmethod
    def arm(cls, seconds: int, *, owner: str, phase: str) -> None:
        cls.owner = owner
        cls.phase = phase
        signal.signal(signal.SIGALRM, cls.expired)
        signal.alarm(seconds)

    @classmethod
    def enter(cls, phase: str) -> None:
        cls.phase = phase

    @staticmethod
    def expired(signum: int, frame: FrameType | None) -> None:
        raise SystemExit(f'{Deadline.owner} exceeded its deadline during phase {Deadline.phase}.')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--retired-worker', action='append', default=[])
    parser.add_argument('--legacy-before', type=float, default=0)
    parser.add_argument('--deadline-seconds', type=int, default=900)
    args = parser.parse_args()
    Deadline.arm(args.deadline_seconds, owner='Recovery', phase='retired_workers')
    with DagsterInstance.get() as instance:
        workers = recover_retired_workers(instance, set(args.retired_worker), args.legacy_before)
        # The queue pass waits for the admission lock; deployment starts this command only
        # after the daemon process exists, so bootstrap's own queue pass has finished.
        Deadline.enter('queue')
        queue = recover_queue(instance)
        Deadline.enter('record')
        result = recover_orchestration_job.execute_in_process(
            instance=instance,
            run_config={
                'ops': {
                    'recover_orchestration': {
                        'config': {
                            'retired_workers': args.retired_worker,
                            'legacy_before': args.legacy_before,
                            'retired_worker_runs': workers,
                            'queue_counts': queue,
                        }
                    }
                }
            },
            tags={'origo/recovery_time': datetime.now(UTC).isoformat()},
            raise_on_error=False,
        )
        signal.alarm(0)
        if not result.success:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
