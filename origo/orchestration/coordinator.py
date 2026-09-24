"""Keep the native queued coordinator; serialize admission across app processes."""

from typing import cast

from dagster import DagsterInstance, DagsterRun, DagsterRunStatus, RunsFilter
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.run_coordinator.queued_run_coordinator import QueuedRunCoordinator
from dagster._core.storage.runs.schema import RunsTable, RunTagsTable
from dagster._core.storage.runs.sql_run_storage import SqlRunStorage
from sqlalchemy import Integer, func, select
from sqlalchemy import cast as sql_cast

from .policy import (
    IDENTITY_TAG,
    SHARE_TAG,
    WORKLOAD_TAG,
    admission_lock,
    bulk_order,
    bulk_share,
    execution_tags,
)


def lane_head(instance: DagsterInstance) -> int:
    """The lowest place among queued bulk runs, or 0 when none is queued."""
    priority = RunTagsTable.alias('priority')
    workload = RunTagsTable.alias('workload')
    bulk = (
        select(1)
        .select_from(workload)
        .where(
            workload.c.run_id == RunsTable.c.run_id,
            workload.c.key == WORKLOAD_TAG,
            workload.c.value == 'backfill',
        )
        .correlate(RunsTable)
    )
    query = (
        select(func.max(sql_cast(priority.c.value, Integer)))
        .select_from(RunsTable)
        .join(priority, priority.c.run_id == RunsTable.c.run_id)
        .where(
            RunsTable.c.status == DagsterRunStatus.QUEUED.value,
            priority.c.key == 'dagster/priority',
            bulk.exists(),
        )
    )
    with cast(SqlRunStorage, instance.run_storage).connect() as database:
        highest = database.execute(query).scalar()
    return 0 if highest is None else -int(highest)


class OrigoQueuedRunCoordinator(QueuedRunCoordinator):
    def submit_run(self, context: SubmitRunContext) -> DagsterRun:
        with admission_lock(self._instance):
            run = self._instance.get_run_by_id(context.dagster_run.run_id)
            if run is None:
                raise ValueError('Submitted run does not exist.')
            if run.status != DagsterRunStatus.NOT_STARTED:
                return run
            share = bulk_share(run)
            order = 0
            if share:
                # Start-time fair queuing: join at the lowest queued place, after the share's own queue.
                newest = self._instance.get_runs(
                    RunsFilter(statuses=[DagsterRunStatus.QUEUED], tags={SHARE_TAG: share}),
                    limit=1,
                )
                order = max(
                    lane_head(self._instance), bulk_order(newest[0]) + 1 if newest else 0
                )
            tags = execution_tags(run, order)
            self._instance.add_run_tags(run.run_id, tags)
            duplicates = self._instance.get_runs(
                RunsFilter(
                    statuses=[DagsterRunStatus.QUEUED], tags={IDENTITY_TAG: tags[IDENTITY_TAG]}
                ),
                limit=1,
            )
            if duplicates:
                self._instance.report_run_canceled(
                    run,
                    message=f'Redundant queued request; work is retained in run {duplicates[0].run_id}.',
                )
                result = self._instance.get_run_by_id(run.run_id)
                if result is None:
                    raise RuntimeError('Canceled run disappeared.')
                return result
            return super().submit_run(context)
