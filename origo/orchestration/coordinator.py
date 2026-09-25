"""Keep the native queued coordinator; serialize admission across app processes."""

from dagster import DagsterRun, DagsterRunStatus, RunsFilter
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.run_coordinator.queued_run_coordinator import QueuedRunCoordinator

from .policy import (
    IDENTITY_TAG,
    SHARE_TAG,
    admission_lock,
    bulk_order,
    bulk_share,
    execution_tags,
    frontier,
)


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
                # Start-time fair queuing: join at the service frontier, after the share's own queue.
                newest = self._instance.get_runs(
                    RunsFilter(statuses=[DagsterRunStatus.QUEUED], tags={SHARE_TAG: share}),
                    limit=1,
                )
                order = max(frontier(self._instance), bulk_order(newest[0]) + 1 if newest else 0)
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
