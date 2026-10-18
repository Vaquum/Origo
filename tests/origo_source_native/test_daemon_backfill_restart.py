"""Native control-plane protocol; these controls contain no market data."""
import logging
import subprocess
import sys
import threading
import time

import pytest
from dagster import DagsterEvent
from dagster._core.errors import (
    DagsterBackfillFailedError,
    DagsterCodeLocationLoadError,
    DagsterCodeLocationNotFoundError,
    DagsterRunAlreadyExists,
    DagsterUserCodeUnreachableError,
)
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.remote_origin import RemotePartitionSetOrigin, RemoteRepositoryOrigin
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter
from dagster._core.test_utils import instance_for_test
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import EmptyWorkspaceTarget, PythonFileTarget
from dagster._daemon import backfill as native_backfill
from dagster._daemon.controller import daemon_controller_from_instance
from dagster._daemon.daemon import BackfillDaemon

from origo.orchestration import daemon

LOGGER = logging.getLogger(__name__)
OVERRIDES = {
    'telemetry': {'enabled': False},
    'run_coordinator': {'module': 'dagster._core.run_coordinator',
                        'class': 'QueuedRunCoordinator'},
}


def _parent(target, identity, selected):
    return PartitionBackfill(
        backfill_id=identity, status=BulkActionStatus.REQUESTED,
        from_failure=False, tags={}, backfill_timestamp=time.time(),
        partition_names=selected,
        partition_set_origin=RemotePartitionSetOrigin(
            repository_origin=RemoteRepositoryOrigin(target.create_origins()[0], '__repository__'),
            partition_set_name='control_partition_set',
        ),
    )


def _iteration(instance, workspace, identity):
    return list(native_backfill.execute_backfill_iteration_with_instigation_logger(
        instance.get_backfill(identity), LOGGER, workspace, instance,
    ))


def test_shutdown_location_loss_preserves_native_checkpoint_and_resume(tmp_path, monkeypatch, capfd):
    repository = tmp_path / 'control.py'
    repository.write_text('''import os
from dagster import Definitions, StaticPartitionsDefinition, job, op
@op
def process_identity():
    return os.getpid()
@job(partitions_def=StaticPartitionsDefinition([str(n) for n in range(60)]))
def control():
    process_identity()
defs = Definitions(jobs=[control])
''')
    target = PythonFileTarget(str(repository), 'defs', str(tmp_path), 'origo')
    selected = [str(n) for n in range(60)]
    identity = 'shutdown-control'
    home = tmp_path / 'instance'
    home.mkdir()
    monkeypatch.setattr(native_backfill, daemon._SEAM, daemon._retryable_backfill_error)
    saved = {}
    for stop_at, checkpoint in ((30, '24'), (55, '49')):
        reached = threading.Event()
        released = threading.Event()
        with instance_for_test(temp_dir=str(home), overrides=OVERRIDES) as instance:
            if stop_at == 30:
                instance.add_backfill(_parent(target, identity, selected))
            else:
                assert instance.get_backfill(identity).failure_count == 1
                assert all(instance.get_run_by_id(key).status == value for key, value in saved.items())
            create_run = type(instance).create_run
            workspace_exit = WorkspaceProcessContext.__exit__

            def create_and_pause(self, *args, **kwargs):
                run = create_run(self, *args, **kwargs)
                if run.tags.get('dagster/backfill') == identity:
                    runs = self.get_runs(RunsFilter(tags={'dagster/backfill': identity}))
                    if len(runs) == stop_at:
                        reached.set()
                        assert released.wait(60), 'Actual workspace teardown did not release submission'
                return run

            def close_and_release(self, *args):
                try:
                    return workspace_exit(self, *args)
                finally:
                    released.set()

            with monkeypatch.context() as patch:
                patch.setattr(type(instance), 'create_run', create_and_pause)
                patch.setattr(WorkspaceProcessContext, '__exit__', close_and_release)
                with daemon_controller_from_instance(
                    instance, target, gen_daemons=lambda _: [BackfillDaemon({'num_workers': 1})],
                ):
                    assert reached.wait(60), 'Native daemon did not reach its in-flight chunk'
                    parent = instance.get_backfill(identity)
                    assert parent.last_submitted_partition_name == checkpoint
                    assert parent.failure_count == 0  # Real progress resets consecutive failures.
                    assert parent.status == BulkActionStatus.REQUESTED
            parent = instance.get_backfill(identity)
            assert parent.status == BulkActionStatus.REQUESTED and parent.failure_count == 1
            assert parent.last_submitted_partition_name == checkpoint
            runs = instance.get_runs(RunsFilter(tags={'dagster/backfill': identity}))
            assert len(runs) == stop_at
            assert all(run.status == DagsterRunStatus.QUEUED for run in runs)
            assert {run.tags['dagster/partition'] for run in runs} == set(selected[:stop_at])
            saved = {run.run_id: run.status for run in runs}
    with instance_for_test(temp_dir=str(home), overrides=OVERRIDES) as instance:
        with WorkspaceProcessContext(instance, target) as workspace:
            assert not _iteration(instance, workspace, identity)
            runs = instance.get_runs(RunsFilter(tags={'dagster/backfill': identity}))
            assert len(runs) == 60 and len({run.run_id for run in runs}) == 60
            assert {run.tags['dagster/partition'] for run in runs} == set(selected)
            assert set(saved).issubset({run.run_id for run in runs})
            # Native worker events control termination only, never certify market coverage.
            interrupted = runs[0]
            for run in runs:
                instance.report_dagster_event(DagsterEvent('PIPELINE_START', run.job_name), run.run_id)
                event = 'PIPELINE_FAILURE' if run.run_id == interrupted.run_id else 'PIPELINE_SUCCESS'
                instance.report_dagster_event(DagsterEvent(event, run.job_name), run.run_id)
            assert not _iteration(instance, workspace, identity)
            assert instance.get_backfill(identity).status == BulkActionStatus.COMPLETED_FAILED
            assert instance.get_run_by_id(interrupted.run_id).status == DagsterRunStatus.FAILURE
            assert not instance.get_runs(RunsFilter(
                tags={'dagster/backfill': identity}, statuses=[DagsterRunStatus.NOT_STARTED],
            ))
    captured = capfd.readouterr()
    assert 'DagsterCodeLocationNotFoundError' in captured.out + captured.err
    assert 'will retry' in captured.out + captured.err


def test_missing_location_exhaustion_and_other_fatal_errors_stay_visible(tmp_path, monkeypatch, caplog):
    import dagster._check as check

    monkeypatch.setattr(native_backfill, daemon._SEAM, daemon._retryable_backfill_error)
    original = daemon._native_retryable
    class MissingSubclass(DagsterCodeLocationNotFoundError):
        pass
    for error in (
        DagsterCodeLocationNotFoundError('Location origo does not exist in workspace'),
        MissingSubclass('Location origo is still loading'),
    ):
        assert not original(error) and daemon._retryable_backfill_error(error)
    for error in (DagsterBackfillFailedError('partition set missing'), check.CheckError('invalid'),
                  DagsterCodeLocationLoadError('load', load_error_infos=[]), DagsterUserCodeUnreachableError('offline'),
                  DagsterRunAlreadyExists('exists'), RuntimeError('transient')):
        assert daemon._retryable_backfill_error(error) == original(error)
    target = PythonFileTarget(str(tmp_path / 'missing.py'), 'defs', str(tmp_path), 'origo')
    with instance_for_test(temp_dir=str(tmp_path / 'instance'), overrides=OVERRIDES) as instance:
        with WorkspaceProcessContext(instance, EmptyWorkspaceTarget()) as workspace:
            instance.add_backfill(_parent(target, 'missing', ['0']))
            maximum = native_backfill._get_max_backfill_retries()
            with caplog.at_level(logging.ERROR):
                for count in range(1, maximum + 2):
                    errors = _iteration(instance, workspace, 'missing')
                    assert len(errors) == 1 and errors[0].cls_name == 'DagsterCodeLocationNotFoundError'
                    assert 'Location origo does not exist in workspace' in errors[0].message
                    parent = instance.get_backfill('missing')
                    assert parent.failure_count == count
                    assert parent.status == (BulkActionStatus.REQUESTED if count <= maximum else BulkActionStatus.FAILING)
                assert not _iteration(instance, workspace, 'missing')
            parent = instance.get_backfill('missing')
            assert parent.status == BulkActionStatus.FAILED
            assert parent.error.cls_name == 'DagsterCodeLocationNotFoundError'
            assert 'Location origo does not exist in workspace' in parent.error.message
            assert 'will retry' in caplog.text and 'DagsterCodeLocationNotFoundError' in caplog.text
            for identity, error in (('bad-partition', DagsterBackfillFailedError('partition missing')),
                                    ('bad-invariant', check.CheckError('invariant'))):
                instance.add_backfill(_parent(target, identity, ['0']))
                def fail(*args, **kwargs):
                    raise error
                with monkeypatch.context() as patch:
                    patch.setattr(native_backfill, 'execute_job_backfill_iteration', fail)
                    _iteration(instance, workspace, identity)
                assert instance.get_backfill(identity).status == BulkActionStatus.FAILING
                assert not _iteration(instance, workspace, identity)
                assert instance.get_backfill(identity).status == BulkActionStatus.FAILED


def test_native_daemon_entrypoint_dispatch(monkeypatch):
    dispatched = []
    with monkeypatch.context() as patch:
        patch.setattr(native_backfill, daemon._SEAM, daemon._native_retryable)
        patch.setattr(daemon, 'native_main', lambda: dispatched.append('native'))
        daemon.main()
        installed = getattr(native_backfill, daemon._SEAM)
        daemon.main()
        assert getattr(native_backfill, daemon._SEAM) is installed
        assert installed(DagsterCodeLocationNotFoundError('is still loading'))
        assert not installed(DagsterBackfillFailedError('invalid'))
        assert dispatched == ['native', 'native']
    for arguments, success in ((['--help'], True), (['run', '--help'], True), (['invalid-command'], False)):
        result = subprocess.run([sys.executable, '-m', 'origo.orchestration.daemon', *arguments],
                                capture_output=True, text=True, timeout=30)
        assert (result.returncode == 0) == success, result.stderr
