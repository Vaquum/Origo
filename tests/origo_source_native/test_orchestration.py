from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import time

import pytest
import yaml
from dagster import DagsterRunStatus, DagsterEvent, RunsFilter
from dagster._core.launcher.base import LaunchRunContext, WorkerStatus
from dagster._core.launcher.default_run_launcher import DefaultRunLauncher
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.storage.tags import GRPC_INFO_TAG
from dagster._core.test_utils import create_run_for_test as _create_run, instance_for_test
from dagster._core.remote_origin import (
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
    ManagedGrpcPythonEnvCodeLocationOrigin,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon
from dagster._grpc.types import GetCurrentRunsResult
from dagster._serdes import serialize_value

from origo.orchestration.policy import (
    CLAIM_TAG,
    IDENTITY_TAG,
    REDUNDANT_TAG,
    WORKER_TAG,
    execution_tags,
)
from origo.orchestration.recovery import recover_queue, recover_retired_workers

ROOT = Path(__file__).resolve().parents[2]


def create_run_for_test(instance, **kwargs):
    name = kwargs.setdefault('job_name', 'refresh_binance_spot_depth20_data_source_job')
    kwargs['remote_job_origin'] = RemoteJobOrigin(
        RemoteRepositoryOrigin(
            ManagedGrpcPythonEnvCodeLocationOrigin(
                LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'
            ),
            '__repository__',
        ),
        name,
    )
    return _create_run(instance, **kwargs)


@pytest.fixture
def instance(tmp_path):
    config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
    with instance_for_test(
        temp_dir=str(tmp_path),
        overrides={
            key: config[key] for key in ('run_coordinator', 'run_launcher', 'run_monitoring')
        },
    ) as value:
        yield value


def submit(
    instance,
    *,
    job='refresh_binance_spot_depth20_data_source_job',
    day='2017-08-17',
    config=None,
    tags=None,
):
    run = create_run_for_test(
        instance,
        job_name=job,
        run_config=config or {},
        tags={
            'dagster/partition': day,
            **(tags or {}),
        },
    )
    return instance.run_coordinator.submit_run(SubmitRunContext(run, None))


def test_duplicate_admission_preserves_distinct_source_work(instance):
    with ThreadPoolExecutor(max_workers=8) as pool:
        runs = list(
            pool.map(lambda n: submit(instance, tags={'dagster/run_key': str(n)}), range(16))
        )
    assert sum(r.status == DagsterRunStatus.QUEUED for r in runs) == 1
    assert sum(r.status == DagsterRunStatus.CANCELED for r in runs) == 15
    assert submit(instance, day='2017-08-18').status == DagsterRunStatus.QUEUED
    assert (
        submit(
            instance,
            config={'ops': {'refresh': {'config': {'source_partition_key': '2017-08-18'}}}},
        ).status
        == DagsterRunStatus.QUEUED
    )
    assert (
        submit(instance, tags={'binance_spot_latest_minute_start': '2026-09-17T00:40:00Z'}).status
        == DagsterRunStatus.QUEUED
    )
    assert (
        submit(instance, tags={'origo_source_state_token': 'updated'}).status
        == DagsterRunStatus.QUEUED
    )


def test_each_native_backfill_keeps_its_receipts(instance):
    first = submit(
        instance,
        job='backfill_binance_spot_trades_source_job',
        tags={'dagster/backfill': 'lspzlemm'},
    )
    second = submit(
        instance,
        job='backfill_binance_spot_trades_source_job',
        tags={'dagster/backfill': 'gxioueux'},
    )
    assert first.status == second.status == DagsterRunStatus.QUEUED
    assert first.tags[IDENTITY_TAG] != second.tags[IDENTITY_TAG]


def test_one_pending_refresh_survives_while_a_worker_is_active(instance):
    first = submit(instance, job='build_bar_store_arrow_job', day='dollar_15M')
    instance.report_dagster_event(
        DagsterEvent('PIPELINE_START', first.job_name), run_id=first.run_id
    )
    second = submit(instance, job=first.job_name, day='dollar_15M')
    third = submit(instance, job=first.job_name, day='dollar_15M')
    assert second.status == DagsterRunStatus.QUEUED
    assert third.status == DagsterRunStatus.CANCELED
    assert instance.get_run_by_id(first.run_id).status == DagsterRunStatus.STARTED


def test_native_queue_reserves_both_workloads_and_contains_one_noisy_job(instance):
    for n in range(20):
        submit(instance, job='backfill_binance_spot_trades_source_job', day=f'2020-03-{n + 1:02d}')
    for job in (
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'build_bar_store_arrow_job',
        'refresh_binance_spot_latest_data_source_job',
    ):
        for day in ('2017-08-17', '2017-08-18', '2017-08-19'):
            submit(instance, job=job, day=day)
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert len(runs) == 18
    assert sum(r.tags['origo/workload'] == 'backfill' for r in runs) == 10
    assert sum(r.tags['origo/workload'] == 'routine' for r in runs) == 8
    assert runs[0].tags['origo/workload'] == 'routine'
    for job in {r.job_name for r in runs if r.tags['origo/workload'] == 'routine'}:
        assert sum(r.job_name == job for r in runs) == 2


def test_recovery_preserves_claimed_and_distinct_work(instance):
    retained = create_run_for_test(
        instance,
        job_name='refresh_binance_spot_depth20_data_source_job',
        status=DagsterRunStatus.QUEUED,
        tags={'dagster/partition': '2017-08-17'},
    )
    duplicate = create_run_for_test(
        instance, job_name=retained.job_name, status=DagsterRunStatus.QUEUED, tags=retained.tags
    )
    unique = create_run_for_test(
        instance,
        job_name=retained.job_name,
        status=DagsterRunStatus.QUEUED,
        tags={'dagster/partition': '2017-08-18'},
    )
    claimed = create_run_for_test(
        instance, job_name=retained.job_name, status=DagsterRunStatus.QUEUED, tags=retained.tags
    )
    instance.add_run_tags(claimed.run_id, {CLAIM_TAG: claimed.run_id})
    result = recover_queue(instance)
    assert result['redundant_canceled'] == 1
    assert instance.get_run_by_id(duplicate.run_id).status == DagsterRunStatus.CANCELED
    for run in (retained, unique, claimed):
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.QUEUED
    assert recover_queue(instance)['redundant_canceled'] == 0


def test_recovery_cancellation_wins_a_dequeuer_race(instance, monkeypatch):
    run = create_run_for_test(instance, status=DagsterRunStatus.STARTING, tags={})
    instance.add_run_tags(run.run_id, {REDUNDANT_TAG: run.run_id})
    launches = []
    monkeypatch.setattr(
        DefaultRunLauncher,
        'launch_run',
        lambda self, context: launches.append(context.dagster_run.run_id),
    )
    instance.run_launcher.launch_run(LaunchRunContext(run, None))
    assert not launches
    assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.CANCELED


def test_worker_health_requires_positive_evidence(instance, monkeypatch):
    from origo.orchestration import launcher
    from dagster._core.errors import DagsterUserCodeUnreachableError

    run = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'localhost', 'socket': '/tmp/worker'})},
    )
    monkeypatch.setattr(
        launcher.DagsterGrpcClient,
        'get_current_runs',
        lambda self: serialize_value(
            GetCurrentRunsResult(current_runs=[run.run_id], serializable_error_info=None)
        ),
    )
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.RUNNING
    monkeypatch.setattr(
        launcher.DagsterGrpcClient,
        'get_current_runs',
        lambda self: serialize_value(
            GetCurrentRunsResult(current_runs=[], serializable_error_info=None)
        ),
    )
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.NOT_FOUND

    def unreachable(self):
        raise DagsterUserCodeUnreachableError('transport unavailable')

    monkeypatch.setattr(launcher.DagsterGrpcClient, 'get_current_runs', unreachable)
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.UNKNOWN


def test_retirement_preserves_current_and_uncertain_workers(instance):
    dead = create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={WORKER_TAG: 'retired-container'}
    )
    living = create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={WORKER_TAG: 'current-container'}
    )
    unknown = create_run_for_test(instance, status=DagsterRunStatus.STARTED)
    assert recover_retired_workers(instance, {'retired-container'}) == 1
    assert instance.get_run_by_id(dead.run_id).status == DagsterRunStatus.FAILURE
    for run in (living, unknown):
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.STARTED


def test_legacy_retirement_only_covers_old_local_grpc_workers(instance):
    local = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'localhost', 'socket': '/tmp/retired-worker'})},
    )
    remote = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'another-server', 'port': 4000})},
    )
    assert recover_retired_workers(instance, set(), legacy_before=time.time() + 1) == 1
    assert instance.get_run_by_id(local.run_id).status == DagsterRunStatus.FAILURE
    assert instance.get_run_by_id(remote.run_id).status == DagsterRunStatus.STARTED


def test_runtime_bounds_preserve_daily_retry_envelope(instance):
    short = create_run_for_test(instance, job_name='refresh_binance_spot_depth20_data_source_job')
    daily = create_run_for_test(instance, job_name='refresh_binance_spot_data_source_job')
    bulk = create_run_for_test(
        instance,
        job_name='backfill_binance_spot_trades_source_job',
        tags={'dagster/max_runtime': '0'},
    )
    assert execution_tags(short)['dagster/max_runtime'] == '1800'
    assert execution_tags(daily)['dagster/max_runtime'] == '93600'
    assert execution_tags(bulk)['dagster/max_runtime'] == '93600'


def test_benchmark_help_is_available():
    import os
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, 'tools/benchmark_orchestration.py', '--help'],
        cwd=ROOT,
        env={**os.environ, 'PYTHONPATH': str(ROOT)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert '--archives' in result.stdout


def test_depth_chunk_identity_keeps_both_feeds_and_minutes(instance):
    from origo.orchestration.policy import outstanding_configs

    job = 'build_depth_snapshot_store_arrow_job'

    def config(minute):
        return {
            'ops': {
                'build_depth_snapshot_store_arrow': {'config': {'source_partition_key': minute}}
            }
        }

    submit(instance, job=job, day='depth20_snapshots', config=config('2026-09-17T00:40:00+0000'))
    submit(instance, job=job, day='depth200_snapshots', config=config('2026-09-17T00:41:00+0000'))
    assert outstanding_configs(
        instance,
        job,
        'build_depth_snapshot_store_arrow',
        'source_partition_key',
        partition='depth20_snapshots',
    ) == {'2026-09-17T00:40:00+0000'}
    assert outstanding_configs(
        instance,
        job,
        'build_depth_snapshot_store_arrow',
        'source_partition_key',
        partition='depth200_snapshots',
    ) == {'2026-09-17T00:41:00+0000'}


def test_daily_publication_precedes_minute_catchup(instance):
    for job in (
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'refresh_binance_spot_latest_data_source_job',
        'build_depth_snapshot_store_arrow_job',
    ):
        for minute in range(15):
            submit(instance, job=job, day=f'2026-09-17T00:{minute:02d}:00+0000')
    daily = submit(instance, job='refresh_binance_spot_data_source_job', day='2026-09-16')
    feed = submit(instance, job='publish_btc_briefing_feed_job', day='2026-09-16')
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert [r.run_id for r in runs[:2]] == [daily.run_id, feed.run_id]


def test_startup_recovery_completes_outside_captured_logging(origo_test_env, tmp_path, monkeypatch):
    """Both deployment entry points cancel redundant queued runs before any captured op
    exists. Cancelling a run whose event shard is uninitialised logs through Alembic
    under the storage lock; with root python-log capture that log re-enters the same
    storage, and on the pre-fix shape both commands hang forever."""
    import os
    import subprocess
    import sys

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    home = tmp_path / 'dagster-home'
    home.mkdir()
    storage = {'config': {'base_dir': str(home / 'storage')}}
    overrides = {
        'run_storage': {
            'module': 'origo.maintenance.run_storage',
            'class': 'OrigoSqliteRunStorage',
            **storage,
        },
        'event_log_storage': {
            'module': 'origo.maintenance.event_storage',
            'class': 'OrigoSqliteEventLogStorage',
            **storage,
        },
        'python_logs': {'managed_python_loggers': [''], 'python_log_level': 'INFO'},
    }
    commands = (
        ('origo.sources.bootstrap', 'prepare_revisioned_sources_job', '2017-08-17', []),
        (
            'origo.orchestration.recovery',
            'recover_orchestration_job',
            '2017-08-18',
            ['--deadline-seconds', '30'],
        ),
    )
    with instance_for_test(temp_dir=str(home), overrides=overrides) as instance:
        env = {
            **os.environ,
            'DAGSTER_HOME': str(home),
            'PYTHONPATH': str(ROOT),
            'ORIGO_STARTUP_DEADLINE_SECONDS': '60',
        }
        for module, job_name, day, arguments in commands:
            kept, redundant = (
                create_run_for_test(
                    instance, status=DagsterRunStatus.QUEUED, tags={'dagster/partition': day}
                )
                for _ in range(2)
            )
            completed = subprocess.run(
                [sys.executable, '-m', module, *arguments],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert completed.returncode == 0, completed.stdout + completed.stderr
            assert instance.get_run_by_id(kept.run_id).status == DagsterRunStatus.QUEUED
            assert instance.get_run_by_id(redundant.run_id).status == DagsterRunStatus.CANCELED
            assert any(
                entry.dagster_event is not None
                and entry.dagster_event.event_type_value == 'PIPELINE_CANCELED'
                for entry in instance.all_logs(redundant.run_id)
            )
            recorded = instance.get_runs(RunsFilter(job_name=job_name))
            assert recorded and recorded[0].status == DagsterRunStatus.SUCCESS
            messages = [entry.user_message for entry in instance.all_logs(recorded[0].run_id)]
            assert any("'redundant_canceled': 1" in message for message in messages)
