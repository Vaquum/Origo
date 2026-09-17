from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import pytest
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.instance.config import dagster_instance_config
from dagster._core.test_utils import create_run_for_test

from origo.assets.daily_futures_trades_to_origo import (
    insert_daily_binance_futures_trades_to_origo,
)
from origo.assets.daily_trades_to_origo import insert_daily_binance_spot_trades_to_origo
from origo.definitions import _active_partition_days, refresh_binance_spot_data_source_job

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

EXPECTED_RUN_MONITORING: Final[dict[str, object]] = {
    'enabled': True,
    'start_timeout_seconds': 300,
    'cancel_timeout_seconds': 180,
    'max_runtime_seconds': 93600,
    'poll_interval_seconds': 120,
}

# The 26h cap decomposes as: 23h of retry delays plus a shared execution
# budget of 450s per attempt across all 24 attempts. A fully-retrying run
# whose average attempt exceeds that budget would be reaped by design.
PER_ATTEMPT_EXECUTION_BUDGET_SECONDS: Final[int] = 450


def _validated_instance_config() -> dict[str, object]:
    config, _custom_instance_class = dagster_instance_config(str(REPO_ROOT), 'dagster.yaml')
    return dict(config)


def test_run_monitoring_matches_zombie_guard_contract() -> None:
    config = _validated_instance_config()
    assert config['run_monitoring'] == EXPECTED_RUN_MONITORING


@pytest.mark.parametrize(
    'daily_asset',
    [insert_daily_binance_spot_trades_to_origo, insert_daily_binance_futures_trades_to_origo],
    ids=['spot', 'futures'],
)
def test_max_runtime_covers_daily_asset_retry_envelope(daily_asset: object) -> None:
    """The cap must clear retry delays plus a per-attempt execution budget.

    Op retries run in-process, so a legitimately retrying run consumes
    max_retries * delay of backoff PLUS the execution time of every
    attempt; the budget makes the shared execution allowance explicit.
    """
    config = _validated_instance_config()
    run_monitoring = config['run_monitoring']
    assert isinstance(run_monitoring, dict)
    max_runtime = run_monitoring['max_runtime_seconds']
    assert isinstance(max_runtime, int)

    retry_policy = daily_asset.op.retry_policy
    assert retry_policy is not None
    assert retry_policy.delay is not None
    attempts = retry_policy.max_retries + 1
    required = int(
        retry_policy.max_retries * retry_policy.delay
        + attempts * PER_ATTEMPT_EXECUTION_BUDGET_SECONDS
    )
    assert max_runtime >= required


def test_run_retries_stay_disabled_until_replacement_is_atomic() -> None:
    """Tracker #275 item 21: retries re-run delete-then-insert partitions.

    Until partition replacement is atomic, an automatic run-level retry of
    a partially-applied run risks double-applied inserts, so run_retries
    must not appear in the instance config.
    """
    assert 'run_retries' not in _validated_instance_config()


def test_active_partition_days_covers_in_progress_and_recent_terminal() -> None:
    """Repair must skip in-progress runs AND freshly-terminated ones.

    Run monitoring force-marks a timed-out run FAILED without confirming
    the worker exited, so a just-terminated partition may still be written
    by the old worker and stays excluded for the grace period.
    """
    job_name = refresh_binance_spot_data_source_job.name
    with DagsterInstance.ephemeral() as instance:
        create_run_for_test(
            instance,
            job_name=job_name,
            status=DagsterRunStatus.STARTED,
            tags={'dagster/partition': '2024-01-01'},
        )
        create_run_for_test(
            instance,
            job_name=job_name,
            status=DagsterRunStatus.FAILURE,
            tags={'dagster/partition': '2024-01-02'},
        )
        create_run_for_test(
            instance,
            job_name=job_name,
            status=DagsterRunStatus.SUCCESS,
            tags={'dagster/partition': '2024-01-03'},
        )

        excluded = _active_partition_days(instance, job_name)

    assert excluded == {date(2024, 1, 1), date(2024, 1, 2)}


def test_source_backfill_pool_and_system_logs_are_configured() -> None:
    import yaml

    config = _validated_instance_config()
    assert config['concurrency']['pools'] == {'default_limit': 1, 'granularity': 'op'}
    assert config['python_logs'] == {'managed_python_loggers': [''], 'python_log_level': 'INFO'}
    assert config['compute_logs']['config']['base_dir'] == '/opt/dagster-instance/compute_logs'
    dependencies = (REPO_ROOT / 'docker-requirements.txt').read_text().splitlines()
    assert 'dagster[redis,postgres]==1.13.21' in dependencies
    assert 'dagit==1.13.21' in dependencies
    for name in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        compose = yaml.safe_load((REPO_ROOT / name).read_text())
        for worker in ('dagit', 'dagster'):
            environment = compose['services'][worker]['environment']
            assert 'DAGSTER_DEFAULT_IO_MANAGER_MODULE=origo.maintenance.io_manager' in environment
            assert 'DAGSTER_DEFAULT_IO_MANAGER_ATTRIBUTE=packed_io_manager' in environment
            assert not any(
                'DAGSTER_DEFAULT_IO_MANAGER_SILENCE_FAILURES' in value for value in environment
            )
            mounts = compose['services'][worker]['volumes']
            assert 'source-locks:/opt/origo/locks' in mounts
            assert 'source-publications:/opt/origo/shadow' in mounts
            assert 'clickhouse-data:/opt/origo/clickhouse-data:ro' in mounts
            assert 'dagster-instance:/opt/dagster-instance' in mounts

        assert compose['services']['dagster']['command'] == [
            '/bin/sh',
            '-ec',
            'python -m origo.sources.bootstrap && exec dagster-daemon run',
        ]
        assert compose['services']['dagster']['healthcheck']['test'] == [
            'CMD',
            'python',
            '-m',
            'origo.sources.prepare',
            '--check',
        ]


def test_sensor_tick_history_has_bounded_retention() -> None:
    assert _validated_instance_config()['retention']['sensor']['purge_after_days'] == {
        'skipped': 1,
        'success': 1,
        'failure': 7,
    }


def test_schedule_tick_history_has_bounded_retention() -> None:
    assert _validated_instance_config()['retention']['schedule']['purge_after_days'] == {
        'skipped': 1,
        'success': 1,
        'failure': 7,
    }


def test_queue_preserves_backfill_and_routine_capacity() -> None:
    config = _validated_instance_config()
    coordinator = config['run_coordinator']
    assert coordinator['class'] == 'OrigoQueuedRunCoordinator'
    queue = coordinator['config']
    assert queue['max_concurrent_runs'] == 16
    assert queue['dequeue_use_threads'] is True
    assert queue['dequeue_num_workers'] == 16
    assert queue['dequeue_interval_seconds'] == 1
    assert {'key': 'origo/workload', 'value': 'backfill', 'limit': 8} in queue['tag_concurrency_limits']
    assert {'key': 'origo/workload', 'value': 'routine', 'limit': 8} in queue['tag_concurrency_limits']
    assert config['run_launcher']['class'] == 'OrigoRunLauncher'
