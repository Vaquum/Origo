from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from origo.sources.contracts import RolloutStage
from origo.sources.registry import SOURCE_REGISTRY
from origo.workers.report import Reporter
from origo.workers.runtime import (
    HEARTBEAT_MAX_AGE_SECONDS,
    LIVE_FEED_FRESHNESS_WINDOW,
    WATCHDOG_EXIT_CODE,
    heartbeat_is_fresh,
    heartbeat_path,
    touch_heartbeat,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_watchdog_exits_when_heartbeat_is_stale_and_check_reports_it(tmp_path: Path) -> None:
    heartbeat = heartbeat_path(tmp_path, 'monitor')
    assert not heartbeat_is_fresh(heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS, now=0.0)
    touch_heartbeat(heartbeat)
    written = heartbeat.stat().st_mtime
    assert heartbeat_is_fresh(heartbeat, max_age_seconds=180, now=written + 180)
    assert not heartbeat_is_fresh(heartbeat, max_age_seconds=180, now=written + 181)

    environment = {**os.environ, 'ORIGO_HEARTBEAT_DIR': str(tmp_path), 'PYTHONPATH': str(REPO_ROOT)}
    # The container healthcheck: fresh is 0, stale or missing is 1.
    assert subprocess.run(
        [sys.executable, '-m', 'origo.workers.monitor', '--check'], env=environment
    ).returncode == 0
    stale = written - HEARTBEAT_MAX_AGE_SECONDS - 60
    os.utime(heartbeat, (stale, stale))
    assert subprocess.run(
        [sys.executable, '-m', 'origo.workers.monitor', '--check'], env=environment
    ).returncode == 1

    # The in-process watchdog exits the worker with code 3 once the heartbeat is stale.
    started = time.monotonic()
    watchdog = subprocess.run(
        [
            sys.executable,
            '-c',
            'import sys, time; from pathlib import Path; '
            'from origo.workers.runtime import start_watchdog; '
            'start_watchdog(Path(sys.argv[1]), max_age_seconds=1, poll_seconds=0.1); '
            'time.sleep(10)',
            str(heartbeat),
        ],
        env=environment,
        capture_output=True,
        text=True,
    )
    assert watchdog.returncode == WATCHDOG_EXIT_CODE, watchdog.stderr
    assert time.monotonic() - started < 8
    assert 'exiting for a restart' in watchdog.stderr


def test_reporter_outage_does_not_block_a_tick() -> None:
    reporter = Reporter('http://127.0.0.1:1', timeout_seconds=0.5)
    started = time.monotonic()
    assert reporter.check('origo_monitor', 'dagster_reachable', passed=True, metadata={}) is False
    assert reporter.materialized('feed', partition=None, metadata={'rows': 1}) is False
    assert time.monotonic() - started < 5


@pytest.mark.parametrize(
    ('feed', 'source'),
    [('depth', None), ('provisional', None)]
    + [('provisional', spec.key) for spec in SOURCE_REGISTRY if spec.provisional is not None],
)
def test_feed_worker_check_reports_its_heartbeat(
    feed: str, source: str | None, tmp_path: Path
) -> None:
    environment = {**os.environ, 'ORIGO_HEARTBEAT_DIR': str(tmp_path), 'PYTHONPATH': str(REPO_ROOT)}
    environment.pop('ORIGO_PROVISIONAL_SOURCE', None)
    if source is not None:
        environment['ORIGO_PROVISIONAL_SOURCE'] = source
        touch_heartbeat(heartbeat_path(tmp_path, 'provisional'))
    command = [sys.executable, '-m', f'origo.workers.{feed}', '--check']
    assert subprocess.run(command, env=environment).returncode == 1
    touch_heartbeat(heartbeat_path(tmp_path, feed if source is None else f'{feed}_{source}'))
    assert subprocess.run(command, env=environment).returncode == 0


def test_no_per_minute_schedules_or_run_status_feed_sensors_remain() -> None:
    from origo.definitions import defs

    repository = defs.get_repository_def()
    assert all(schedule.cron_schedule != '* * * * *' for schedule in repository.schedule_defs)
    names = {sensor.name for sensor in defs.sensors}
    assert 'depth_snapshot_store_source_sensor' not in names
    assert 'binance_spot_trades_mount_sensor' in names
    assert not [name for name in names if name.endswith('_freshness_sensor')]
    assert 'binance_spot_trades_huggingface_sensor' in names


def test_live_feed_assets_carry_the_freshness_policy() -> None:
    from dagster import AssetKey, FreshnessPolicy

    from origo.definitions import defs

    graph = defs.get_repository_def().asset_graph
    expected = FreshnessPolicy.time_window(fail_window=LIVE_FEED_FRESHNESS_WINDOW)
    assert LIVE_FEED_FRESHNESS_WINDOW == timedelta(minutes=5)
    for key in (
        'binance_spot_depth_live_feed',
        'binance_spot_trades_provisional_feed',
        'binance_perp_trades_provisional_feed',
        'binance_spot_aggtrades_provisional_feed',
    ):
        node = graph.get(AssetKey(key))
        assert node.freshness_policy_or_from_metadata == expected
        assert not node.is_materializable, f'{key} is materialized by its worker, not by a run'
    assert not [check for check in graph.asset_check_keys if check.name == 'freshness_check']


def test_compose_and_deploy_assign_every_provisional_source_once() -> None:
    sources = {
        spec.key for spec in SOURCE_REGISTRY
        if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
    }
    for path in (REPO_ROOT / 'docker-compose.yml', REPO_ROOT / 'docker-compose.deploy.yml'):
        services = yaml.safe_load(path.read_text())['services']
        provisional = {
            name: service for name, service in services.items()
            if service.get('command') == 'python -m origo.workers.provisional'
        }
        assigned = [service['environment']['ORIGO_PROVISIONAL_SOURCE'] for service in provisional.values()]
        assert set(assigned) == sources and len(assigned) == len(sources)
        assert services['provisional-worker']['environment']['ORIGO_PROVISIONAL_SOURCE'] == 'binance_spot_trades'
        for name, service in {'depth-worker': services['depth-worker'], **provisional}.items():
            feed = 'depth' if name == 'depth-worker' else 'provisional'
            assert service['healthcheck']['test'] == [
                'CMD', 'python', '-m', f'origo.workers.{feed}', '--check'
            ], path
            assert 'worker-heartbeats:/opt/origo/heartbeats' in service['volumes'], path
            assert service['restart'] == 'unless-stopped', path
            assert service['depends_on']['clickhouse'] == {'condition': 'service_healthy'}, path
            if feed == 'provisional':
                assert 'ORIGO_WORKER_HEARTBEAT' not in service['environment']
                assert 'source-locks:/opt/origo/locks' in service['volumes']
                assert 'source-publications:/opt/origo/shadow' in service['volumes']
                if path.name == 'docker-compose.deploy.yml':
                    assert service['mem_limit'] == '16g'
        workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
        launch = next(
            line for line in workflow.splitlines()
            if 'up -d --wait' in line and 'depth-worker' in line
        ).split()
        assert set(provisional) <= set(launch)
        assert 'depth-worker' in launch and 'provisional-worker' in launch
