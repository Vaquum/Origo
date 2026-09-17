from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from origo.workers.report import Reporter
from origo.workers.runtime import (
    HEARTBEAT_MAX_AGE_SECONDS,
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
