"""Shared runtime of an observed worker: the minute loop, the heartbeat and the watchdog.

The loop calls ``feed.tick`` once per interval and touches the heartbeat after every tick,
and the feed touches it as it completes each unit of work inside a slow tick,
so the heartbeat proves the loop is alive, not that the tick succeeded; failures are the
tick's own job to record. A watchdog thread exits the process when the heartbeat is stale,
which under ``restart: unless-stopped`` restarts the container, and ``check_heartbeat`` is
the container healthcheck.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NoReturn, Protocol

HEARTBEAT_MAX_AGE_SECONDS = 180
# A live feed asset the worker has not materialized for this long fails its freshness
# policy in Dagit; the monitor's heartbeat check alerts on the worker itself.
LIVE_FEED_FRESHNESS_WINDOW = timedelta(minutes=5)
WATCHDOG_EXIT_CODE = 3
DEFAULT_HEARTBEAT_DIR = '/opt/origo/heartbeats'
log = logging.getLogger('origo.workers')


@dataclass(frozen=True)
class TickOutcome:
    feed: str
    minute: datetime
    processed: tuple[str, ...]
    failed: tuple[str, ...]


class Feed(Protocol):
    name: str
    lookback_minutes: int

    def tick(self, now: datetime) -> TickOutcome: ...


def utc_now() -> datetime:
    return datetime.now(UTC)


def heartbeat_directory() -> Path:
    return Path(os.environ.get('ORIGO_HEARTBEAT_DIR', DEFAULT_HEARTBEAT_DIR))


def heartbeat_path(directory: Path, feed: str) -> Path:
    return directory / f'{feed}.heartbeat'


def touch_heartbeat(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'{time.time():.3f}\n')


def heartbeat_is_fresh(path: Path, *, max_age_seconds: int, now: float) -> bool:
    try:
        modified = path.stat().st_mtime
    except FileNotFoundError:
        return False
    return now - modified <= max_age_seconds


def check_heartbeat(path: Path, *, max_age_seconds: int = HEARTBEAT_MAX_AGE_SECONDS) -> int:
    """Exit status for a container healthcheck: 0 when the heartbeat is fresh, 1 otherwise."""
    fresh = heartbeat_is_fresh(path, max_age_seconds=max_age_seconds, now=time.time())
    print(f'{path}: {"fresh" if fresh else "stale or missing"}')
    return 0 if fresh else 1


def start_watchdog(
    path: Path, *, max_age_seconds: int, poll_seconds: float = 15.0
) -> threading.Thread:
    """Exit the process with ``WATCHDOG_EXIT_CODE`` once the heartbeat is older than the bound."""

    def watch() -> None:
        while True:
            time.sleep(poll_seconds)
            if not heartbeat_is_fresh(path, max_age_seconds=max_age_seconds, now=time.time()):
                log.error(
                    'heartbeat %s is older than %s seconds; exiting for a restart',
                    path,
                    max_age_seconds,
                )
                os._exit(WATCHDOG_EXIT_CODE)

    thread = threading.Thread(target=watch, name='heartbeat-watchdog', daemon=True)
    thread.start()
    return thread


def run_forever(
    feed: Feed,
    *,
    heartbeat: Path,
    watchdog_seconds: int = HEARTBEAT_MAX_AGE_SECONDS,
    interval_seconds: float = 60.0,
    clock: Callable[[], datetime] = utc_now,
) -> NoReturn:
    touch_heartbeat(heartbeat)
    start_watchdog(heartbeat, max_age_seconds=watchdog_seconds)
    while True:
        started = time.monotonic()
        try:
            outcome = feed.tick(clock())
            log.info(
                '%s tick %s processed=%d failed=%d',
                feed.name,
                outcome.minute.isoformat(),
                len(outcome.processed),
                len(outcome.failed),
            )
        except Exception:
            # The loop stays alive and the failure is loud: it is logged at ERROR, which the
            # log capture turns into an alert, and a hung tick still trips the watchdog.
            log.exception('%s tick failed', feed.name)
        touch_heartbeat(heartbeat)
        time.sleep(max(0.0, interval_seconds - (time.monotonic() - started)))
