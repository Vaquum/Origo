"""Bound aggregate render memory without letting bulk work take the live slots."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from origo.sources.contracts import SourceError, beat_worker
from origo.sources.locking import source_lock

log = logging.getLogger(__name__)
RENDER_SLOTS_PER_LANE = 2
LIVE_ADMISSION_SECONDS = 60.0
BULK_ADMISSION_SECONDS = 300.0


@contextmanager
def render_admission(root: Path, *, bulk: bool) -> Iterator[None]:
    lane = 'bulk' if bulk else 'live'
    deadline = time.monotonic() + (BULK_ADMISSION_SECONDS if bulk else LIVE_ADMISSION_SECONDS)
    next_beat = time.monotonic()
    while time.monotonic() < deadline:
        for number in range(RENDER_SLOTS_PER_LANE):
            stack = ExitStack()
            try:
                stack.enter_context(source_lock(root, 'render_budget', f'{lane}_{number}'))
            except SourceError as error:
                stack.close()
                if error.code != 'SOURCE_LOCK_BUSY':
                    raise
            else:
                try:
                    yield
                finally:
                    stack.close()
                return
        if time.monotonic() >= next_beat:
            try:
                beat_worker()
            except OSError as error:
                log.warning('Render admission heartbeat could not be written: %s', error)
            next_beat = time.monotonic() + 15.0
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    raise SourceError('RENDER_ADMISSION_TIMEOUT', f'The bounded {lane} render admission expired.')
