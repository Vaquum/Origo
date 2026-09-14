"""Per-run POSIX record locks in one file, keyed by SQLite's unique run ID."""

import atexit
import fcntl
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from weakref import WeakValueDictionary


class _LocalLock:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.holders = 0
        self.shared = False


_registry_lock = threading.Lock()
_files: dict[tuple[int, Path], int] = {}
_threads: WeakValueDictionary[tuple[int, Path, int], _LocalLock] = WeakValueDictionary()


def _after_fork() -> None:
    global _registry_lock, _threads
    _registry_lock = threading.Lock()
    _threads = WeakValueDictionary()


def _close_files() -> None:
    for descriptor in _files.values():
        os.close(descriptor)
    _files.clear()


os.register_at_fork(after_in_child=_after_fork)
atexit.register(_close_files)


@contextmanager
def run_lock(
    path: Path, storage_id: int, wait_seconds: float, *, shared: bool = False
) -> Iterator[None]:
    if not 0 <= storage_id < 2**63:
        raise ValueError('A run lock requires its nonnegative SQLite storage ID.')
    path = path.resolve()
    deadline = time.monotonic() + wait_seconds
    pid = os.getpid()
    with _registry_lock:
        key = (pid, path)
        if key not in _files:
            path.parent.mkdir(parents=True, exist_ok=True)
            # POSIX locks are process-owned: closing ANY descriptor for this file
            # releases that process's locks. Keep one descriptor until process exit.
            _files[key] = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        descriptor = _files[key]
        thread_key = (pid, path, storage_id)
        local = _threads.get(thread_key)
        if local is None:
            local = _LocalLock()
            _threads[thread_key] = local
    error_message = f'Run {storage_id} has an active reader, writer or retirement.'
    if not local.condition.acquire(timeout=max(0, deadline - time.monotonic())):
        raise TimeoutError(error_message)
    try:
        while local.holders and not (shared and local.shared):
            if not local.condition.wait(timeout=max(0, deadline - time.monotonic())):
                raise TimeoutError(error_message)
        if not local.holders:
            acquired = False
            while not acquired:
                try:
                    mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                    fcntl.lockf(descriptor, mode | fcntl.LOCK_NB, 1, storage_id, os.SEEK_SET)
                    acquired = True
                except BlockingIOError as error:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(error_message) from error
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            local.shared = shared
        local.holders += 1
    finally:
        local.condition.release()
    try:
        yield
    finally:
        with local.condition:
            local.holders -= 1
            if not local.holders:
                fcntl.lockf(descriptor, fcntl.LOCK_UN, 1, storage_id, os.SEEK_SET)
                local.condition.notify_all()
