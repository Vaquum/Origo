"""Result files of the market state query service and their 24-hour idle expiry (PRD-0022 L16).

A result is ``results/<uuid>/`` with ``cells.arrow`` and ``summary.arrow``. ``lifecycle.sqlite``
owns every result from before its staging directory exists: ``register`` records the id,
``publish`` writes the file rows with creation as the first access and renames the synced
staging directory into place, and ``recover`` rolls every interrupted step back or forward.
Every read through the cube reader (``origo.query.market_state_reader``) calls ``access``,
which renews that file's clock unless the file is already retired; ``expire`` retires a file
idle for 24 hours in the same kind of immediate transaction and only then unlinks it, so a
read and a deletion never both win. Recovery, cleanup and bookkeeping touch only registered
results, never follow a symlink and never renew a clock.
"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from uuid import UUID

RESULT_ROOT: Final = Path('/opt/origo/market-state')
IDLE_EXPIRY_SECONDS: Final = 86_400
MAX_CONCURRENT_QUERIES: Final = 2
# About 300 full-history base-resolution results of 207 MB (measured 2026-09-25).
RESULT_BYTES_BUDGET: Final = 64 * 1024**3
# Each admitted query holds 2.5x the largest measured result until its bytes are on disk.
QUERY_RESERVATION_BYTES: Final = 512 * 1024**2
# Free space never falls within this margin of the largest source capacity reserve.
FLOOR_MARGIN_BYTES: Final = 8 * 1024**3
RESULT_FILES: Final = ('cells.arrow', 'summary.arrow')
# The canonical text of a result id, exactly as ``str(uuid4())`` writes it.
_RESULT_ID: Final = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

_EXPIRY_NS: Final = IDLE_EXPIRY_SECONDS * 1_000_000_000
_SCHEMA: Final = (
    """CREATE TABLE IF NOT EXISTS results (
        result_id TEXT PRIMARY KEY, state TEXT NOT NULL, created_ns INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS files (
        result_id TEXT NOT NULL, name TEXT NOT NULL, bytes INTEGER NOT NULL,
        last_access_ns INTEGER NOT NULL, retired INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (result_id, name))""",
)


@dataclass(frozen=True)
class DiskSample:
    total: int
    free: int
    inodes: int
    free_inodes: int


def sample_disk(path: Path) -> DiskSample:
    stats = os.statvfs(path)
    return DiskSample(
        stats.f_blocks * stats.f_frsize, stats.f_bavail * stats.f_frsize, stats.f_files, stats.f_favail
    )


class StorageFull(RuntimeError):
    """Writing more would breach the result budget or the filesystem floor."""

    def __init__(self, used: int, budget: int, free: int, floor: int) -> None:
        super().__init__(
            f'Result storage is full: {used} of {budget} bytes used, {free} bytes free, floor {floor}.'
        )
        self.used, self.budget, self.free, self.floor = used, budget, free, floor


class ResultStore:
    def __init__(
        self,
        root: Path = RESULT_ROOT,
        *,
        clock: Callable[[], int] = time.time_ns,
        disk: Callable[[Path], DiskSample] = sample_disk,
        budget_bytes: int = RESULT_BYTES_BUDGET,
    ) -> None:
        self.root, self.clock, self.disk, self.budget_bytes = root, clock, disk, budget_bytes
        self.staging, self.results = root / 'staging', root / 'results'
        self.database = root / 'lifecycle.sqlite'
        self._lock = threading.Lock()
        self._staged: dict[str, int] = {}
        for directory in (root, self.staging, self.results):
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o755)
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        try:
            connection.execute('PRAGMA journal_mode = DELETE')
        finally:
            connection.close()
        with self._transaction() as connection:
            for statement in _SCHEMA:
                connection.execute(statement)

    def recover(self) -> int:
        """After a restart, roll every registered step back or forward; returns interrupted queries."""
        with self._transaction() as connection:
            rows = connection.execute('SELECT result_id, state FROM results').fetchall()
        interrupted = 0
        for result_id, state in rows:
            identity = str(result_id)
            staging, final = self.staging / identity, self.results / identity
            if state == 'staging':
                if _owned_directory(staging):
                    shutil.rmtree(staging)
                with self._transaction() as connection:
                    connection.execute('DELETE FROM results WHERE result_id = ?', (identity,))
                interrupted += 1
            elif state == 'discarding':
                self._remove(identity)
            elif not _owned_directory(final) and _owned_directory(staging):
                staging.rename(final)
                _sync(self.staging)
                _sync(self.results)
            elif not _owned_directory(final):
                with self._transaction() as connection:
                    connection.execute('DELETE FROM files WHERE result_id = ?', (identity,))
                    connection.execute('DELETE FROM results WHERE result_id = ?', (identity,))
        self._reclaim()
        return interrupted

    def register(self, result_id: str) -> Path:
        """Record ownership, then create the staging directory it owns."""
        identity = str(UUID(result_id))
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO results (result_id, state, created_ns) VALUES (?, 'staging', ?)",
                (identity, self.clock()),
            )
        staging = self.staging / identity
        staging.mkdir()
        staging.chmod(0o755)
        return staging

    def admit(self, result_id: str, staged_bytes: int, floor_bytes: int) -> None:
        """Reserve room for a query's bytes, or raise ``StorageFull`` and reserve nothing.

        Every in-flight query holds ``QUERY_RESERVATION_BYTES`` or its actual bytes, whichever
        is larger; the part not yet written is subtracted from free space. The check and the
        reservation happen under one lock, so concurrent admissions cannot both pass.
        """
        with self._lock:
            staged = {**self._staged, result_id: staged_bytes}
            reserved = sum(max(QUERY_RESERVATION_BYTES, size) for size in staged.values())
            pending = sum(max(QUERY_RESERVATION_BYTES - size, 0) for size in staged.values())
            used = self.usage()[1] + reserved
            disk = self.disk(self.root)
            if used > self.budget_bytes or disk.free - pending < floor_bytes or disk.free_inodes * 10 < disk.inodes:
                raise StorageFull(used, self.budget_bytes, disk.free, floor_bytes)
            self._staged[result_id] = staged_bytes

    def publish(self, result_id: str) -> tuple[Path, Path]:
        identity = str(UUID(result_id))
        staging, final = self.staging / identity, self.results / identity
        for name in RESULT_FILES:
            (staging / name).chmod(0o644)
            _sync(staging / name)
        _sync(staging)
        now = self.clock()
        with self._transaction() as connection:
            connection.execute("UPDATE results SET state = 'published' WHERE result_id = ?", (identity,))
            connection.executemany(
                'INSERT INTO files (result_id, name, bytes, last_access_ns) VALUES (?, ?, ?, ?)',
                [(identity, name, (staging / name).stat().st_size, now) for name in RESULT_FILES],
            )
        staging.rename(final)
        _sync(self.staging)
        _sync(self.results)
        with self._lock:
            self._staged.pop(identity, None)
        return final / RESULT_FILES[0], final / RESULT_FILES[1]

    def discard(self, result_id: str) -> None:
        """Remove a result whose paths were never returned, in whatever state it reached.

        A publication that failed after its lifecycle commit leaves the result registered as
        published, in staging or already renamed; nobody holds its paths, so it goes too. The
        result is first marked ``discarding`` with its files retired, so a crash before the
        rows go leaves a registration that recovery finishes, never unowned files.
        """
        identity = str(UUID(result_id))
        with self._transaction() as connection:
            connection.execute("UPDATE results SET state = 'discarding' WHERE result_id = ?", (identity,))
            connection.execute('UPDATE files SET retired = 1 WHERE result_id = ?', (identity,))
        self._remove(identity)
        with self._lock:
            self._staged.pop(identity, None)

    def _remove(self, identity: str) -> None:
        for directory in (self.staging / identity, self.results / identity):
            if _owned_directory(directory):
                shutil.rmtree(directory)
        with self._transaction() as connection:
            connection.execute('DELETE FROM files WHERE result_id = ?', (identity,))
            connection.execute('DELETE FROM results WHERE result_id = ?', (identity,))

    def access(self, result_id: str, name: str) -> datetime | None:
        """Renew a published file's clock; ``None`` when it is unknown or already retired."""
        with self._transaction() as connection:
            row = connection.execute(
                'SELECT retired FROM files WHERE result_id = ? AND name = ?', (result_id, name)
            ).fetchone()
            if row is None or row[0]:
                return None
            now = self.clock()
            connection.execute(
                'UPDATE files SET last_access_ns = ? WHERE result_id = ? AND name = ?', (now, result_id, name)
            )
        return _instant(now + _EXPIRY_NS)

    def expire(self) -> int:
        """Retire and remove every file idle for 24 hours; returns the files removed."""
        with self._transaction() as connection:
            connection.execute(
                'UPDATE files SET retired = 1 WHERE retired = 0 AND last_access_ns + ? <= ?',
                (_EXPIRY_NS, self.clock()),
            )
        return self._reclaim()

    def usage(self) -> tuple[int, int]:
        """Live results, and the bytes every recorded file still holds on disk."""
        with self._transaction() as connection:
            live, size = connection.execute(
                'SELECT count(DISTINCT CASE WHEN retired = 0 THEN result_id END), coalesce(sum(bytes), 0) FROM files'
            ).fetchone()
        return int(live), int(size)

    def _reclaim(self) -> int:
        with self._transaction() as connection:
            retired = connection.execute('SELECT result_id, name FROM files WHERE retired = 1').fetchall()
        removed = 0
        for result_id, name in retired:
            identity, file_name = str(UUID(str(result_id))), str(name)
            if file_name not in RESULT_FILES:
                raise RuntimeError(f'Refusing to remove an unrecognised result file: {file_name!r}.')
            directory = self.results / identity
            target = directory / file_name
            # Only inside the store's own real directory: a result directory replaced by a
            # symlink must never lead the unlink to a foreign file.
            if _owned_directory(directory) and (target.is_file() or target.is_symlink()):
                target.unlink()
                removed += 1
            with self._transaction() as connection:
                connection.execute('DELETE FROM files WHERE result_id = ? AND name = ?', (identity, file_name))
                remaining = connection.execute(
                    'SELECT count(*) FROM files WHERE result_id = ?', (identity,)
                ).fetchone()[0]
                if not remaining:
                    connection.execute('DELETE FROM results WHERE result_id = ?', (identity,))
            if not remaining and _owned_directory(directory) and not any(directory.iterdir()):
                directory.rmdir()
        return removed

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        try:
            connection.execute('PRAGMA synchronous = FULL')
            connection.execute('PRAGMA temp_store = MEMORY')
            connection.execute('BEGIN IMMEDIATE')
            try:
                yield connection
            except BaseException:
                connection.execute('ROLLBACK')
                raise
            connection.execute('COMMIT')
        finally:
            connection.close()


def parse_result_path(path: str) -> tuple[str, str] | None:
    """The ``(result_id, name)`` of ``.../<uuid>/<cells|summary>.arrow``; ``None`` otherwise.

    Only the last two components count, so a caller may mount the volume anywhere; the
    filesystem is never consulted.
    """
    parts = path.replace('\\', '/').split('/')
    if len(parts) < 2 or parts[-1] not in RESULT_FILES or not _RESULT_ID.fullmatch(parts[-2]):
        return None
    return parts[-2], parts[-1]


def _owned_directory(path: Path) -> bool:
    """A real directory named by a canonical result id, never a symlink or a foreign entry."""
    return not path.is_symlink() and path.is_dir() and _RESULT_ID.fullmatch(path.name) is not None


def _sync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _instant(nanoseconds: int) -> datetime:
    return datetime.fromtimestamp(nanoseconds / 1_000_000_000, UTC)
