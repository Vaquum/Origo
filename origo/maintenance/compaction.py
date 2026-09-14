"""Reclaim free SQLite pages; the initial conversion requires quiesced writers."""

import time
from pathlib import Path

from .sqlite import MaintenanceDeadlineReached, allocated, connection


def incremental_compaction(
    path: Path, deadline: float, lock_wait: float, pages: int = 32768
) -> int:
    """Bound each write transaction and leave a busy WAL for the next maintenance run."""
    before = sum(allocated(Path(str(path) + suffix)) for suffix in ('', '-wal', '-shm'))
    with connection(path, deadline, lock_wait, write=True) as database:
        if database.execute('PRAGMA auto_vacuum').fetchone()[0] != 2:
            raise RuntimeError(f'Initial offline SQLite compaction is required: {path.name}')
        for offset in range(0, pages, 128):
            if time.monotonic() >= deadline:
                raise MaintenanceDeadlineReached('SQLite page reclamation reached its work limit.')
            if database.execute('PRAGMA freelist_count').fetchone()[0] == 0:
                break
            database.execute('BEGIN IMMEDIATE')
            database.execute(f'PRAGMA incremental_vacuum({min(128, pages - offset)})').fetchall()
            database.commit()
        database.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
    after = sum(allocated(Path(str(path) + suffix)) for suffix in ('', '-wal', '-shm'))
    return max(0, before - after)


def initialize_compaction(path: Path, deadline: float, lock_wait: float) -> int:
    """Use only during the initial, backed-up maintenance outage."""
    before = sum(allocated(Path(str(path) + suffix)) for suffix in ('', '-wal', '-shm'))
    with connection(path, deadline, lock_wait, write=True) as database:
        database.execute('PRAGMA auto_vacuum=INCREMENTAL')
        database.execute('VACUUM')
        database.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
        if [tuple(row) for row in database.execute('PRAGMA quick_check')] != [('ok',)]:
            raise RuntimeError(f'Compacted database failed integrity check: {path.name}')
        if database.execute('PRAGMA auto_vacuum').fetchone()[0] != 2:
            raise RuntimeError(f'Compacted database has incorrect vacuum mode: {path.name}')
    after = sum(allocated(Path(str(path) + suffix)) for suffix in ('', '-wal', '-shm'))
    return max(0, before - after)
