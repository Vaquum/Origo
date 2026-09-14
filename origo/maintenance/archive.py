"""Lossless source-event databases packed into one durable SQLite archive.

Readers deserialize the original schema; event IDs and Dagster payloads do not
change. The transition lock prevents a reader opening a shard while it is moved.
"""

import fcntl
import hashlib
import os
import sqlite3
import time
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .sqlite import MaintenanceDeadlineReached, connection

MAX_DATABASE_BYTES = 64 * 1024**2
_SCHEMA = """CREATE TABLE IF NOT EXISTS source_runs (
    run_id TEXT PRIMARY KEY,
    format_version INTEGER NOT NULL CHECK(format_version=1),
    raw_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    database_image BLOB NOT NULL
)"""


def archive_path(base: Path) -> Path:
    return base / 'operational-maintenance' / 'source-archive.sqlite'


@contextmanager
def transition_lock(base: Path, *, write: bool, deadline: float) -> Iterator[None]:
    path = archive_path(base).with_suffix('.lock')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as handle:
        mode = fcntl.LOCK_EX if write else fcntl.LOCK_SH
        acquired = False
        while not acquired:
            try:
                fcntl.flock(handle, mode | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError as error:
                if time.monotonic() >= deadline:
                    raise MaintenanceDeadlineReached(
                        'Source archive transition is busy.'
                    ) from error
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def initialize_archive(base: Path) -> Path:
    path = archive_path(base)
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path, timeout=1)
    try:
        database.execute('PRAGMA auto_vacuum=INCREMENTAL')
        database.execute('PRAGMA journal_mode=WAL')
        database.execute('PRAGMA synchronous=FULL')
        database.execute(_SCHEMA)
        database.commit()
    finally:
        database.close()
    return path


def read_image(base: Path, run_id: str, deadline: float) -> bytes | None:
    path = archive_path(base)
    if not path.exists():
        return None
    with connection(path, deadline, 1) as database:
        row = database.execute(
            'SELECT format_version,raw_bytes,sha256,database_image FROM source_runs WHERE run_id=?',
            (run_id,),
        ).fetchone()
    if row is None:
        return None
    size = int(row['raw_bytes'])
    if row['format_version'] != 1 or not 0 < size <= MAX_DATABASE_BYTES:
        raise ValueError(f'Unsupported source archive image: {run_id}')
    decoder = zlib.decompressobj()
    payload = decoder.decompress(bytes(row['database_image']), size + 1)
    if (
        len(payload) != size
        or not decoder.eof
        or decoder.unused_data
        or hashlib.sha256(payload).hexdigest() != row['sha256']
    ):
        raise ValueError(f'Source archive checksum or length mismatch: {run_id}')
    return payload


def snapshot_image(shard: Path, deadline: float) -> bytes:
    with connection(shard, deadline, 1) as original:
        pages = int(original.execute('PRAGMA page_count').fetchone()[0])
        page_size = int(original.execute('PRAGMA page_size').fetchone()[0])
        if pages * page_size > MAX_DATABASE_BYTES:
            raise ValueError(f'Source event database exceeds archive bound: {shard.name}')
        memory = sqlite3.connect(':memory:')
        try:

            def progress(status: int, remaining: int, total: int) -> None:
                if time.monotonic() >= deadline:
                    raise MaintenanceDeadlineReached('Source event snapshot exceeded deadline.')

            original.backup(memory, pages=128, progress=progress, sleep=0.01)
            payload = memory.serialize()
        finally:
            memory.close()
    # SQLite documents this normalization for deserializing WAL databases:
    # https://www.sqlite.org/c3ref/deserialize.html. All database rows stay intact.
    payload = payload[:18] + bytes((1, 1)) + payload[20:]
    verified = sqlite3.connect(':memory:')
    try:
        verified.deserialize(payload)
        if verified.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise RuntimeError(f'Source event snapshot failed integrity check: {shard.name}')
    finally:
        verified.close()
    return payload


def store_image(base: Path, run_id: str, payload: bytes, deadline: float) -> None:
    if not 0 < len(payload) <= MAX_DATABASE_BYTES:
        raise ValueError(f'Source image exceeds supported archive size: {run_id}')
    path = initialize_archive(base)
    with connection(path, deadline, 1, write=True) as database:
        database.execute('PRAGMA synchronous=FULL')
        database.execute(
            'INSERT INTO source_runs VALUES (?,1,?,?,?) ON CONFLICT(run_id) DO UPDATE SET '
            'raw_bytes=excluded.raw_bytes,sha256=excluded.sha256,database_image=excluded.database_image',
            (run_id, len(payload), hashlib.sha256(payload).hexdigest(), zlib.compress(payload, 6)),
        )
        database.commit()
    sync_directory(path.parent)
    if read_image(base, run_id, deadline) != payload:
        raise RuntimeError(f'Source archive read-back failed: {run_id}')


def remove_image(base: Path, run_id: str, deadline: float) -> None:
    path = archive_path(base)
    if path.exists():
        with connection(path, deadline, 1, write=True) as database:
            database.execute('DELETE FROM source_runs WHERE run_id=?', (run_id,))
            database.commit()


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def restore_for_write(base: Path, shard: Path, run_id: str, deadline: float) -> None:
    """Called under the run writer lock and the exclusive transition lock."""
    payload = read_image(base, run_id, deadline)
    if payload is None:
        return
    temporary = shard.with_suffix('.restore')
    if temporary.is_symlink() or (temporary.exists() and temporary.stat().st_nlink != 1):
        raise ValueError(f'Unsafe source restoration file: {temporary}')
    with temporary.open('wb') as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    # Archive commit preceded retirement. A leftover live shard at this point is
    # that same generation: no writer can append until this restoration finishes.
    for suffix in ('-wal', '-shm'):
        auxiliary = Path(str(shard) + suffix)
        if auxiliary.exists():
            auxiliary.unlink()
    temporary.replace(shard)
    sync_directory(base)
    remove_image(base, run_id, deadline)
