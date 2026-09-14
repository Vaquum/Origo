"""Pack small, unchanged filesystem IO values without per-file block overhead."""

import hashlib
import os
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack
from itertools import islice
from pathlib import Path

from .archive import sync_directory
from .run_locks import run_lock
from .sqlite import connection

MAX_OUTPUT_BYTES = 64 * 1024


class OutputStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.path = self.root / '.origo-outputs.sqlite'
        self.lock_path = self.root / '.origo-output-locks'

    def key(self, path: Path) -> str:
        absolute = path.absolute()
        resolved = path.resolve()
        if resolved != absolute or not resolved.is_relative_to(self.root):
            raise ValueError(f'Unsafe local IO path: {path}')
        relative = str(resolved.relative_to(self.root))
        if relative.startswith('.origo-'):
            raise ValueError('The .origo- prefix is reserved for IO storage.')
        return relative

    def lock(self, key: str) -> AbstractContextManager[None]:
        identity = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % (2**63)
        return run_lock(self.lock_path, identity, 5)

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(self.path, timeout=5)
        try:
            database.execute('PRAGMA auto_vacuum=INCREMENTAL')
            database.execute('PRAGMA journal_mode=WAL')
            database.execute('PRAGMA synchronous=FULL')
            database.execute(
                'CREATE TABLE IF NOT EXISTS outputs (path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, payload BLOB NOT NULL)'
            )
            database.commit()
        finally:
            database.close()

    def read(self, key: str) -> bytes | None:
        return self.read_many([key]).get(key)

    def read_many(self, keys: list[str]) -> dict[str, bytes]:
        if not keys or not self.path.exists():
            return {}
        placeholders = ','.join('?' for _ in keys)
        with connection(self.path, time.monotonic() + 5, 1) as database:
            rows = database.execute(
                f'SELECT path,sha256,payload FROM outputs WHERE path IN ({placeholders})', keys
            ).fetchall()
        values: dict[str, bytes] = {}
        for row in rows:
            payload = bytes(row['payload'])
            if (
                len(payload) > MAX_OUTPUT_BYTES
                or hashlib.sha256(payload).hexdigest() != row['sha256']
            ):
                raise ValueError(
                    f'Packed IO value failed its checksum or size bound: {row["path"]}'
                )
            values[str(row['path'])] = payload
        return values

    def commit(self, values: dict[str, bytes]) -> None:
        self.initialize()
        with connection(self.path, time.monotonic() + 5, 1, write=True) as database:
            database.execute('PRAGMA synchronous=FULL')
            database.executemany(
                'INSERT INTO outputs VALUES (?,?,?) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,payload=excluded.payload',
                (
                    (key, hashlib.sha256(payload).hexdigest(), payload)
                    for key, payload in values.items()
                ),
            )
            database.commit()
        sync_directory(self.root)
        if self.read_many(list(values)) != values:
            raise RuntimeError('Packed IO values differ after commit.')

    def remove(self, key: str) -> None:
        if self.path.exists():
            with connection(self.path, time.monotonic() + 5, 1, write=True) as database:
                database.execute('DELETE FROM outputs WHERE path=?', (key,))
                database.commit()

    def pack(self, path: Path) -> int:
        """Caller holds the output lock; commit and read back before removing a file."""
        key = self.key(path)
        metadata = path.stat()
        if metadata.st_nlink != 1:
            raise ValueError(f'Refusing a multiply linked IO file: {path}')
        if metadata.st_size > MAX_OUTPUT_BYTES:
            with path.open('rb') as source:
                os.fsync(source.fileno())
            self.remove(key)
            return 0
        payload = path.read_bytes()
        self.commit({key: payload})
        path.unlink()
        sync_directory(path.parent)
        return metadata.st_blocks * 512


def asset_files(store: OutputStore, asset_paths: Iterable[list[str]]) -> Iterator[Path]:
    """Only asset outputs are packed; run-scoped op files keep their normal lifetime."""
    for components in asset_paths:
        path = store.root.joinpath(*components)
        store.key(path)
        if path.is_file():
            yield path
        elif path.is_dir():
            for directory, _, names in os.walk(path, followlinks=False):
                for name in names:
                    yield Path(directory) / name


def pack_existing_assets(
    store: OutputStore, asset_paths: Iterable[list[str]], deadline: float
) -> int:
    """Commit at most 100 values together while holding their live-writer locks."""
    from .sqlite import MaintenanceDeadlineReached

    packed = 0
    reported = time.monotonic()
    files = asset_files(store, asset_paths)
    while batch := list(islice(files, 100)):
        if time.monotonic() >= deadline:
            raise MaintenanceDeadlineReached(f'Output packing paused after {packed} files.')
        values: dict[str, bytes] = {}
        with ExitStack() as locks:
            for path in dict.fromkeys(batch):
                key = store.key(path)
                locks.enter_context(store.lock(key))
                if not path.exists():
                    continue  # A live writer packed it after directory enumeration.
                metadata = path.stat()
                if metadata.st_nlink != 1:
                    raise ValueError(f'Refusing a multiply linked IO file: {path}')
                if metadata.st_size <= MAX_OUTPUT_BYTES:
                    values[key] = path.read_bytes()
            previous = store.read_many(list(values))
            for key, payload in previous.items():
                if payload != values[key]:
                    raise RuntimeError(f'Raw and committed packed output differ: {key}')
            if values:
                store.commit(values)
                parents: set[Path] = set()
                for key in values:
                    path = store.root / key
                    path.unlink()
                    parents.add(path.parent)
                for parent in parents:
                    sync_directory(parent)
                packed += len(values)
        if time.monotonic() - reported >= 10:
            print(f'Asset output packing: {packed} files committed and verified.', flush=True)
            reported = time.monotonic()
    return packed
