from __future__ import annotations

import fcntl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .contracts import SourceError, identifier


@contextmanager
def source_lock(
    root: Path, source: str, name: str, *, shared: bool = False, wait: bool = False
) -> Iterator[None]:
    identifier(source)
    identifier(name)
    if not root.is_absolute():
        raise ValueError('Source lock root must be an absolute shared mount path.')
    directory = root / source
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f'{name}.lock').open('a+b') as handle:
        try:
            fcntl.flock(
                handle,
                (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | (0 if wait else fcntl.LOCK_NB),
            )
        except BlockingIOError as error:
            raise SourceError(
                'SOURCE_LOCK_BUSY', f'Source lock is already held: {source}/{name}'
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@contextmanager
def partition_work(root: Path, source: str, partition_lock: str) -> Iterator[None]:
    # Concurrent partitions share the maintenance fence, but never their generation lock.
    with source_lock(root, source, 'heavy', shared=True):
        with source_lock(root, source, partition_lock):
            yield
