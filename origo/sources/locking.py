from __future__ import annotations

import fcntl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .contracts import identifier


@contextmanager
def source_lock(root: Path, source: str, name: str) -> Iterator[None]:
    identifier(source)
    identifier(name)
    if not root.is_absolute():
        raise ValueError('Source lock root must be an absolute shared mount path.')
    directory = root / source
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f'{name}.lock').open('a+b') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f'Source lock is already held: {source}/{name}') from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
