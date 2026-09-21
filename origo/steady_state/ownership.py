"""Positive single-host ownership using the shared source-lock filesystem.

A feed worker holds an exclusive ``flock`` on its owner marker for the life of its
process. Death is proven only by acquiring that lock from another process; a fence
file then records the retirement durably. Elapsed time is never evidence.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import socket
import weakref
from collections.abc import Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO, cast
from uuid import UUID, uuid4

from origo.sources.contracts import SourceError, identifier
from origo.sources.locking import source_lock

from .contracts import AttemptIdentity

log = logging.getLogger(__name__)
RETIREMENT_EVIDENCE = 'exclusive_lifetime_lock_acquired'


@dataclass(frozen=True)
class OwnerProbe:
    retired: tuple[str, ...]
    unknown: tuple[str, ...]


def _directory(root: Path, feed: str) -> Path:
    if not root.is_absolute():
        raise ValueError('Ownership requires the shared absolute lock root.')
    return root / 'worker-owners' / identifier(feed)


def _epoch(value: str) -> str:
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError('Owner epoch must be a canonical nonzero UUID.')
    return value


def _object(text: str) -> dict[str, object]:
    value: object = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError('Owner marker must be an object.')
    return cast(dict[str, object], value)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _registry(root: Path, feed: str) -> AbstractContextManager[None]:
    # Marker creation, fencing and pruning serialize on one per-feed lock so a
    # marker is never pruned between its creation and its lifetime flock.
    return source_lock(root, 'worker_owners', identifier(feed), wait=True)


class WorkerOwner:
    def __init__(self, root: Path, feed: str) -> None:
        self.root = root
        self.feed = identifier(feed)
        self.epoch = str(uuid4())
        directory = _directory(root, feed)
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / (self.epoch + '.json')
        with _registry(root, feed):
            handle = self.path.open('x+', encoding='utf-8')
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                handle.write(
                    json.dumps(
                        {
                            'schema_version': 1,
                            'owner_epoch': self.epoch,
                            'feed': self.feed,
                            'host': socket.gethostname(),
                            'pid': os.getpid(),
                            'started_at': datetime.now(UTC).isoformat(),
                        },
                        sort_keys=True,
                    )
                    + '\n'
                )
                handle.flush()
                os.fsync(handle.fileno())
                _sync_directory(directory)
            except BaseException:
                handle.close()
                raise
        self._finalizer = weakref.finalize(self, handle.close)

    def assert_active(self) -> None:
        if not self.path.is_file():
            raise SourceError('WORKER_OWNER_UNKNOWN', 'The worker owner marker is missing.')
        if not self._finalizer.alive or self.path.with_suffix('.retired').exists():
            raise SourceError('WORKER_OWNER_FENCED', 'The worker owner epoch is retired.')

    def attempt(
        self,
        work_id: str,
        *,
        state_token: str = '',
        prerequisite_key: str = '',
    ) -> AttemptIdentity:
        self.assert_active()
        return AttemptIdentity(work_id, uuid4(), self.epoch, state_token, prerequisite_key)

    def close(self) -> None:
        self._finalizer()


def _publish_fence(path: Path, epoch: str) -> None:
    fence = path.with_suffix('.retired')
    if fence.exists():
        existing = _object(fence.read_text())
        if existing.get('owner_epoch') != epoch:
            raise ValueError('Retirement fence identity does not match.')
        return
    temporary = path.with_name(path.stem + '.' + uuid4().hex + '.pending')
    with temporary.open('x', encoding='utf-8') as output:
        output.write(
            json.dumps(
                {
                    'owner_epoch': epoch,
                    'evidence': RETIREMENT_EVIDENCE,
                    'retired_at': datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            )
            + '\n'
        )
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, fence)
    _sync_directory(path.parent)


def _lifetime_lock_released(handle: TextIO) -> bool:
    """Whether the owner's exclusive lifetime lock can be taken, i.e. its process is gone."""
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _fence_one(root: Path, feed: str, epoch: str) -> bool:
    path = _directory(root, feed) / (_epoch(epoch) + '.json')
    with path.open('r', encoding='utf-8') as handle:
        if not _lifetime_lock_released(handle):
            return False
        marker = _object(handle.read())
        if marker.get('owner_epoch') != epoch or marker.get('feed') != feed:
            raise ValueError('Owner marker identity does not match.')
        _publish_fence(path, epoch)
        return True


def fence_retired_owners(root: Path, feed: str, epochs: Sequence[str]) -> OwnerProbe:
    """Fence named unresolved owners only after acquiring their lifetime lock."""
    retired: list[str] = []
    unknown: list[str] = []
    with _registry(root, feed):
        for epoch in sorted(set(epochs)):
            try:
                if _fence_one(root, feed, epoch):
                    retired.append(epoch)
            except (OSError, ValueError) as error:
                # Missing/corrupt metadata or storage failure is not evidence of death.
                log.error('Owner %r remains unknown: %s', epoch[:100], error)
                unknown.append(epoch)
    return OwnerProbe(tuple(retired), tuple(unknown))


def require_retired_owner(root: Path, feed: str, epoch: str) -> None:
    path = _directory(root, feed) / (_epoch(epoch) + '.retired')
    if not path.is_file():
        raise SourceError('OWNER_DEATH_UNPROVEN', 'No durable retirement evidence exists.')
    marker = _object(path.read_text())
    if marker.get('owner_epoch') != epoch or marker.get('evidence') != RETIREMENT_EVIDENCE:
        raise SourceError('OWNER_DEATH_UNPROVEN', 'Retirement evidence is inconsistent.')


def assert_owner_unfenced(root: Path, feed: str, epoch: str) -> None:
    """A retired epoch cannot start or complete work; only recovery may fail it."""
    if (_directory(root, feed) / (_epoch(epoch) + '.retired')).exists():
        raise SourceError('WORKER_OWNER_FENCED', 'The worker owner epoch is retired.')


def prune_released_owners(root: Path, feed: str, *, outstanding: Sequence[str]) -> tuple[str, ...]:
    """Remove marker and fence of owners whose lifetime lock is free and whose every
    attempt has a terminal receipt. ``outstanding`` is the receipt table's answer; an
    owner named there keeps its evidence until recovery has failed its attempts."""
    directory = _directory(root, feed)
    keep = {_epoch(epoch) for epoch in outstanding}
    pruned: list[str] = []
    with _registry(root, feed):
        for path in sorted(directory.glob('*.json')) if directory.is_dir() else []:
            epoch = path.stem
            if epoch in keep:
                continue
            with path.open('r', encoding='utf-8') as handle:
                if not _lifetime_lock_released(handle):
                    continue
                fence = path.with_suffix('.retired')
                if fence.exists():
                    fence.unlink()
                path.unlink()
            pruned.append(epoch)
        if pruned:
            _sync_directory(directory)
    return tuple(pruned)


# Context is local to the execution thread; native Dagster work has no worker owner.
# The originating feed retains the lifetime lock until all its work has finished.
_EXECUTING: ContextVar[WorkerOwner | None] = ContextVar('origo_worker_owner', default=None)


@contextmanager
def worker_execution(owner: WorkerOwner) -> Iterator[None]:
    owner.assert_active()
    token = _EXECUTING.set(owner)
    try:
        yield
    finally:
        _EXECUTING.reset(token)


def assert_execution_owner() -> None:
    owner = _EXECUTING.get()
    if owner is not None:
        owner.assert_active()
