"""Reclaim aged local payload copies only after native persistence is verified."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from .adapters.book_local import LocalBookCanonical, LocalBookProvisional, _revision_key, book_root
from .adapters.book_spool import (
    _metadata, atomic_write, object_mapping, remove_sealed_payload,
)
from .contracts import Partition, SourceError, failure_code, failure_message
from .locking import partition_work, source_lock

if TYPE_CHECKING:
    from .lifecycle import SourceRuntime

BOOK_RETENTION_BATCH_MINUTES = 64
BOOK_RETENTION_BATCH_DAYS = 8
BOOK_PAYLOAD_RETENTION = timedelta(days=2)
_DAY_SEALS = frozenset(f'{hour:02}-{minute:02}.seal.json'
                       for hour in range(24) for minute in range(60))


def _complete_day(seals: tuple[Path, ...]) -> bool:
    return _DAY_SEALS <= {path.name for path in seals}


def _reclaim(
    runtime: SourceRuntime, payload: Path, market: str, now: datetime,
    partition_lock: Callable[[Partition], str],
) -> bool:
    minute = _metadata(payload.with_suffix('.json'))
    start = datetime.fromisoformat(minute['minute_start'])
    expected_payload = book_root() / market / start.strftime('%Y-%m-%d') / start.strftime('%H-%M.seal.gz')
    if minute['market'] != market or payload != expected_payload:
        raise ValueError('Book retention payload contradicts its sealed identity.')
    partition = Partition(start.strftime('%Y-%m-%dT%H:%M:%SZ'), start,
                          start + timedelta(minutes=1), provisional=True)
    with partition_work(runtime.lock_root, runtime.spec.key, partition_lock(partition)):
        accepted = runtime.store.record(partition)
        if accepted is None:
            return False
        if accepted.partition != partition or accepted.revision != _revision_key((minute,)):
            raise SourceError('BOOK_RETENTION_IDENTITY_INVALID',
                              'Retained native generation does not match the immutable minute seal.')
        proofs = runtime.store.execute(
            f'SELECT component,row_count FROM {runtime.store.table("source_component_log")} '
            'WHERE source_key=%(source)s AND partition_key=%(partition)s '
            'AND revision=%(revision)s AND build_id=%(build)s',
            {'source': runtime.spec.key, 'partition': partition.key,
             'revision': accepted.revision, 'build': accepted.build_id},
        )
        expected = {'depth20_latest': 600, 'depth200_latest': 60,
                    'depth20_1m_latest': 1, 'depth200_1m_latest': 1}
        if len(proofs) != 4 or {str(key): count for key, count in proofs} != expected:
            raise SourceError('BOOK_RETENTION_PROOF_INVALID',
                              'All four complete native minute proofs are required before payload cleanup.')
        runtime._validate_retained(accepted)
        evidence = {
            'operation': 'native_payload_reclaimed', 'revision': accepted.revision,
            'build_id': str(accepted.build_id), 'component_hashes': accepted.component_hashes,
            'payload_sha256': minute['payload_sha256'],
        }
        runtime.store.execute(
            f'INSERT INTO {runtime.store.table("source_observation_log")} VALUES',
            [(runtime.spec.key, partition.key, json.dumps({**evidence, 'phase': 'prepared'},
              separators=(',', ':')), 0, now)],
        )
        removed = remove_sealed_payload(book_root(), minute)
        if removed:
            runtime.store.execute(
                f'INSERT INTO {runtime.store.table("source_observation_log")} VALUES',
                [(runtime.spec.key, partition.key, json.dumps({**evidence, 'phase': 'completed'},
                  separators=(',', ':')), 1, now)],
            )
            runtime.failures.recover(operation='payload_cleanup', partition=partition.key)
        return removed


def reclaim_native_payloads(
    runtime: SourceRuntime, now: datetime, partition_lock: Callable[[Partition], str],
) -> int:
    adapter = runtime.spec.canonical
    provisional = runtime.spec.provisional
    if not isinstance(adapter, LocalBookCanonical) or not isinstance(provisional, LocalBookProvisional):
        raise ValueError('Native book payload retention requires both local book adapters.')
    if adapter.market != provisional.market:
        raise ValueError('Native book retention markets must agree.')
    root = book_root()
    market_root = root / adapter.market
    if not market_root.exists():
        return 0
    cutoff = now.astimezone(UTC) - BOOK_PAYLOAD_RETENTION
    cursor = runtime.lock_root / runtime.spec.key / 'book_retention.cursor.json'
    with source_lock(runtime.lock_root, runtime.spec.key, 'book_retention'):
        position: dict[str, object]
        if cursor.exists():
            with cursor.open('rb') as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                raise ValueError('Book retention cursor exceeds its bound.')
            position = dict(object_mapping(json.loads(raw)))
        else:
            first = min((path.name for path in market_root.iterdir() if path.is_dir()), default='')
            if not first:
                return 0
            position = {'frontier': first, 'day': first, 'after': '', 'pending': []}
        pending_value = position['pending']
        if not isinstance(pending_value, list):
            raise ValueError('Book retention pending days must be a list.')
        pending_items = cast(list[object], pending_value)
        if any(not isinstance(item, str) for item in pending_items):
            raise ValueError('Book retention pending days must be strings.')
        pending = cast(list[str], pending_items)
        frontier = datetime.strptime(str(position['frontier']), '%Y-%m-%d').replace(tzinfo=UTC)
        day = datetime.strptime(str(position['day']), '%Y-%m-%d').replace(tzinfo=UTC)
        after = str(position['after'])
        examined, reclaimed = 0, 0
        visited: set[str] = set()
        try:
            for _ in range(BOOK_RETENTION_BATCH_DAYS):
                if day + timedelta(days=1) > cutoff or day.date().isoformat() in visited:
                    break
                visited.add(day.date().isoformat())
                directory = market_root / day.date().isoformat()
                seals = tuple(directory.glob('*.seal.json'))
                complete = _complete_day(seals)
                if not complete:
                    for payload in sorted(directory.glob('*.seal.gz')):
                        if payload.name <= after:
                            continue
                        key = day.strftime('%Y-%m-%dT') + payload.stem.removesuffix('.seal').replace('-', ':') + ':00Z'
                        try:
                            reclaimed += _reclaim(runtime, payload, adapter.market, now, partition_lock)
                        except (OSError, ValueError, RuntimeError) as error:
                            runtime.failures.record(
                                operation='payload_cleanup', scope='NONE', partition=key,
                                error_code=failure_code(error), message=failure_message(error),
                            )
                        examined += 1
                        position.update(day=day.date().isoformat(), after=payload.name)
                        if examined >= BOOK_RETENTION_BATCH_MINUTES:
                            return reclaimed
                    if next(directory.glob('*.seal.gz'), None) is not None:
                        pending.append(day.date().isoformat())
                if day == frontier:
                    frontier += timedelta(days=1)
                    position['frontier'] = frontier.date().isoformat()
                # Discover each old day once. Only unresolved payload days are revisited;
                # immutable seals from cleaned/complete days never consume later batches.
                if frontier + timedelta(days=1) <= cutoff or not pending:
                    day = frontier
                else:
                    day = datetime.strptime(pending.pop(0), '%Y-%m-%d').replace(tzinfo=UTC)
                after = ''
                position.update(day=day.date().isoformat(), after='', pending=pending)
            return reclaimed
        finally:
            encoded = json.dumps(position, separators=(',', ':')).encode()
            if len(encoded) > 65536:
                raise ValueError('Book retention cursor exceeds its bound.')
            atomic_write(cursor, encoded)
