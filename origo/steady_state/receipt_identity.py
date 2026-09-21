"""Additive worker receipt identities and idempotent event writes.

The table keeps its eleven insertable columns and its sorting key. A modern write
names those eleven plus ``_attempt``, an EPHEMERAL JSON string that six MATERIALIZED
columns extract the identity from. An old binary's eleven-value positional insert and
``SELECT *`` are unchanged, and its rows read an empty/zero identity.

ReplacingMergeTree collapses rows sharing ``(feed, series, minute, recorded_at)``, so
every modern write of one triple is serialized under a fixed hash-shard lock and its
``recorded_at`` strictly exceeds the triple's stored maximum before the insert.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid5

from origo.sources.contracts import Client, SourceError, identifier
from origo.sources.locking import source_lock

from .contracts import AttemptIdentity
from .ownership import assert_owner_unfenced, require_retired_owner

BASE_COLUMNS = (
    'feed',
    'series',
    'minute',
    'rows',
    'sha256',
    'duration_ms',
    'status',
    'error_code',
    'error',
    'worker_host',
    'recorded_at',
)
ATTEMPT_COLUMN = '_attempt'
IDENTITY_COLUMNS = (
    ('work_id', 'String', "JSONExtractString(_attempt, 'work_id')"),
    ('attempt_id', 'UUID', "toUUIDOrZero(JSONExtractString(_attempt, 'attempt_id'))"),
    ('owner_epoch', 'String', "JSONExtractString(_attempt, 'owner_epoch')"),
    ('state_token', 'String', "JSONExtractString(_attempt, 'state_token')"),
    ('prerequisite_key', 'String', "JSONExtractString(_attempt, 'prerequisite_key')"),
    ('event_id', 'UUID', "toUUIDOrZero(JSONExtractString(_attempt, 'event_id'))"),
)
SORTING_KEY = 'feed, series, minute, recorded_at'
ZERO_UUID = UUID(int=0)
RECEIPT_SHARDS = 64
CLOCK_WAIT_BOUND = timedelta(seconds=2)
OWNER_LIMIT = 10000
LEGACY_PAGE_LIMIT = 1000
LEGACY_PAGE_BOUND = 10
# Point reads on the sorting key prefix.
QUERY_LIMITS = {
    'max_execution_time': 5,
    'max_threads': 2,
    'max_memory_usage': 134217728,
    'max_rows_to_read': 1000000,
    'max_result_rows': 10000,
    'result_overflow_mode': 'throw',
}
# Feed-wide scans; an exceeded bound is a loud error, never a truncated answer.
SCAN_LIMITS = {
    'max_execution_time': 15,
    'max_threads': 2,
    'max_memory_usage': 268435456,
    'max_rows_to_read': 5000000,
    'max_result_rows': 100000,
    'result_overflow_mode': 'throw',
}


@dataclass(frozen=True)
class LegacyCursor:
    series: str
    minute: datetime
    recorded_at_ms: int


@dataclass(frozen=True)
class LegacyReceipt:
    series: str
    minute: datetime
    worker_host: str
    recorded_at: datetime


@dataclass(frozen=True)
class LegacyPage:
    rows: tuple[LegacyReceipt, ...]
    next: LegacyCursor | None


@dataclass(frozen=True)
class LegacyUnresolved:
    count: int
    exhausted: bool


def lock_root() -> Path:
    return Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))


def receipt_shard(feed: str, series: str, minute: datetime) -> str:
    key = f'{feed}\0{series}\0{minute.replace(tzinfo=None).isoformat()}'.encode()
    return 'shard_%02d' % (hashlib.sha256(key).digest()[0] % RECEIPT_SHARDS)


def _now() -> datetime:
    return datetime.now(UTC)


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f'Expected a datetime, got {type(value).__name__}.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _table(database: str) -> str:
    return f'{identifier(database)}.worker_minute_log'


def ensure_attempt_columns(client: Client, database: str) -> None:
    # Monitor, depth and source children start concurrently on a new release.
    # Serialize the check-and-ALTER sequence on their common receipt lock mount.
    with source_lock(lock_root(), 'worker_receipts', 'schema', wait=True):
        _ensure_attempt_columns(client, database)


def _ensure_attempt_columns(client: Client, database: str) -> None:
    """Add the identity columns once and refuse any other shape, including the
    physical-column layout an earlier draft used, which old writers cannot insert into."""
    expected = {ATTEMPT_COLUMN: ('String', 'EPHEMERAL', "''")}
    expected.update(
        (name, (kind, 'MATERIALIZED', expression.replace(' ', '')))
        for name, kind, expression in IDENTITY_COLUMNS
    )

    def columns() -> dict[str, tuple[str, str, str]]:
        rows = client.execute(
            'SELECT name, type, default_kind, default_expression FROM system.columns '
            "WHERE database=%(database)s AND table='worker_minute_log' ORDER BY position",
            {'database': database},
            settings=QUERY_LIMITS,
        )
        return {
            str(name): (str(kind), str(default_kind), str(expression).replace(' ', ''))
            for name, kind, default_kind, expression in rows
        }

    present = columns()
    if not any(name in present for name in expected):
        additions = ', '.join(
            f'ADD COLUMN {name} {kind} MATERIALIZED {expression}'
            for name, kind, expression in IDENTITY_COLUMNS
        )
        client.execute(
            f"ALTER TABLE {_table(database)} ADD COLUMN {ATTEMPT_COLUMN} String EPHEMERAL '', "
            + additions
        )
        present = columns()
    insertable = tuple(
        name for name, (_, default_kind, _) in present.items() if default_kind in ('', 'DEFAULT')
    )
    if insertable != BASE_COLUMNS or {name: present.get(name) for name in expected} != expected:
        raise SourceError('RECEIPT_SCHEMA_PARTIAL', 'Receipt identity schema is inconsistent.')
    keys = client.execute(
        "SELECT sorting_key FROM system.tables WHERE database=%(database)s AND name='worker_minute_log'",
        {'database': database},
        settings=QUERY_LIMITS,
    )
    if [str(row[0]) for row in keys] != [SORTING_KEY]:
        raise SourceError('RECEIPT_SCHEMA_PARTIAL', 'Receipt sorting key is inconsistent.')


def _recorded_at(existing_max: datetime | None) -> datetime:
    """The real clock, truncated to the stored millisecond, strictly after the triple's
    stored maximum; a clock that cannot get there within the bound is an error."""
    now = _now()
    if existing_max is not None:
        floor = _utc(existing_max) + timedelta(milliseconds=1)
        if floor - now > CLOCK_WAIT_BOUND:
            raise SourceError(
                'RECEIPT_CLOCK_SKEW',
                'A stored receipt is ahead of this clock beyond the collision bound.',
            )
        started = time.monotonic()
        while now < floor:
            if time.monotonic() - started > CLOCK_WAIT_BOUND.total_seconds():
                raise SourceError(
                    'RECEIPT_CLOCK_SKEW', 'The clock did not advance past the stored receipt.'
                )
            time.sleep((floor - now).total_seconds())
            now = _now()
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def write_receipt(
    client: Client,
    database: str,
    *,
    feed: str,
    series: str,
    minute: datetime,
    rows: int,
    sha256: str,
    duration_ms: int,
    status: Literal['OK', 'FAILED', 'STARTED'],
    error_code: str,
    error: str,
    attempt: AttemptIdentity | None,
) -> None:
    table = _table(database)
    naive_minute = _utc(minute).replace(tzinfo=None)
    parameters = {'feed': feed, 'series': series, 'minute': naive_minute}
    if attempt is not None and attempt.state_token and sha256 != attempt.state_token:
        raise ValueError('Publication receipt must preserve its intended state token.')
    root = lock_root()
    with source_lock(root, 'worker_receipts', receipt_shard(feed, series, minute), wait=True):
        if attempt is not None and status != 'FAILED':
            assert_owner_unfenced(root, feed, attempt.owner_epoch)
        stored = client.execute(
            'SELECT attempt_id, event_id, status, error_code, work_id, owner_epoch, state_token, '
            f'prerequisite_key, sha256, rows, recorded_at FROM {table} '
            'WHERE feed=%(feed)s AND series=%(series)s AND minute=%(minute)s',
            parameters,
            settings=QUERY_LIMITS,
        )
        columns: list[object] = [
            feed,
            series,
            naive_minute,
            rows,
            sha256,
            duration_ms,
            status,
            error_code,
            error[:2000],
            socket.gethostname(),
        ]
        names: list[str] = list(BASE_COLUMNS)
        if attempt is not None:
            event_id = uuid5(attempt.attempt_id, status)
            expected = (
                status,
                error_code,
                attempt.work_id,
                attempt.owner_epoch,
                attempt.state_token,
                attempt.prerequisite_key,
                sha256,
                rows,
            )
            own = [row for row in stored if row[0] == attempt.attempt_id]
            same = [row[2:10] for row in own if row[1] == event_id]
            if same:
                if same != [expected]:
                    raise SourceError(
                        'RECEIPT_IDENTITY_CONFLICT', 'An event identity has conflicting contents.'
                    )
                return
            if any(row[2] != 'STARTED' for row in own):
                raise SourceError(
                    'WORKER_ATTEMPT_TERMINAL', 'The attempt already has a terminal event.'
                )
            columns.append(
                json.dumps(
                    {
                        'work_id': attempt.work_id,
                        'attempt_id': str(attempt.attempt_id),
                        'owner_epoch': attempt.owner_epoch,
                        'state_token': attempt.state_token,
                        'prerequisite_key': attempt.prerequisite_key,
                        'event_id': str(event_id),
                    },
                    sort_keys=True,
                )
            )
            names.append(ATTEMPT_COLUMN)
        latest = max((_utc(row[10]) for row in stored), default=None)
        columns.insert(len(BASE_COLUMNS) - 1, _recorded_at(latest))
        client.execute(f'INSERT INTO {table} ({", ".join(names)}) VALUES', [tuple(columns)])


def outstanding_owner_epochs(client: Client, database: str, feed: str) -> tuple[str, ...]:
    """Owners with an attempt that has no terminal event yet."""
    rows = client.execute(
        'SELECT DISTINCT owner_epoch FROM ('
        f'SELECT owner_epoch, attempt_id FROM {_table(database)} '
        'WHERE feed=%(feed)s AND attempt_id != %(zero)s '
        "GROUP BY owner_epoch, attempt_id HAVING countIf(status != 'STARTED') = 0"
        f') ORDER BY owner_epoch LIMIT {OWNER_LIMIT + 1}',
        {'feed': feed, 'zero': ZERO_UUID},
        settings=SCAN_LIMITS,
    )
    if len(rows) > OWNER_LIMIT:
        raise SourceError(
            'OWNER_QUERY_LIMIT', 'Too many unresolved owners; evidence was not truncated.'
        )
    return tuple(str(row[0]) for row in rows)


def recover_confirmed_dead(
    client: Client,
    database: str,
    *,
    feed: str,
    owner_epochs: Sequence[str],
) -> int:
    """Append one FAILED/WORKER_DIED event per open attempt of a positively retired owner,
    carrying the attempt's own identity and intended token."""
    if not owner_epochs:
        return 0
    root = lock_root()
    for epoch in owner_epochs:
        require_retired_owner(root, feed, epoch)
    with source_lock(root, 'worker_receipts', 'recover_' + identifier(feed), wait=True):
        rows = client.execute(
            'SELECT attempt_id, min(series), min(minute), min(work_id), min(owner_epoch), '
            f'min(state_token), min(prerequisite_key) FROM {_table(database)} '
            'WHERE feed=%(feed)s AND owner_epoch IN %(owners)s AND attempt_id != %(zero)s '
            "GROUP BY attempt_id HAVING countIf(status = 'STARTED') > 0 "
            "AND countIf(status != 'STARTED') = 0 ORDER BY attempt_id",
            {'feed': feed, 'owners': tuple(owner_epochs), 'zero': ZERO_UUID},
            settings=SCAN_LIMITS,
        )
        for attempt_id, series, minute, work, owner, token, prerequisite in rows:
            identity = AttemptIdentity(
                str(work), UUID(str(attempt_id)), str(owner), str(token), str(prerequisite)
            )
            write_receipt(
                client,
                database,
                feed=feed,
                series=str(series),
                minute=_utc(minute),
                rows=0,
                sha256=identity.state_token,
                duration_ms=0,
                status='FAILED',
                error_code='WORKER_DIED',
                error='The owner was positively fenced after lifetime-lock release; the attempt had no terminal receipt.',
                attempt=identity,
            )
        return len(rows)


def legacy_unresolved_page(
    client: Client,
    database: str,
    feed: str,
    cutoff: datetime,
    *,
    after: LegacyCursor | None = None,
    limit: int = LEGACY_PAGE_LIMIT,
) -> LegacyPage:
    """One keyset page of STARTED rows written without identity, older than ``cutoff``,
    that no later terminal row of the same ``(feed, series, minute)`` unit pairs with,
    the pre-identity unit rule. Nothing here proves death."""
    if not 0 < limit <= LEGACY_PAGE_LIMIT:
        raise ValueError('Legacy page limit is out of bounds.')
    table = _table(database)
    parameters: dict[str, object] = {
        'feed': feed,
        'zero': ZERO_UUID,
        'cutoff': _utc(cutoff).replace(tzinfo=None),
        'series': after.series if after else '',
        'minute': _utc(after.minute).replace(tzinfo=None) if after else datetime(1970, 1, 1),
        'recorded_ms': after.recorded_at_ms if after else -1,
    }
    rows = client.execute(
        f"""SELECT s.series, s.minute, s.worker_host, s.recorded_at FROM (
            SELECT series, minute, worker_host, recorded_at FROM {table}
            WHERE feed=%(feed)s AND attempt_id=%(zero)s AND status='STARTED'
              AND recorded_at < %(cutoff)s
        ) AS s LEFT ANTI JOIN (
            SELECT series, minute, recorded_at FROM {table}
            WHERE feed=%(feed)s AND status != 'STARTED'
        ) AS t ON s.series=t.series AND s.minute=t.minute AND t.recorded_at >= s.recorded_at
        WHERE s.series > %(series)s OR (s.series = %(series)s AND (s.minute > %(minute)s
           OR (s.minute = %(minute)s AND toUnixTimestamp64Milli(s.recorded_at) > %(recorded_ms)s)))
        ORDER BY s.series, s.minute, s.recorded_at LIMIT {limit}""",
        parameters,
        settings=SCAN_LIMITS,
    )
    receipts = tuple(
        LegacyReceipt(str(series), _utc(minute), str(host), _utc(recorded))
        for series, minute, host, recorded in rows
    )
    last = receipts[-1] if len(receipts) == limit else None
    cursor = (
        LegacyCursor(last.series, last.minute, int(last.recorded_at.timestamp() * 1000))
        if last
        else None
    )
    return LegacyPage(receipts, cursor)


def legacy_unresolved_count(
    client: Client,
    database: str,
    feed: str,
    cutoff: datetime,
    *,
    page_bound: int = LEGACY_PAGE_BOUND,
) -> LegacyUnresolved:
    """Count unpaired legacy starts across at most ``page_bound`` pages; a count that
    stopped at the bound is reported as not exhausted, never as the whole truth."""
    count = 0
    cursor: LegacyCursor | None = None
    for _ in range(page_bound):
        page = legacy_unresolved_page(client, database, feed, cutoff, after=cursor)
        count += len(page.rows)
        cursor = page.next
        if cursor is None:
            return LegacyUnresolved(count, True)
    return LegacyUnresolved(count, False)
