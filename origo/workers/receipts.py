"""Worker receipts and the container log in ClickHouse.

``worker_minute_log`` holds one row per feed, series and minute a worker processed; it
replaces the Dagster run record as provenance for minute work. ``container_log`` holds
every container's stdout and stderr shipped by Vector, with daily partitions and the
14-day diagnostic retention.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from origo.sources.contracts import Client, identifier
from origo.steady_state.contracts import AttemptIdentity
from origo.steady_state.receipt_identity import (
    ensure_attempt_columns,
    legacy_unresolved_count,
    recover_confirmed_dead,
    write_receipt,
)

log = logging.getLogger(__name__)

WORKER_MINUTE_LOG = 'worker_minute_log'
CONTAINER_LOG = 'container_log'
_LIMIT = 1000
# Only the horizon behind which an identity-less STARTED row is reported as UNKNOWN.
# Elapsed time is not evidence of death: units beat mid-request and may run past it.
DIED_RECEIPT_STALE_AFTER_SECONDS = 300.0


@dataclass(frozen=True)
class Receipt:
    feed: str
    series: str
    minute: datetime
    status: str
    error_code: str
    error: str
    worker_host: str
    recorded_at: datetime


@dataclass(frozen=True)
class LogRow:
    timestamp: datetime
    service: str
    container: str
    level: str
    message: str


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f'Expected a datetime, got {type(value).__name__}.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def ensure_monitoring_tables(client: Client, database: str) -> None:
    prefix = identifier(database)
    client.execute(f'CREATE DATABASE IF NOT EXISTS {prefix}')
    client.execute(
        f"""CREATE TABLE IF NOT EXISTS {prefix}.{WORKER_MINUTE_LOG} (
            feed LowCardinality(String), series LowCardinality(String), minute DateTime,
            rows UInt64, sha256 String, duration_ms UInt32, status LowCardinality(String),
            error_code LowCardinality(String), error String, worker_host String,
            recorded_at DateTime64(3, 'UTC')
        ) ENGINE = ReplacingMergeTree ORDER BY (feed, series, minute, recorded_at)"""
    )
    ensure_attempt_columns(client, database)
    client.execute(
        f"""CREATE TABLE IF NOT EXISTS {prefix}.{CONTAINER_LOG} (
            timestamp DateTime64(3, 'UTC'), service LowCardinality(String), container String,
            stream LowCardinality(String), level LowCardinality(String), message String
        ) ENGINE = MergeTree PARTITION BY toDate(timestamp) ORDER BY (service, timestamp)
        TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE SETTINGS ttl_only_drop_parts = 1"""
    )


def record_receipt(
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
    error_code: str = '',
    error: str = '',
    attempt: AttemptIdentity | None = None,
) -> None:
    """Append one receipt. With ``attempt`` the row carries the work, attempt, owner,
    intended token and prerequisite identity and the write is idempotent per
    ``(attempt, status)``; without it the row is a legacy-shaped receipt."""
    write_receipt(
        client,
        database,
        feed=feed,
        series=series,
        minute=_utc(minute),
        rows=rows,
        sha256=sha256,
        duration_ms=duration_ms,
        status=status,
        error_code=error_code,
        error=error,
        attempt=attempt,
    )


def reconcile_died_receipts(
    client: Client,
    database: str,
    *,
    feed: str,
    now: datetime,
    stale_after_seconds: float = DIED_RECEIPT_STALE_AFTER_SECONDS,
    confirmed_dead_owner_epochs: Sequence[str] = (),
) -> int:
    """Append one FAILED/WORKER_DIED event per open attempt of each positively retired
    owner and return how many. Only epochs whose retirement fence exists are recovered;
    an empty sequence recovers nothing. Identity-less STARTED rows older than
    ``stale_after_seconds`` without a later terminal row are counted and reported as
    UNKNOWN ownership, never failed by inference."""
    cutoff = _utc(now) - timedelta(seconds=stale_after_seconds)
    if stale_after_seconds < 0:
        raise ValueError('The legacy stale-after argument cannot be negative.')
    legacy = legacy_unresolved_count(client, database, feed, cutoff)
    if legacy.count:
        log.error(
            '%s: %s%d unpaired legacy receipts retain UNKNOWN ownership; no death was inferred',
            feed,
            '' if legacy.exhausted else 'at least ',
            legacy.count,
        )
    return recover_confirmed_dead(
        client, database, feed=feed, owner_epochs=confirmed_dead_owner_epochs
    )


def failed_receipts_since(
    client: Client, database: str, since: datetime, until: datetime
) -> list[Receipt]:
    """Failed receipts recorded in ``(since, until]``; the caller keeps ``until`` behind the
    clock so a receipt stamped just before a read and inserted after it is read next time."""
    rows = client.execute(
        f"""SELECT feed, series, minute, status, error_code, error, worker_host, recorded_at
        FROM {identifier(database)}.{WORKER_MINUTE_LOG}
        WHERE status = 'FAILED' AND recorded_at > %(since)s AND recorded_at <= %(until)s
        ORDER BY recorded_at LIMIT {_LIMIT}""",
        {'since': _utc(since).replace(tzinfo=None), 'until': _utc(until).replace(tzinfo=None)},
    )
    return [
        Receipt(
            str(row[0]),
            str(row[1]),
            _utc(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            _utc(row[7]),
        )
        for row in rows
    ]


def failed_attempts(
    client: Client,
    database: str,
    *,
    feed: str,
    series: str,
    minute: datetime | None = None,
    token: str | None = None,
) -> tuple[int, datetime | None]:
    """How many FAILED receipts one unit of work holds for ``feed``/``series`` and when the
    last one was recorded. A build is identified by its ``minute``; a publication by the
    pinned state ``token`` it tried to publish (its ``sha256``)."""
    if (minute is None) == (token is None):
        raise ValueError('Identify the work by its minute or by its token, not both.')
    params: dict[str, object] = {'feed': feed, 'series': series}
    if minute is not None:
        params['minute'] = _utc(minute).replace(tzinfo=None)
        identity = 'minute = %(minute)s'
    else:
        params['token'] = token
        identity = 'sha256 = %(token)s'
    rows = client.execute(
        f"""SELECT count(), max(recorded_at) FROM {identifier(database)}.{WORKER_MINUTE_LOG}
        WHERE feed = %(feed)s AND series = %(series)s AND {identity} AND status = 'FAILED'""",
        params,
    )
    count = int(str(rows[0][0]))
    return count, (_utc(rows[0][1]) if count else None)


def error_log_rows_since(
    client: Client, database: str, since: datetime, until: datetime
) -> list[LogRow]:
    """Error rows stamped in ``(since, until]``; Vector delivers a row seconds after its
    stamp, so the caller keeps ``until`` behind the clock by the delivery lag."""
    rows = client.execute(
        f"""SELECT timestamp, service, container, level, message
        FROM {identifier(database)}.{CONTAINER_LOG}
        WHERE level = 'ERROR' AND timestamp > %(since)s AND timestamp <= %(until)s
        ORDER BY timestamp LIMIT {_LIMIT}""",
        {'since': _utc(since).replace(tzinfo=None), 'until': _utc(until).replace(tzinfo=None)},
    )
    return [
        LogRow(_utc(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])) for row in rows
    ]
