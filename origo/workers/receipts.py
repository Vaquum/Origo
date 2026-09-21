"""Worker receipts and the container log in ClickHouse.

``worker_minute_log`` holds one row per feed, series and minute a worker processed; it
replaces the Dagster run record as provenance for minute work. ``container_log`` holds
every container's stdout and stderr shipped by Vector, with daily partitions and the
14-day diagnostic retention.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from origo.sources.contracts import Client, identifier

WORKER_MINUTE_LOG = 'worker_minute_log'
CONTAINER_LOG = 'container_log'
_LIMIT = 1000
# A unit that started longer ago than this without a terminal receipt died with
# its process: the heartbeat watchdog kills any beat-less unit past 180s, and
# no worker unit beats mid-flight, so 300s leaves no false positives.
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
) -> None:
    client.execute(
        f'INSERT INTO {identifier(database)}.{WORKER_MINUTE_LOG} VALUES',
        [
            (
                feed,
                series,
                _utc(minute).replace(tzinfo=None),
                rows,
                sha256,
                duration_ms,
                status,
                error_code,
                error[:2000],
                socket.gethostname(),
                datetime.now(UTC),
            )
        ],
    )


def reconcile_died_receipts(
    client: Client,
    database: str,
    *,
    feed: str,
    now: datetime,
    stale_after_seconds: float = DIED_RECEIPT_STALE_AFTER_SECONDS,
) -> int:
    """Mark STARTED units whose process died as FAILED so backoff and paging see them.

    Watchdog exits and SIGKILLs leave no terminal receipt, so without this the
    next tick retries instantly forever. A unit whose latest receipt is a STARTED
    older than ``stale_after_seconds`` cannot still be running (see
    ``DIED_RECEIPT_STALE_AFTER_SECONDS``); append one FAILED/WORKER_DIED row per
    such unit. Returns the rows appended.

    The unit key is ``(feed, series, minute)``: a STARTED row is written before
    the unit's hash exists, so it can never match a terminal row on sha256. One
    anti-join finds every outstanding unit in a single round trip no matter how
    many STARTED rows the feed has accumulated, and the appended FAILED row
    guards the next pass, so each dead unit is marked exactly once.
    """
    cutoff = _utc(now).replace(tzinfo=None) - timedelta(seconds=stale_after_seconds)
    outstanding = client.execute(
        f"""SELECT s.series, s.minute
        FROM {identifier(database)}.{WORKER_MINUTE_LOG} AS s
        LEFT ANTI JOIN {identifier(database)}.{WORKER_MINUTE_LOG} AS t
          ON t.feed = s.feed AND t.series = s.series AND t.minute = s.minute
          AND t.status != 'STARTED' AND t.recorded_at >= s.recorded_at
        WHERE s.feed = %(feed)s AND s.status = 'STARTED' AND s.recorded_at < %(cutoff)s
        GROUP BY s.series, s.minute""",
        {'feed': feed, 'cutoff': cutoff},
    )
    reconciled = 0
    for series, minute in outstanding:
        record_receipt(
            client,
            database,
            feed=feed,
            series=str(series),
            minute=_utc(minute),
            rows=0,
            sha256='',
            duration_ms=0,
            status='FAILED',
            error_code='WORKER_DIED',
            error='Started receipt has no terminal row; the process died mid-unit.',
        )
        reconciled += 1
    return reconciled


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
