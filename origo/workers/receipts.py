"""Worker receipts and the container log in ClickHouse.

``worker_minute_log`` holds one row per feed, series and minute a worker processed; it
replaces the Dagster run record as provenance for minute work. ``container_log`` holds
every container's stdout and stderr shipped by Vector, with daily partitions and the
14-day diagnostic retention.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from origo.sources.contracts import Client, identifier

WORKER_MINUTE_LOG = 'worker_minute_log'
CONTAINER_LOG = 'container_log'
_LIMIT = 1000


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
    status: Literal['OK', 'FAILED'],
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


def failed_receipts_since(client: Client, database: str, since: datetime) -> list[Receipt]:
    rows = client.execute(
        f"""SELECT feed, series, minute, status, error_code, error, worker_host, recorded_at
        FROM {identifier(database)}.{WORKER_MINUTE_LOG}
        WHERE status = 'FAILED' AND recorded_at > %(since)s
        ORDER BY recorded_at LIMIT {_LIMIT}""",
        {'since': _utc(since).replace(tzinfo=None)},
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


def error_log_rows_since(client: Client, database: str, since: datetime) -> list[LogRow]:
    rows = client.execute(
        f"""SELECT timestamp, service, container, level, message
        FROM {identifier(database)}.{CONTAINER_LOG}
        WHERE level = 'ERROR' AND timestamp > %(since)s
        ORDER BY timestamp LIMIT {_LIMIT}""",
        {'since': _utc(since).replace(tzinfo=None)},
    )
    return [
        LogRow(_utc(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])) for row in rows
    ]
