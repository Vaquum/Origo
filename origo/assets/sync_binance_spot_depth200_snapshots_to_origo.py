import json
import os
from datetime import datetime, timedelta, timezone

import requests
from dagster import AssetExecutionContext, TimeWindowPartitionsDefinition, asset

from .create_binance_spot_depth200_snapshots_table_origo import (
    ClickHouseClient,
    SNAPSHOTS_TABLE_NAME,
    clickhouse_scalar_int,
    create_binance_spot_depth200_snapshots_table_origo,
    get_clickhouse_settings,
    make_clickhouse_client,
)

DEPTH200_FIRST_MINUTE_UTC = '2026-06-14T12:56:00+0000'
DEPTH200_PARTITION_KEY_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
depth200_minute_partitions = TimeWindowPartitionsDefinition(
    start=DEPTH200_FIRST_MINUTE_UTC,
    fmt=DEPTH200_PARTITION_KEY_FORMAT,
    cron_schedule='* * * * *',
    timezone='UTC',
)
SnapshotRow = tuple[datetime, int, int, list[tuple[float, float]], list[tuple[float, float]]]


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def minute_start_from_context(context: AssetExecutionContext) -> datetime:
    value = context.partition_key
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _clickhouse_datetime64(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S.000')


def _snapshot_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc).replace(tzinfo=None)


def _price_levels(value: list[list[str]], label: str) -> list[tuple[float, float]]:
    if len(value) != 200:
        raise ValueError(f'{label} must contain 200 price levels.')
    return [(float(price), float(quantity)) for price, quantity in value]


def _parse_snapshot_line(line: str) -> SnapshotRow:
    record = json.loads(line)
    timestamp_ms = int(record['t'])
    depth = record['d']
    return (
        _snapshot_datetime(timestamp_ms),
        timestamp_ms,
        int(depth['lastUpdateId']),
        _price_levels(depth['bids'], 'bids'),
        _price_levels(depth['asks'], 'asks'),
    )


def _history_url(base_url: str, minute_start: datetime) -> str:
    unix_seconds = int(minute_start.timestamp())
    return f'{base_url.rstrip("/")}/history?from={unix_seconds}&to={unix_seconds}'


def _download_history(base_url: str, auth_token: str, minute_start: datetime) -> str:
    response = requests.get(
        _history_url(base_url, minute_start),
        headers={
            'Accept': 'application/x-ndjson',
            'Authorization': f'Bearer {auth_token}',
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def _count_minute_rows(
    client: ClickHouseClient,
    database: str,
    minute_start: datetime,
) -> int:
    minute_end = minute_start + timedelta(minutes=1)
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{SNAPSHOTS_TABLE_NAME} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime64(minute_start)}', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime64(minute_end)}', 3)
        """
    )
    return clickhouse_scalar_int(result)


def sync_minute(
    client: ClickHouseClient,
    database: str,
    minute_start: datetime,
    *,
    base_url: str,
    auth_token: str,
) -> int:
    """Download one closed minute of depth200 snapshots from the history API and insert it.

    The context-free entry point the depth worker calls every minute; the asset below wraps
    it for operator-launched runs. Returns the rows present for the minute after the insert
    and raises when the collector returned no rows.
    """
    history = _download_history(base_url, auth_token, minute_start)
    rows = [_parse_snapshot_line(line) for line in history.splitlines() if line.strip()]
    if not rows:
        raise RuntimeError(f'No Binance spot depth200 snapshots found for {minute_start.isoformat()}')
    client.execute(
        f"""
        INSERT INTO {database}.{SNAPSHOTS_TABLE_NAME}
        (
            datetime,
            source_timestamp_ms,
            last_update_id,
            bids,
            asks
        ) VALUES
        """,
        rows,
    )
    return _count_minute_rows(client, database, minute_start)


@asset(
    partitions_def=depth200_minute_partitions,
    group_name='binance_spot_depth200_data',
    deps=[create_binance_spot_depth200_snapshots_table_origo],
    description='Syncs the last completed minute of Binance spot depth200 snapshots from the history API',
)
def sync_binance_spot_depth200_snapshots_to_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    minute_start = minute_start_from_context(context)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        inserted_count = sync_minute(
            client,
            settings.database,
            minute_start,
            base_url=_require_env('BINANCE_SPOT_DEPTH200_BASE_URL'),
            auth_token=_require_env('BINANCE_SPOT_DEPTH200_AUTH_TOKEN'),
        )

        return {
            'status': 'success',
            'minute_start': minute_start.isoformat(),
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{SNAPSHOTS_TABLE_NAME}',
        }
    finally:
        client.disconnect()
