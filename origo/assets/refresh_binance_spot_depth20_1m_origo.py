from datetime import datetime, timedelta, timezone

from dagster import AssetExecutionContext, asset

from .create_binance_spot_depth20_1m_table_origo import (
    DEPTH20_1M_TABLE_NAME,
    create_binance_spot_depth20_1m_table_origo,
)
from .create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient,
    SNAPSHOTS_TABLE_NAME,
    clickhouse_scalar_int,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .sync_binance_spot_depth20_snapshots_to_origo import (
    depth20_minute_partitions,
    minute_start_from_context,
    sync_binance_spot_depth20_snapshots_to_origo,
)


def _clickhouse_datetime(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S')


def _count_source_rows(
    client: ClickHouseClient,
    database: str,
    minute_start: datetime,
) -> int:
    minute_end = minute_start + timedelta(minutes=1)
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{SNAPSHOTS_TABLE_NAME} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime(minute_start)}.000', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime(minute_end)}.000', 3)
        """
    )
    return clickhouse_scalar_int(result)


def _count_projection_rows(
    client: ClickHouseClient,
    database: str,
    minute_start: datetime,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{DEPTH20_1M_TABLE_NAME} FINAL
        WHERE datetime = toDateTime('{_clickhouse_datetime(minute_start)}')
        """
    )
    return clickhouse_scalar_int(result)


def _insert_minute_rows(
    client: ClickHouseClient,
    database: str,
    start: datetime,
    end: datetime,
) -> None:
    """Project every minute in [start, end) from its latest snapshot."""
    client.execute(
        f"""
        INSERT INTO {database}.{DEPTH20_1M_TABLE_NAME}
        SELECT
            toStartOfMinute(datetime) AS minute,
            source_timestamp_ms,
            (bids[1].1 + asks[1].1) / 2 AS book_mid_price,
            ((asks[1].1 - bids[1].1) / book_mid_price) * 10000 AS book_spread_bps,
            arraySum(arrayMap(x -> x.1 * x.2, bids)) AS book_bid_depth_20_notional,
            arraySum(arrayMap(x -> x.1 * x.2, asks)) AS book_ask_depth_20_notional,
            (book_bid_depth_20_notional - book_ask_depth_20_notional)
              / (book_bid_depth_20_notional + book_ask_depth_20_notional) AS book_imbalance_20
        FROM {database}.{SNAPSHOTS_TABLE_NAME} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime(start)}.000', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime(end)}.000', 3)
        ORDER BY source_timestamp_ms DESC
        LIMIT 1 BY minute
        """
    )


def _snapshot_day(client: ClickHouseClient, database: str, bound: str) -> datetime:
    seconds = clickhouse_scalar_int(
        client.execute(
            f'SELECT toUnixTimestamp(toStartOfDay({bound}(datetime))) '
            f'FROM {database}.{SNAPSHOTS_TABLE_NAME}'
        )
    )
    return datetime.fromtimestamp(seconds, timezone.utc)


def refresh_minute(client: ClickHouseClient, database: str, minute_start: datetime) -> int:
    """Project one minute of depth20 snapshots into the 1m table; the depth worker's entry
    point, wrapped by the asset below. Raises when the minute has no snapshots."""
    source_count = _count_source_rows(client, database, minute_start)
    if source_count == 0:
        raise RuntimeError(
            f'No Binance spot depth20 source snapshots found for {minute_start.isoformat()}'
        )
    _insert_minute_rows(client, database, minute_start, minute_start + timedelta(minutes=1))
    return _count_projection_rows(client, database, minute_start)


@asset(
    partitions_def=depth20_minute_partitions,
    group_name='binance_spot_depth20_data',
    deps=[
        create_binance_spot_depth20_1m_table_origo,
        sync_binance_spot_depth20_snapshots_to_origo,
    ],
    description='Refreshes the Binance spot depth20 1m source projection from source-native snapshots',
)
def refresh_binance_spot_depth20_1m_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    minute_start = minute_start_from_context(context)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        source_count = _count_source_rows(client, settings.database, minute_start)
        inserted_count = refresh_minute(client, settings.database, minute_start)

        return {
            'status': 'success',
            'minute_start': minute_start.isoformat(),
            'source_rows': source_count,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{DEPTH20_1M_TABLE_NAME}',
        }
    finally:
        client.disconnect()


@asset(
    group_name='binance_spot_depth20_data',
    description='Re-projects every depth20 1m minute from its latest retained snapshot, one UTC day at a time',
)
def repair_binance_spot_depth20_1m_history_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        snapshots = clickhouse_scalar_int(
            client.execute(f'SELECT count() FROM {settings.database}.{SNAPSHOTS_TABLE_NAME}')
        )
        if snapshots == 0:
            raise RuntimeError('No Binance spot depth20 snapshots to project.')
        first_day = _snapshot_day(client, settings.database, 'min')
        last_day = _snapshot_day(client, settings.database, 'max')
        day = first_day
        while day <= last_day:
            _insert_minute_rows(client, settings.database, day, day + timedelta(days=1))
            context.log.info(f'Re-projected depth20 minutes of {day.date().isoformat()}')
            day += timedelta(days=1)

        return {
            'first_day': first_day.date().isoformat(),
            'last_day': last_day.date().isoformat(),
            'days': (last_day - first_day).days + 1,
            'table': f'{settings.database}.{DEPTH20_1M_TABLE_NAME}',
        }
    finally:
        client.disconnect()
