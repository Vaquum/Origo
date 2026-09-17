from datetime import datetime, timedelta, timezone

from dagster import AssetExecutionContext, asset

from .create_binance_spot_depth200_1m_table_origo import (
    DEPTH200_1M_TABLE_NAME,
    create_binance_spot_depth200_1m_table_origo,
)
from .create_binance_spot_depth200_snapshots_table_origo import (
    ClickHouseClient,
    SNAPSHOTS_TABLE_NAME,
    clickhouse_scalar_int,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .sync_binance_spot_depth200_snapshots_to_origo import (
    depth200_minute_partitions,
    minute_start_from_context,
    sync_binance_spot_depth200_snapshots_to_origo,
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
        FROM {database}.{DEPTH200_1M_TABLE_NAME} FINAL
        WHERE datetime = toDateTime('{_clickhouse_datetime(minute_start)}')
        """
    )
    return clickhouse_scalar_int(result)


def _insert_minute_row(
    client: ClickHouseClient,
    database: str,
    minute_start: datetime,
) -> None:
    minute_end = minute_start + timedelta(minutes=1)
    client.execute(
        f"""
        INSERT INTO {database}.{DEPTH200_1M_TABLE_NAME}
        SELECT
            toStartOfMinute(datetime) AS datetime,
            source_timestamp_ms,
            (bids[1].1 + asks[1].1) / 2 AS book_mid_price,
            ((asks[1].1 - bids[1].1) / book_mid_price) * 10000 AS book_spread_bps,
            arraySum(arrayMap(x -> x.1 * x.2, bids)) AS book_bid_depth_200_notional,
            arraySum(arrayMap(x -> x.1 * x.2, asks)) AS book_ask_depth_200_notional,
            (book_bid_depth_200_notional - book_ask_depth_200_notional)
              / (book_bid_depth_200_notional + book_ask_depth_200_notional) AS book_imbalance_200
        FROM {database}.{SNAPSHOTS_TABLE_NAME} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime(minute_start)}.000', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime(minute_end)}.000', 3)
        ORDER BY source_timestamp_ms DESC
        LIMIT 1
        """
    )


def refresh_minute(client: ClickHouseClient, database: str, minute_start: datetime) -> int:
    """Project one minute of depth200 snapshots into the 1m table; the depth worker's entry
    point, wrapped by the asset below. Raises when the minute has no snapshots."""
    source_count = _count_source_rows(client, database, minute_start)
    if source_count == 0:
        raise RuntimeError(
            f'No Binance spot depth200 source snapshots found for {minute_start.isoformat()}'
        )
    _insert_minute_row(client, database, minute_start)
    return _count_projection_rows(client, database, minute_start)


@asset(
    partitions_def=depth200_minute_partitions,
    group_name='binance_spot_depth200_data',
    deps=[
        create_binance_spot_depth200_1m_table_origo,
        sync_binance_spot_depth200_snapshots_to_origo,
    ],
    description='Refreshes the Binance spot depth200 1m source projection from source-native snapshots',
)
def refresh_binance_spot_depth200_1m_origo(
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
            'table': f'{settings.database}.{DEPTH200_1M_TABLE_NAME}',
        }
    finally:
        client.disconnect()
