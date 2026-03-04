from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from origo_control_plane.config import resolve_clickhouse_native_settings

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database


@asset(
    group_name='origo_setup',
    description='Creates the binance_trades_hourly_summary table for aggregated hourly statistics',
)
def create_binance_trades_hourly_summary(context: AssetExecutionContext):
    """
    Creates a materialized view for hourly trade statistics from Binance trades data.
    """
    client = None
    try:
        # Connect to ClickHouse
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

        # Check if the database exists
        db_exists = client.execute(
            f"SELECT count() FROM system.databases WHERE name = '{CLICKHOUSE_DATABASE}'"
        )
        if not db_exists[0][0]:
            context.log.error(
                f'Database {CLICKHOUSE_DATABASE} does not exist. Please create it first.'
            )
            return {
                'status': 'error',
                'message': f'Database {CLICKHOUSE_DATABASE} does not exist',
            }

        # Check if the table already exists
        table_exists = client.execute(
            f"SELECT count() FROM system.tables WHERE database = '{CLICKHOUSE_DATABASE}' AND name = 'binance_trades_hourly_summary'"
        )
        was_dropped = False

        # If the table exists, drop it
        if table_exists[0][0]:
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary already exists. Dropping it...'
            )
            client.execute(
                f'DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
            )
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary has been dropped.'
            )
            was_dropped = True
        else:
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary does not exist. Creating it...'
            )

        # Create the binance_trades_hourly_summary table
        context.log.info(
            f'Creating table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary...'
        )
        client.execute(f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary
            (
                hour_start               DateTime,
                total_trades             UInt64,
                trades_per_minute        Float64,
                total_quantity           Float64,
                total_quote_quantity     Float64,
                vwap                     Float64,
                total_notional           Float64,
                avg_liquidity_per_trade  Float64,
                liquidity_per_trade      Float64,
                min_liquidity_per_trade  Float64,
                max_liquidity_per_trade  Float64,
                quantile_25_price        Float64,
                quantile_50_price        Float64,
                quantile_75_price        Float64,
                avg_price                Float64,
                min_price                Float64,
                max_price                Float64,
                price_stddev             Float64,
                quantity_per_trade       Float64,
                maker_ratio              Float64
            )
            ENGINE = MergeTree
            ORDER BY hour_start
            SETTINGS index_granularity = 8192
        """)
        context.log.info(
            f'Table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary has been created successfully.'
        )

        # Backfill the table with aggregated data
        context.log.info(
            f'Backfilling {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary with aggregated data...'
        )
        client.execute(f"""
            INSERT INTO {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary
            SELECT
              toStartOfHour(datetime)                                                                      AS hour_start,
              toUInt64(max(trade_id) - min(trade_id))                                                      AS total_trades,
              (max(trade_id) - min(trade_id)) / 60                                                         AS trades_per_minute,
              sum(quantity)                                                                                AS total_quantity,
              sum(quote_quantity)                                                                          AS total_quote_quantity,
              sum(price * quantity) / sum(quantity)                                                        AS vwap,
              sum(price * quantity)                                                                        AS total_notional,
              avg(price * quantity)                                                                        AS avg_liquidity_per_trade,
              sum(price * quantity) / (max(trade_id) - min(trade_id))                                      AS liquidity_per_trade,
              min(price * quantity)                                                                        AS min_liquidity_per_trade,
              max(price * quantity)                                                                        AS max_liquidity_per_trade,
              quantile(0.25)(price)                                                                        AS quantile_25_price,
              quantile(0.5)(price)                                                                         AS quantile_50_price,
              quantile(0.75)(price)                                                                        AS quantile_75_price,
              avg(price)                                                                                   AS avg_price,
              min(price)                                                                                   AS min_price,
              max(price)                                                                                   AS max_price,
              stddevPop(price)                                                                             AS price_stddev,
              sum(quantity) / (max(trade_id) - min(trade_id))                                              AS quantity_per_trade,
              countIf(is_buyer_maker = 1) / (max(trade_id) - min(trade_id))                                AS maker_ratio
            FROM {CLICKHOUSE_DATABASE}.binance_trades
            GROUP BY hour_start
            ORDER BY hour_start
            SETTINGS max_execution_time = 300
        """)
        context.log.info(
            f'Successfully backfilled table {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
        )

        # Verify the data was inserted
        count_result = client.execute(
            f'SELECT count() FROM {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
        )
        row_count = count_result[0][0]
        context.log.info(
            f'Inserted {row_count} hourly summaries into {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
        )

        # Get summary statistics for verification
        min_hour_result = client.execute(
            f'SELECT min(hour_start) FROM {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
        )
        max_hour_result = client.execute(
            f'SELECT max(hour_start) FROM {CLICKHOUSE_DATABASE}.binance_trades_hourly_summary'
        )
        min_hour = min_hour_result[0][0]
        max_hour = max_hour_result[0][0]

        return {
            'status': 'success',
            'table': f'{CLICKHOUSE_DATABASE}.binance_trades_hourly_summary',
            'action': 'recreated' if was_dropped else 'created',
            'row_count': row_count,
            'date_range': f'{min_hour} to {max_hour}',
        }

    except Exception as e:
        context.log.error(f'Error creating binance_trades_hourly_summary table: {e!s}')
        return {'status': 'error', 'message': str(e)}

    finally:
        # Ensure client is disconnected
        if client:
            try:
                client.disconnect()
            except Exception as e:
                context.log.warning(f'Error disconnecting from ClickHouse: {e!s}')
