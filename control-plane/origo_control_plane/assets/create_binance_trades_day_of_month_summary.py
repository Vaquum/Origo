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
    description='Creates the binance_trades_day_of_month_summary table for statistics aggregated by day of month',
)
def create_binance_trades_day_of_month_summary(context: AssetExecutionContext):
    """
    Creates a table for trade statistics aggregated by day of month from Binance trades data.
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
            f"SELECT count() FROM system.tables WHERE database = '{CLICKHOUSE_DATABASE}' AND name = 'binance_trades_day_of_month_summary'"
        )
        was_dropped = False

        # If the table exists, drop it
        if table_exists[0][0]:
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary already exists. Dropping it...'
            )
            client.execute(
                f'DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
            )
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary has been dropped.'
            )
            was_dropped = True
        else:
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary does not exist. Creating it...'
            )

        # Create the binance_trades_day_of_month_summary table
        context.log.info(
            f'Creating table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary...'
        )
        client.execute(f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary
            (
                nth_day                   UInt8,
                total_trades               UInt64,
                trades_per_minute          Float64,
                total_quantity             Float64,
                total_quote_quantity       Float64,
                vwap                       Float64,
                total_notional             Float64,
                avg_liquidity_per_trade    Float64,
                liquidity_per_trade        Float64,
                min_liquidity_per_trade    Float64,
                max_liquidity_per_trade    Float64,
                quantile_25_price          Float64,
                quantile_50_price          Float64,
                quantile_75_price          Float64,
                avg_price                  Float64,
                min_price                  Float64,
                max_price                  Float64,
                price_stddev               Float64,
                quantity_per_trade         Float64,
                maker_ratio                Float64
            )
            ENGINE = MergeTree
            ORDER BY nth_day
            SETTINGS index_granularity = 8192
        """)
        context.log.info(
            f'Table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary has been created successfully.'
        )

        # Backfill the table with aggregated data
        context.log.info(
            f'Backfilling {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary with aggregated data...'
        )
        client.execute(f"""
            INSERT INTO {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary
            SELECT
              toDayOfMonth(datetime)                                 AS nth_day,
              count()                                                AS total_trades,
              round(count() / 60, 2)                                 AS trades_per_minute,
              round(sum(quantity), 2)                                AS total_quantity,
              round(sum(quote_quantity), 2)                          AS total_quote_quantity,
              round(sum(price * quantity) / sum(quantity), 2)         AS vwap,
              round(sum(price * quantity), 2)                        AS total_notional,
              round(avg(price * quantity), 2)                        AS avg_liquidity_per_trade,
              round(sum(price * quantity) / count(), 2)              AS liquidity_per_trade,
              round(min(price * quantity), 7)                        AS min_liquidity_per_trade,
              round(max(price * quantity), 2)                        AS max_liquidity_per_trade,
              round(quantile(0.25)(price), 2)                        AS quantile_25_price,
              round(quantile(0.5)(price), 2)                         AS quantile_50_price,
              round(quantile(0.75)(price), 2)                        AS quantile_75_price,
              round(avg(price), 2)                                   AS avg_price,
              round(min(price), 2)                                   AS min_price,
              round(max(price), 2)                                   AS max_price,
              round(stddevPop(price), 2)                             AS price_stddev,
              round(sum(quantity) / count(), 7)                      AS quantity_per_trade,
              round(countIf(is_buyer_maker = 1) / count(), 2)         AS maker_ratio
            FROM {CLICKHOUSE_DATABASE}.binance_trades
            GROUP BY nth_day
            ORDER BY nth_day
            SETTINGS max_execution_time = 300
        """)
        context.log.info(
            f'Successfully backfilled table {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
        )

        # Verify the data was inserted
        count_result = client.execute(
            f'SELECT count() FROM {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
        )
        row_count = count_result[0][0]
        context.log.info(
            f'Inserted {row_count} day-of-month summaries into {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
        )

        # Get min and max day for verification
        min_day_result = client.execute(
            f'SELECT min(nth_day) FROM {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
        )
        max_day_result = client.execute(
            f'SELECT max(nth_day) FROM {CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary'
        )
        min_day = min_day_result[0][0]
        max_day = max_day_result[0][0]

        return {
            'status': 'success',
            'table': f'{CLICKHOUSE_DATABASE}.binance_trades_day_of_month_summary',
            'action': 'recreated' if was_dropped else 'created',
            'row_count': row_count,
            'day_range': f'{min_day} to {max_day}',
        }

    except Exception as e:
        context.log.error(
            f'Error creating binance_trades_day_of_month_summary table: {e!s}'
        )
        return {'status': 'error', 'message': str(e)}

    finally:
        # Ensure client is disconnected
        if client:
            try:
                client.disconnect()
            except Exception as e:
                context.log.warning(f'Error disconnecting from ClickHouse: {e!s}')
