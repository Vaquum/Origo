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
    description='Creates the binance_trades table with optimal settings for high-volume trade data',
)
def create_binance_trades_table_origo(context: AssetExecutionContext):
    """
    Creates a highly optimized ClickHouse table for Binance trades data.
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
            f"SELECT count() FROM system.tables WHERE database = '{CLICKHOUSE_DATABASE}' AND name = 'binance_trades'"
        )
        was_dropped = False

        # If the table exists, drop it
        if table_exists[0][0]:
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades already exists. Dropping it...'
            )
            client.execute(f'DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.binance_trades')
            context.log.info(
                f'Table {CLICKHOUSE_DATABASE}.binance_trades has been dropped.'
            )
            was_dropped = True

        # Create the binance_trades table
        context.log.info(f'Creating table {CLICKHOUSE_DATABASE}.binance_trades...')
        client.execute(f"""
            CREATE TABLE {CLICKHOUSE_DATABASE}.binance_trades (
                trade_id        UInt64  CODEC(Delta(8), ZSTD(3)),
                price           Float64 CODEC(Delta, ZSTD(3)),
                quantity        Float64 CODEC(ZSTD(3)),
                quote_quantity  Float64 CODEC(ZSTD(3)),
                timestamp       UInt64  CODEC(Delta, ZSTD(3)),
                is_buyer_maker  UInt8   CODEC(ZSTD(1)),
                is_best_match   UInt8   CODEC(ZSTD(1)),
                datetime        DateTime CODEC(Delta, ZSTD(3))
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(datetime)
            ORDER BY (toStartOfDay(datetime), trade_id)
            SAMPLE BY trade_id
            SETTINGS 
                index_granularity = 8192,
                enable_mixed_granularity_parts = 1,
                min_rows_for_wide_part = 1000000,
                min_bytes_for_wide_part = 10000000,
                min_rows_for_compact_part = 10000,
                write_final_mark = 0
        """)
        context.log.info(
            f'Table {CLICKHOUSE_DATABASE}.binance_trades has been created successfully.'
        )

        return {
            'status': 'success',
            'table': f'{CLICKHOUSE_DATABASE}.binance_trades',
            'action': 'recreated' if was_dropped else 'created',
        }

    except Exception as e:
        context.log.error(f'Error creating binance_trades table: {e!s}')
        return {'status': 'error', 'message': str(e)}

    finally:
        # Ensure client is disconnected
        if client:
            try:
                client.disconnect()
            except Exception as e:
                context.log.warning(f'Error disconnecting from ClickHouse: {e!s}')
