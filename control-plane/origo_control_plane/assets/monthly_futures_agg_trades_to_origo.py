from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.asset_insert_to_origo import asset_insert_to_origo
from origo_control_plane.utils.binance_file_to_polars import binance_file_to_polars
from origo_control_plane.utils.check_if_has_header import check_if_has_header
from origo_control_plane.utils.get_clickhouse_client import get_clickhouse_client
from origo_control_plane.utils.get_origo_monthly_table_config import (
    get_origo_monthly_table_config,
)

# There are two things to do here:
# 1) Go through the config below
# 2) Update the function names to end with {CLICKHOUSE_TABLE}
# 3) Update the `client.command` in `create_{CLICKHOUSE_TABLE}`
# 4) Update `file_url` initialization in `insert_monthly_{CLICKHOUSE_TABLE}`
# 5) Update the function names

## CONFIG STARTS ##

_CLICKHOUSE = resolve_clickhouse_native_settings()

# Set the database to be used
CLICKHOUSE_DATABASE = _CLICKHOUSE.database

# Set the table to be used
CLICKHOUSE_TABLE = 'binance_futures_agg_trades'

# This is left as it is (for config)
ID_COL = f'{CLICKHOUSE_TABLE.replace("binance_", "")}_id'

# Set the starting month
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date='2020-01-01')

# Set the base url for the files to download
BASE_URL = 'https://data.binance.vision/data/futures/um/monthly/aggTrades/BTCUSDT/'

# Set the column names as per the data
DATA_COLS = [
    ID_COL,
    'price',
    'quantity',
    'first_trade_id',
    'last_trade_id',
    'timestamp',
    'is_buyer_maker',
]

## CONFIG ENDS ##

## ASSETS START ##

client = get_clickhouse_client()


@asset(
    group_name=f'create_db_table_{CLICKHOUSE_DATABASE}_{CLICKHOUSE_TABLE}',
    description=f'Creates the db table {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}',
)
def create_binance_futures_agg_trades_table(context: AssetExecutionContext):

    client.command(f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} (
            {ID_COL}        UInt64  CODEC(Delta(8), ZSTD(3)),
            price           Float64 CODEC(Delta, ZSTD(3)),
            quantity        Float64 CODEC(ZSTD(3)),
            first_trade_id  UInt64  CODEC(Delta(8), ZSTD(3)),
            last_trade_id   UInt64  CODEC(Delta(8), ZSTD(3)),
            timestamp       UInt64  CODEC(Delta, ZSTD(3)),
            is_buyer_maker  UInt8   CODEC(ZSTD(1)),
            datetime        DateTime CODEC(Delta, ZSTD(3))
        )
        {get_origo_monthly_table_config(ID_COL)}""")
    context.log.info(
        f'Created database table {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}.'
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name='insert_monthly_data',
    description=f'Inserts monthly data into {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}.',
)
def insert_monthly_binance_futures_agg_trades_to_origo(
    context: AssetExecutionContext,
) -> None:

    partition_date_str = context.asset_partition_key_for_output()
    date_parts = partition_date_str.split('-')
    year, month = date_parts[0], date_parts[1]

    file_url = f'BTCUSDT-aggTrades-{year}-{month}.zip'
    context.log.info(
        f'Processing selected partition: {partition_date_str}, file: {file_url}'
    )

    full_url = BASE_URL + file_url

    data = binance_file_to_polars(full_url, has_header=check_if_has_header(full_url))
    data.columns = DATA_COLS
    context.log.info(f'Completed reading {BASE_URL} into a DataFrame.')

    asset_insert_to_origo(
        data, client, context, file_url, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE
    )


## ASSETS END ##
