from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.asset_insert_to_origo import asset_insert_to_origo
from origo_control_plane.utils.binance_file_to_polars import binance_file_to_polars
from origo_control_plane.utils.check_if_has_header import check_if_has_header
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_frame,
)
from origo_control_plane.utils.get_clickhouse_client import get_clickhouse_client

from .schema_migration_only import raise_schema_migration_only

## CONFIG STARTS ##

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


@asset(
    group_name=f'create_db_table_{CLICKHOUSE_TABLE}',
    description=f'Creates the db table for {CLICKHOUSE_TABLE}',
)
def create_binance_futures_agg_trades_table(context: AssetExecutionContext) -> None:
    raise_schema_migration_only(
        context=context,
        asset_name='create_binance_futures_agg_trades_table',
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name='insert_monthly_data',
    description=f'Inserts monthly data into configured database table {CLICKHOUSE_TABLE}.',
)
def insert_monthly_binance_futures_agg_trades_to_origo(
    context: AssetExecutionContext,
) -> None:
    settings = resolve_clickhouse_native_settings()
    client = get_clickhouse_client()
    try:
        partition_date_str = context.asset_partition_key_for_output()
        date_parts = partition_date_str.split('-')
        year, month = date_parts[0], date_parts[1]

        file_url = f'BTCUSDT-aggTrades-{year}-{month}.zip'
        context.log.info(
            f'Processing selected partition: {partition_date_str}, file: {file_url}'
        )

        full_url = BASE_URL + file_url

        data = binance_file_to_polars(
            full_url, has_header=check_if_has_header(full_url)
        )
        data.columns = DATA_COLS
        context.log.info(f'Completed reading {BASE_URL} into a DataFrame.')

        integrity_report = run_exchange_integrity_suite_frame(
            dataset='futures_agg_trades',
            frame=data,
        )
        context.log.info(
            f'Exchange integrity suite passed: {integrity_report.to_dict()}'
        )

        asset_insert_to_origo(
            data,
            client,
            context,
            file_url,
            settings.database,
            CLICKHOUSE_TABLE,
        )
    finally:
        client.close()


## ASSETS END ##
