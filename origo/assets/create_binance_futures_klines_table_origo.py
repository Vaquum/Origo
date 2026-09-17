from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from .create_origo_database import (
    database_exists,
    table_exists,
    ClickHouseSettings,
    _get_clickhouse_settings,
    _make_clickhouse_client,
    create_origo_database,
)

KLINES_TABLE_NAME = 'binance_futures_klines'


def _create_klines_table(client: ClickhouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{KLINES_TABLE_NAME} (
            datetime DateTime,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            mean Float64,
            std Float64,
            median Float64,
            iqr Float64,
            volume Float64,
            maker_ratio Float64,
            no_of_trades UInt64,
            open_liquidity Float64,
            high_liquidity Float64,
            low_liquidity Float64,
            close_liquidity Float64,
            liquidity_sum Float64,
            maker_volume Float64,
            maker_liquidity Float64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY datetime
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates the binance_futures_klines table if it does not exist',
)
def create_binance_futures_klines_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        if not database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_existed = table_exists(client, settings, KLINES_TABLE_NAME)
        _create_klines_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{KLINES_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{KLINES_TABLE_NAME}',
            'table_action': 'already_exists' if table_existed else 'created',
        }
    finally:
        client.disconnect()
