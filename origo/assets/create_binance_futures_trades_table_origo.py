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

RAW_TABLE_NAME = 'binance_daily_futures_trades'
LEDGER_TABLE_NAME = 'binance_daily_futures_trades_ingestion'


def _create_raw_table(client: ClickhouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{RAW_TABLE_NAME} (
            futures_trade_id UInt64 CODEC(Delta(8), ZSTD(3)),
            price Float64 CODEC(Delta, ZSTD(3)),
            quantity Float64 CODEC(ZSTD(3)),
            quote_quantity Float64 CODEC(ZSTD(3)),
            timestamp UInt64 CODEC(Delta, ZSTD(3)),
            is_buyer_maker UInt8 CODEC(ZSTD(1)),
            datetime DateTime64(6) CODEC(Delta, ZSTD(3))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY (datetime, futures_trade_id)
        """
    )


def _create_ingestion_ledger(client: ClickhouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{LEDGER_TABLE_NAME} (
            source_date Date,
            source_file String,
            dagster_run_id String,
            dagster_partition_key String,
            zip_checksum FixedString(64),
            csv_checksum FixedString(64),
            source_row_count UInt64,
            inserted_row_count UInt64,
            loaded_at DateTime,
            status LowCardinality(String)
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(source_date)
        ORDER BY (source_date, source_file)
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates the binance_daily_futures_trades raw table and ingestion ledger if they do not exist',
)
def create_binance_daily_futures_trades_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        if not database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        raw_table_existed = table_exists(client, settings, RAW_TABLE_NAME)
        ledger_table_existed = table_exists(client, settings, LEDGER_TABLE_NAME)

        _create_raw_table(client, settings)
        _create_ingestion_ledger(client, settings)

        context.log.info(
            f'Ensured tables {settings.database}.{RAW_TABLE_NAME} and '
            f'{settings.database}.{LEDGER_TABLE_NAME} exist.'
        )

        return {
            'status': 'success',
            'raw_table': f'{settings.database}.{RAW_TABLE_NAME}',
            'ledger_table': f'{settings.database}.{LEDGER_TABLE_NAME}',
            'raw_table_action': 'already_exists' if raw_table_existed else 'created',
            'ledger_table_action': 'already_exists' if ledger_table_existed else 'created',
        }
    finally:
        client.disconnect()
