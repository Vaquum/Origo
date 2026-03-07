from dagster import AssetExecutionContext, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.get_clickhouse_client import get_clickhouse_client


@asset(
    group_name='create_db', description='Creates the configured ClickHouse database'
)
def create_database(context: AssetExecutionContext) -> None:
    settings = resolve_clickhouse_native_settings()
    client = get_clickhouse_client()
    try:
        res = client.query(f"SHOW DATABASES LIKE '{settings.database}'")

        if not res.result_set:
            client.command(
                f'CREATE DATABASE IF NOT EXISTS {settings.database} ENGINE = Atomic'
            )
            context.log.info(f'Created database {settings.database}.')

        else:
            context.log.info(
                f'Database {settings.database} already exists, did nothing.'
            )
    finally:
        client.close()
