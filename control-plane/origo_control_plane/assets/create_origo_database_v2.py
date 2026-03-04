from dagster import AssetExecutionContext, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.get_clickhouse_client import get_clickhouse_client

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_DATABASE = _CLICKHOUSE.database
client = get_clickhouse_client()


@asset(
    group_name='create_db', description=f'Creates the database {CLICKHOUSE_DATABASE}'
)
def create_database(context: AssetExecutionContext) -> None:

    res = client.query(f"SHOW DATABASES LIKE '{CLICKHOUSE_DATABASE}'")

    if not res.result_set:
        client.command(
            f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE} ENGINE = Atomic'
        )
        context.log.info(f'Created database {CLICKHOUSE_DATABASE}.')

    else:
        context.log.info(f'Database {CLICKHOUSE_DATABASE} already exists, did nothing.')
