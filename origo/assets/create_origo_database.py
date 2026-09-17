import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Protocol

from dagster import AssetExecutionContext, asset

__all__ = [
    'ClickHouseClientProtocol',
    'ClickHouseSettings',
    'create_origo_database',
    'database_exists',
    'get_clickhouse_settings',
    'make_clickhouse_client',
    'table_exists',
]

DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
DEFAULT_CLICKHOUSE_PORT = 9000
DEFAULT_CLICKHOUSE_USER = 'default'


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


class ClickHouseClientProtocol(Protocol):
    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def _get_clickhouse_port() -> int:
    value = os.environ.get('CLICKHOUSE_PORT', str(DEFAULT_CLICKHOUSE_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT environment variable must be an integer.') from exc


def get_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        host=os.environ.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST),
        port=_get_clickhouse_port(),
        user=os.environ.get('CLICKHOUSE_USER', DEFAULT_CLICKHOUSE_USER),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=os.environ.get('CLICKHOUSE_DATABASE', 'origo'),
    )


def make_clickhouse_client(settings: ClickHouseSettings) -> ClickHouseClientProtocol:
    client_factory = getattr(import_module('clickhouse_driver'), 'Client')
    return client_factory(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


_get_clickhouse_settings = get_clickhouse_settings
_make_clickhouse_client = make_clickhouse_client


def database_exists(client: ClickHouseClientProtocol, database: str) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.databases
        WHERE name = '{database}'
        """
    )
    return bool(result[0][0])


def table_exists(
    client: ClickHouseClientProtocol,
    settings: ClickHouseSettings,
    table_name: str,
) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{settings.database}'
          AND name = '{table_name}'
        """
    )
    return bool(result[0][0])


@asset(
    group_name='origo_setup',
    description='Creates the origo database if it does not already exist',
)
def create_origo_database(context: AssetExecutionContext) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        result = client.execute(f"SHOW DATABASES LIKE '{settings.database}'")
        if result:
            return {
                'database_created': False,
                'database_name': settings.database,
                'message': 'Database already exists',
            }

        client.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS {settings.database}
            ENGINE = Atomic
            """
        )
        context.log.info(f'Created database {settings.database}.')
        return {'database_created': True, 'database_name': settings.database}
    finally:
        client.disconnect()
