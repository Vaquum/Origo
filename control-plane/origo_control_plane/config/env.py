import os
from dataclasses import dataclass


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value.strip()


def require_any_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip() != '':
            return value.strip()
    joined = ', '.join(names)
    raise RuntimeError(f'At least one env var must be set and non-empty: {joined}')


def require_int_env(name: str) -> int:
    raw = require_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got '{raw}'") from exc


def require_int_env_with_default(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == '':
        return default
    value = raw.strip()
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got '{value}'") from exc


@dataclass(frozen=True)
class ClickHouseNativeSettings:
    host: str
    port: int
    user: str
    password: str
    database: str
    send_receive_timeout_seconds: int


@dataclass(frozen=True)
class ClickHouseHttpSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


def resolve_clickhouse_native_settings() -> ClickHouseNativeSettings:
    return ClickHouseNativeSettings(
        host=require_env('CLICKHOUSE_HOST'),
        port=require_int_env('CLICKHOUSE_PORT'),
        user=require_env('CLICKHOUSE_USER'),
        password=require_env('CLICKHOUSE_PASSWORD'),
        database=require_env('CLICKHOUSE_DATABASE'),
        send_receive_timeout_seconds=require_int_env_with_default(
            'CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS',
            default=3600,
        ),
    )


def resolve_clickhouse_http_settings() -> ClickHouseHttpSettings:
    return ClickHouseHttpSettings(
        host=require_env('CLICKHOUSE_HOST'),
        port=require_int_env('CLICKHOUSE_HTTP_PORT'),
        user=require_env('CLICKHOUSE_USER'),
        password=require_env('CLICKHOUSE_PASSWORD'),
        database=require_env('CLICKHOUSE_DATABASE'),
    )
