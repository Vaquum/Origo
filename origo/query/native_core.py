from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import polars as pl
from clickhouse_connect import get_client as _raw_get_client

get_client = cast(Any, _raw_get_client)

logger = logging.getLogger(__name__)

IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _require_any_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip() != '':
            return value.strip()
    joined = ', '.join(names)
    raise RuntimeError(f'At least one env var must be set and non-empty: {joined}')


def _require_any_int_env(*names: str) -> int:
    raw = _require_any_env(*names)
    try:
        return int(raw)
    except ValueError as exc:
        joined = ', '.join(names)
        raise RuntimeError(
            f"Expected integer env var in [{joined}], got '{raw}'"
        ) from exc


def _validate_identifier(value: str, label: str) -> None:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f'Invalid {label}: {value}')


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_clickhouse_datetime64_literal(value: str) -> str:
    parsed = _parse_iso_datetime(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


@dataclass(frozen=True)
class ClickHouseHTTPSettings:
    host: str
    port: int
    username: str
    password: str
    database: str


def resolve_clickhouse_http_settings(
    auth_token: str | None = None,
) -> ClickHouseHTTPSettings:
    token = auth_token.strip() if auth_token is not None else ''
    password = (
        token
        if token
        else _require_any_env('ORIGO_CLICKHOUSE_PASSWORD', 'CLICKHOUSE_PASSWORD')
    )
    database = _require_any_env('ORIGO_CLICKHOUSE_DATABASE', 'CLICKHOUSE_DATABASE')
    _validate_identifier(database, 'database')

    return ClickHouseHTTPSettings(
        host=_require_any_env('ORIGO_CLICKHOUSE_HOST', 'CLICKHOUSE_HOST'),
        port=_require_any_int_env(
            'ORIGO_CLICKHOUSE_PORT',
            'ORIGO_CLICKHOUSE_HTTP_PORT',
            'CLICKHOUSE_HTTP_PORT',
        ),
        username=_require_any_env('ORIGO_CLICKHOUSE_USER', 'CLICKHOUSE_USER'),
        password=password,
        database=database,
    )


@dataclass(frozen=True)
class MonthWindow:
    month: int
    year: int

    def __post_init__(self) -> None:
        if self.month < 1 or self.month > 12:
            raise ValueError(f'month must be in 1..12, got {self.month}')
        if self.year < 1970:
            raise ValueError(f'year must be >= 1970, got {self.year}')


@dataclass(frozen=True)
class LatestRowsWindow:
    rows: int

    def __post_init__(self) -> None:
        if self.rows <= 0:
            raise ValueError(f'rows must be > 0, got {self.rows}')


@dataclass(frozen=True)
class RandomRowsWindow:
    rows: int

    def __post_init__(self) -> None:
        if self.rows <= 0:
            raise ValueError(f'rows must be > 0, got {self.rows}')


@dataclass(frozen=True)
class TimeRangeWindow:
    start_iso: str
    end_iso: str

    def __post_init__(self) -> None:
        start_dt = _parse_iso_datetime(self.start_iso)
        end_dt = _parse_iso_datetime(self.end_iso)
        if start_dt >= end_dt:
            raise ValueError(
                f'time range must satisfy start < end, got {self.start_iso} .. {self.end_iso}'
            )


type QueryWindow = MonthWindow | LatestRowsWindow | RandomRowsWindow | TimeRangeWindow


@dataclass(frozen=True)
class NativeQuerySpec:
    table_name: str
    id_column: str
    select_columns: tuple[str, ...]
    window: QueryWindow
    include_datetime: bool = True

    def __post_init__(self) -> None:
        _validate_identifier(self.table_name, 'table_name')
        _validate_identifier(self.id_column, 'id_column')
        if not self.select_columns:
            raise ValueError('select_columns must be non-empty')
        for column in self.select_columns:
            _validate_identifier(column, 'column')


@dataclass(frozen=True)
class _CompiledNativeQuery:
    sql: str
    id_requested: bool
    timestamp_requested: bool
    datetime_requested: bool
    datetime_materialized: bool


def _compile_native_query(spec: NativeQuerySpec, database: str) -> _CompiledNativeQuery:
    _validate_identifier(database, 'database')

    requested_columns = _dedupe_preserve_order(spec.select_columns)
    id_requested = spec.id_column in requested_columns
    timestamp_requested = 'timestamp' in requested_columns
    datetime_requested = spec.include_datetime or 'datetime' in requested_columns

    projected_columns = requested_columns.copy()

    if spec.id_column not in projected_columns:
        projected_columns.append(spec.id_column)

    if datetime_requested or isinstance(spec.window, (MonthWindow, LatestRowsWindow)):
        if 'datetime' not in projected_columns:
            projected_columns.append('datetime')

    if isinstance(spec.window, RandomRowsWindow):
        if 'timestamp' not in projected_columns:
            projected_columns.append('timestamp')

    if isinstance(spec.window, MonthWindow):
        where_clause = (
            f"WHERE datetime >= toDateTime('{spec.window.year:04d}-{spec.window.month:02d}-01 00:00:00') "
            f"AND datetime < addMonths(toDateTime('{spec.window.year:04d}-{spec.window.month:02d}-01 00:00:00'), 1)"
        )
        tail = f'{where_clause} ORDER BY datetime ASC, {spec.id_column} ASC'
    elif isinstance(spec.window, TimeRangeWindow):
        start_ch = _to_clickhouse_datetime64_literal(spec.window.start_iso)
        end_ch = _to_clickhouse_datetime64_literal(spec.window.end_iso)
        where_clause = (
            f"WHERE datetime >= toDateTime64('{start_ch}', 3, 'UTC') "
            f"AND datetime < toDateTime64('{end_ch}', 3, 'UTC')"
        )
        tail = f'{where_clause} ORDER BY datetime ASC, {spec.id_column} ASC'
    elif isinstance(spec.window, LatestRowsWindow):
        tail = f'ORDER BY toStartOfDay(datetime) DESC, {spec.id_column} DESC LIMIT {spec.window.rows}'
    else:
        tail = f'ORDER BY sipHash64(tuple({spec.id_column}, timestamp)) LIMIT {spec.window.rows}'

    projected_expressions: list[str] = []
    datetime_materialized = False

    for column in projected_columns:
        if column == 'datetime':
            projected_expressions.append(
                "toUnixTimestamp64Milli(toDateTime64(datetime, 3, 'UTC')) AS datetime_ms"
            )
            datetime_materialized = True
            continue
        projected_expressions.append(column)

    sql = f'SELECT {", ".join(projected_expressions)} FROM {database}.{spec.table_name} {tail}'

    return _CompiledNativeQuery(
        sql=sql,
        id_requested=id_requested,
        timestamp_requested=timestamp_requested,
        datetime_requested=datetime_requested,
        datetime_materialized=datetime_materialized,
    )


def _shape_native_frame(
    frame: pl.DataFrame, id_column: str, compiled: _CompiledNativeQuery
) -> pl.DataFrame:
    if compiled.datetime_materialized and 'datetime_ms' in frame.columns:
        frame = frame.with_columns(
            [
                pl.col('datetime_ms')
                .cast(pl.UInt64)
                .cast(pl.Datetime('ms', time_zone='UTC'))
                .alias('datetime')
            ]
        ).drop('datetime_ms')

    if 'timestamp' in frame.columns:
        frame = frame.with_columns(
            [
                pl.when(pl.col('timestamp') < 10**13)
                .then(pl.col('timestamp'))
                .otherwise(pl.col('timestamp') // 1000)
                .cast(pl.UInt64)
                .alias('timestamp')
            ]
        )

    if 'datetime' in frame.columns and not compiled.datetime_materialized:
        frame = frame.with_columns(
            [
                pl.col('datetime')
                .cast(pl.Datetime('ms', time_zone='UTC'))
                .alias('datetime')
            ]
        )

    if 'datetime' in frame.columns:
        frame = frame.sort(['datetime', id_column])
    elif id_column in frame.columns:
        frame = frame.sort(id_column)

    return frame


def _drop_internal_columns(
    frame: pl.DataFrame, compiled: _CompiledNativeQuery, id_column: str
) -> pl.DataFrame:
    drop_cols: list[str] = []

    if not compiled.id_requested and id_column in frame.columns:
        drop_cols.append(id_column)

    if not compiled.datetime_requested and 'datetime' in frame.columns:
        drop_cols.append('datetime')

    if not compiled.timestamp_requested and 'timestamp' in frame.columns:
        drop_cols.append('timestamp')

    if drop_cols:
        frame = frame.drop(drop_cols)

    return frame


def execute_native_query(
    spec: NativeQuerySpec,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    compiled = _compile_native_query(spec, database=settings.database)

    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )

    start = time.time()
    try:
        arrow_table = client.query_arrow(compiled.sql)
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    polars_obj = pl.DataFrame(arrow_table)
    if isinstance(polars_obj, pl.Series):
        raise RuntimeError('Expected DataFrame from ClickHouse query, got Series')
    frame = polars_obj
    frame = _shape_native_frame(frame, id_column=spec.id_column, compiled=compiled)
    frame = _drop_internal_columns(frame, compiled=compiled, id_column=spec.id_column)

    if datetime_iso_output and 'datetime' in frame.columns:
        frame = frame.with_columns(
            [pl.col('datetime').dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ').alias('datetime')]
        )

    if show_summary:
        elapsed = time.time() - start
        logger.info(
            '%s s | %d rows | %d cols | %.2f GB RAM',
            f'{elapsed:.2f}',
            frame.shape[0],
            frame.shape[1],
            frame.estimated_size() / (1024**3),
        )

    return frame
