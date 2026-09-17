"""Relocated verbatim from origo/utils/publish_binance_spot_dollar_kline_snapshot_to_huggingface.py; behaviour unchanged."""

import hashlib
import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast
import pyarrow as pa
import polars as pl
from origo.sources.profiles.formulas.spot_dollar_klines import DOLLAR_KLINE_SIZE
from origo.assets.create_origo_database import ClickHouseSettings, get_clickhouse_settings

DOLLAR_KLINES_TABLE_NAME = 'binance_spot_dollar_klines'

EXPORT_START_DATE = '2020-01-01 00:00:00'


DEFAULT_CLICKHOUSE_HTTP_PORT = 8123


_SUPPORTED_DATETIME_FORMATS = (
    '%Y-%m-%d',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
)


DOLLAR_KLINE_EXPORT_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'dollar_bar_id',
    'open',
    'high',
    'low',
    'close',
    'mean',
    'std',
    'volume',
    'maker_ratio',
    'no_of_trades',
    'open_liquidity',
    'high_liquidity',
    'low_liquidity',
    'close_liquidity',
    'liquidity_sum',
    'maker_volume',
    'maker_liquidity',
]


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(
        self,
        query: str,
        parameters: Mapping[str, object] | None = None,
    ) -> pa.Table:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _get_clickhouse_http_port() -> int:
    value = os.environ.get('CLICKHOUSE_HTTP_PORT', str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_HTTP_PORT environment variable must be an integer.') from exc


def _make_clickhouse_arrow_client(
    settings: ClickHouseSettings,
) -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=settings.host,
            port=_get_clickhouse_http_port(),
            username=settings.user,
            password=settings.password,
            database=settings.database,
        ),
    )


def get_huggingface_token() -> str:
    token = os.environ.get('HF_TOKEN') or os.environ.get('HUGGINGFACE_HUB_TOKEN')
    if not token:
        raise RuntimeError(
            'HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set before publishing to Hugging Face.'
        )

    return token


def get_huggingface_dataset_repo_id(
    *,
    repo_id_env: str | None,
    default_repo_id: str,
) -> str:
    if repo_id_env is None:
        return default_repo_id

    return os.environ.get(repo_id_env, default_repo_id)


def _base_bar_count_for_dollar_size(dollar_size: float) -> int:
    if dollar_size <= 0:
        raise ValueError('dollar_size must be positive.')

    base_bar_count = dollar_size / DOLLAR_KLINE_SIZE
    if not base_bar_count.is_integer():
        raise ValueError(
            f'dollar_size must be an integer multiple of the {DOLLAR_KLINE_SIZE:g} base dollar kline.'
        )

    return int(base_bar_count)


def _validate_clickhouse_identifier(value: str, field_name: str) -> str:
    if re.fullmatch(r'[A-Za-z0-9_]+', value) is None:
        raise ValueError(f'Invalid ClickHouse {field_name}: {value}')
    return value


def _normalize_datetime_literal(value: str, field_name: str) -> str:
    last_error: ValueError | None = None
    for fmt in _SUPPORTED_DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as exc:
            last_error = exc

    message = (
        f'{field_name} must match one of: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS.'
    )
    if last_error is None:
        raise ValueError(message)
    raise ValueError(message) from last_error


def get_binance_spot_dollar_klines(
    *,
    dollar_size: float,
    start_date_limit: str,
    end_date_limit: str,
    table_name: str,
    database_name: str,
) -> pl.DataFrame:
    base_bar_count = _base_bar_count_for_dollar_size(dollar_size)
    table_name = _validate_clickhouse_identifier(table_name, 'table name')
    database_name = _validate_clickhouse_identifier(database_name, 'database name')
    start_date_limit = _normalize_datetime_literal(start_date_limit, 'start_date_limit')
    end_date_limit = _normalize_datetime_literal(end_date_limit, 'end_date_limit')
    settings = get_clickhouse_settings()
    client = _make_clickhouse_arrow_client(settings)

    try:
        query_parameters: dict[str, int | str] = {
            'base_bar_count': base_bar_count,
            'start_dt': start_date_limit,
            'end_dt': end_date_limit,
        }
        arrow_table = client.query_arrow(
            f"""
            SELECT
                -- toDateTime64(_, 3): emit ms-precision so polars >=1.40 from_arrow keeps the
                -- value. A plain min()/max() on these DateTime columns arrives as epoch seconds
                -- and the Datetime("ms") cast below would reinterpret it as ms, collapsing every
                -- timestamp to ~1970 (the same guard dollar_month uses in binance_spot_kline_rollups).
                toDateTime64(min(source_start_datetime), 3) AS start_datetime,
                toDateTime64(max(source_end_datetime), 3) AS end_datetime,
                target_dollar_bar_id AS dollar_bar_id,
                argMin(source_open, tuple(source_start_datetime, source_dollar_bar_id)) AS open,
                max(source_high) AS high,
                min(source_low) AS low,
                argMax(source_close, tuple(source_end_datetime, source_dollar_bar_id)) AS close,
                sum(source_no_of_trades * source_mean) / sum(source_no_of_trades) AS mean,
                sqrt(
                    greatest(
                        sum(source_no_of_trades * ((source_std * source_std) + (source_mean * source_mean))) / sum(source_no_of_trades)
                        - pow(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 2),
                        0
                    )
                ) AS std,
                sumKahan(source_volume) AS volume,
                sum(source_no_of_trades * source_maker_ratio) / sum(source_no_of_trades) AS maker_ratio,
                sum(source_no_of_trades) AS no_of_trades,
                argMin(source_open_liquidity, tuple(source_start_datetime, source_dollar_bar_id)) AS open_liquidity,
                max(source_high_liquidity) AS high_liquidity,
                min(source_low_liquidity) AS low_liquidity,
                argMax(source_close_liquidity, tuple(source_end_datetime, source_dollar_bar_id)) AS close_liquidity,
                sum(source_liquidity_sum) AS liquidity_sum,
                sumKahan(source_maker_volume) AS maker_volume,
                sum(source_maker_liquidity) AS maker_liquidity
            FROM (
                SELECT
                    start_datetime AS source_start_datetime,
                    end_datetime AS source_end_datetime,
                    dollar_bar_id AS source_dollar_bar_id,
                    open AS source_open,
                    high AS source_high,
                    low AS source_low,
                    close AS source_close,
                    mean AS source_mean,
                    std AS source_std,
                    volume AS source_volume,
                    maker_ratio AS source_maker_ratio,
                    no_of_trades AS source_no_of_trades,
                    open_liquidity AS source_open_liquidity,
                    high_liquidity AS source_high_liquidity,
                    low_liquidity AS source_low_liquidity,
                    close_liquidity AS source_close_liquidity,
                    liquidity_sum AS source_liquidity_sum,
                    maker_volume AS source_maker_volume,
                    maker_liquidity AS source_maker_liquidity,
                    toDate(start_datetime) AS bar_date,
                    intDiv(dollar_bar_id, {{base_bar_count:UInt64}}) AS target_dollar_bar_id
                FROM {database_name}.{table_name}
                WHERE start_datetime >= toDateTime({{start_dt:String}})
                  AND start_datetime < toDateTime({{end_dt:String}})
            )
            GROUP BY bar_date, target_dollar_bar_id
            ORDER BY start_datetime ASC, dollar_bar_id ASC
            """,
            parameters=query_parameters,
        )
    finally:
        client.close()

    data = cast(pl.DataFrame, pl.from_arrow(arrow_table)).select(DOLLAR_KLINE_EXPORT_COLUMNS)
    if data.height == 0:
        return data

    return data.with_columns(
        [
            pl.col('start_datetime').cast(pl.Datetime('ms', time_zone='UTC')),
            pl.col('end_datetime').cast(pl.Datetime('ms', time_zone='UTC')),
            pl.col('mean').round(5),
            pl.col('std').round(6),
            pl.col('volume').round(9),
            pl.col('liquidity_sum').round(1),
            pl.col('maker_liquidity').round(1),
        ]
    ).sort(['start_datetime', 'dollar_bar_id'])


def build_dataset_card(
    *,
    export_end_date: str,
    row_count: int,
    file_name: str,
    size_label: str,
    resolution_label: str,
    database_name: str,
) -> str:
    return f"""# BTCUSDT {size_label} dollar spot klines

This dataset is exported daily from `{database_name}.{DOLLAR_KLINES_TABLE_NAME}` using {resolution_label} dollar bars derived from the 1M Origo dollar-kline foundation.

Latest snapshot:

- file: `{file_name}`
- start date: `2020-01-01`
- rows: `{row_count}`
- end date: `{export_end_date}`
- columns: `start_datetime`, `end_datetime`, `dollar_bar_id`, `open`, `high`, `low`, `close`, `mean`, `std`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`

Notes:

- Source market: Binance spot BTCUSDT
- Source table: `{database_name}.{DOLLAR_KLINES_TABLE_NAME}`
- `median` and `iqr` are intentionally omitted from the exported Parquet snapshot
- Timestamps are UTC
"""


def build_snapshot_metadata(
    export_end_date: str,
    file_name: str,
    row_count: int,
    file_sha256: str,
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    return (
        json.dumps(
            {
                'file_name': file_name,
                'export_start_date': '2020-01-01',
                'export_end_date': export_end_date,
                'row_count': row_count,
                'sha256': file_sha256,
                'generated_at_utc': generated_at,
            },
            indent=2,
        )
        + '\n'
    )
