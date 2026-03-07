from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

import polars as pl
from clickhouse_connect import get_client as _raw_get_client

from .native_core import (
    LatestRowsWindow,
    MonthWindow,
    QueryWindow,
    RandomRowsWindow,
    TimeRangeWindow,
    resolve_clickhouse_http_settings,
)

logger = logging.getLogger(__name__)
get_client = cast(Any, _raw_get_client)

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_ALIGNED_MS_ALIAS = '__origo_aligned_ms'
BinanceAlignedDataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']


@dataclass(frozen=True)
class BinanceAligned1sMaterialization:
    table_name: str
    id_column: str
    datetime_column: str
    price_column: str
    quantity_column: str

    def __post_init__(self) -> None:
        for value, label in [
            (self.table_name, 'table_name'),
            (self.id_column, 'id_column'),
            (self.datetime_column, 'datetime_column'),
            (self.price_column, 'price_column'),
            (self.quantity_column, 'quantity_column'),
        ]:
            if not _IDENTIFIER_PATTERN.match(value):
                raise ValueError(f'Invalid {label}: {value}')


_BINANCE_ALIGNED_1S_MATERIALIZATIONS: dict[
    BinanceAlignedDataset, BinanceAligned1sMaterialization
] = {
    'spot_trades': BinanceAligned1sMaterialization(
        table_name='binance_trades',
        id_column='trade_id',
        datetime_column='datetime',
        price_column='price',
        quantity_column='quantity',
    ),
    'spot_agg_trades': BinanceAligned1sMaterialization(
        table_name='binance_agg_trades',
        id_column='agg_trade_id',
        datetime_column='datetime',
        price_column='price',
        quantity_column='quantity',
    ),
    'futures_trades': BinanceAligned1sMaterialization(
        table_name='binance_futures_trades',
        id_column='futures_trade_id',
        datetime_column='datetime',
        price_column='price',
        quantity_column='quantity',
    ),
}


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_clickhouse_datetime64_literal(value: str) -> str:
    parsed = _parse_iso_datetime(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _build_where_clause(
    *, datetime_column: str, window: QueryWindow
) -> str:
    if isinstance(window, MonthWindow):
        month_start = f'{window.year:04d}-{window.month:02d}-01'
        return (
            f"WHERE {datetime_column} >= toDateTime('{month_start} 00:00:00') "
            f"AND {datetime_column} < addMonths(toDateTime('{month_start} 00:00:00'), 1)"
        )
    if isinstance(window, TimeRangeWindow):
        start_ch = _to_clickhouse_datetime64_literal(window.start_iso)
        end_ch = _to_clickhouse_datetime64_literal(window.end_iso)
        return (
            f"WHERE {datetime_column} >= toDateTime64('{start_ch}', 3, 'UTC') "
            f"AND {datetime_column} < toDateTime64('{end_ch}', 3, 'UTC')"
        )
    return ''


def _build_grouped_query(
    *,
    database: str,
    materialization: BinanceAligned1sMaterialization,
    where_clause: str,
) -> str:
    datetime_expr = (
        f"toDateTime64({materialization.datetime_column}, 3, 'UTC')"
    )
    return (
        'SELECT '
        f"toUnixTimestamp64Milli(toStartOfSecond({datetime_expr})) AS {_ALIGNED_MS_ALIAS}, "
        f'argMin({materialization.price_column}, {materialization.id_column}) AS open_price, '
        f'max({materialization.price_column}) AS high_price, '
        f'min({materialization.price_column}) AS low_price, '
        f'argMax({materialization.price_column}, {materialization.id_column}) AS close_price, '
        f'sum({materialization.quantity_column}) AS quantity_sum, '
        f'sum({materialization.price_column} * {materialization.quantity_column}) AS quote_volume_sum, '
        'count() AS trade_count '
        f'FROM {database}.{materialization.table_name} '
        f'{where_clause} '
        f'GROUP BY {_ALIGNED_MS_ALIAS}'
    )


def build_binance_aligned_1s_sql(
    *,
    dataset: BinanceAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')

    materialization = _BINANCE_ALIGNED_1S_MATERIALIZATIONS[dataset]
    where_clause = _build_where_clause(
        datetime_column=materialization.datetime_column,
        window=window,
    )
    grouped_query = _build_grouped_query(
        database=database,
        materialization=materialization,
        where_clause=where_clause,
    )

    if isinstance(window, LatestRowsWindow):
        return (
            'SELECT * FROM ('
            f'SELECT * FROM ({grouped_query}) '
            f'ORDER BY {_ALIGNED_MS_ALIAS} DESC LIMIT {window.rows}'
            f') ORDER BY {_ALIGNED_MS_ALIAS} ASC'
        )

    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT * FROM ('
            f'{grouped_query}'
            f') ORDER BY sipHash64({_ALIGNED_MS_ALIAS}) LIMIT {window.rows}'
        )

    return (
        'SELECT * FROM ('
        f'{grouped_query}'
        f') ORDER BY {_ALIGNED_MS_ALIAS} ASC'
    )


def _shape_aligned_frame(
    *, frame: pl.DataFrame, datetime_iso_output: bool
) -> pl.DataFrame:
    if _ALIGNED_MS_ALIAS not in frame.columns:
        raise RuntimeError(
            f'Aligned query result is missing expected column: {_ALIGNED_MS_ALIAS}'
        )

    shaped = frame.with_columns(
        pl.col(_ALIGNED_MS_ALIAS)
        .cast(pl.Int64)
        .cast(pl.Datetime('ms', time_zone='UTC'))
        .alias('aligned_at_utc')
    ).drop(_ALIGNED_MS_ALIAS)
    shaped = shaped.sort('aligned_at_utc')

    if datetime_iso_output:
        shaped = shaped.with_columns(
            pl.col('aligned_at_utc')
            .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
            .alias('aligned_at_utc')
        )

    return shaped


def query_binance_aligned_1s_data(
    *,
    dataset: BinanceAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_binance_aligned_1s_sql(
        dataset=dataset,
        window=window,
        database=settings.database,
    )

    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )

    started_at = time.time()
    try:
        arrow_table = client.query_arrow(sql)
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    raw_frame = pl.DataFrame(arrow_table)
    if isinstance(raw_frame, pl.Series):
        raise RuntimeError('Expected DataFrame from aligned query, got Series')

    shaped = _shape_aligned_frame(
        frame=raw_frame,
        datetime_iso_output=datetime_iso_output,
    )

    if show_summary:
        elapsed = time.time() - started_at
        logger.info(
            '%s | dataset=%s | rows=%d | cols=%d | %.2f GB RAM',
            f'{elapsed:.2f}s',
            dataset,
            shaped.shape[0],
            shaped.shape[1],
            shaped.estimated_size() / (1024**3),
        )

    return shaped
