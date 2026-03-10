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
_BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS: tuple[str, ...] = (
    'aligned_at_utc',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'dimensions_json',
    'provenance_json',
    'latest_ingested_at_utc',
    'records_in_bucket',
)

BitcoinDerivedAlignedDataset = Literal[
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]


@dataclass(frozen=True)
class BitcoinDerivedAligned1sMaterialization:
    table_name: str
    datetime_column: str
    rank_id_column: str
    metric_name: str
    metric_unit: str
    metric_value_float_column: str
    source_id: str
    dataset_name: str

    def __post_init__(self) -> None:
        for value, label in [
            (self.table_name, 'table_name'),
            (self.datetime_column, 'datetime_column'),
            (self.rank_id_column, 'rank_id_column'),
            (self.metric_value_float_column, 'metric_value_float_column'),
        ]:
            if not _IDENTIFIER_PATTERN.match(value):
                raise ValueError(f'Invalid {label}: {value}')
        for value, label in [
            (self.metric_name, 'metric_name'),
            (self.metric_unit, 'metric_unit'),
            (self.source_id, 'source_id'),
            (self.dataset_name, 'dataset_name'),
        ]:
            if value.strip() == '':
                raise ValueError(f'{label} must be non-empty')


_BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS: dict[
    BitcoinDerivedAlignedDataset, BitcoinDerivedAligned1sMaterialization
] = {
    'bitcoin_block_fee_totals': BitcoinDerivedAligned1sMaterialization(
        table_name='bitcoin_block_fee_totals',
        datetime_column='datetime',
        rank_id_column='block_height',
        metric_name='block_fee_total_btc',
        metric_unit='BTC',
        metric_value_float_column='fee_total_btc',
        source_id='bitcoin_core',
        dataset_name='bitcoin_block_fee_totals',
    ),
    'bitcoin_block_subsidy_schedule': BitcoinDerivedAligned1sMaterialization(
        table_name='bitcoin_block_subsidy_schedule',
        datetime_column='datetime',
        rank_id_column='block_height',
        metric_name='block_subsidy_btc',
        metric_unit='BTC',
        metric_value_float_column='subsidy_btc',
        source_id='bitcoin_core',
        dataset_name='bitcoin_block_subsidy_schedule',
    ),
    'bitcoin_network_hashrate_estimate': BitcoinDerivedAligned1sMaterialization(
        table_name='bitcoin_network_hashrate_estimate',
        datetime_column='datetime',
        rank_id_column='block_height',
        metric_name='network_hashrate_hs',
        metric_unit='H/s',
        metric_value_float_column='hashrate_hs',
        source_id='bitcoin_core',
        dataset_name='bitcoin_network_hashrate_estimate',
    ),
    'bitcoin_circulating_supply': BitcoinDerivedAligned1sMaterialization(
        table_name='bitcoin_circulating_supply',
        datetime_column='datetime',
        rank_id_column='block_height',
        metric_name='btc_circulating_supply_btc',
        metric_unit='BTC',
        metric_value_float_column='circulating_supply_btc',
        source_id='bitcoin_core',
        dataset_name='bitcoin_circulating_supply',
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


def _build_where_clause(*, datetime_column: str, window: QueryWindow) -> str:
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
    materialization: BitcoinDerivedAligned1sMaterialization,
    where_clause: str,
) -> str:
    datetime_expr = f"toDateTime64({materialization.datetime_column}, 3, 'UTC')"
    observation_rank = f'tuple({datetime_expr}, {materialization.rank_id_column})'
    provenance_json = (
        '{"source":"'
        + materialization.source_id
        + '","dataset":"'
        + materialization.dataset_name
        + '"}'
    )
    return (
        'SELECT '
        f"toUnixTimestamp64Milli(toStartOfSecond({datetime_expr})) AS {_ALIGNED_MS_ALIAS}, "
        f"'{materialization.source_id}' AS source_id, "
        f"'{materialization.metric_name}' AS metric_name, "
        f"'{materialization.metric_unit}' AS metric_unit, "
        'CAST(NULL AS Nullable(String)) AS metric_value_string, '
        'CAST(NULL AS Nullable(Int64)) AS metric_value_int, '
        f'argMax({materialization.metric_value_float_column}, {observation_rank}) AS metric_value_float, '
        'CAST(NULL AS Nullable(UInt8)) AS metric_value_bool, '
        "'{}' AS dimensions_json, "
        f"'{provenance_json}' AS provenance_json, "
        f'max({datetime_expr}) AS latest_ingested_at_utc, '
        'count() AS records_in_bucket '
        f'FROM {database}.{materialization.table_name} '
        f'{where_clause} '
        f'GROUP BY {_ALIGNED_MS_ALIAS}, source_id, metric_name'
    )


def build_bitcoin_derived_aligned_1s_sql(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')

    materialization = _BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS[dataset]
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
            f'ORDER BY {_ALIGNED_MS_ALIAS} DESC, source_id ASC, metric_name ASC LIMIT {window.rows}'
            f') ORDER BY {_ALIGNED_MS_ALIAS} ASC, source_id ASC, metric_name ASC'
        )

    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT * FROM ('
            f'{grouped_query}'
            f') ORDER BY sipHash64(tuple({_ALIGNED_MS_ALIAS}, source_id, metric_name)) '
            f'LIMIT {window.rows}'
        )

    return (
        'SELECT * FROM ('
        f'{grouped_query}'
        f') ORDER BY {_ALIGNED_MS_ALIAS} ASC, source_id ASC, metric_name ASC'
    )


def _shape_aligned_frame(*, frame: pl.DataFrame, datetime_iso_output: bool) -> pl.DataFrame:
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
    shaped = shaped.sort(['aligned_at_utc', 'source_id', 'metric_name'])

    if datetime_iso_output:
        shaped = shaped.with_columns(
            pl.col('aligned_at_utc')
            .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
            .alias('aligned_at_utc')
        )
        if 'latest_ingested_at_utc' in shaped.columns:
            shaped = shaped.with_columns(
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc')
            )

    return shaped


def _query_bitcoin_derived_aligned_prior_state(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    start_iso: str,
    auth_token: str | None,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    materialization = _BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS[dataset]
    start_ch = _to_clickhouse_datetime64_literal(start_iso)
    datetime_expr = f"toDateTime64({materialization.datetime_column}, 3, 'UTC')"
    observation_rank = f'tuple({datetime_expr}, {materialization.rank_id_column})'
    provenance_json = (
        '{"source":"'
        + materialization.source_id
        + '","dataset":"'
        + materialization.dataset_name
        + '"}'
    )

    sql = (
        'SELECT '
        f'max(toUnixTimestamp64Milli(toStartOfSecond({datetime_expr}))) AS {_ALIGNED_MS_ALIAS}, '
        f"'{materialization.source_id}' AS source_id, "
        f"'{materialization.metric_name}' AS metric_name, "
        f"'{materialization.metric_unit}' AS metric_unit, "
        'CAST(NULL AS Nullable(String)) AS metric_value_string, '
        'CAST(NULL AS Nullable(Int64)) AS metric_value_int, '
        f'argMax({materialization.metric_value_float_column}, {observation_rank}) AS metric_value_float, '
        'CAST(NULL AS Nullable(UInt8)) AS metric_value_bool, '
        "'{}' AS dimensions_json, "
        f"'{provenance_json}' AS provenance_json, "
        f'max({datetime_expr}) AS latest_ingested_at_utc, '
        'count() AS records_in_bucket '
        f'FROM {settings.database}.{materialization.table_name} '
        f"WHERE {materialization.datetime_column} < toDateTime64('{start_ch}', 3, 'UTC') "
        'GROUP BY source_id, metric_name '
        f'ORDER BY source_id ASC, metric_name ASC'
    )

    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
    try:
        arrow_table = client.query_arrow(sql)
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    raw_frame = pl.DataFrame(arrow_table)
    if isinstance(raw_frame, pl.Series):
        raise RuntimeError(
            'Expected DataFrame from Bitcoin aligned prior-state query, got Series'
        )
    return _shape_aligned_frame(frame=raw_frame, datetime_iso_output=False)


def _build_bitcoin_derived_forward_fill_intervals(
    *,
    observations: pl.DataFrame,
    window_start: datetime,
    window_end: datetime,
    datetime_iso_output: bool,
) -> pl.DataFrame:
    if window_start >= window_end:
        raise ValueError(
            'forward-fill window must satisfy start < end '
            f'(start={window_start.isoformat()}, end={window_end.isoformat()})'
        )

    if observations.height == 0:
        return pl.DataFrame(
            schema={
                'source_id': pl.Utf8,
                'metric_name': pl.Utf8,
                'metric_unit': pl.Utf8,
                'metric_value_string': pl.Utf8,
                'metric_value_int': pl.Int64,
                'metric_value_float': pl.Float64,
                'metric_value_bool': pl.UInt8,
                'dimensions_json': pl.Utf8,
                'provenance_json': pl.Utf8,
                'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'records_in_bucket': pl.UInt64,
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'valid_from_utc': pl.Datetime('ms', time_zone='UTC'),
                'valid_to_utc_exclusive': pl.Datetime('ms', time_zone='UTC'),
            }
        )

    for column in _BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS:
        if column not in observations.columns:
            raise RuntimeError(
                f'forward-fill observations are missing required column: {column}'
            )

    normalized = (
        observations.select(_BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS)
        .sort(['source_id', 'metric_name', 'aligned_at_utc', 'latest_ingested_at_utc'])
        .unique(
            subset=['source_id', 'metric_name', 'aligned_at_utc'],
            keep='last',
            maintain_order=True,
        )
    )

    intervals = normalized.with_columns(
        pl.col('aligned_at_utc')
        .shift(-1)
        .over(['source_id', 'metric_name'])
        .alias('_next_aligned_at_utc')
    ).with_columns(
        [
            pl.when(pl.col('aligned_at_utc') < pl.lit(window_start))
            .then(pl.lit(window_start))
            .otherwise(pl.col('aligned_at_utc'))
            .alias('valid_from_utc'),
            pl.when(
                pl.col('_next_aligned_at_utc').is_null()
                | (pl.col('_next_aligned_at_utc') > pl.lit(window_end))
            )
            .then(pl.lit(window_end))
            .otherwise(pl.col('_next_aligned_at_utc'))
            .alias('valid_to_utc_exclusive'),
        ]
    ).drop('_next_aligned_at_utc')

    intervals = intervals.filter(
        (pl.col('valid_to_utc_exclusive') > pl.col('valid_from_utc'))
        & (pl.col('valid_to_utc_exclusive') > pl.lit(window_start))
        & (pl.col('valid_from_utc') < pl.lit(window_end))
    ).sort(['source_id', 'metric_name', 'valid_from_utc'])

    if datetime_iso_output:
        intervals = intervals.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
                pl.col('valid_from_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('valid_from_utc'),
                pl.col('valid_to_utc_exclusive')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('valid_to_utc_exclusive'),
            ]
        )

    return intervals


def query_bitcoin_derived_aligned_1s_data(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_bitcoin_derived_aligned_1s_sql(
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
        raise RuntimeError(
            'Expected DataFrame from Bitcoin derived aligned query, got Series'
        )

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


def query_bitcoin_derived_forward_fill_intervals(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: TimeRangeWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    window_start = _parse_iso_datetime(window.start_iso)
    window_end = _parse_iso_datetime(window.end_iso)

    in_window = query_bitcoin_derived_aligned_1s_data(
        dataset=dataset,
        window=window,
        auth_token=auth_token,
        show_summary=False,
        datetime_iso_output=False,
    )
    prior_state = _query_bitcoin_derived_aligned_prior_state(
        dataset=dataset,
        start_iso=window.start_iso,
        auth_token=auth_token,
    )

    combined = pl.concat([prior_state, in_window], how='vertical_relaxed')
    started_at = time.time()
    intervals = _build_bitcoin_derived_forward_fill_intervals(
        observations=combined,
        window_start=window_start,
        window_end=window_end,
        datetime_iso_output=datetime_iso_output,
    )
    if show_summary:
        elapsed = time.time() - started_at
        logger.info(
            '%s | dataset=%s | intervals=%d | cols=%d | %.2f GB RAM',
            f'{elapsed:.2f}s',
            dataset,
            intervals.shape[0],
            intervals.shape[1],
            intervals.estimated_size() / (1024**3),
        )
    return intervals
