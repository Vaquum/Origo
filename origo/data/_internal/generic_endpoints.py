import logging
import time
from typing import Any, cast

import polars as pl
from clickhouse_connect import get_client as _raw_get_client

from origo.query.aligned_core import AlignedDataset, query_aligned_data
from origo.query.binance_native import BinanceDataset, query_binance_native_data
from origo.query.bitcoin_native import BitcoinDataset, query_bitcoin_native_data
from origo.query.bybit_native import BybitDataset, query_bybit_native_data
from origo.query.etf_native import ETFDataset, query_etf_native_data
from origo.query.fred_native import FREDDataset, query_fred_native_data
from origo.query.native_core import (
    LatestRowsWindow,
    MonthWindow,
    NativeQuerySpec,
    QueryWindow,
    RandomRowsWindow,
    TimeRangeWindow,
    execute_native_query,
    resolve_clickhouse_http_settings,
)
from origo.query.okx_native import OKXDataset, query_okx_native_data
from origo.query.response import build_wide_rows_envelope

logger = logging.getLogger(__name__)
get_client = cast(Any, _raw_get_client)
_ALLOWED_FILTER_OPS: frozenset[str] = frozenset(
    {'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in'}
)

type NativeQueryDataset = (
    BinanceDataset
    | BitcoinDataset
    | BybitDataset
    | ETFDataset
    | FREDDataset
    | OKXDataset
)


def _resolve_window(
    month_year: tuple[int, int] | None,
    n_rows: int | None,
    n_random: int | None,
    time_range: tuple[str, str] | None = None,
) -> QueryWindow:
    param_count = sum(
        [
            month_year is not None,
            n_rows is not None,
            n_random is not None,
            time_range is not None,
        ]
    )
    if param_count != 1:
        raise ValueError(
            'Exactly one of month_year, n_rows, n_random, or time_range must be provided. '
            f'Got: month_year={month_year}, n_rows={n_rows}, n_random={n_random}, time_range={time_range}'
        )

    if month_year is not None:
        month, year = month_year
        return MonthWindow(month=month, year=year)
    if time_range is not None:
        return TimeRangeWindow(start_iso=time_range[0], end_iso=time_range[1])
    if n_rows is not None:
        return LatestRowsWindow(rows=n_rows)
    if n_random is None:
        raise ValueError('n_random must be set when selecting random window mode')
    return RandomRowsWindow(rows=n_random)  # n_random is guaranteed non-None here


def _normalize_filters(filters: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    normalized_filters: list[dict[str, Any]] = []
    for idx, raw_filter in enumerate(filters):
        field_obj = raw_filter.get('field')
        if not isinstance(field_obj, str) or field_obj.strip() == '':
            raise ValueError(f'filters[{idx}].field must be a non-empty string')
        field = field_obj.strip()

        op_obj = raw_filter.get('op')
        if not isinstance(op_obj, str) or op_obj not in _ALLOWED_FILTER_OPS:
            raise ValueError(
                f'filters[{idx}].op must be one of {sorted(_ALLOWED_FILTER_OPS)}'
            )
        op = op_obj

        if 'value' not in raw_filter:
            raise ValueError(f'filters[{idx}].value must be provided')
        value = raw_filter['value']

        if op in {'in', 'not_in'}:
            if not isinstance(value, list):
                raise ValueError(
                    f'filters[{idx}].value must be a non-empty list when op={op}'
                )
            values = cast(list[Any], value)
            if len(values) == 0:
                raise ValueError(
                    f'filters[{idx}].value must be a non-empty list when op={op}'
                )

        normalized_filters.append({'field': field, 'op': op, 'value': value})
    return normalized_filters


def _filter_expression(*, field: str, op: str, value: Any) -> pl.Expr:
    if op == 'eq':
        return pl.col(field) == value
    if op == 'ne':
        return pl.col(field) != value
    if op == 'gt':
        return pl.col(field) > value
    if op == 'gte':
        return pl.col(field) >= value
    if op == 'lt':
        return pl.col(field) < value
    if op == 'lte':
        return pl.col(field) <= value
    if op == 'in':
        return pl.col(field).is_in(cast(list[Any], value))
    if op == 'not_in':
        return ~pl.col(field).is_in(cast(list[Any], value))
    raise ValueError(f'Unsupported filter op: {op}')


def _apply_filters(
    *,
    frame: pl.DataFrame,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> pl.DataFrame:
    if filters is None or len(filters) == 0:
        return frame

    normalized_filters = _normalize_filters(filters)

    for idx, clause in enumerate(normalized_filters):
        field = cast(str, clause['field'])
        if field not in frame.columns:
            raise ValueError(
                f'filters[{idx}].field is not available in result columns: {field}'
            )
        op = cast(str, clause['op'])
        value = clause['value']
        try:
            frame = frame.filter(_filter_expression(field=field, op=op, value=value))
        except Exception as exc:
            raise ValueError(
                f'Invalid filter at filters[{idx}] for field={field}, op={op}: {exc}'
            ) from exc

    return frame


def query_binance_native(
    dataset: BinanceDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    include_datetime_col: bool = True,
    datetime_iso_output: bool = False,
    show_summary: bool = False,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> pl.DataFrame:
    window = _resolve_window(
        month_year=month_year, n_rows=n_rows, n_random=n_random, time_range=time_range
    )
    frame = query_binance_native_data(
        dataset=dataset,
        select_columns=select_cols,
        window=window,
        include_datetime=include_datetime_col,
        datetime_iso_output=datetime_iso_output,
        auth_token=auth_token,
        show_summary=show_summary,
    )
    return _apply_filters(frame=frame, filters=filters)


def query_native(
    dataset: NativeQueryDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    include_datetime_col: bool = True,
    datetime_iso_output: bool = False,
    show_summary: bool = False,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> pl.DataFrame:
    window = _resolve_window(
        month_year=month_year, n_rows=n_rows, n_random=n_random, time_range=time_range
    )

    if dataset in {'spot_trades', 'spot_agg_trades', 'futures_trades'}:
        frame = query_binance_native_data(
            dataset=cast(BinanceDataset, dataset),
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    if dataset == 'okx_spot_trades':
        frame = query_okx_native_data(
            dataset=dataset,
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    if dataset == 'bybit_spot_trades':
        frame = query_bybit_native_data(
            dataset=dataset,
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    if dataset == 'etf_daily_metrics':
        frame = query_etf_native_data(
            dataset=dataset,
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    if dataset == 'fred_series_metrics':
        frame = query_fred_native_data(
            dataset=dataset,
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    if dataset in {
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }:
        frame = query_bitcoin_native_data(
            dataset=cast(BitcoinDataset, dataset),
            select_columns=select_cols,
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=datetime_iso_output,
            auth_token=auth_token,
            show_summary=show_summary,
        )
        return _apply_filters(frame=frame, filters=filters)

    raise ValueError(f'Unsupported dataset: {dataset}')


def query_aligned(
    dataset: AlignedDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    datetime_iso_output: bool = False,
    show_summary: bool = False,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> pl.DataFrame:
    window = _resolve_window(
        month_year=month_year,
        n_rows=n_rows,
        n_random=n_random,
        time_range=time_range,
    )
    frame = query_aligned_data(
        dataset=dataset,
        window=window,
        selected_columns=select_cols,
        datetime_iso_output=datetime_iso_output,
        show_summary=show_summary,
        auth_token=auth_token,
    )
    return _apply_filters(frame=frame, filters=filters)


def query_raw_data(
    table_name: str,
    id_col: str,
    select_cols: list[str],
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    include_datetime_col: bool = True,
    datetime_iso_output: bool = False,
    show_summary: bool = False,
    auth_token: str | None = None,
) -> pl.DataFrame:
    window = _resolve_window(
        month_year=month_year, n_rows=n_rows, n_random=n_random, time_range=time_range
    )
    spec = NativeQuerySpec(
        table_name=table_name,
        id_column=id_col,
        select_columns=tuple(select_cols),
        window=window,
        include_datetime=include_datetime_col,
    )
    return execute_native_query(
        spec=spec,
        auth_token=auth_token,
        show_summary=show_summary,
        datetime_iso_output=datetime_iso_output,
    )


def query_binance_native_wide_rows_envelope(
    dataset: BinanceDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    include_datetime_col: bool = True,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> dict[str, Any]:
    frame = query_binance_native(
        dataset=dataset,
        select_cols=select_cols,
        month_year=month_year,
        n_rows=n_rows,
        n_random=n_random,
        time_range=time_range,
        include_datetime_col=include_datetime_col,
        datetime_iso_output=True,
        auth_token=auth_token,
        filters=filters,
        show_summary=False,
    )
    return build_wide_rows_envelope(frame=frame, mode='native', source=dataset)


def query_native_wide_rows_envelope(
    dataset: NativeQueryDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    include_datetime_col: bool = True,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> dict[str, Any]:
    frame = query_native(
        dataset=dataset,
        select_cols=select_cols,
        month_year=month_year,
        n_rows=n_rows,
        n_random=n_random,
        time_range=time_range,
        include_datetime_col=include_datetime_col,
        datetime_iso_output=True,
        auth_token=auth_token,
        filters=filters,
        show_summary=False,
    )
    return build_wide_rows_envelope(frame=frame, mode='native', source=dataset)


def query_aligned_wide_rows_envelope(
    dataset: AlignedDataset,
    select_cols: list[str] | tuple[str, ...] | None,
    month_year: tuple[int, int] | None = None,
    n_rows: int | None = None,
    n_random: int | None = None,
    time_range: tuple[str, str] | None = None,
    auth_token: str | None = None,
    filters: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> dict[str, Any]:
    frame = query_aligned(
        dataset=dataset,
        select_cols=select_cols,
        month_year=month_year,
        n_rows=n_rows,
        n_random=n_random,
        time_range=time_range,
        datetime_iso_output=True,
        auth_token=auth_token,
        filters=filters,
        show_summary=False,
    )
    return build_wide_rows_envelope(frame=frame, mode='aligned_1s', source=dataset)


def query_klines_data(
    n_rows: int | None = None,
    kline_size: int = 1,
    start_date_limit: str | None = None,
    futures: bool = False,
    show_summary: bool = False,
    auth_token: str | None = None,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )

    limit = f'LIMIT {n_rows}' if n_rows is not None else ''
    date_filter = (
        f"WHERE datetime >= toDateTime('{start_date_limit}') "
        if start_date_limit is not None
        else ''
    )

    if futures:
        db_table = f'FROM {settings.database}.binance_futures_trades '
        id_col = 'futures_trade_id'
    else:
        db_table = f'FROM {settings.database}.binance_trades '
        id_col = 'trade_id'

    query = (
        f'SELECT '
        f'    toDateTime({kline_size} * intDiv(toUnixTimestamp(datetime), {kline_size})) AS datetime, '
        f'    argMin(price, {id_col})       AS open, '
        f'    max(price)                    AS high, '
        f'    min(price)                    AS low, '
        f'    argMax(price, {id_col})       AS close, '
        f'    avg(price)                    AS mean, '
        f'    stddevPopStable(price)        AS std, '
        f'    quantileExact(0.5)(price)     AS median, '
        f'    quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr, '
        f'    sumKahan(quantity)            AS volume, '
        f'    avg(is_buyer_maker)           AS maker_ratio, '
        f'    count()                       AS no_of_trades, '
        f'    argMin(price * quantity, {id_col})    AS open_liquidity, '
        f'    max(price * quantity)         AS high_liquidity, '
        f'    min(price * quantity)         AS low_liquidity, '
        f'    argMax(price * quantity, {id_col})    AS close_liquidity, '
        f'    sum(price * quantity)         AS liquidity_sum, '
        f'    sumKahan(is_buyer_maker * quantity)   AS maker_volume, '
        f'    sum(is_buyer_maker * price * quantity) AS maker_liquidity '
        f'{db_table}'
        f'{date_filter}'
        f'GROUP BY datetime '
        f'ORDER BY datetime ASC '
        f'{limit}'
    )

    start = time.time()
    try:
        arrow_table = client.query_arrow(query)
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    polars_obj = pl.DataFrame(arrow_table)
    if isinstance(polars_obj, pl.Series):
        raise RuntimeError('Expected DataFrame from ClickHouse kline query, got Series')
    polars_df = polars_obj
    polars_df = polars_df.with_columns(
        [
            (pl.col('datetime').cast(pl.Int64) * 1000)
            .cast(pl.Datetime('ms', time_zone='UTC'))
            .alias('datetime')
        ]
    )
    polars_df = polars_df.with_columns(
        [
            pl.col('mean').round(5),
            pl.col('std').round(6),
            pl.col('volume').round(9),
            pl.col('liquidity_sum').round(1),
            pl.col('maker_liquidity').round(1),
        ]
    )
    polars_df = polars_df.sort('datetime')

    if show_summary:
        elapsed = time.time() - start
        logger.info(
            '%s s | %d rows | %d cols | %.2f GB RAM',
            f'{elapsed:.2f}',
            polars_df.shape[0],
            polars_df.shape[1],
            polars_df.estimated_size() / (1024**3),
        )

    return polars_df
