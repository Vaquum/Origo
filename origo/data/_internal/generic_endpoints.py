import logging
import re
import time
from datetime import UTC, date, datetime, timedelta
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
_STRICT_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')

type NativeQueryDataset = (
    BinanceDataset
    | BitcoinDataset
    | BybitDataset
    | ETFDataset
    | FREDDataset
    | OKXDataset
)
type HistoricalSpotSource = str

_HISTORICAL_SOURCE_TO_DATASET: dict[str, NativeQueryDataset] = {
    'binance': 'spot_trades',
    'okx': 'okx_spot_trades',
    'bybit': 'bybit_spot_trades',
}
_HISTORICAL_SOURCE_TO_TABLE: dict[str, str] = {
    'binance': 'canonical_binance_spot_trades_native_v1',
    'okx': 'canonical_okx_spot_trades_native_v1',
    'bybit': 'canonical_bybit_spot_trades_native_v1',
}
_HISTORICAL_SOURCE_TO_ID_COLUMN: dict[str, str] = {
    'binance': 'trade_id',
    'okx': 'trade_id',
    'bybit': 'trade_id',
}


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


def _require_historical_source(source: str) -> None:
    if source not in _HISTORICAL_SOURCE_TO_DATASET:
        raise ValueError(
            'source must be one of '
            f'{sorted(_HISTORICAL_SOURCE_TO_DATASET)}; got source={source!r}'
        )


def _parse_strict_date(*, label: str, value: str) -> date:
    if not _STRICT_DATE_PATTERN.fullmatch(value):
        raise ValueError(
            f'{label} must be strict YYYY-MM-DD (UTC day); got {value!r}'
        )
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as exc:
        raise ValueError(
            f'{label} must be a valid YYYY-MM-DD date (UTC day); got {value!r}'
        ) from exc


def _to_clickhouse_datetime64_literal(value: str) -> str:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _read_table_day_bounds(*, table_name: str, auth_token: str | None) -> tuple[date | None, date | None]:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
    query = (
        f'SELECT toDate(min(datetime)) AS min_day, toDate(max(datetime)) AS max_day '
        f'FROM {settings.database}.{table_name}'
    )
    try:
        rows = client.query(query).result_rows
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    if len(rows) == 0:
        return None, None
    first_row = rows[0]
    if len(first_row) != 2:
        raise RuntimeError(
            f'Expected min/max day row with two values for table={table_name}'
        )
    min_raw = first_row[0]
    max_raw = first_row[1]
    if min_raw is None or max_raw is None:
        return None, None

    if isinstance(min_raw, date):
        min_day = min_raw
    else:
        min_day = _parse_strict_date(label='resolved min_day', value=str(min_raw))

    if isinstance(max_raw, date):
        max_day = max_raw
    else:
        max_day = _parse_strict_date(label='resolved max_day', value=str(max_raw))

    return min_day, max_day


def _resolve_historical_window(
    *,
    table_name: str,
    start_date: str | None,
    end_date: str | None,
    n_latest_rows: int | None,
    n_random_rows: int | None,
    auth_token: str | None,
) -> QueryWindow:
    date_mode_selected = start_date is not None or end_date is not None
    selected_modes = sum(
        [
            date_mode_selected,
            n_latest_rows is not None,
            n_random_rows is not None,
        ]
    )
    if selected_modes != 1:
        raise ValueError(
            'Exactly one window mode must be provided: '
            'date-window(start_date/end_date) | n_latest_rows | n_random_rows'
        )

    if n_latest_rows is not None:
        return LatestRowsWindow(rows=n_latest_rows)
    if n_random_rows is not None:
        return RandomRowsWindow(rows=n_random_rows)

    start_day = (
        _parse_strict_date(label='start_date', value=start_date)
        if start_date is not None
        else None
    )
    end_day = (
        _parse_strict_date(label='end_date', value=end_date)
        if end_date is not None
        else None
    )

    if start_day is None or end_day is None:
        min_day, max_day = _read_table_day_bounds(
            table_name=table_name,
            auth_token=auth_token,
        )
        if min_day is None or max_day is None:
            raise ValueError(
                'date-window mode cannot resolve open bounds because source table has no rows'
            )
        if start_day is None:
            start_day = min_day
        if end_day is None:
            end_day = max_day

    if start_day > end_day:
        raise ValueError(
            'date-window must satisfy start_date <= end_date; '
            f'got start_date={start_day.isoformat()} end_date={end_day.isoformat()}'
        )

    start_iso = f'{start_day.isoformat()}T00:00:00Z'
    end_exclusive_iso = f'{(end_day + timedelta(days=1)).isoformat()}T00:00:00Z'
    return TimeRangeWindow(start_iso=start_iso, end_iso=end_exclusive_iso)


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


def _normalize_spot_trades_frame(
    *,
    source: str,
    frame: pl.DataFrame,
    include_datetime_col: bool,
) -> pl.DataFrame:
    _require_historical_source(source)

    if source == 'binance':
        selected_columns = [
            'trade_id',
            'timestamp',
            'price',
            'quantity',
            'is_buyer_maker',
        ]
        if include_datetime_col:
            selected_columns.append('datetime')
        return frame.select(selected_columns)

    if 'side' not in frame.columns:
        raise RuntimeError(f'side column missing for source={source} spot trades')
    if 'size' not in frame.columns:
        raise RuntimeError(f'size column missing for source={source} spot trades')

    normalized = frame.with_columns(
        [pl.col('side').cast(pl.Utf8).str.to_lowercase().alias('_side_lower')]
    )
    invalid_side_rows = normalized.filter(
        ~pl.col('_side_lower').is_in(['buy', 'sell'])
    ).height
    if invalid_side_rows > 0:
        raise ValueError(
            f'Unsupported side values in source={source} spot trades: expected buy/sell only'
        )

    normalized = normalized.with_columns(
        [
            pl.col('size').alias('quantity'),
            pl.when(pl.col('_side_lower') == 'buy')
            .then(pl.lit(0))
            .otherwise(pl.lit(1))
            .cast(pl.UInt8)
            .alias('is_buyer_maker'),
        ]
    )

    selected_columns = [
        'trade_id',
        'timestamp',
        'price',
        'quantity',
        'is_buyer_maker',
    ]
    if include_datetime_col:
        selected_columns.append('datetime')
    return normalized.select(selected_columns)


def query_spot_trades_data(
    *,
    source: str,
    start_date: str | None = None,
    end_date: str | None = None,
    n_latest_rows: int | None = None,
    n_random_rows: int | None = None,
    include_datetime_col: bool = True,
    show_summary: bool = False,
    auth_token: str | None = None,
) -> pl.DataFrame:
    _require_historical_source(source)
    table_name = _HISTORICAL_SOURCE_TO_TABLE[source]
    dataset = _HISTORICAL_SOURCE_TO_DATASET[source]
    window = _resolve_historical_window(
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        n_latest_rows=n_latest_rows,
        n_random_rows=n_random_rows,
        auth_token=auth_token,
    )

    if source == 'binance':
        raw_frame = query_binance_native_data(
            dataset='spot_trades',
            select_columns=(
                'trade_id',
                'timestamp',
                'price',
                'quantity',
                'is_buyer_maker',
            ),
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=False,
            auth_token=auth_token,
            show_summary=show_summary,
        )
    elif source == 'okx':
        raw_frame = query_okx_native_data(
            dataset='okx_spot_trades',
            select_columns=('trade_id', 'timestamp', 'price', 'size', 'side'),
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=False,
            auth_token=auth_token,
            show_summary=show_summary,
        )
    else:
        raw_frame = query_bybit_native_data(
            dataset='bybit_spot_trades',
            select_columns=('trade_id', 'timestamp', 'price', 'size', 'side'),
            window=window,
            include_datetime=include_datetime_col,
            datetime_iso_output=False,
            auth_token=auth_token,
            show_summary=show_summary,
        )

    normalized = _normalize_spot_trades_frame(
        source=source,
        frame=raw_frame,
        include_datetime_col=include_datetime_col,
    )
    if show_summary:
        logger.info(
            'historical_spot_trades source=%s dataset=%s rows=%d cols=%d',
            source,
            dataset,
            normalized.shape[0],
            normalized.shape[1],
        )
    return normalized


def _build_kline_where_clause(window: QueryWindow) -> str:
    if not isinstance(window, TimeRangeWindow):
        return ''
    start_ch = _to_clickhouse_datetime64_literal(window.start_iso)
    end_ch = _to_clickhouse_datetime64_literal(window.end_iso)
    return (
        "WHERE datetime >= toDateTime64('"
        + start_ch
        + "', 3, 'UTC') "
        + "AND datetime < toDateTime64('"
        + end_ch
        + "', 3, 'UTC')"
    )


def _build_kline_final_select(window: QueryWindow) -> str:
    if isinstance(window, LatestRowsWindow):
        return (
            'SELECT * FROM ('
            f'SELECT * FROM aggregated ORDER BY datetime DESC LIMIT {window.rows}'
            ') ORDER BY datetime ASC'
        )
    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT * FROM ('
            'SELECT * FROM aggregated '
            f'ORDER BY sipHash64(toUnixTimestamp64Milli(datetime)) LIMIT {window.rows}'
            ') ORDER BY datetime ASC'
        )
    return 'SELECT * FROM aggregated ORDER BY datetime ASC'


def _maker_expression_for_source(source: str) -> str:
    if source == 'binance':
        return 'toFloat64(is_buyer_maker)'
    return (
        "multiIf("
        "lowerUTF8(side) = 'buy', toFloat64(0), "
        "lowerUTF8(side) = 'sell', toFloat64(1), "
        "throwIf(1, 'Unsupported side value in spot trades stream')"
        ')'
    )


def _quantity_expression_for_source(source: str) -> str:
    if source == 'binance':
        return 'quantity'
    return 'size'


def query_spot_klines_data(
    *,
    source: str,
    start_date: str | None = None,
    end_date: str | None = None,
    n_latest_rows: int | None = None,
    n_random_rows: int | None = None,
    kline_size: int = 1,
    show_summary: bool = False,
    auth_token: str | None = None,
) -> pl.DataFrame:
    _require_historical_source(source)
    if kline_size <= 0:
        raise ValueError(f'kline_size must be > 0, got {kline_size}')

    table_name = _HISTORICAL_SOURCE_TO_TABLE[source]
    id_column = _HISTORICAL_SOURCE_TO_ID_COLUMN[source]
    window = _resolve_historical_window(
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
        n_latest_rows=n_latest_rows,
        n_random_rows=n_random_rows,
        auth_token=auth_token,
    )

    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )

    quantity_expression = _quantity_expression_for_source(source)
    maker_expression = _maker_expression_for_source(source)
    where_clause = _build_kline_where_clause(window)
    final_select = _build_kline_final_select(window)

    query = (
        'WITH aggregated AS ('
        'SELECT '
        f"toDateTime64({kline_size} * intDiv(toUnixTimestamp(datetime), {kline_size}), 3, 'UTC') AS datetime, "
        f'argMin(price, {id_column}) AS open, '
        'max(price) AS high, '
        'min(price) AS low, '
        f'argMax(price, {id_column}) AS close, '
        'avg(price) AS mean, '
        'stddevPopStable(price) AS std, '
        'quantileExact(0.5)(price) AS median, '
        'quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr, '
        f'sumKahan({quantity_expression}) AS volume, '
        f'avg({maker_expression}) AS maker_ratio, '
        'count() AS no_of_trades, '
        f'argMin(price * {quantity_expression}, {id_column}) AS open_liquidity, '
        f'max(price * {quantity_expression}) AS high_liquidity, '
        f'min(price * {quantity_expression}) AS low_liquidity, '
        f'argMax(price * {quantity_expression}, {id_column}) AS close_liquidity, '
        f'sum(price * {quantity_expression}) AS liquidity_sum, '
        f'sumKahan({maker_expression} * {quantity_expression}) AS maker_volume, '
        f'sum({maker_expression} * price * {quantity_expression}) AS maker_liquidity '
        f'FROM {settings.database}.{table_name} '
        f'{where_clause} '
        'GROUP BY datetime'
        ') '
        f'{final_select}'
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
    polars_df = polars_obj.with_columns(
        [pl.col('datetime').cast(pl.Datetime('ms', time_zone='UTC')).alias('datetime')]
    )
    polars_df = polars_df.with_columns(
        [
            pl.col('mean').round(5),
            pl.col('std').round(6),
            pl.col('volume').round(9),
            pl.col('liquidity_sum').round(1),
            pl.col('maker_liquidity').round(1),
        ]
    ).sort('datetime')

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
