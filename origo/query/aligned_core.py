from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

import polars as pl

from .binance_aligned_1s import BinanceAlignedDataset, query_binance_aligned_1s_data
from .etf_aligned_1s import (
    ETFAlignedDataset,
    query_etf_aligned_1s_data,
    query_etf_forward_fill_intervals,
)
from .fred_aligned_1s import (
    FREDAlignedDataset,
    query_fred_aligned_1s_data,
    query_fred_forward_fill_intervals,
)
from .native_core import (
    MonthWindow,
    QueryWindow,
    TimeRangeWindow,
)
from .okx_aligned_1s import OKXAlignedDataset, query_okx_aligned_1s_data

type AlignedDataset = (
    BinanceAlignedDataset | ETFAlignedDataset | FREDAlignedDataset | OKXAlignedDataset
)
type AlignedExecutionPath = Literal[
    'binance_aligned',
    'okx_aligned',
    'etf_aligned_observation',
    'etf_aligned_forward_fill',
    'fred_aligned_observation',
    'fred_aligned_forward_fill',
]

_BINANCE_DATASETS: frozenset[str] = frozenset(
    {'spot_trades', 'spot_agg_trades', 'futures_trades'}
)
_OKX_DATASETS: frozenset[str] = frozenset({'okx_spot_trades'})
_BINANCE_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
        'aligned_at_utc',
        'open_price',
        'high_price',
        'low_price',
        'close_price',
        'quantity_sum',
        'quote_volume_sum',
        'trade_count',
    }
)
_OKX_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
        'aligned_at_utc',
        'open_price',
        'high_price',
        'low_price',
        'close_price',
        'quantity_sum',
        'quote_volume_sum',
        'trade_count',
    }
)
_ETF_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
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
        'valid_from_utc',
        'valid_to_utc_exclusive',
    }
)
_FRED_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
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
        'valid_from_utc',
        'valid_to_utc_exclusive',
    }
)


@dataclass(frozen=True)
class AlignedQueryPlan:
    dataset: AlignedDataset
    execution_path: AlignedExecutionPath
    window: QueryWindow


def _dedupe_preserve_order(values: list[str] | tuple[str, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _month_window_to_time_range(window: MonthWindow) -> TimeRangeWindow:
    start = datetime(window.year, window.month, 1, tzinfo=UTC)
    if window.month == 12:
        end = datetime(window.year + 1, 1, 1, tzinfo=UTC)
    else:
        end = datetime(window.year, window.month + 1, 1, tzinfo=UTC)
    return TimeRangeWindow(
        start_iso=start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        end_iso=end.strftime('%Y-%m-%dT%H:%M:%SZ'),
    )


def build_aligned_query_plan(*, dataset: AlignedDataset, window: QueryWindow) -> AlignedQueryPlan:
    if dataset in _BINANCE_DATASETS:
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='binance_aligned',
            window=window,
        )

    if dataset in _OKX_DATASETS:
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='okx_aligned',
            window=window,
        )

    if dataset == 'etf_daily_metrics':
        if isinstance(window, MonthWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='etf_aligned_forward_fill',
                window=_month_window_to_time_range(window),
            )
        if isinstance(window, TimeRangeWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='etf_aligned_forward_fill',
                window=window,
            )
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='etf_aligned_observation',
            window=window,
        )

    if dataset == 'fred_series_metrics':
        if isinstance(window, MonthWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='fred_aligned_forward_fill',
                window=_month_window_to_time_range(window),
            )
        if isinstance(window, TimeRangeWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='fred_aligned_forward_fill',
                window=window,
            )
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='fred_aligned_observation',
            window=window,
        )

    raise ValueError(f'Unsupported aligned dataset: {dataset}')


def _validate_selected_columns(
    *, dataset: AlignedDataset, selected_columns: list[str] | tuple[str, ...]
) -> list[str]:
    columns = _dedupe_preserve_order(selected_columns)
    if len(columns) == 0:
        raise ValueError('selected_columns cannot be empty when provided')

    if dataset in _BINANCE_DATASETS:
        allowed_columns = _BINANCE_ALIGNED_COLUMNS
    elif dataset in _OKX_DATASETS:
        allowed_columns = _OKX_ALIGNED_COLUMNS
    elif dataset == 'etf_daily_metrics':
        allowed_columns = _ETF_ALIGNED_COLUMNS
    elif dataset == 'fred_series_metrics':
        allowed_columns = _FRED_ALIGNED_COLUMNS
    else:
        raise ValueError(f'Unsupported aligned dataset: {dataset}')
    invalid_columns = sorted(set(columns).difference(allowed_columns))
    if len(invalid_columns) > 0:
        raise ValueError(
            f'Unsupported aligned projection fields for dataset={dataset}: {invalid_columns}'
        )
    return columns


def _apply_projection(
    *,
    frame: pl.DataFrame,
    dataset: AlignedDataset,
    selected_columns: list[str] | tuple[str, ...] | None,
) -> pl.DataFrame:
    if selected_columns is None:
        return frame

    projection = _validate_selected_columns(
        dataset=dataset,
        selected_columns=selected_columns,
    )
    missing_columns = sorted(set(projection).difference(frame.columns))
    if len(missing_columns) > 0:
        raise ValueError(
            'Requested aligned projection fields are not available for '
            f'dataset={dataset} in this planner path: {missing_columns}'
        )
    return frame.select(projection)


def query_aligned_data(
    *,
    dataset: AlignedDataset,
    window: QueryWindow,
    selected_columns: list[str] | tuple[str, ...] | None = None,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    plan = build_aligned_query_plan(dataset=dataset, window=window)

    if plan.execution_path == 'binance_aligned':
        frame = query_binance_aligned_1s_data(
            dataset=cast(BinanceAlignedDataset, plan.dataset),
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'okx_aligned':
        frame = query_okx_aligned_1s_data(
            dataset=cast(OKXAlignedDataset, plan.dataset),
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'etf_aligned_observation':
        frame = query_etf_aligned_1s_data(
            dataset='etf_daily_metrics',
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'etf_aligned_forward_fill':
        if not isinstance(plan.window, TimeRangeWindow):
            raise RuntimeError(
                'Internal aligned planner error: forward-fill path requires TimeRangeWindow'
            )
        frame = query_etf_forward_fill_intervals(
            dataset='etf_daily_metrics',
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'fred_aligned_observation':
        frame = query_fred_aligned_1s_data(
            dataset='fred_series_metrics',
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'fred_aligned_forward_fill':
        if not isinstance(plan.window, TimeRangeWindow):
            raise RuntimeError(
                'Internal aligned planner error: forward-fill path requires TimeRangeWindow'
            )
        frame = query_fred_forward_fill_intervals(
            dataset='fred_series_metrics',
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    else:
        raise RuntimeError(
            f'Internal aligned planner error: unsupported execution path {plan.execution_path}'
        )

    return _apply_projection(
        frame=frame,
        dataset=dataset,
        selected_columns=selected_columns,
    )
