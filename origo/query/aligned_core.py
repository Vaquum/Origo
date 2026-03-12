from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

import polars as pl

from .binance_aligned_1s import BinanceAlignedDataset, query_binance_aligned_1s_data
from .bitcoin_derived_aligned_1s import (
    BitcoinDerivedAlignedDataset,
    query_bitcoin_derived_aligned_1s_data,
    query_bitcoin_derived_forward_fill_intervals,
)
from .bitcoin_stream_aligned_1s import (
    BitcoinStreamAlignedDataset,
    query_bitcoin_stream_aligned_1s_data,
)
from .bybit_aligned_1s import BybitAlignedDataset, query_bybit_aligned_1s_data
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
    BinanceAlignedDataset
    | BitcoinStreamAlignedDataset
    | BitcoinDerivedAlignedDataset
    | BybitAlignedDataset
    | ETFAlignedDataset
    | FREDAlignedDataset
    | OKXAlignedDataset
)
type AlignedExecutionPath = Literal[
    'binance_aligned',
    'bybit_aligned',
    'okx_aligned',
    'bitcoin_stream_aligned_observation',
    'bitcoin_derived_aligned_observation',
    'bitcoin_derived_aligned_forward_fill',
    'etf_aligned_observation',
    'etf_aligned_forward_fill',
    'fred_aligned_observation',
    'fred_aligned_forward_fill',
]

_BINANCE_DATASETS: frozenset[str] = frozenset(
    {'spot_trades', 'spot_agg_trades', 'futures_trades'}
)
_OKX_DATASETS: frozenset[str] = frozenset({'okx_spot_trades'})
_BYBIT_DATASETS: frozenset[str] = frozenset({'bybit_spot_trades'})
_BITCOIN_STREAM_DATASETS: frozenset[str] = frozenset(
    {
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
    }
)
_BITCOIN_DERIVED_DATASETS: frozenset[str] = frozenset(
    {
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)
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
_BYBIT_ALIGNED_COLUMNS: frozenset[str] = frozenset(
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
_BITCOIN_STREAM_HEADER_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
        'aligned_at_utc',
        'records_in_bucket',
        'latest_height',
        'latest_block_hash',
        'latest_prev_hash',
        'latest_merkle_root',
        'latest_version',
        'latest_nonce',
        'latest_difficulty',
        'latest_timestamp_ms',
        'latest_source_chain',
        'latest_ingested_at_utc',
        'first_source_offset_or_equivalent',
        'last_source_offset_or_equivalent',
        'bucket_sha256',
    }
)
_BITCOIN_STREAM_TRANSACTION_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
        'aligned_at_utc',
        'records_in_bucket',
        'tx_count',
        'coinbase_tx_count',
        'total_input_value_btc',
        'total_output_value_btc',
        'first_block_height',
        'last_block_height',
        'latest_block_timestamp_ms',
        'latest_txid',
        'latest_source_chain',
        'latest_ingested_at_utc',
        'first_source_offset_or_equivalent',
        'last_source_offset_or_equivalent',
        'bucket_sha256',
    }
)
_BITCOIN_STREAM_MEMPOOL_ALIGNED_COLUMNS: frozenset[str] = frozenset(
    {
        'aligned_at_utc',
        'records_in_bucket',
        'tx_count',
        'fee_rate_sat_vb_min',
        'fee_rate_sat_vb_max',
        'fee_rate_sat_vb_avg',
        'total_vsize',
        'first_seen_timestamp_min',
        'first_seen_timestamp_max',
        'rbf_true_count',
        'latest_snapshot_at_unix_ms',
        'latest_txid',
        'latest_source_chain',
        'latest_ingested_at_utc',
        'first_source_offset_or_equivalent',
        'last_source_offset_or_equivalent',
        'bucket_sha256',
    }
)
_BITCOIN_DERIVED_ALIGNED_COLUMNS: frozenset[str] = frozenset(
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
    if dataset in _BYBIT_DATASETS:
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='bybit_aligned',
            window=window,
        )
    if dataset in _BITCOIN_STREAM_DATASETS:
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='bitcoin_stream_aligned_observation',
            window=window,
        )

    if dataset in _BITCOIN_DERIVED_DATASETS:
        if isinstance(window, MonthWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='bitcoin_derived_aligned_forward_fill',
                window=_month_window_to_time_range(window),
            )
        if isinstance(window, TimeRangeWindow):
            return AlignedQueryPlan(
                dataset=dataset,
                execution_path='bitcoin_derived_aligned_forward_fill',
                window=window,
            )
        return AlignedQueryPlan(
            dataset=dataset,
            execution_path='bitcoin_derived_aligned_observation',
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
    elif dataset in _BYBIT_DATASETS:
        allowed_columns = _BYBIT_ALIGNED_COLUMNS
    elif dataset == 'bitcoin_block_headers':
        allowed_columns = _BITCOIN_STREAM_HEADER_ALIGNED_COLUMNS
    elif dataset == 'bitcoin_block_transactions':
        allowed_columns = _BITCOIN_STREAM_TRANSACTION_ALIGNED_COLUMNS
    elif dataset == 'bitcoin_mempool_state':
        allowed_columns = _BITCOIN_STREAM_MEMPOOL_ALIGNED_COLUMNS
    elif dataset in _BITCOIN_DERIVED_DATASETS:
        allowed_columns = _BITCOIN_DERIVED_ALIGNED_COLUMNS
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
    elif plan.execution_path == 'bybit_aligned':
        frame = query_bybit_aligned_1s_data(
            dataset=cast(BybitAlignedDataset, plan.dataset),
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'bitcoin_stream_aligned_observation':
        frame = query_bitcoin_stream_aligned_1s_data(
            dataset=cast(BitcoinStreamAlignedDataset, plan.dataset),
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'bitcoin_derived_aligned_observation':
        frame = query_bitcoin_derived_aligned_1s_data(
            dataset=cast(BitcoinDerivedAlignedDataset, plan.dataset),
            window=plan.window,
            auth_token=auth_token,
            show_summary=show_summary,
            datetime_iso_output=datetime_iso_output,
        )
    elif plan.execution_path == 'bitcoin_derived_aligned_forward_fill':
        if not isinstance(plan.window, TimeRangeWindow):
            raise RuntimeError(
                'Internal aligned planner error: forward-fill path requires TimeRangeWindow'
            )
        frame = query_bitcoin_derived_forward_fill_intervals(
            dataset=cast(BitcoinDerivedAlignedDataset, plan.dataset),
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
