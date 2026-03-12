from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from .native_core import NativeQuerySpec, QueryWindow, execute_native_query

BinanceDataset = Literal['binance_spot_trades']


@dataclass(frozen=True)
class _BinanceSourceSpec:
    table_name: str
    id_column: str
    allowed_columns: tuple[str, ...]
    default_columns: tuple[str, ...]


_BINANCE_SOURCE_SPECS: dict[BinanceDataset, _BinanceSourceSpec] = {
    'binance_spot_trades': _BinanceSourceSpec(
        table_name='canonical_binance_spot_trades_native_v1',
        id_column='trade_id',
        allowed_columns=(
            'trade_id',
            'timestamp',
            'price',
            'quantity',
            'quote_quantity',
            'is_buyer_maker',
            'is_best_match',
            'datetime',
        ),
        default_columns=(
            'trade_id',
            'timestamp',
            'price',
            'quantity',
            'is_buyer_maker',
        ),
    ),
}


def build_binance_native_query_spec(
    dataset: BinanceDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
) -> NativeQuerySpec:
    source_spec = _BINANCE_SOURCE_SPECS[dataset]
    requested_columns = (
        tuple(select_columns)
        if select_columns is not None
        else source_spec.default_columns
    )

    if not requested_columns:
        raise ValueError('select_columns must be non-empty')

    invalid_columns = sorted(
        set(requested_columns).difference(source_spec.allowed_columns)
    )
    if invalid_columns:
        raise ValueError(
            f'Unsupported columns for {dataset}: {invalid_columns}. '
            f'Allowed={list(source_spec.allowed_columns)}'
        )

    return NativeQuerySpec(
        table_name=source_spec.table_name,
        id_column=source_spec.id_column,
        select_columns=requested_columns,
        window=window,
        include_datetime=include_datetime,
    )


def query_binance_native_data(
    dataset: BinanceDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    spec = build_binance_native_query_spec(
        dataset=dataset,
        select_columns=select_columns,
        window=window,
        include_datetime=include_datetime,
    )
    return execute_native_query(
        spec=spec,
        auth_token=auth_token,
        show_summary=show_summary,
        datetime_iso_output=datetime_iso_output,
    )
