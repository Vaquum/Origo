from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from .native_core import NativeQuerySpec, QueryWindow, execute_native_query

BybitDataset = Literal['bybit_spot_trades']


@dataclass(frozen=True)
class _BybitSourceSpec:
    table_name: str
    id_column: str
    allowed_columns: tuple[str, ...]
    default_columns: tuple[str, ...]


_BYBIT_SOURCE_SPECS: dict[BybitDataset, _BybitSourceSpec] = {
    'bybit_spot_trades': _BybitSourceSpec(
        table_name='bybit_spot_trades',
        id_column='trade_id',
        allowed_columns=(
            'symbol',
            'trade_id',
            'trd_match_id',
            'side',
            'price',
            'size',
            'quote_quantity',
            'timestamp',
            'datetime',
            'tick_direction',
            'gross_value',
            'home_notional',
            'foreign_notional',
        ),
        default_columns=(
            'trade_id',
            'timestamp',
            'price',
            'size',
            'side',
            'trd_match_id',
        ),
    )
}


def build_bybit_native_query_spec(
    dataset: BybitDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
) -> NativeQuerySpec:
    source_spec = _BYBIT_SOURCE_SPECS[dataset]
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


def query_bybit_native_data(
    dataset: BybitDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    spec = build_bybit_native_query_spec(
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
