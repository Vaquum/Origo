from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from .native_core import NativeQuerySpec, QueryWindow, execute_native_query

ETFDataset = Literal['etf_daily_metrics']


@dataclass(frozen=True)
class _ETFSourceSpec:
    table_name: str
    id_column: str
    datetime_column: str
    random_seed_column: str
    allowed_columns: tuple[str, ...]
    default_columns: tuple[str, ...]


_ETF_SOURCE_SPECS: dict[ETFDataset, _ETFSourceSpec] = {
    'etf_daily_metrics': _ETFSourceSpec(
        table_name='canonical_etf_daily_metrics_native_v1',
        id_column='metric_id',
        datetime_column='observed_at_utc',
        random_seed_column='observed_at_utc',
        allowed_columns=(
            'metric_id',
            'source_id',
            'metric_name',
            'metric_unit',
            'metric_value_string',
            'metric_value_int',
            'metric_value_float',
            'metric_value_bool',
            'observed_at_utc',
            'dimensions_json',
            'provenance_json',
            'ingested_at_utc',
        ),
        default_columns=(
            'source_id',
            'metric_name',
            'metric_unit',
            'metric_value_float',
            'metric_value_int',
            'metric_value_string',
            'observed_at_utc',
        ),
    ),
}


def build_etf_native_query_spec(
    dataset: ETFDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
) -> NativeQuerySpec:
    source_spec = _ETF_SOURCE_SPECS[dataset]
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
        datetime_column=source_spec.datetime_column,
        random_seed_column=source_spec.random_seed_column,
    )


def query_etf_native_data(
    dataset: ETFDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    spec = build_etf_native_query_spec(
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
