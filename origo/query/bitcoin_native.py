from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from .native_core import NativeQuerySpec, QueryWindow, execute_native_query

BitcoinDataset = Literal[
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]


@dataclass(frozen=True)
class _BitcoinSourceSpec:
    table_name: str
    id_column: str
    datetime_column: str
    random_seed_column: str
    allowed_columns: tuple[str, ...]
    default_columns: tuple[str, ...]


_BITCOIN_SOURCE_SPECS: dict[BitcoinDataset, _BitcoinSourceSpec] = {
    'bitcoin_block_headers': _BitcoinSourceSpec(
        table_name='bitcoin_block_headers',
        id_column='height',
        datetime_column='datetime',
        random_seed_column='timestamp',
        allowed_columns=(
            'height',
            'block_hash',
            'prev_hash',
            'merkle_root',
            'version',
            'nonce',
            'difficulty',
            'timestamp',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'height',
            'timestamp',
            'difficulty',
            'nonce',
            'version',
            'merkle_root',
            'prev_hash',
        ),
    ),
    'bitcoin_block_transactions': _BitcoinSourceSpec(
        table_name='bitcoin_block_transactions',
        id_column='txid',
        datetime_column='datetime',
        random_seed_column='block_timestamp',
        allowed_columns=(
            'block_height',
            'block_hash',
            'block_timestamp',
            'transaction_index',
            'txid',
            'inputs',
            'outputs',
            'values',
            'scripts',
            'witness_data',
            'coinbase',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'block_height',
            'block_timestamp',
            'transaction_index',
            'txid',
            'coinbase',
        ),
    ),
    'bitcoin_mempool_state': _BitcoinSourceSpec(
        table_name='bitcoin_mempool_state',
        id_column='txid',
        datetime_column='snapshot_at',
        random_seed_column='snapshot_at_unix_ms',
        allowed_columns=(
            'snapshot_at',
            'snapshot_at_unix_ms',
            'txid',
            'fee_rate_sat_vb',
            'vsize',
            'first_seen_timestamp',
            'rbf_flag',
            'source_chain',
        ),
        default_columns=(
            'snapshot_at_unix_ms',
            'txid',
            'fee_rate_sat_vb',
            'vsize',
            'first_seen_timestamp',
            'rbf_flag',
        ),
    ),
    'bitcoin_block_fee_totals': _BitcoinSourceSpec(
        table_name='bitcoin_block_fee_totals',
        id_column='block_height',
        datetime_column='datetime',
        random_seed_column='block_timestamp',
        allowed_columns=(
            'block_height',
            'block_hash',
            'block_timestamp',
            'fee_total_btc',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'block_height',
            'block_timestamp',
            'fee_total_btc',
        ),
    ),
    'bitcoin_block_subsidy_schedule': _BitcoinSourceSpec(
        table_name='bitcoin_block_subsidy_schedule',
        id_column='block_height',
        datetime_column='datetime',
        random_seed_column='block_timestamp',
        allowed_columns=(
            'block_height',
            'block_hash',
            'block_timestamp',
            'halving_interval',
            'subsidy_sats',
            'subsidy_btc',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'block_height',
            'block_timestamp',
            'halving_interval',
            'subsidy_sats',
            'subsidy_btc',
        ),
    ),
    'bitcoin_network_hashrate_estimate': _BitcoinSourceSpec(
        table_name='bitcoin_network_hashrate_estimate',
        id_column='block_height',
        datetime_column='datetime',
        random_seed_column='block_timestamp',
        allowed_columns=(
            'block_height',
            'block_hash',
            'block_timestamp',
            'difficulty',
            'observed_interval_seconds',
            'hashrate_hs',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'block_height',
            'block_timestamp',
            'difficulty',
            'observed_interval_seconds',
            'hashrate_hs',
        ),
    ),
    'bitcoin_circulating_supply': _BitcoinSourceSpec(
        table_name='bitcoin_circulating_supply',
        id_column='block_height',
        datetime_column='datetime',
        random_seed_column='block_timestamp',
        allowed_columns=(
            'block_height',
            'block_hash',
            'block_timestamp',
            'circulating_supply_sats',
            'circulating_supply_btc',
            'datetime',
            'source_chain',
        ),
        default_columns=(
            'block_height',
            'block_timestamp',
            'circulating_supply_sats',
            'circulating_supply_btc',
        ),
    ),
}


def build_bitcoin_native_query_spec(
    dataset: BitcoinDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
) -> NativeQuerySpec:
    source_spec = _BITCOIN_SOURCE_SPECS[dataset]
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


def query_bitcoin_native_data(
    dataset: BitcoinDataset,
    select_columns: Sequence[str] | None,
    window: QueryWindow,
    include_datetime: bool = True,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    spec = build_bitcoin_native_query_spec(
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
