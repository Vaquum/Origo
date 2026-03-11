from __future__ import annotations

from typing import Literal

from origo.query.bitcoin_derived_aligned_1s import build_bitcoin_derived_aligned_1s_sql
from origo.query.bitcoin_native import build_bitcoin_native_query_spec
from origo.query.native_core import LatestRowsWindow

BitcoinDataset = Literal[
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]

_BITCOIN_NATIVE_EXPECTED_TABLES: dict[BitcoinDataset, str] = {
    'bitcoin_block_headers': 'canonical_bitcoin_block_headers_native_v1',
    'bitcoin_block_transactions': 'canonical_bitcoin_block_transactions_native_v1',
    'bitcoin_mempool_state': 'canonical_bitcoin_mempool_state_native_v1',
    'bitcoin_block_fee_totals': 'canonical_bitcoin_block_fee_totals_native_v1',
    'bitcoin_block_subsidy_schedule': 'canonical_bitcoin_block_subsidy_schedule_native_v1',
    'bitcoin_network_hashrate_estimate': 'canonical_bitcoin_network_hashrate_estimate_native_v1',
    'bitcoin_circulating_supply': 'canonical_bitcoin_circulating_supply_native_v1',
}


def test_bitcoin_native_query_specs_target_canonical_projection_tables() -> None:
    window = LatestRowsWindow(rows=10)
    for dataset, expected_table in _BITCOIN_NATIVE_EXPECTED_TABLES.items():
        spec = build_bitcoin_native_query_spec(
            dataset=dataset,
            select_columns=None,
            window=window,
        )
        assert spec.table_name == expected_table


def test_bitcoin_derived_aligned_query_targets_canonical_aligned_table() -> None:
    window = LatestRowsWindow(rows=5)
    for dataset in (
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ):
        sql = build_bitcoin_derived_aligned_1s_sql(
            dataset=dataset,
            window=window,
            database='origo',
        )
        assert 'FROM origo.canonical_aligned_1s_aggregates' in sql
        assert "source_id = 'bitcoin_core'" in sql
        assert f"stream_id = '{dataset}'" in sql
        assert 'FROM origo.bitcoin_block_fee_totals' not in sql
        assert 'FROM origo.bitcoin_block_subsidy_schedule' not in sql
        assert 'FROM origo.bitcoin_network_hashrate_estimate' not in sql
        assert 'FROM origo.bitcoin_circulating_supply' not in sql
