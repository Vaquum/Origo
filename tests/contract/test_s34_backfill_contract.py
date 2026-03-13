from __future__ import annotations

from origo_control_plane.backfill.s34_contract import (
    assert_s34_backfill_contract_consistency_or_raise,
    list_s34_dataset_contracts,
)


def test_s34_backfill_contract_matches_locked_dataset_order() -> None:
    contracts = list_s34_dataset_contracts()
    observed_order = [contract.dataset for contract in contracts]
    assert observed_order == [
        'binance_spot_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ]


def test_s34_backfill_contract_is_consistent() -> None:
    assert_s34_backfill_contract_consistency_or_raise()
