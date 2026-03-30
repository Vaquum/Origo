from __future__ import annotations

from origo_control_plane.backfill.s34_contract import (
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
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


def test_s34_okx_contract_uses_numeric_monotonic_ordering() -> None:
    contract = get_s34_dataset_contract('okx_spot_trades')
    assert contract.offset_ordering == 'numeric_monotonic'
    assert contract.source_safe_min_request_interval_seconds == 0.75
    assert contract.max_concurrent_partition_runs == 20


def test_s34_bybit_contract_uses_operator_safe_partition_run_ceiling() -> None:
    contract = get_s34_dataset_contract('bybit_spot_trades')
    assert contract.source_safe_concurrency_ceiling == 20
    assert contract.max_concurrent_partition_runs == 20


def test_s34_bitcoin_partition_scheme_split_is_explicit() -> None:
    assert get_s34_dataset_contract('bitcoin_block_headers').partition_scheme == 'height_range'
    assert (
        get_s34_dataset_contract('bitcoin_block_transactions').partition_scheme
        == 'height_range'
    )
    assert get_s34_dataset_contract('bitcoin_mempool_state').partition_scheme == 'daily'
    assert (
        get_s34_dataset_contract('bitcoin_block_fee_totals').partition_scheme
        == 'height_range'
    )
    assert (
        get_s34_dataset_contract('bitcoin_block_subsidy_schedule').partition_scheme
        == 'height_range'
    )
    assert (
        get_s34_dataset_contract('bitcoin_network_hashrate_estimate').partition_scheme
        == 'height_range'
    )
    assert (
        get_s34_dataset_contract('bitcoin_circulating_supply').partition_scheme
        == 'height_range'
    )


def test_s34_bitcoin_offset_ordering_split_is_explicit() -> None:
    assert get_s34_dataset_contract('bitcoin_block_headers').offset_ordering == 'numeric'
    assert (
        get_s34_dataset_contract('bitcoin_block_transactions').offset_ordering
        == 'lexicographic'
    )
    assert get_s34_dataset_contract('bitcoin_mempool_state').offset_ordering == 'lexicographic'
    assert get_s34_dataset_contract('bitcoin_block_fee_totals').offset_ordering == 'numeric'
    assert (
        get_s34_dataset_contract('bitcoin_block_subsidy_schedule').offset_ordering
        == 'numeric'
    )
    assert (
        get_s34_dataset_contract('bitcoin_network_hashrate_estimate').offset_ordering
        == 'numeric'
    )
    assert get_s34_dataset_contract('bitcoin_circulating_supply').offset_ordering == 'numeric'
