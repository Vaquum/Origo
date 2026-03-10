from __future__ import annotations

import json

import pytest
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_fee_total_integrity,
    run_bitcoin_block_header_integrity,
    run_bitcoin_block_transaction_integrity,
    run_bitcoin_circulating_supply_integrity,
    run_bitcoin_mempool_state_integrity,
    run_bitcoin_network_hashrate_integrity,
    run_bitcoin_subsidy_schedule_integrity,
)


def _header_rows() -> list[dict[str, object]]:
    return [
        {
            'height': 100,
            'block_hash': 'a' * 64,
            'prev_hash': '9' * 64,
            'merkle_root': '1' * 64,
            'version': 1,
            'nonce': 1000,
            'difficulty': 1.0,
            'timestamp_ms': 1713600000000,
            'source_chain': 'main',
        },
        {
            'height': 101,
            'block_hash': 'b' * 64,
            'prev_hash': 'a' * 64,
            'merkle_root': '2' * 64,
            'version': 1,
            'nonce': 1001,
            'difficulty': 1.1,
            'timestamp_ms': 1713600001000,
            'source_chain': 'main',
        },
    ]


def _transaction_rows() -> list[dict[str, object]]:
    return [
        {
            'block_height': 100,
            'block_hash': 'a' * 64,
            'block_timestamp_ms': 1713600000000,
            'transaction_index': 0,
            'txid': 'c' * 64,
            'inputs_json': json.dumps([{'coinbase_hex': '03f0d90c'}], sort_keys=True),
            'outputs_json': json.dumps([{'value_btc': 6.25}], sort_keys=True),
            'values_json': json.dumps(
                {
                    'input_values_btc': [],
                    'output_values_btc': [6.25],
                    'input_value_btc_sum': 0.0,
                    'output_value_btc_sum': 6.25,
                },
                sort_keys=True,
            ),
            'scripts_json': json.dumps({}, sort_keys=True),
            'witness_data_json': json.dumps([], sort_keys=True),
            'coinbase': True,
            'source_chain': 'main',
        },
        {
            'block_height': 101,
            'block_hash': 'b' * 64,
            'block_timestamp_ms': 1713600001000,
            'transaction_index': 0,
            'txid': 'd' * 64,
            'inputs_json': json.dumps([{'prev_txid': 'e' * 64}], sort_keys=True),
            'outputs_json': json.dumps([{'value_btc': 0.9}], sort_keys=True),
            'values_json': json.dumps(
                {
                    'input_values_btc': [1.0],
                    'output_values_btc': [0.9],
                    'input_value_btc_sum': 1.0,
                    'output_value_btc_sum': 0.9,
                },
                sort_keys=True,
            ),
            'scripts_json': json.dumps({}, sort_keys=True),
            'witness_data_json': json.dumps([], sort_keys=True),
            'coinbase': False,
            'source_chain': 'main',
        },
    ]


def _mempool_rows() -> list[dict[str, object]]:
    return [
        {
            'snapshot_at_unix_ms': 1713600100000,
            'txid': 'f' * 64,
            'fee_rate_sat_vb': 5.0,
            'vsize': 200,
            'first_seen_timestamp': 1713600000,
            'rbf_flag': True,
            'source_chain': 'main',
        },
        {
            'snapshot_at_unix_ms': 1713600100000,
            'txid': '0' * 64,
            'fee_rate_sat_vb': 12.5,
            'vsize': 300,
            'first_seen_timestamp': 1713600001,
            'rbf_flag': False,
            'source_chain': 'main',
        },
    ]


def test_bitcoin_stream_integrity_suites_pass() -> None:
    header_report = run_bitcoin_block_header_integrity(rows=_header_rows())
    assert header_report.rows_checked == 2

    transaction_report = run_bitcoin_block_transaction_integrity(rows=_transaction_rows())
    assert transaction_report.rows_checked == 2

    mempool_report = run_bitcoin_mempool_state_integrity(rows=_mempool_rows())
    assert mempool_report.rows_checked == 2


def test_bitcoin_header_integrity_rejects_linkage_break() -> None:
    rows = _header_rows()
    rows[1]['prev_hash'] = 'f' * 64
    with pytest.raises(ValueError, match='linkage check failed'):
        run_bitcoin_block_header_integrity(rows=rows)


def test_bitcoin_transaction_integrity_rejects_nonzero_first_index() -> None:
    rows = _transaction_rows()
    rows[0]['transaction_index'] = 1
    with pytest.raises(ValueError, match='transaction_index must be 0'):
        run_bitcoin_block_transaction_integrity(rows=rows)


def test_bitcoin_mempool_integrity_rejects_mixed_snapshots() -> None:
    rows = _mempool_rows()
    rows[1]['snapshot_at_unix_ms'] = 1713600100001
    with pytest.raises(ValueError, match='mixed snapshot_at_unix_ms'):
        run_bitcoin_mempool_state_integrity(rows=rows)


def _fee_rows() -> list[dict[str, object]]:
    return [
        {
            'block_height': 100,
            'block_hash': 'a' * 64,
            'block_timestamp_ms': 1713600000000,
            'fee_total_btc': 0.01,
            'source_chain': 'main',
        },
        {
            'block_height': 101,
            'block_hash': 'b' * 64,
            'block_timestamp_ms': 1713600001000,
            'fee_total_btc': 0.02,
            'source_chain': 'main',
        },
    ]


def _subsidy_rows() -> list[dict[str, object]]:
    return [
        {
            'block_height': 0,
            'block_hash': '1' * 64,
            'block_timestamp_ms': 1713600000000,
            'halving_interval': 0,
            'subsidy_sats': 5_000_000_000,
            'subsidy_btc': 50.0,
            'source_chain': 'main',
        },
        {
            'block_height': 1,
            'block_hash': '2' * 64,
            'block_timestamp_ms': 1713600001000,
            'halving_interval': 0,
            'subsidy_sats': 5_000_000_000,
            'subsidy_btc': 50.0,
            'source_chain': 'main',
        },
    ]


def _hashrate_rows() -> list[dict[str, object]]:
    return [
        {
            'block_height': 100,
            'block_hash': 'a' * 64,
            'block_timestamp_ms': 1713600000000,
            'difficulty': 1.0,
            'observed_interval_seconds': 600,
            'hashrate_hs': float(2**32) / 600.0,
            'source_chain': 'main',
        },
        {
            'block_height': 101,
            'block_hash': 'b' * 64,
            'block_timestamp_ms': 1713600001000,
            'difficulty': 2.0,
            'observed_interval_seconds': 600,
            'hashrate_hs': (float(2**32) * 2.0) / 600.0,
            'source_chain': 'main',
        },
    ]


def _supply_rows() -> list[dict[str, object]]:
    return [
        {
            'block_height': 0,
            'block_hash': '1' * 64,
            'block_timestamp_ms': 1713600000000,
            'circulating_supply_sats': 5_000_000_000,
            'circulating_supply_btc': 50.0,
            'source_chain': 'main',
        },
        {
            'block_height': 1,
            'block_hash': '2' * 64,
            'block_timestamp_ms': 1713600001000,
            'circulating_supply_sats': 10_000_000_000,
            'circulating_supply_btc': 100.0,
            'source_chain': 'main',
        },
    ]


def test_bitcoin_derived_integrity_suites_pass() -> None:
    assert run_bitcoin_block_fee_total_integrity(rows=_fee_rows()).rows_checked == 2
    assert run_bitcoin_subsidy_schedule_integrity(rows=_subsidy_rows()).rows_checked == 2
    assert run_bitcoin_network_hashrate_integrity(rows=_hashrate_rows()).rows_checked == 2
    assert run_bitcoin_circulating_supply_integrity(rows=_supply_rows()).rows_checked == 2


def test_bitcoin_subsidy_integrity_rejects_formula_violation() -> None:
    rows = _subsidy_rows()
    rows[1]['subsidy_sats'] = 2_500_000_000
    with pytest.raises(ValueError, match='formula check failed'):
        run_bitcoin_subsidy_schedule_integrity(rows=rows)
