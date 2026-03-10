from __future__ import annotations

import json
from copy import deepcopy

import pytest
from origo_control_plane.assets.bitcoin_block_transactions_to_origo import (
    normalize_block_transactions_or_raise,
)


def _sample_block_payload() -> dict[str, object]:
    return {
        'hash': '0' * 63 + '1',
        'height': 840000,
        'time': 1713600000,
        'tx': [
            {
                'txid': '1' * 64,
                'vin': [
                    {
                        'coinbase': '03f0d90c',
                        'sequence': 4294967295,
                    }
                ],
                'vout': [
                    {
                        'n': 0,
                        'value': 6.25,
                        'scriptPubKey': {
                            'asm': '0 00112233445566778899aabbccddeeff00112233',
                            'hex': '001400112233445566778899aabbccddeeff00112233',
                            'type': 'witness_v0_keyhash',
                            'address': 'bc1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqd3a7n',
                        },
                    }
                ],
            },
            {
                'txid': '2' * 64,
                'vin': [
                    {
                        'txid': '3' * 64,
                        'vout': 1,
                        'sequence': 4294967294,
                        'scriptSig': {
                            'asm': '30440220',
                            'hex': '30440220',
                        },
                        'txinwitness': ['30440220', '02abcdef'],
                        'prevout': {'value': 0.75},
                    }
                ],
                'vout': [
                    {
                        'n': 0,
                        'value': 0.5,
                        'scriptPubKey': {
                            'asm': '0 0011223344',
                            'hex': '00140011223344',
                            'type': 'witness_v0_keyhash',
                            'addresses': ['bc1qsampleaddress0000000000000000000000000q'],
                        },
                    },
                    {
                        'n': 1,
                        'value': 0.2499,
                        'scriptPubKey': {
                            'asm': '1 5120abcdef',
                            'hex': '5120abcdef',
                            'type': 'witness_v1_taproot',
                        },
                    },
                ],
            },
        ],
    }


def test_normalize_block_transactions_is_deterministic() -> None:
    block_payload = _sample_block_payload()

    run_1 = normalize_block_transactions_or_raise(
        block_payload=deepcopy(block_payload),
        expected_height=840000,
        expected_block_hash='0' * 63 + '1',
        source_chain='main',
    )
    run_2 = normalize_block_transactions_or_raise(
        block_payload=deepcopy(block_payload),
        expected_height=840000,
        expected_block_hash='0' * 63 + '1',
        source_chain='main',
    )

    assert [row.as_canonical_map() for row in run_1] == [
        row.as_canonical_map() for row in run_2
    ]


def test_normalize_block_transactions_coinbase_and_witness_fields() -> None:
    rows = normalize_block_transactions_or_raise(
        block_payload=_sample_block_payload(),
        expected_height=840000,
        expected_block_hash='0' * 63 + '1',
        source_chain='main',
    )
    assert len(rows) == 2

    coinbase_row = rows[0]
    assert coinbase_row.coinbase is True
    assert json.loads(coinbase_row.witness_data_json) == []

    spend_row = rows[1]
    assert spend_row.coinbase is False
    witness_data = json.loads(spend_row.witness_data_json)
    assert witness_data == [{'input_index': 0, 'stack': ['30440220', '02abcdef']}]
    values_payload = json.loads(spend_row.values_json)
    assert values_payload['input_values_btc'] == [0.75]
    assert values_payload['output_values_btc'] == [0.5, 0.2499]


def test_normalize_block_transactions_rejects_block_hash_mismatch() -> None:
    with pytest.raises(RuntimeError, match='block hash mismatch'):
        normalize_block_transactions_or_raise(
            block_payload=_sample_block_payload(),
            expected_height=840000,
            expected_block_hash='f' * 64,
            source_chain='main',
        )
