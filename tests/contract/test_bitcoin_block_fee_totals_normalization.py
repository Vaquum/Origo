from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from origo_control_plane.assets.bitcoin_block_fee_totals_to_origo import (
    normalize_block_fee_total_or_raise,
)


def _sample_block_payload() -> dict[str, Any]:
    return {
        'hash': 'a' * 64,
        'height': 840001,
        'time': 1713601000,
        'tx': [
            {
                'txid': '1' * 64,
                'vin': [{'coinbase': '03f0d90c', 'sequence': 4294967295}],
                'vout': [{'value': 6.25}],
            },
            {
                'txid': '2' * 64,
                'vin': [{'prevout': {'value': 1.25}}],
                'vout': [{'value': 1.0}, {'value': 0.2499}],
            },
            {
                'txid': '3' * 64,
                'vin': [{'prevout': {'value': 0.5}}, {'prevout': {'value': 0.25}}],
                'vout': [{'value': 0.749}],
            },
        ],
    }


def test_normalize_block_fee_total_is_deterministic() -> None:
    payload = _sample_block_payload()

    run_1 = normalize_block_fee_total_or_raise(
        block_payload=deepcopy(payload),
        expected_height=840001,
        expected_block_hash='a' * 64,
        source_chain='main',
    )
    run_2 = normalize_block_fee_total_or_raise(
        block_payload=deepcopy(payload),
        expected_height=840001,
        expected_block_hash='a' * 64,
        source_chain='main',
    )

    assert run_1.as_canonical_map() == run_2.as_canonical_map()


def test_normalize_block_fee_total_computes_expected_sum() -> None:
    normalized = normalize_block_fee_total_or_raise(
        block_payload=_sample_block_payload(),
        expected_height=840001,
        expected_block_hash='a' * 64,
        source_chain='main',
    )
    assert normalized.block_height == 840001
    assert normalized.fee_total_btc == pytest.approx(0.0011)


def test_normalize_block_fee_total_rejects_negative_transaction_fee() -> None:
    payload = _sample_block_payload()
    tx_payload = payload['tx']
    tx_payload[1]['vout'] = [{'value': 1.5}]

    with pytest.raises(RuntimeError, match='Negative transaction fee computed'):
        normalize_block_fee_total_or_raise(
            block_payload=payload,
            expected_height=840001,
            expected_block_hash='a' * 64,
            source_chain='main',
        )
