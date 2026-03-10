from __future__ import annotations

import pytest
from origo_control_plane.assets.bitcoin_network_hashrate_estimate_to_origo import (
    _HASHRATE_CONSTANT,
    _HeaderPoint,
    normalize_network_hashrate_rows_or_raise,
)
from origo_control_plane.bitcoin_core.rpc import BitcoinCoreRpcClient


def test_normalize_network_hashrate_rows_uses_observed_intervals() -> None:
    headers = [
        _HeaderPoint(height=0, block_hash='0' * 64, timestamp_seconds=100, difficulty=1.0),
        _HeaderPoint(height=1, block_hash='1' * 64, timestamp_seconds=700, difficulty=2.0),
    ]

    rows = normalize_network_hashrate_rows_or_raise(
        headers=headers,
        source_chain='main',
        client=BitcoinCoreRpcClient.__new__(BitcoinCoreRpcClient),
    )

    assert len(rows) == 2
    assert rows[0].observed_interval_seconds == 600
    assert rows[1].observed_interval_seconds == 600
    assert rows[0].hashrate_hs == pytest.approx(_HASHRATE_CONSTANT / 600.0)
    assert rows[1].hashrate_hs == pytest.approx((2.0 * _HASHRATE_CONSTANT) / 600.0)
