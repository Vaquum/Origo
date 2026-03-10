from __future__ import annotations

import pytest
from origo_control_plane.assets.bitcoin_circulating_supply_to_origo import (
    circulating_supply_sats_at_height,
    normalize_circulating_supply_row_or_raise,
)


def test_circulating_supply_formula_matches_known_heights() -> None:
    assert circulating_supply_sats_at_height(0) == 5_000_000_000
    assert circulating_supply_sats_at_height(209_999) == 1_050_000_000_000_000
    assert circulating_supply_sats_at_height(210_000) == 1_050_002_500_000_000
    assert circulating_supply_sats_at_height(419_999) == 1_575_000_000_000_000


def test_normalize_circulating_supply_row_includes_expected_values() -> None:
    row = normalize_circulating_supply_row_or_raise(
        block_hash='e' * 64,
        block_header={'hash': 'e' * 64, 'height': 210_000, 'time': 1713601000},
        expected_height=210_000,
        source_chain='main',
    )
    assert row.circulating_supply_sats == 1_050_002_500_000_000
    assert row.circulating_supply_btc == pytest.approx(10_500_025.0)


def test_normalize_circulating_supply_row_rejects_header_mismatch() -> None:
    with pytest.raises(RuntimeError, match='header height mismatch'):
        normalize_circulating_supply_row_or_raise(
            block_hash='e' * 64,
            block_header={'hash': 'e' * 64, 'height': 100, 'time': 1713601000},
            expected_height=101,
            source_chain='main',
        )
