from __future__ import annotations

import pytest
from origo_control_plane.assets.bitcoin_block_subsidy_schedule_to_origo import (
    _subsidy_sats_for_height,
    normalize_block_subsidy_or_raise,
)


def test_subsidy_formula_matches_halving_schedule() -> None:
    assert _subsidy_sats_for_height(0) == (0, 5_000_000_000)
    assert _subsidy_sats_for_height(209_999) == (0, 5_000_000_000)
    assert _subsidy_sats_for_height(210_000) == (1, 2_500_000_000)
    assert _subsidy_sats_for_height(420_000) == (2, 1_250_000_000)
    assert _subsidy_sats_for_height(13_440_000) == (64, 0)


def test_normalize_block_subsidy_includes_expected_fields() -> None:
    row = normalize_block_subsidy_or_raise(
        block_hash='b' * 64,
        block_header={'hash': 'b' * 64, 'height': 210000, 'time': 1713601000},
        expected_height=210000,
        source_chain='main',
    )
    assert row.halving_interval == 1
    assert row.subsidy_sats == 2_500_000_000
    assert row.subsidy_btc == pytest.approx(25.0)


def test_normalize_block_subsidy_rejects_hash_mismatch() -> None:
    with pytest.raises(RuntimeError, match='header hash mismatch'):
        normalize_block_subsidy_or_raise(
            block_hash='c' * 64,
            block_header={'hash': 'd' * 64, 'height': 100, 'time': 1713601000},
            expected_height=100,
            source_chain='main',
        )
