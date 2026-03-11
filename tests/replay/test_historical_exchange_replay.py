from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from origo.data._internal import generic_endpoints as historical_endpoints


def _native_binance_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'trade_id': [1],
            'timestamp': [1704067200000],
            'price': [42000.0],
            'quantity': [0.1],
            'is_buyer_maker': [0],
            'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _native_side_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'trade_id': [1],
            'timestamp': [1704067200000],
            'price': [42000.0],
            'size': [0.1],
            'side': ['buy'],
            'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _aligned_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'open_price': [42000.0],
            'high_price': [42000.0],
            'low_price': [42000.0],
            'close_price': [42000.0],
            'quantity_sum': [0.1],
            'quote_volume_sum': [4200.0],
            'trade_count': [1],
        }
    )


def test_historical_spot_trades_replay_is_deterministic_for_native_and_aligned(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_binance_native_data',
        lambda **_: _native_binance_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_okx_native_data',
        lambda **_: _native_side_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_bybit_native_data',
        lambda **_: _native_side_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _aligned_frame(),
    )

    for source in ('binance', 'okx', 'bybit'):
        native_run_1 = historical_endpoints.query_spot_trades_data(
            source=source,
            mode='native',
            n_latest_rows=1,
        )
        native_run_2 = historical_endpoints.query_spot_trades_data(
            source=source,
            mode='native',
            n_latest_rows=1,
        )
        assert native_run_1.to_dict(as_series=False) == native_run_2.to_dict(
            as_series=False
        )

        aligned_run_1 = historical_endpoints.query_spot_trades_data(
            source=source,
            mode='aligned_1s',
            n_latest_rows=1,
        )
        aligned_run_2 = historical_endpoints.query_spot_trades_data(
            source=source,
            mode='aligned_1s',
            n_latest_rows=1,
        )
        assert aligned_run_1.to_dict(as_series=False) == aligned_run_2.to_dict(
            as_series=False
        )
