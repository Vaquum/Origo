from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_frame,
    run_exchange_integrity_suite_rows,
)


def test_spot_trades_integrity_passes_for_valid_rows() -> None:
    rows = [
        (100, 50000.0, 0.1, 5000.0, 1704067200000, True, True, datetime(2024, 1, 1, tzinfo=UTC)),
        (101, 50001.0, 0.2, 10000.2, 1704067201000, False, True, datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC)),
    ]
    report = run_exchange_integrity_suite_rows(dataset='spot_trades', rows=rows)
    assert report.rows_checked == 2
    assert report.sequence_gap_count == 0
    assert report.min_id == 100
    assert report.max_id == 101


def test_spot_trades_integrity_fails_on_sequence_gap() -> None:
    rows = [
        (100, 50000.0, 0.1, 5000.0, 1704067200000, True, True, datetime(2024, 1, 1, tzinfo=UTC)),
        (102, 50001.0, 0.2, 10000.2, 1704067201000, False, True, datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC)),
    ]
    with pytest.raises(ValueError, match='sequence-gap'):
        run_exchange_integrity_suite_rows(dataset='spot_trades', rows=rows)


def test_spot_trades_integrity_allows_zero_starting_trade_id() -> None:
    rows = [
        (0, 50000.0, 0.1, 5000.0, 1502928000000, True, True, datetime(2017, 8, 17, tzinfo=UTC)),
        (1, 50001.0, 0.2, 10000.2, 1502928001000, False, True, datetime(2017, 8, 17, 0, 0, 1, tzinfo=UTC)),
    ]
    report = run_exchange_integrity_suite_rows(dataset='spot_trades', rows=rows)
    assert report.rows_checked == 2
    assert report.sequence_gap_count == 0
    assert report.min_id == 0
    assert report.max_id == 1


def test_futures_trades_integrity_fails_on_anomaly() -> None:
    rows = [
        (200, -1.0, 0.1, 10.0, 1704067200000, True, datetime(2024, 1, 1, tzinfo=UTC)),
    ]
    with pytest.raises(ValueError, match='anomaly check failed'):
        run_exchange_integrity_suite_rows(dataset='futures_trades', rows=rows)


def test_futures_agg_frame_integrity_passes() -> None:
    frame = pl.DataFrame(
        {
            'futures_agg_trades_id': [1, 2],
            'price': [100.0, 101.0],
            'quantity': [0.5, 0.6],
            'first_trade_id': [10, 11],
            'last_trade_id': [10, 11],
            'timestamp': [1704067200000, 1704067201000],
            'is_buyer_maker': [True, False],
        }
    )
    report = run_exchange_integrity_suite_frame(
        dataset='futures_agg_trades',
        frame=frame,
    )
    assert report.rows_checked == 2
    assert report.sequence_gap_count == 0


def test_okx_spot_trades_integrity_passes_for_valid_rows() -> None:
    rows = [
        (
            'BTC-USDT',
            465953984,
            'buy',
            42457.6,
            0.00118219,
            50.190759744,
            1704038400149,
            datetime(2024, 1, 1, tzinfo=UTC),
        ),
        (
            'BTC-USDT',
            465953985,
            'sell',
            42457.6,
            0.00604855,
            256.79700568000004,
            1704038401149,
            datetime(2024, 1, 1, 0, 0, 1, 149000, tzinfo=UTC),
        ),
    ]
    report = run_exchange_integrity_suite_rows(dataset='okx_spot_trades', rows=rows)
    assert report.rows_checked == 2
    assert report.sequence_gap_count == 0
    assert report.min_id == 465953984
    assert report.max_id == 465953985


def test_okx_spot_trades_integrity_fails_on_invalid_side() -> None:
    rows = [
        (
            'BTC-USDT',
            465953984,
            'maker',
            42457.6,
            0.00118219,
            50.190759744,
            1704038400149,
            datetime(2024, 1, 1, tzinfo=UTC),
        ),
    ]
    with pytest.raises(ValueError, match='expected one of \\[buy, sell\\]'):
        run_exchange_integrity_suite_rows(dataset='okx_spot_trades', rows=rows)


def test_bybit_spot_trades_integrity_passes_for_valid_rows() -> None:
    rows = [
        (
            'BTCUSDT',
            1,
            '3b55416a-1b32-502f-b282-419772dea4fe',
            'sell',
            42324.9,
            0.002,
            84.6498,
            1704067200235,
            datetime(2024, 1, 1, 0, 0, 0, 235000, tzinfo=UTC),
            'PlusTick',
            8.46498e9,
            0.002,
            84.6498,
        ),
        (
            'BTCUSDT',
            2,
            '0449d2db-fd95-5f37-90ad-649ae284acd0',
            'buy',
            42325.0,
            0.001,
            42.325,
            1704067200327,
            datetime(2024, 1, 1, 0, 0, 0, 327000, tzinfo=UTC),
            'PlusTick',
            4.2325e9,
            0.001,
            42.325,
        ),
    ]
    report = run_exchange_integrity_suite_rows(dataset='bybit_spot_trades', rows=rows)
    assert report.rows_checked == 2
    assert report.sequence_gap_count == 0
    assert report.min_id == 1
    assert report.max_id == 2


def test_bybit_spot_trades_integrity_fails_on_invalid_side() -> None:
    rows = [
        (
            'BTCUSDT',
            1,
            '3b55416a-1b32-502f-b282-419772dea4fe',
            'maker',
            42324.9,
            0.002,
            84.6498,
            1704067200235,
            datetime(2024, 1, 1, 0, 0, 0, 235000, tzinfo=UTC),
            'PlusTick',
            8.46498e9,
            0.002,
            84.6498,
        ),
    ]
    with pytest.raises(ValueError, match='expected one of \\[buy, sell\\]'):
        run_exchange_integrity_suite_rows(dataset='bybit_spot_trades', rows=rows)


def test_bybit_spot_trades_integrity_fails_on_non_monotonic_timestamp() -> None:
    rows = [
        (
            'BTCUSDT',
            1,
            '3b55416a-1b32-502f-b282-419772dea4fe',
            'sell',
            42324.9,
            0.002,
            84.6498,
            1704067200235,
            datetime(2024, 1, 1, 0, 0, 0, 235000, tzinfo=UTC),
            'PlusTick',
            8.46498e9,
            0.002,
            84.6498,
        ),
        (
            'BTCUSDT',
            2,
            '0449d2db-fd95-5f37-90ad-649ae284acd0',
            'buy',
            42325.0,
            0.001,
            42.325,
            1704067200200,
            datetime(2024, 1, 1, 0, 0, 0, 200000, tzinfo=UTC),
            'PlusTick',
            4.2325e9,
            0.001,
            42.325,
        ),
    ]
    with pytest.raises(ValueError, match='monotonic-time check failed'):
        run_exchange_integrity_suite_rows(dataset='bybit_spot_trades', rows=rows)
