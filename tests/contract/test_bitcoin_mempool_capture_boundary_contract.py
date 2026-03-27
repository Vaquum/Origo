from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from origo.data._internal import generic_endpoints
from origo.query import bitcoin_mempool_boundary
from origo.query.native_core import AllRowsWindow, MonthWindow, TimeRangeWindow


def test_enforce_bitcoin_mempool_query_window_rejects_pre_capture_time_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boundary(**_: object) -> datetime:
        return datetime(2026, 3, 27, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(
        bitcoin_mempool_boundary,
        'resolve_bitcoin_mempool_capture_boundary_or_raise',
        _boundary,
    )

    with pytest.raises(
        ValueError,
        match='bitcoin_mempool_state availability begins at 2026-03-27T12:00:00Z',
    ):
        bitcoin_mempool_boundary.enforce_bitcoin_mempool_query_window_or_raise(
            window=TimeRangeWindow(
                start_iso='2026-03-27T11:59:59Z',
                end_iso='2026-03-27T12:10:00Z',
            ),
            auth_token=None,
        )


def test_enforce_bitcoin_mempool_query_window_rejects_pre_capture_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boundary(**_: object) -> datetime:
        return datetime(2026, 3, 27, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(
        bitcoin_mempool_boundary,
        'resolve_bitcoin_mempool_capture_boundary_or_raise',
        _boundary,
    )

    bitcoin_mempool_boundary.enforce_bitcoin_mempool_query_window_or_raise(
        window=TimeRangeWindow(
            start_iso='2026-03-27T12:00:00Z',
            end_iso='2026-03-27T12:10:00Z',
        ),
        auth_token=None,
    )
    with pytest.raises(
        ValueError,
        match='bitcoin_mempool_state availability begins on 2026-03-27; requested month starts on 2026-03-01',
    ):
        bitcoin_mempool_boundary.enforce_bitcoin_mempool_query_window_or_raise(
            window=MonthWindow(month=3, year=2026),
            auth_token=None,
        )


def test_enforce_bitcoin_mempool_historical_window_rejects_pre_capture_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boundary(**_: object) -> datetime:
        return datetime(2026, 3, 27, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(
        bitcoin_mempool_boundary,
        'resolve_bitcoin_mempool_capture_boundary_or_raise',
        _boundary,
    )

    with pytest.raises(
        ValueError,
        match='bitcoin_mempool_state availability begins on 2026-03-27; requested start_date=2026-03-26',
    ):
        bitcoin_mempool_boundary.enforce_bitcoin_mempool_historical_window_or_raise(
            start_date=date(2026, 3, 26),
            end_date=date(2026, 3, 27),
            auth_token=None,
        )

    with pytest.raises(
        ValueError,
        match='bitcoin_mempool_state availability begins on 2026-03-27; requested end_date=2026-03-26',
    ):
        bitcoin_mempool_boundary.enforce_bitcoin_mempool_historical_window_or_raise(
            start_date=None,
            end_date=date(2026, 3, 26),
            auth_token=None,
        )


def test_query_native_enforces_bitcoin_mempool_capture_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_enforce(*, window: object, auth_token: str | None) -> None:
        captured['window'] = window
        captured['auth_token'] = auth_token

    monkeypatch.setattr(
        generic_endpoints,
        'enforce_bitcoin_mempool_query_window_or_raise',
        _fake_enforce,
    )
    monkeypatch.setattr(
        generic_endpoints,
        'query_bitcoin_native_data',
        _fake_bitcoin_native_data,
    )

    generic_endpoints.query_native(
        dataset='bitcoin_mempool_state',
        select_cols=None,
        time_range=('2026-03-27T12:00:00Z', '2026-03-27T12:10:00Z'),
        auth_token='token',
    )

    assert isinstance(captured['window'], TimeRangeWindow)
    assert captured['auth_token'] == 'token'


def _fake_bitcoin_native_data(**_: object) -> pl.DataFrame:
    return pl.DataFrame(
            {
                'snapshot_at': [datetime(2026, 3, 27, 12, 0, tzinfo=UTC)],
                'txid': ['a' * 64],
            }
    )


def test_query_aligned_enforces_bitcoin_mempool_capture_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_enforce(*, window: object, auth_token: str | None) -> None:
        captured['window'] = window
        captured['auth_token'] = auth_token

    monkeypatch.setattr(
        generic_endpoints,
        'enforce_bitcoin_mempool_query_window_or_raise',
        _fake_enforce,
    )
    monkeypatch.setattr(
        generic_endpoints,
        'query_aligned_data',
        _fake_aligned_data,
    )

    generic_endpoints.query_aligned(
        dataset='bitcoin_mempool_state',
        select_cols=None,
        time_range=('2026-03-27T12:00:00Z', '2026-03-27T12:10:00Z'),
        auth_token='token',
    )

    assert isinstance(captured['window'], TimeRangeWindow)
    assert captured['auth_token'] == 'token'


def _fake_aligned_data(**_: object) -> pl.DataFrame:
    return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2026, 3, 27, 12, 0, tzinfo=UTC)],
                'tx_count': [1],
            }
    )


def test_query_bitcoin_dataset_data_enforces_historical_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_enforce(
        *,
        start_date: date | None,
        end_date: date | None,
        auth_token: str | None,
    ) -> None:
        captured['start_date'] = start_date
        captured['end_date'] = end_date
        captured['auth_token'] = auth_token

    monkeypatch.setattr(
        generic_endpoints,
        'enforce_bitcoin_mempool_historical_window_or_raise',
        _fake_enforce,
    )
    monkeypatch.setattr(
        generic_endpoints,
        '_resolve_historical_window',
        _all_rows_window,
    )
    monkeypatch.setattr(
        generic_endpoints,
        'query_bitcoin_native_data',
        _fake_bitcoin_native_data,
    )

    generic_endpoints.query_bitcoin_dataset_data(
        dataset='bitcoin_mempool_state',
        mode='native',
        start_date='2026-03-27',
        end_date='2026-03-28',
        auth_token='token',
    )

    assert captured['start_date'] == date(2026, 3, 27)
    assert captured['end_date'] == date(2026, 3, 28)
    assert captured['auth_token'] == 'token'


def _all_rows_window(**_: object) -> AllRowsWindow:
    return AllRowsWindow()
