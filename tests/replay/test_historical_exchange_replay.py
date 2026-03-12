from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl
import pytest

from origo.data._internal import generic_endpoints as historical_endpoints
from origo.query.native_core import TimeRangeWindow


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


def _aligned_kline_input_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'aligned_at_utc': [
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            ],
            'open_price': [42000.0, 42001.0],
            'high_price': [42002.0, 42003.0],
            'low_price': [41999.0, 42000.0],
            'close_price': [42001.0, 42002.0],
            'quantity_sum': [0.1, 0.2],
            'quote_volume_sum': [4200.0, 8400.0],
            'trade_count': [1, 2],
        }
    )


def _etf_native_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'metric_id': ['m1'],
            'source_id': ['etf_ishares_ibit_daily'],
            'metric_name': ['btc_units'],
            'metric_unit': ['BTC'],
            'metric_value_string': [None],
            'metric_value_int': [None],
            'metric_value_float': [10.0],
            'metric_value_bool': [None],
            'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'dimensions_json': ['{}'],
            'provenance_json': ['{}'],
            'ingested_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _etf_aligned_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'source_id': ['etf_ishares_ibit_daily'],
            'metric_name': ['btc_units'],
            'metric_unit': ['BTC'],
            'metric_value_string': [None],
            'metric_value_int': [None],
            'metric_value_float': [10.0],
            'metric_value_bool': [None],
            'dimensions_json': ['{}'],
            'provenance_json': ['{}'],
            'latest_ingested_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'records_in_bucket': [1],
            'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
        }
    )


def _fred_native_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'metric_id': ['f1'],
            'source_id': ['fred_walcl'],
            'metric_name': ['WALCL'],
            'metric_unit': ['USD'],
            'metric_value_string': [None],
            'metric_value_int': [None],
            'metric_value_float': [100.0],
            'metric_value_bool': [None],
            'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'dimensions_json': ['{}'],
            'provenance_json': ['{}'],
            'ingested_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _fred_aligned_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'source_id': ['fred_walcl'],
            'metric_name': ['WALCL'],
            'metric_unit': ['USD'],
            'metric_value_string': [None],
            'metric_value_int': [None],
            'metric_value_float': [100.0],
            'metric_value_bool': [None],
            'dimensions_json': ['{}'],
            'provenance_json': ['{}'],
            'latest_ingested_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'records_in_bucket': [1],
            'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
        }
    )


def _bitcoin_native_frame(dataset: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'dataset': [dataset],
            'height': [100],
            'difficulty': [2.0],
            'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _bitcoin_aligned_frame(dataset: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            'dataset': [dataset],
            'records_in_bucket': [1],
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


def test_historical_spot_trades_aligned_filters_apply_before_projection(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _aligned_frame(),
    )
    filtered = historical_endpoints.query_spot_trades_data(
        source='binance',
        mode='aligned_1s',
        n_latest_rows=1,
        fields=['open_price'],
        filters=[{'field': 'close_price', 'op': 'gte', 'value': 42000.0}],
    )
    assert filtered.columns == ['open_price']
    assert filtered.height == 1


def test_historical_spot_klines_aligned_replay_is_deterministic(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _aligned_kline_input_frame(),
    )

    for source in ('binance', 'okx', 'bybit'):
        run_1 = historical_endpoints.query_spot_klines_data(
            source=source,
            mode='aligned_1s',
            n_latest_rows=2,
            kline_size=2,
        )
        run_2 = historical_endpoints.query_spot_klines_data(
            source=source,
            mode='aligned_1s',
            n_latest_rows=2,
            kline_size=2,
        )
        assert run_1.to_dict(as_series=False) == run_2.to_dict(as_series=False)


def test_historical_etf_daily_metrics_replay_is_deterministic_for_native_and_aligned(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_etf_native_data',
        lambda **_: _etf_native_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _etf_aligned_frame(),
    )

    native_run_1 = historical_endpoints.query_etf_daily_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    native_run_2 = historical_endpoints.query_etf_daily_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    assert native_run_1.to_dict(as_series=False) == native_run_2.to_dict(
        as_series=False
    )

    aligned_run_1 = historical_endpoints.query_etf_daily_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_run_2 = historical_endpoints.query_etf_daily_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    assert aligned_run_1.to_dict(as_series=False) == aligned_run_2.to_dict(
        as_series=False
    )


def test_historical_etf_aligned_date_window_uses_time_range_window(
    monkeypatch: Any,
) -> None:
    captured_window: list[TimeRangeWindow] = []

    def _fake_query_aligned_data(**kwargs: Any) -> pl.DataFrame:
        window = kwargs['window']
        if not isinstance(window, TimeRangeWindow):
            raise AssertionError('expected TimeRangeWindow for aligned ETF date-window')
        captured_window.append(window)
        return _etf_aligned_frame()

    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        _fake_query_aligned_data,
    )

    historical_endpoints.query_etf_daily_metrics_data(
        mode='aligned_1s',
        start_date='2024-01-01',
        end_date='2024-01-01',
    )

    assert len(captured_window) == 1
    window = captured_window[0]
    assert window.start_iso == '2024-01-01T00:00:00Z'
    assert window.end_iso == '2024-01-02T00:00:00Z'


def test_historical_etf_outputs_match_raw_query_shape_for_equivalent_windows(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_etf_native_data',
        lambda **_: _etf_native_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _etf_aligned_frame(),
    )

    native_historical = historical_endpoints.query_etf_daily_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    native_raw = historical_endpoints.query_native(
        dataset='etf_daily_metrics',
        select_cols=None,
        n_rows=1,
        include_datetime_col=True,
    )
    assert native_historical.to_dict(as_series=False) == native_raw.to_dict(
        as_series=False
    )

    aligned_historical = historical_endpoints.query_etf_daily_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_raw = historical_endpoints.query_aligned(
        dataset='etf_daily_metrics',
        select_cols=None,
        n_rows=1,
    )
    assert aligned_historical.to_dict(as_series=False) == aligned_raw.to_dict(
        as_series=False
    )


def test_historical_fred_series_metrics_replay_is_deterministic_for_native_and_aligned(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_fred_native_data',
        lambda **_: _fred_native_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _fred_aligned_frame(),
    )

    native_run_1 = historical_endpoints.query_fred_series_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    native_run_2 = historical_endpoints.query_fred_series_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    assert native_run_1.to_dict(as_series=False) == native_run_2.to_dict(
        as_series=False
    )

    aligned_run_1 = historical_endpoints.query_fred_series_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_run_2 = historical_endpoints.query_fred_series_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    assert aligned_run_1.to_dict(as_series=False) == aligned_run_2.to_dict(
        as_series=False
    )


def test_historical_fred_aligned_date_window_uses_time_range_window(
    monkeypatch: Any,
) -> None:
    captured_window: list[TimeRangeWindow] = []

    def _fake_query_aligned_data(**kwargs: Any) -> pl.DataFrame:
        window = kwargs['window']
        if not isinstance(window, TimeRangeWindow):
            raise AssertionError(
                'expected TimeRangeWindow for aligned FRED date-window'
            )
        captured_window.append(window)
        return _fred_aligned_frame()

    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        _fake_query_aligned_data,
    )

    historical_endpoints.query_fred_series_metrics_data(
        mode='aligned_1s',
        start_date='2024-01-01',
        end_date='2024-01-01',
    )

    assert len(captured_window) == 1
    window = captured_window[0]
    assert window.start_iso == '2024-01-01T00:00:00Z'
    assert window.end_iso == '2024-01-02T00:00:00Z'


def test_historical_fred_outputs_match_raw_query_shape_for_equivalent_windows(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_fred_native_data',
        lambda **_: _fred_native_frame(),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **_: _fred_aligned_frame(),
    )

    native_historical = historical_endpoints.query_fred_series_metrics_data(
        mode='native',
        n_latest_rows=1,
    )
    native_raw = historical_endpoints.query_native(
        dataset='fred_series_metrics',
        select_cols=None,
        n_rows=1,
        include_datetime_col=True,
    )
    assert native_historical.to_dict(as_series=False) == native_raw.to_dict(
        as_series=False
    )

    aligned_historical = historical_endpoints.query_fred_series_metrics_data(
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_raw = historical_endpoints.query_aligned(
        dataset='fred_series_metrics',
        select_cols=None,
        n_rows=1,
    )
    assert aligned_historical.to_dict(as_series=False) == aligned_raw.to_dict(
        as_series=False
    )


@pytest.mark.parametrize(
    'dataset',
    [
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ],
)
def test_historical_bitcoin_dataset_replay_is_deterministic_for_native_and_aligned(
    monkeypatch: Any,
    dataset: str,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_bitcoin_native_data',
        lambda **kwargs: _bitcoin_native_frame(kwargs['dataset']),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **kwargs: _bitcoin_aligned_frame(kwargs['dataset']),
    )

    native_run_1 = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='native',
        n_latest_rows=1,
    )
    native_run_2 = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='native',
        n_latest_rows=1,
    )
    assert native_run_1.to_dict(as_series=False) == native_run_2.to_dict(as_series=False)

    aligned_run_1 = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_run_2 = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='aligned_1s',
        n_latest_rows=1,
    )
    assert aligned_run_1.to_dict(as_series=False) == aligned_run_2.to_dict(
        as_series=False
    )


@pytest.mark.parametrize(
    'dataset',
    [
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ],
)
def test_historical_bitcoin_aligned_date_window_uses_time_range_window(
    monkeypatch: Any,
    dataset: str,
) -> None:
    captured_window: list[TimeRangeWindow] = []

    def _fake_query_aligned_data(**kwargs: Any) -> pl.DataFrame:
        window = kwargs['window']
        if not isinstance(window, TimeRangeWindow):
            raise AssertionError(
                'expected TimeRangeWindow for aligned Bitcoin date-window'
            )
        captured_window.append(window)
        return _bitcoin_aligned_frame(kwargs['dataset'])

    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        _fake_query_aligned_data,
    )

    historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='aligned_1s',
        start_date='2024-01-01',
        end_date='2024-01-01',
    )

    assert len(captured_window) == 1
    window = captured_window[0]
    assert window.start_iso == '2024-01-01T00:00:00Z'
    assert window.end_iso == '2024-01-02T00:00:00Z'


@pytest.mark.parametrize(
    'dataset',
    [
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ],
)
def test_historical_bitcoin_outputs_match_raw_query_shape_for_equivalent_windows(
    monkeypatch: Any,
    dataset: str,
) -> None:
    monkeypatch.setattr(
        historical_endpoints,
        'query_bitcoin_native_data',
        lambda **kwargs: _bitcoin_native_frame(kwargs['dataset']),
    )
    monkeypatch.setattr(
        historical_endpoints,
        'query_aligned_data',
        lambda **kwargs: _bitcoin_aligned_frame(kwargs['dataset']),
    )

    native_historical = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='native',
        n_latest_rows=1,
    )
    native_raw = historical_endpoints.query_native(
        dataset=dataset,
        select_cols=None,
        n_rows=1,
        include_datetime_col=True,
    )
    assert native_historical.to_dict(as_series=False) == native_raw.to_dict(
        as_series=False
    )

    aligned_historical = historical_endpoints.query_bitcoin_dataset_data(
        dataset=dataset,
        mode='aligned_1s',
        n_latest_rows=1,
    )
    aligned_raw = historical_endpoints.query_aligned(
        dataset=dataset,
        select_cols=None,
        n_rows=1,
    )
    assert aligned_historical.to_dict(as_series=False) == aligned_raw.to_dict(
        as_series=False
    )
