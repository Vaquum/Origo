from __future__ import annotations

import importlib
import inspect
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.origo_api.schemas import (
    HistoricalBitcoinDatasetRequest,
    HistoricalETFDailyMetricsRequest,
    HistoricalFREDSeriesMetricsRequest,
    HistoricalSpotKlinesRequest,
    HistoricalSpotTradesRequest,
)
from origo.data.historical_data import HistoricalData


def _load_main_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ModuleType:
    rights_module = importlib.import_module('api.origo_api.rights')
    monkeypatch.setattr(
        rights_module,
        '_assert_promoted_serving_projection_coverage_or_raise',
        lambda *, dataset: None,
    )

    matrix_path = tmp_path / 'rights-matrix.json'
    binance_legal = tmp_path / 'binance-legal.md'
    okx_legal = tmp_path / 'okx-legal.md'
    bybit_legal = tmp_path / 'bybit-legal.md'
    etf_legal = tmp_path / 'etf-legal.md'
    fred_legal = tmp_path / 'fred-legal.md'
    bitcoin_legal = tmp_path / 'bitcoin-legal.md'
    binance_legal.write_text('# legal', encoding='utf-8')
    okx_legal.write_text('# legal', encoding='utf-8')
    bybit_legal.write_text('# legal', encoding='utf-8')
    etf_legal.write_text('# legal', encoding='utf-8')
    fred_legal.write_text('# legal', encoding='utf-8')
    bitcoin_legal.write_text('# legal', encoding='utf-8')

    matrix_payload = {
        'version': 'historical-contract-test',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['binance_spot_trades'],
                'legal_signoff_artifact': str(binance_legal),
            },
            'okx': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['okx_spot_trades'],
                'legal_signoff_artifact': str(okx_legal),
            },
            'bybit': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['bybit_spot_trades'],
                'legal_signoff_artifact': str(bybit_legal),
            },
            'etf': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['etf_daily_metrics'],
                'legal_signoff_artifact': str(etf_legal),
            },
            'fred': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['fred_series_metrics'],
                'legal_signoff_artifact': str(fred_legal),
            },
            'bitcoin_core': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': [
                    'bitcoin_block_headers',
                    'bitcoin_block_transactions',
                    'bitcoin_mempool_state',
                    'bitcoin_block_fee_totals',
                    'bitcoin_block_subsidy_schedule',
                    'bitcoin_network_hashrate_estimate',
                    'bitcoin_circulating_supply',
                ],
                'legal_signoff_artifact': str(bitcoin_legal),
            },
        },
    }
    matrix_path.write_text(json.dumps(matrix_payload), encoding='utf-8')

    monkeypatch.setenv('ORIGO_INTERNAL_API_KEY', 'test-internal-key')
    monkeypatch.setenv('ORIGO_QUERY_MAX_CONCURRENCY', '2')
    monkeypatch.setenv('ORIGO_QUERY_MAX_QUEUE', '8')
    monkeypatch.setenv('ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY', '2')
    monkeypatch.setenv('ORIGO_ALIGNED_QUERY_MAX_QUEUE', '8')
    monkeypatch.setenv('ORIGO_EXPORT_MAX_CONCURRENCY', '2')
    monkeypatch.setenv('ORIGO_EXPORT_MAX_QUEUE', '8')
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))
    monkeypatch.setenv('ORIGO_EXPORT_AUDIT_LOG_PATH', str(tmp_path / 'export-audit.jsonl'))
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv('ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS', '2')
    monkeypatch.setenv('ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS', '2')
    monkeypatch.setenv('ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS', '3600')
    monkeypatch.setenv('ORIGO_ETF_QUERY_SERVING_STATE', 'promoted')
    monkeypatch.setenv('ORIGO_FRED_QUERY_SERVING_STATE', 'promoted')

    module_name = 'api.origo_api.main'
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def test_historical_data_method_contract_and_dropped_methods() -> None:
    trades_signature = inspect.signature(HistoricalData.get_binance_spot_trades)
    assert inspect.signature(HistoricalData.get_okx_spot_trades) == trades_signature
    assert inspect.signature(HistoricalData.get_bybit_spot_trades) == trades_signature
    assert list(trades_signature.parameters) == [
        'self',
        'mode',
        'start_date',
        'end_date',
        'n_latest_rows',
        'n_random_rows',
        'fields',
        'filters',
        'strict',
        'include_datetime_col',
    ]

    klines_signature = inspect.signature(HistoricalData.get_binance_spot_klines)
    assert inspect.signature(HistoricalData.get_okx_spot_klines) == klines_signature
    assert inspect.signature(HistoricalData.get_bybit_spot_klines) == klines_signature
    assert list(klines_signature.parameters) == [
        'self',
        'mode',
        'start_date',
        'end_date',
        'n_latest_rows',
        'n_random_rows',
        'fields',
        'filters',
        'strict',
        'kline_size',
    ]

    etf_signature = inspect.signature(HistoricalData.get_etf_daily_metrics)
    assert list(etf_signature.parameters) == [
        'self',
        'mode',
        'start_date',
        'end_date',
        'n_latest_rows',
        'n_random_rows',
        'fields',
        'filters',
        'strict',
    ]

    fred_signature = inspect.signature(HistoricalData.get_fred_series_metrics)
    assert list(fred_signature.parameters) == [
        'self',
        'mode',
        'start_date',
        'end_date',
        'n_latest_rows',
        'n_random_rows',
        'fields',
        'filters',
        'strict',
    ]

    bitcoin_signature = inspect.signature(HistoricalData.get_bitcoin_block_headers)
    assert inspect.signature(HistoricalData.get_bitcoin_block_transactions) == bitcoin_signature
    assert inspect.signature(HistoricalData.get_bitcoin_mempool_state) == bitcoin_signature
    assert inspect.signature(HistoricalData.get_bitcoin_block_fee_totals) == bitcoin_signature
    assert (
        inspect.signature(HistoricalData.get_bitcoin_block_subsidy_schedule)
        == bitcoin_signature
    )
    assert (
        inspect.signature(HistoricalData.get_bitcoin_network_hashrate_estimate)
        == bitcoin_signature
    )
    assert inspect.signature(HistoricalData.get_bitcoin_circulating_supply) == bitcoin_signature
    assert list(bitcoin_signature.parameters) == [
        'self',
        'mode',
        'start_date',
        'end_date',
        'n_latest_rows',
        'n_random_rows',
        'fields',
        'filters',
        'strict',
    ]

    dropped_methods = {
        'get_spot_trades',
        'get_spot_klines',
        'get_spot_agg_trades',
        'get_futures_trades',
        'get_futures_klines',
    }
    for method_name in dropped_methods:
        assert not hasattr(HistoricalData, method_name)


def test_historical_data_trade_methods_accept_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.data import historical_data as historical_data_module

    captured_kwargs: dict[str, Any] = {}

    def _fake_query_spot_trades_data(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
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

    monkeypatch.setattr(
        historical_data_module,
        'query_spot_trades_data',
        _fake_query_spot_trades_data,
    )
    historical = HistoricalData()
    historical.get_binance_spot_trades(mode='aligned_1s')

    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['source'] == 'binance'
    assert historical.data.height == 1


def test_historical_data_kline_methods_accept_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.data import historical_data as historical_data_module

    captured_kwargs: dict[str, Any] = {}

    def _fake_query_spot_klines_data(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
                'open': [42000.0],
                'high': [42000.0],
                'low': [42000.0],
                'close': [42000.0],
                'volume': [0.1],
                'no_of_trades': [1],
                'liquidity_sum': [4200.0],
            }
        )

    monkeypatch.setattr(
        historical_data_module,
        'query_spot_klines_data',
        _fake_query_spot_klines_data,
    )
    historical = HistoricalData()
    historical.get_binance_spot_klines(mode='aligned_1s')

    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['source'] == 'binance'
    assert historical.data.height == 1


def test_historical_data_etf_method_accepts_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.data import historical_data as historical_data_module

    captured_kwargs: dict[str, Any] = {}

    def _fake_query_etf_daily_metrics_data(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'source_id': ['etf_ishares_ibit_daily'],
                'metric_name': ['btc_units'],
                'metric_value_float': [10.0],
                'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(
        historical_data_module,
        'query_etf_daily_metrics_data',
        _fake_query_etf_daily_metrics_data,
    )
    historical = HistoricalData()
    historical.get_etf_daily_metrics(mode='aligned_1s')

    assert captured_kwargs['mode'] == 'aligned_1s'
    assert historical.data.height == 1


def test_historical_data_fred_method_accepts_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.data import historical_data as historical_data_module

    captured_kwargs: dict[str, Any] = {}

    def _fake_query_fred_series_metrics_data(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'source_id': ['fred_walcl'],
                'metric_name': ['WALCL'],
                'metric_value_float': [100.0],
                'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(
        historical_data_module,
        'query_fred_series_metrics_data',
        _fake_query_fred_series_metrics_data,
    )
    historical = HistoricalData()
    historical.get_fred_series_metrics(mode='aligned_1s')

    assert captured_kwargs['mode'] == 'aligned_1s'
    assert historical.data.height == 1


@pytest.mark.parametrize(
    'method_name,dataset',
    [
        ('get_bitcoin_block_headers', 'bitcoin_block_headers'),
        ('get_bitcoin_block_transactions', 'bitcoin_block_transactions'),
        ('get_bitcoin_mempool_state', 'bitcoin_mempool_state'),
        ('get_bitcoin_block_fee_totals', 'bitcoin_block_fee_totals'),
        ('get_bitcoin_block_subsidy_schedule', 'bitcoin_block_subsidy_schedule'),
        ('get_bitcoin_network_hashrate_estimate', 'bitcoin_network_hashrate_estimate'),
        ('get_bitcoin_circulating_supply', 'bitcoin_circulating_supply'),
    ],
)
def test_historical_data_bitcoin_methods_accept_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    dataset: str,
) -> None:
    from origo.data import historical_data as historical_data_module

    captured_kwargs: dict[str, Any] = {}

    def _fake_query_bitcoin_dataset_data(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'records_in_bucket': [1],
            }
        )

    monkeypatch.setattr(
        historical_data_module,
        'query_bitcoin_dataset_data',
        _fake_query_bitcoin_dataset_data,
    )
    historical = HistoricalData()
    method = getattr(historical, method_name)
    method(mode='aligned_1s')

    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['dataset'] == dataset
    assert historical.data.height == 1


def test_historical_request_rejects_multiple_window_modes() -> None:
    with pytest.raises(ValidationError, match='At most one window mode can be provided'):
        HistoricalSpotTradesRequest(start_date='2024-01-01', n_latest_rows=10)


def test_historical_request_rejects_invalid_date() -> None:
    with pytest.raises(ValidationError, match='valid strict YYYY-MM-DD'):
        HistoricalSpotTradesRequest(start_date='2022-02-30', end_date='2022-03-01')


def test_historical_request_accepts_no_window_mode() -> None:
    request = HistoricalSpotTradesRequest()
    assert request.mode == 'native'
    assert request.start_date is None
    assert request.end_date is None


def test_historical_klines_request_accepts_random_rows_mode() -> None:
    request = HistoricalSpotKlinesRequest(n_random_rows=100, kline_size=60)
    assert request.n_random_rows == 100
    assert request.kline_size == 60


def test_historical_etf_request_rejects_invalid_date() -> None:
    with pytest.raises(ValidationError, match='valid strict YYYY-MM-DD'):
        HistoricalETFDailyMetricsRequest(
            start_date='2022-02-30',
            end_date='2022-03-01',
        )


def test_historical_etf_request_supports_shared_contract_fields() -> None:
    request = HistoricalETFDailyMetricsRequest(
        mode='aligned_1s',
        fields=['source_id', 'metric_name'],
        filters=[{'field': 'metric_name', 'op': 'eq', 'value': 'btc_units'}],
    )
    assert request.mode == 'aligned_1s'
    assert request.fields == ['source_id', 'metric_name']
    assert request.filters is not None
    assert request.filters[0].field == 'metric_name'


def test_historical_fred_request_rejects_invalid_date() -> None:
    with pytest.raises(ValidationError, match='valid strict YYYY-MM-DD'):
        HistoricalFREDSeriesMetricsRequest(
            start_date='2022-02-30',
            end_date='2022-03-01',
        )


def test_historical_fred_request_supports_shared_contract_fields() -> None:
    request = HistoricalFREDSeriesMetricsRequest(
        mode='aligned_1s',
        fields=['source_id', 'metric_name'],
        filters=[{'field': 'metric_name', 'op': 'eq', 'value': 'WALCL'}],
    )
    assert request.mode == 'aligned_1s'
    assert request.fields == ['source_id', 'metric_name']
    assert request.filters is not None
    assert request.filters[0].field == 'metric_name'


def test_historical_bitcoin_request_rejects_invalid_date() -> None:
    with pytest.raises(ValidationError, match='valid strict YYYY-MM-DD'):
        HistoricalBitcoinDatasetRequest(
            start_date='2022-02-30',
            end_date='2022-03-01',
        )


def test_historical_bitcoin_request_supports_shared_contract_fields() -> None:
    request = HistoricalBitcoinDatasetRequest(
        mode='aligned_1s',
        fields=['height', 'difficulty'],
        filters=[{'field': 'height', 'op': 'gte', 'value': 1000}],
    )
    assert request.mode == 'aligned_1s'
    assert request.fields == ['height', 'difficulty']
    assert request.filters is not None
    assert request.filters[0].field == 'height'


def test_historical_request_supports_shared_contract_fields() -> None:
    request = HistoricalSpotTradesRequest(
        mode='aligned_1s',
        fields=['trade_id', 'price'],
        filters=[{'field': 'price', 'op': 'gte', 'value': 1000}],
    )
    assert request.mode == 'aligned_1s'
    assert request.fields == ['trade_id', 'price']
    assert request.filters is not None
    assert request.filters[0].field == 'price'


def test_historical_routes_are_registered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    paths = {route.path for route in main_module.app.routes}
    assert '/v1/historical/binance/spot/trades' in paths
    assert '/v1/historical/binance/spot/klines' in paths
    assert '/v1/historical/okx/spot/trades' in paths
    assert '/v1/historical/okx/spot/klines' in paths
    assert '/v1/historical/bybit/spot/trades' in paths
    assert '/v1/historical/bybit/spot/klines' in paths
    assert '/v1/historical/etf/daily_metrics' in paths
    assert '/v1/historical/fred/series_metrics' in paths
    assert '/v1/historical/bitcoin/block_headers' in paths
    assert '/v1/historical/bitcoin/block_transactions' in paths
    assert '/v1/historical/bitcoin/mempool_state' in paths
    assert '/v1/historical/bitcoin/block_fee_totals' in paths
    assert '/v1/historical/bitcoin/block_subsidy_schedule' in paths
    assert '/v1/historical/bitcoin/network_hashrate_estimate' in paths
    assert '/v1/historical/bitcoin/circulating_supply' in paths
    assert '/v1/historical/binance/spot/agg_trades' not in paths
    assert '/v1/historical/binance/futures/trades' not in paths


@pytest.mark.parametrize(
    ('source_path', 'dataset_name'),
    [
        ('/v1/historical/binance/spot/trades', 'binance_spot_trades'),
        ('/v1/historical/okx/spot/trades', 'okx_spot_trades'),
        ('/v1/historical/bybit/spot/trades', 'bybit_spot_trades'),
    ],
)
def test_historical_trades_endpoint_returns_raw_envelope_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_path: str,
    dataset_name: str,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_trades_query(**_: Any) -> pl.DataFrame:
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

    monkeypatch.setattr(main_module, 'query_spot_trades_data', _fake_trades_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            source_path,
            headers={'X-API-Key': 'test-internal-key'},
            json={},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'native'
    assert payload['source'] == dataset_name
    assert payload['sources'] == [dataset_name]
    assert payload['row_count'] == 1
    assert isinstance(payload['schema'], list)
    assert isinstance(payload['warnings'], list)
    assert isinstance(payload['rows'], list)


def test_historical_etf_endpoint_returns_raw_envelope_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_etf_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': ['etf_ishares_ibit_daily'],
                'metric_name': ['btc_units'],
                'metric_value_float': [10.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', _fake_etf_query)
    monkeypatch.setattr(
        main_module,
        'build_etf_daily_quality_warnings',
        lambda *_: [],
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/etf/daily_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'native'
    assert payload['source'] == 'etf_daily_metrics'
    assert payload['sources'] == ['etf_daily_metrics']
    assert payload['row_count'] == 1
    assert isinstance(payload['schema'], list)
    assert isinstance(payload['warnings'], list)
    assert isinstance(payload['rows'], list)


def test_historical_fred_endpoint_returns_raw_envelope_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_fred_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': ['fred_walcl'],
                'metric_name': ['WALCL'],
                'metric_value_float': [100.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', _fake_fred_query)
    monkeypatch.setattr(
        main_module,
        'build_fred_publish_freshness_warnings',
        lambda *_: [],
    )
    monkeypatch.setattr(
        main_module,
        'emit_fred_warning_alerts_and_audit',
        lambda **_: None,
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/fred/series_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'native'
    assert payload['source'] == 'fred_series_metrics'
    assert payload['sources'] == ['fred_series_metrics']
    assert payload['row_count'] == 1
    assert isinstance(payload['schema'], list)
    assert isinstance(payload['warnings'], list)
    assert isinstance(payload['rows'], list)


@pytest.mark.parametrize(
    ('source_path', 'dataset_name'),
    [
        ('/v1/historical/bitcoin/block_headers', 'bitcoin_block_headers'),
        ('/v1/historical/bitcoin/block_transactions', 'bitcoin_block_transactions'),
        ('/v1/historical/bitcoin/mempool_state', 'bitcoin_mempool_state'),
        ('/v1/historical/bitcoin/block_fee_totals', 'bitcoin_block_fee_totals'),
        (
            '/v1/historical/bitcoin/block_subsidy_schedule',
            'bitcoin_block_subsidy_schedule',
        ),
        (
            '/v1/historical/bitcoin/network_hashrate_estimate',
            'bitcoin_network_hashrate_estimate',
        ),
        ('/v1/historical/bitcoin/circulating_supply', 'bitcoin_circulating_supply'),
    ],
)
def test_historical_bitcoin_endpoint_returns_raw_envelope_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_path: str,
    dataset_name: str,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_bitcoin_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'height': [1],
                'difficulty': [2.0],
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', _fake_bitcoin_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            source_path,
            headers={'X-API-Key': 'test-internal-key'},
            json={},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'native'
    assert payload['source'] == dataset_name
    assert payload['sources'] == [dataset_name]
    assert payload['row_count'] == 1
    assert isinstance(payload['schema'], list)
    assert isinstance(payload['warnings'], list)
    assert isinstance(payload['rows'], list)


@pytest.mark.parametrize(
    ('source_path', 'source_name', 'dataset_name'),
    [
        ('/v1/historical/binance/spot/trades', 'binance', 'binance_spot_trades'),
        ('/v1/historical/okx/spot/trades', 'okx', 'okx_spot_trades'),
        ('/v1/historical/bybit/spot/trades', 'bybit', 'bybit_spot_trades'),
    ],
)
def test_historical_trades_endpoint_supports_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_path: str,
    source_name: str,
    dataset_name: str,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_trades_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
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

    monkeypatch.setattr(main_module, 'query_spot_trades_data', _fake_trades_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            source_path,
            headers={'X-API-Key': 'test-internal-key'},
            json={'mode': 'aligned_1s'},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'aligned_1s'
    assert payload['source'] == dataset_name
    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['source'] == source_name


def test_historical_etf_endpoint_supports_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_etf_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'source_id': ['etf_ishares_ibit_daily'],
                'metric_name': ['btc_units'],
                'metric_value_float': [10.0],
                'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', _fake_etf_query)
    monkeypatch.setattr(
        main_module,
        'build_etf_daily_quality_warnings',
        lambda *_: [],
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/etf/daily_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'mode': 'aligned_1s'},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'aligned_1s'
    assert payload['source'] == 'etf_daily_metrics'
    assert captured_kwargs['mode'] == 'aligned_1s'


def test_historical_fred_endpoint_supports_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_fred_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'source_id': ['fred_walcl'],
                'metric_name': ['WALCL'],
                'metric_value_float': [100.0],
                'valid_from_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'valid_to_utc_exclusive': [datetime(2024, 1, 2, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', _fake_fred_query)
    monkeypatch.setattr(
        main_module,
        'build_fred_publish_freshness_warnings',
        lambda *_: [],
    )
    monkeypatch.setattr(
        main_module,
        'emit_fred_warning_alerts_and_audit',
        lambda **_: None,
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/fred/series_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'mode': 'aligned_1s'},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'aligned_1s'
    assert payload['source'] == 'fred_series_metrics'
    assert captured_kwargs['mode'] == 'aligned_1s'


@pytest.mark.parametrize(
    ('source_path', 'dataset_name'),
    [
        ('/v1/historical/bitcoin/block_headers', 'bitcoin_block_headers'),
        ('/v1/historical/bitcoin/block_transactions', 'bitcoin_block_transactions'),
        ('/v1/historical/bitcoin/mempool_state', 'bitcoin_mempool_state'),
        ('/v1/historical/bitcoin/block_fee_totals', 'bitcoin_block_fee_totals'),
        (
            '/v1/historical/bitcoin/block_subsidy_schedule',
            'bitcoin_block_subsidy_schedule',
        ),
        (
            '/v1/historical/bitcoin/network_hashrate_estimate',
            'bitcoin_network_hashrate_estimate',
        ),
        ('/v1/historical/bitcoin/circulating_supply', 'bitcoin_circulating_supply'),
    ],
)
def test_historical_bitcoin_endpoint_supports_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_path: str,
    dataset_name: str,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_bitcoin_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'aligned_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
                'records_in_bucket': [1],
            }
        )

    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', _fake_bitcoin_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            source_path,
            headers={'X-API-Key': 'test-internal-key'},
            json={'mode': 'aligned_1s'},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'aligned_1s'
    assert payload['source'] == dataset_name
    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['dataset'] == dataset_name


def test_historical_trades_endpoint_passes_fields_and_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_trades_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'trade_id': [1],
                'price': [42000.0],
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_spot_trades_data', _fake_trades_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/binance/spot/trades',
            headers={'X-API-Key': 'test-internal-key'},
            json={
                'fields': ['trade_id', 'price'],
                'filters': [{'field': 'price', 'op': 'gte', 'value': 41000}],
            },
        )

    assert response.status_code == 200
    assert captured_kwargs['fields'] == ['trade_id', 'price']
    assert captured_kwargs['filters'] == [
        {'field': 'price', 'op': 'gte', 'value': 41000}
    ]


def test_historical_etf_endpoint_passes_fields_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_etf_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'source_id': ['etf_ishares_ibit_daily'],
                'metric_name': ['btc_units'],
                'metric_value_float': [10.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', _fake_etf_query)
    monkeypatch.setattr(
        main_module,
        'build_etf_daily_quality_warnings',
        lambda *_: [],
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/etf/daily_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={
                'fields': ['source_id', 'metric_name'],
                'filters': [
                    {'field': 'metric_name', 'op': 'eq', 'value': 'btc_units'},
                ],
            },
        )

    assert response.status_code == 200
    assert captured_kwargs['fields'] == ['source_id', 'metric_name']
    assert captured_kwargs['filters'] == [
        {'field': 'metric_name', 'op': 'eq', 'value': 'btc_units'}
    ]


def test_historical_fred_endpoint_passes_fields_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_fred_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'source_id': ['fred_walcl'],
                'metric_name': ['WALCL'],
                'metric_value_float': [100.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', _fake_fred_query)
    monkeypatch.setattr(
        main_module,
        'build_fred_publish_freshness_warnings',
        lambda *_: [],
    )
    monkeypatch.setattr(
        main_module,
        'emit_fred_warning_alerts_and_audit',
        lambda **_: None,
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/fred/series_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={
                'fields': ['source_id', 'metric_name'],
                'filters': [
                    {'field': 'metric_name', 'op': 'eq', 'value': 'WALCL'},
                ],
            },
        )

    assert response.status_code == 200
    assert captured_kwargs['fields'] == ['source_id', 'metric_name']
    assert captured_kwargs['filters'] == [
        {'field': 'metric_name', 'op': 'eq', 'value': 'WALCL'}
    ]


def test_historical_bitcoin_endpoint_passes_fields_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_bitcoin_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'height': [100],
                'difficulty': [2.0],
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', _fake_bitcoin_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/bitcoin/block_headers',
            headers={'X-API-Key': 'test-internal-key'},
            json={
                'fields': ['height', 'difficulty'],
                'filters': [{'field': 'height', 'op': 'gte', 'value': 99}],
            },
        )

    assert response.status_code == 200
    assert captured_kwargs['dataset'] == 'bitcoin_block_headers'
    assert captured_kwargs['fields'] == ['height', 'difficulty']
    assert captured_kwargs['filters'] == [
        {'field': 'height', 'op': 'gte', 'value': 99}
    ]


def test_historical_endpoint_invalid_date_is_contract_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/okx/spot/trades',
            headers={'X-API-Key': 'test-internal-key'},
            json={'start_date': '2022-02-30', 'end_date': '2022-03-01'},
        )

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['code'] == 'HISTORICAL_CONTRACT_ERROR'


def test_historical_endpoint_strict_warning_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_trades_query(**_: Any) -> pl.DataFrame:
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

    monkeypatch.setattr(main_module, 'query_spot_trades_data', _fake_trades_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/bybit/spot/trades',
            headers={'X-API-Key': 'test-internal-key'},
            json={'n_latest_rows': 10, 'strict': True},
        )

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['code'] == 'STRICT_MODE_WARNING_FAILURE'


def test_historical_etf_endpoint_strict_warning_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_etf_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': ['etf_ishares_ibit_daily'],
                'metric_name': ['btc_units'],
                'metric_value_float': [10.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', _fake_etf_query)
    monkeypatch.setattr(
        main_module,
        'build_etf_daily_quality_warnings',
        lambda *_: [
            main_module.RawQueryWarning(
                code='ETF_DAILY_STALE_RECORDS',
                message='stale',
            )
        ],
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/etf/daily_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'strict': True},
        )

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['code'] == 'STRICT_MODE_WARNING_FAILURE'


def test_historical_fred_endpoint_strict_warning_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_fred_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': ['fred_walcl'],
                'metric_name': ['WALCL'],
                'metric_value_float': [100.0],
                'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', _fake_fred_query)
    monkeypatch.setattr(
        main_module,
        'build_fred_publish_freshness_warnings',
        lambda *_: [
            main_module.RawQueryWarning(
                code='FRED_SOURCE_PUBLISH_STALE',
                message='stale',
            )
        ],
    )
    monkeypatch.setattr(
        main_module,
        'emit_fred_warning_alerts_and_audit',
        lambda **_: None,
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/fred/series_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'strict': True},
        )

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['code'] == 'STRICT_MODE_WARNING_FAILURE'


def test_historical_bitcoin_endpoint_strict_warning_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _fake_bitcoin_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'height': [1],
                'difficulty': [2.0],
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
            }
        )

    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', _fake_bitcoin_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/bitcoin/block_headers',
            headers={'X-API-Key': 'test-internal-key'},
            json={'n_latest_rows': 10, 'strict': True},
        )

    assert response.status_code == 409
    detail = response.json()['detail']
    assert detail['code'] == 'STRICT_MODE_WARNING_FAILURE'


@pytest.mark.parametrize(
    ('source_path', 'source_name', 'dataset_name'),
    [
        ('/v1/historical/binance/spot/klines', 'binance', 'binance_spot_trades'),
        ('/v1/historical/okx/spot/klines', 'okx', 'okx_spot_trades'),
        ('/v1/historical/bybit/spot/klines', 'bybit', 'bybit_spot_trades'),
    ],
)
def test_historical_klines_endpoint_supports_aligned_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    source_path: str,
    source_name: str,
    dataset_name: str,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    captured_kwargs: dict[str, Any] = {}

    def _fake_klines_query(**kwargs: Any) -> pl.DataFrame:
        captured_kwargs.update(kwargs)
        return pl.DataFrame(
            {
                'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
                'open': [42000.0],
                'high': [42000.0],
                'low': [42000.0],
                'close': [42000.0],
                'volume': [0.1],
                'no_of_trades': [1],
                'liquidity_sum': [4200.0],
            }
        )

    monkeypatch.setattr(main_module, 'query_spot_klines_data', _fake_klines_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            source_path,
            headers={'X-API-Key': 'test-internal-key'},
            json={'mode': 'aligned_1s'},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload['mode'] == 'aligned_1s'
    assert payload['source'] == dataset_name
    assert captured_kwargs['mode'] == 'aligned_1s'
    assert captured_kwargs['source'] == source_name


def test_historical_endpoint_returns_404_for_empty_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _empty_klines_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'datetime': [],
                'open': [],
                'high': [],
                'low': [],
                'close': [],
                'mean': [],
                'std': [],
                'median': [],
                'iqr': [],
                'volume': [],
                'maker_ratio': [],
                'no_of_trades': [],
                'open_liquidity': [],
                'high_liquidity': [],
                'low_liquidity': [],
                'close_liquidity': [],
                'liquidity_sum': [],
                'maker_volume': [],
                'maker_liquidity': [],
            }
        )

    monkeypatch.setattr(main_module, 'query_spot_klines_data', _empty_klines_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/binance/spot/klines',
            headers={'X-API-Key': 'test-internal-key'},
            json={'start_date': '2024-01-01', 'end_date': '2024-01-01', 'kline_size': 60},
        )

    assert response.status_code == 404
    detail = response.json()['detail']
    assert detail['code'] == 'HISTORICAL_NO_DATA'


def test_historical_etf_endpoint_returns_404_for_empty_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _empty_etf_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': [],
                'metric_name': [],
                'metric_value_float': [],
                'observed_at_utc': [],
            }
        )

    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', _empty_etf_query)
    monkeypatch.setattr(
        main_module,
        'build_etf_daily_quality_warnings',
        lambda *_: [],
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/etf/daily_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'start_date': '2024-01-01', 'end_date': '2024-01-01'},
        )

    assert response.status_code == 404
    detail = response.json()['detail']
    assert detail['code'] == 'HISTORICAL_NO_DATA'


def test_historical_fred_endpoint_returns_404_for_empty_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _empty_fred_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'source_id': [],
                'metric_name': [],
                'metric_value_float': [],
                'observed_at_utc': [],
            }
        )

    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', _empty_fred_query)
    monkeypatch.setattr(
        main_module,
        'build_fred_publish_freshness_warnings',
        lambda *_: [],
    )
    monkeypatch.setattr(
        main_module,
        'emit_fred_warning_alerts_and_audit',
        lambda **_: None,
    )

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/fred/series_metrics',
            headers={'X-API-Key': 'test-internal-key'},
            json={'start_date': '2024-01-01', 'end_date': '2024-01-01'},
        )

    assert response.status_code == 404
    detail = response.json()['detail']
    assert detail['code'] == 'HISTORICAL_NO_DATA'


def test_historical_bitcoin_endpoint_returns_404_for_empty_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)

    def _empty_bitcoin_query(**_: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                'height': [],
                'difficulty': [],
                'datetime': [],
            }
        )

    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', _empty_bitcoin_query)

    with TestClient(main_module.app) as client:
        response = client.post(
            '/v1/historical/bitcoin/block_headers',
            headers={'X-API-Key': 'test-internal-key'},
            json={'start_date': '2024-01-01', 'end_date': '2024-01-01'},
        )

    assert response.status_code == 404
    detail = response.json()['detail']
    assert detail['code'] == 'HISTORICAL_NO_DATA'
