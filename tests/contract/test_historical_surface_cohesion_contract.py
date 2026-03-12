from __future__ import annotations

import importlib
import inspect
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import get_args

import polars as pl
import pytest
from fastapi.testclient import TestClient

from api.origo_api.schemas import RawQuerySource
from origo.data.historical_data import HistoricalData

_DEFERRED_HISTORICAL_DATASETS: frozenset[str] = frozenset(
    {'spot_agg_trades', 'futures_trades'}
)
_HISTORICAL_DATASET_TO_HTTP_ROUTE: dict[str, str] = {
    'spot_trades': '/v1/historical/binance/spot/trades',
    'okx_spot_trades': '/v1/historical/okx/spot/trades',
    'bybit_spot_trades': '/v1/historical/bybit/spot/trades',
    'etf_daily_metrics': '/v1/historical/etf/daily_metrics',
    'fred_series_metrics': '/v1/historical/fred/series_metrics',
    'bitcoin_block_headers': '/v1/historical/bitcoin/block_headers',
    'bitcoin_block_transactions': '/v1/historical/bitcoin/block_transactions',
    'bitcoin_mempool_state': '/v1/historical/bitcoin/mempool_state',
    'bitcoin_block_fee_totals': '/v1/historical/bitcoin/block_fee_totals',
    'bitcoin_block_subsidy_schedule': '/v1/historical/bitcoin/block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate': '/v1/historical/bitcoin/network_hashrate_estimate',
    'bitcoin_circulating_supply': '/v1/historical/bitcoin/circulating_supply',
}
_HISTORICAL_DATASET_TO_PYTHON_METHOD: dict[str, str] = {
    'spot_trades': 'get_binance_spot_trades',
    'okx_spot_trades': 'get_okx_spot_trades',
    'bybit_spot_trades': 'get_bybit_spot_trades',
    'etf_daily_metrics': 'get_etf_daily_metrics',
    'fred_series_metrics': 'get_fred_series_metrics',
    'bitcoin_block_headers': 'get_bitcoin_block_headers',
    'bitcoin_block_transactions': 'get_bitcoin_block_transactions',
    'bitcoin_mempool_state': 'get_bitcoin_mempool_state',
    'bitcoin_block_fee_totals': 'get_bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule': 'get_bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate': 'get_bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply': 'get_bitcoin_circulating_supply',
}
_HISTORICAL_KLINE_ROUTES: tuple[str, ...] = (
    '/v1/historical/binance/spot/klines',
    '/v1/historical/okx/spot/klines',
    '/v1/historical/bybit/spot/klines',
)


def _load_main_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ModuleType:
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
        'version': 'historical-s31-contract-test',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['spot_trades'],
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


def _fake_trades_frame() -> pl.DataFrame:
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


def _fake_klines_frame() -> pl.DataFrame:
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


def _fake_etf_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'source_id': ['etf_ishares_ibit_daily'],
            'metric_name': ['btc_units'],
            'metric_value_float': [10.0],
            'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _fake_fred_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'source_id': ['fred_walcl'],
            'metric_name': ['WALCL'],
            'metric_value_float': [100.0],
            'observed_at_utc': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def _fake_bitcoin_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'height': [100],
            'difficulty': [2.0],
            'datetime': [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )


def test_historical_surface_dataset_matrix_is_complete() -> None:
    query_datasets = set(get_args(RawQuerySource))
    in_scope = query_datasets - _DEFERRED_HISTORICAL_DATASETS

    assert set(_HISTORICAL_DATASET_TO_HTTP_ROUTE) == in_scope
    assert set(_HISTORICAL_DATASET_TO_PYTHON_METHOD) == in_scope
    assert query_datasets.intersection(_DEFERRED_HISTORICAL_DATASETS) == {
        'spot_agg_trades',
        'futures_trades',
    }


def test_historical_surface_routes_and_methods_are_registered(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    paths = {route.path for route in main_module.app.routes}

    for route in _HISTORICAL_DATASET_TO_HTTP_ROUTE.values():
        assert route in paths
    for route in _HISTORICAL_KLINE_ROUTES:
        assert route in paths

    for method_name in _HISTORICAL_DATASET_TO_PYTHON_METHOD.values():
        assert hasattr(HistoricalData, method_name)


def test_historical_surface_method_signatures_are_cohesive() -> None:
    common_params = [
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
    dataset_methods = [
        'get_etf_daily_metrics',
        'get_fred_series_metrics',
        'get_bitcoin_block_headers',
        'get_bitcoin_block_transactions',
        'get_bitcoin_mempool_state',
        'get_bitcoin_block_fee_totals',
        'get_bitcoin_block_subsidy_schedule',
        'get_bitcoin_network_hashrate_estimate',
        'get_bitcoin_circulating_supply',
    ]
    for method_name in dataset_methods:
        signature = inspect.signature(getattr(HistoricalData, method_name))
        assert list(signature.parameters) == common_params


def test_historical_surface_docs_cover_routes_and_datasets() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    historical_contract_doc = (
        repo_root / 'docs' / 'historical-contract-reference.md'
    ).read_text(encoding='utf-8')
    taxonomy_doc = (repo_root / 'docs' / 'data-taxonomy.md').read_text(
        encoding='utf-8'
    )
    historical_reference_doc = (repo_root / 'docs' / 'historical-bitcoin-reference.md').read_text(
        encoding='utf-8'
    )

    for route in _HISTORICAL_DATASET_TO_HTTP_ROUTE.values():
        assert route in historical_contract_doc
        assert route in taxonomy_doc

    for dataset in _HISTORICAL_DATASET_TO_HTTP_ROUTE:
        assert dataset in taxonomy_doc

    assert 'spot_agg_trades' in taxonomy_doc
    assert 'futures_trades' in taxonomy_doc
    assert 'explicitly excludes `spot_agg_trades` and `futures_trades`' in taxonomy_doc
    assert '/v1/historical/bitcoin/block_headers' in historical_reference_doc


def test_historical_surface_http_mode_matrix_support(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    main_module = _load_main_module(monkeypatch, tmp_path)
    monkeypatch.setattr(main_module, 'query_spot_trades_data', lambda **_: _fake_trades_frame())
    monkeypatch.setattr(main_module, 'query_spot_klines_data', lambda **_: _fake_klines_frame())
    monkeypatch.setattr(main_module, 'query_etf_daily_metrics_data', lambda **_: _fake_etf_frame())
    monkeypatch.setattr(main_module, 'build_etf_daily_quality_warnings', lambda *_: [])
    monkeypatch.setattr(main_module, 'query_fred_series_metrics_data', lambda **_: _fake_fred_frame())
    monkeypatch.setattr(main_module, 'build_fred_publish_freshness_warnings', lambda *_: [])
    monkeypatch.setattr(main_module, 'emit_fred_warning_alerts_and_audit', lambda **_: None)
    monkeypatch.setattr(main_module, 'query_bitcoin_dataset_data', lambda **_: _fake_bitcoin_frame())

    routes = list(_HISTORICAL_DATASET_TO_HTTP_ROUTE.values()) + list(
        _HISTORICAL_KLINE_ROUTES
    )

    with TestClient(main_module.app) as client:
        for route in routes:
            native_response = client.post(
                route,
                headers={'X-API-Key': 'test-internal-key'},
                json={},
            )
            assert native_response.status_code == 200
            aligned_response = client.post(
                route,
                headers={'X-API-Key': 'test-internal-key'},
                json={'mode': 'aligned_1s'},
            )
            assert aligned_response.status_code == 200
