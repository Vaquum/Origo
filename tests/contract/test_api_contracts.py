from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from api.origo_api.rights import resolve_export_rights
from api.origo_api.schemas import RawExportRequest, RawQueryRequest


def test_raw_query_requires_exactly_one_window_selector() -> None:
    with pytest.raises(ValidationError, match='Exactly one window mode must be provided'):
        RawQueryRequest(
            mode='native',
            sources=['spot_trades'],
            n_rows=10,
            n_random=5,
        )


def test_raw_query_requires_single_source() -> None:
    with pytest.raises(
        ValidationError,
        match='Exactly one source is currently supported per request',
    ):
        RawQueryRequest(
            mode='native',
            sources=['spot_trades', 'spot_agg_trades'],
            n_rows=10,
        )


def test_raw_query_request_accepts_okx_source() -> None:
    request = RawQueryRequest(
        mode='native',
        sources=['okx_spot_trades'],
        n_rows=10,
    )
    assert request.sources == ['okx_spot_trades']


def test_raw_query_request_accepts_bybit_source() -> None:
    request = RawQueryRequest(
        mode='native',
        sources=['bybit_spot_trades'],
        n_rows=10,
    )
    assert request.sources == ['bybit_spot_trades']


def test_raw_export_request_accepts_fred_dataset() -> None:
    request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='fred_series_metrics',
        time_range=('2024-01-01T00:00:00Z', '2024-02-01T00:00:00Z'),
    )
    assert request.dataset == 'fred_series_metrics'


def test_raw_export_request_accepts_okx_dataset() -> None:
    request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='okx_spot_trades',
        time_range=('2024-01-01T00:00:00Z', '2024-02-01T00:00:00Z'),
    )
    assert request.dataset == 'okx_spot_trades'


def test_raw_export_request_accepts_bybit_dataset() -> None:
    request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='bybit_spot_trades',
        time_range=('2024-01-01T00:00:00Z', '2024-02-01T00:00:00Z'),
    )
    assert request.dataset == 'bybit_spot_trades'


def test_export_rights_supports_fred_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matrix_path = tmp_path / 'rights.json'
    matrix_payload = {
        'version': 'test',
        'sources': {
            'fred': {
                'rights_state': 'Hosted Allowed',
                'datasets': ['fred_series_metrics'],
                'legal_signoff_artifact': 'contracts/legal/fred-hosted-allowed.md',
            }
        },
    }
    matrix_path.write_text(json.dumps(matrix_payload), encoding='utf-8')
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))

    decision = resolve_export_rights(dataset='fred_series_metrics', auth_token=None)
    assert decision.source == 'fred'
    assert decision.dataset == 'fred_series_metrics'
    assert decision.rights_state == 'Hosted Allowed'


def test_export_rights_supports_okx_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    legal_artifact = tmp_path / 'okx-legal-signoff.md'
    legal_artifact.write_text('# legal', encoding='utf-8')
    matrix_path = tmp_path / 'rights.json'
    matrix_payload = {
        'version': 'test',
        'sources': {
            'okx': {
                'rights_state': 'Hosted Allowed',
                'datasets': ['okx_spot_trades'],
                'legal_signoff_artifact': str(legal_artifact),
            }
        },
    }
    matrix_path.write_text(json.dumps(matrix_payload), encoding='utf-8')
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))

    decision = resolve_export_rights(dataset='okx_spot_trades', auth_token=None)
    assert decision.source == 'okx'
    assert decision.dataset == 'okx_spot_trades'
    assert decision.rights_state == 'Hosted Allowed'


def test_export_rights_supports_bybit_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    legal_artifact = tmp_path / 'bybit-legal-signoff.md'
    legal_artifact.write_text('# legal', encoding='utf-8')
    matrix_path = tmp_path / 'rights.json'
    matrix_payload = {
        'version': 'test',
        'sources': {
            'bybit': {
                'rights_state': 'Hosted Allowed',
                'datasets': ['bybit_spot_trades'],
                'legal_signoff_artifact': str(legal_artifact),
            }
        },
    }
    matrix_path.write_text(json.dumps(matrix_payload), encoding='utf-8')
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))

    decision = resolve_export_rights(dataset='bybit_spot_trades', auth_token=None)
    assert decision.source == 'bybit'
    assert decision.dataset == 'bybit_spot_trades'
    assert decision.rights_state == 'Hosted Allowed'
