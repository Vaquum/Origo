from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from api.origo_api.rights import resolve_export_rights
from api.origo_api.schemas import (
    RawExportRequest,
    RawExportStatusResponse,
    RawQueryRequest,
    RawQueryResponse,
)


def test_raw_query_requires_exactly_one_window_selector() -> None:
    with pytest.raises(ValidationError, match='Exactly one window mode must be provided'):
        RawQueryRequest(
            mode='native',
            sources=['spot_trades'],
            n_rows=10,
            n_random=5,
        )


def test_raw_query_accepts_multi_source_contract_shape() -> None:
    request = RawQueryRequest(
        mode='native',
        sources=['spot_trades', 'spot_agg_trades'],
        n_rows=10,
    )
    assert request.sources == ['spot_trades', 'spot_agg_trades']


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


def test_raw_query_request_accepts_bitcoin_source() -> None:
    request = RawQueryRequest(
        mode='native',
        sources=['bitcoin_block_headers'],
        n_rows=10,
    )
    assert request.sources == ['bitcoin_block_headers']


def test_raw_query_view_fields_must_be_set_together() -> None:
    with pytest.raises(
        ValidationError,
        match='view_id and view_version must both be set or both be omitted',
    ):
        RawQueryRequest(
            mode='native',
            sources=['spot_trades'],
            n_rows=10,
            view_id='aligned_1s_raw',
        )


def test_raw_query_view_fields_accept_target_shape() -> None:
    request = RawQueryRequest(
        mode='native',
        sources=['spot_trades', 'spot_agg_trades'],
        n_rows=10,
        view_id='aligned_1s_raw',
        view_version=1,
    )
    assert request.view_id == 'aligned_1s_raw'
    assert request.view_version == 1


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


def test_raw_export_request_accepts_bitcoin_dataset() -> None:
    request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='bitcoin_block_fee_totals',
        time_range=('2024-01-01T00:00:00Z', '2024-02-01T00:00:00Z'),
    )
    assert request.dataset == 'bitcoin_block_fee_totals'


def test_raw_export_view_fields_must_be_set_together() -> None:
    with pytest.raises(
        ValidationError,
        match='view_id and view_version must both be set or both be omitted',
    ):
        RawExportRequest(
            mode='native',
            format='parquet',
            dataset='spot_trades',
            n_rows=10,
            view_id='aligned_1s_raw',
        )


def test_raw_export_view_fields_accept_target_shape() -> None:
    request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='spot_trades',
        n_rows=10,
        view_id='aligned_1s_raw',
        view_version=1,
    )
    assert request.view_id == 'aligned_1s_raw'
    assert request.view_version == 1


def test_raw_query_response_requires_rights_metadata() -> None:
    response = RawQueryResponse.model_validate(
        {
            'mode': 'native',
            'source': 'spot_trades',
            'sources': ['spot_trades'],
            'row_count': 1,
            'schema': [{'name': 'event_time', 'dtype': 'DateTime64(3)'}],
            'rights_state': 'Hosted Allowed',
            'rights_provisional': False,
            'rows': [{'event_time': '2024-01-01T00:00:00.000Z'}],
        }
    )
    assert response.rights_state == 'Hosted Allowed'
    assert response.rights_provisional is False


def test_raw_export_status_requires_rights_metadata() -> None:
    now = datetime.now(UTC)
    response = RawExportStatusResponse.model_validate(
        {
            'export_id': 'export-1',
            'status': 'queued',
            'mode': 'native',
            'format': 'parquet',
            'dataset': 'spot_trades',
            'source': 'binance',
            'rights_state': 'Hosted Allowed',
            'rights_provisional': False,
            'submitted_at': now,
            'updated_at': now,
            'artifact': None,
            'error_code': None,
            'error_message': None,
        }
    )
    assert response.rights_state == 'Hosted Allowed'
    assert response.rights_provisional is False


def test_export_rights_supports_fred_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matrix_path = tmp_path / 'rights.json'
    matrix_payload = {
        'version': 'test',
        'sources': {
            'fred': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
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
    assert decision.rights_provisional is False


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
                'rights_provisional': False,
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
    assert decision.rights_provisional is False


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
                'rights_provisional': False,
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
    assert decision.rights_provisional is False


def test_export_rights_supports_bitcoin_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    legal_artifact = tmp_path / 'bitcoin-core-legal-signoff.md'
    legal_artifact.write_text('# legal', encoding='utf-8')
    matrix_path = tmp_path / 'rights.json'
    matrix_payload = {
        'version': 'test',
        'sources': {
            'bitcoin_core': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['bitcoin_block_fee_totals'],
                'legal_signoff_artifact': str(legal_artifact),
            }
        },
    }
    matrix_path.write_text(json.dumps(matrix_payload), encoding='utf-8')
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))

    decision = resolve_export_rights(
        dataset='bitcoin_block_fee_totals', auth_token=None
    )
    assert decision.source == 'bitcoin_core'
    assert decision.dataset == 'bitcoin_block_fee_totals'
    assert decision.rights_state == 'Hosted Allowed'
    assert decision.rights_provisional is False


def test_export_rights_requires_provisional_flag_in_matrix(
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

    with pytest.raises(RuntimeError, match='rights_provisional'):
        resolve_export_rights(dataset='okx_spot_trades', auth_token=None)
