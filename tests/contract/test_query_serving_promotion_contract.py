from __future__ import annotations

import json
from pathlib import Path

import pytest

from api.origo_api.rights import RightsGateError, resolve_query_rights


def _write_rights_matrix(
    *,
    tmp_path: Path,
    dataset: str,
    source_name: str,
) -> Path:
    legal_artifact = tmp_path / f'{source_name}-legal.md'
    legal_artifact.write_text('# legal\n', encoding='utf-8')
    matrix_path = tmp_path / 'rights.json'
    matrix_path.write_text(
        json.dumps(
            {
                'version': 'test',
                'sources': {
                    source_name: {
                        'rights_state': 'Hosted Allowed',
                        'rights_provisional': False,
                        'datasets': [dataset],
                        'legal_signoff_artifact': str(legal_artifact),
                    }
                },
            }
        ),
        encoding='utf-8',
    )
    return matrix_path


def test_query_rights_promoted_requires_projection_coverage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    matrix_path = _write_rights_matrix(
        tmp_path=tmp_path,
        dataset='etf_daily_metrics',
        source_name='etf',
    )
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))
    monkeypatch.setenv('ORIGO_ETF_QUERY_SERVING_STATE', 'promoted')

    def _raise_incomplete(*, dataset: str) -> None:
        raise RightsGateError(
            code='QUERY_SERVING_PROMOTION_INCOMPLETE',
            message=f'promotion incomplete for {dataset}',
        )

    monkeypatch.setattr(
        'api.origo_api.rights._assert_promoted_serving_projection_coverage_or_raise',
        _raise_incomplete,
    )

    with pytest.raises(RightsGateError, match='promotion incomplete') as exc_info:
        resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
    assert exc_info.value.code == 'QUERY_SERVING_PROMOTION_INCOMPLETE'


def test_query_rights_promoted_allows_serving_when_projection_coverage_matches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    matrix_path = _write_rights_matrix(
        tmp_path=tmp_path,
        dataset='fred_series_metrics',
        source_name='fred',
    )
    monkeypatch.setenv('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', str(matrix_path))
    monkeypatch.setenv('ORIGO_FRED_QUERY_SERVING_STATE', 'promoted')
    monkeypatch.setattr(
        'api.origo_api.rights._assert_promoted_serving_projection_coverage_or_raise',
        lambda *, dataset: None,
    )

    decision = resolve_query_rights(dataset='fred_series_metrics', auth_token=None)
    assert decision.dataset == 'fred_series_metrics'
    assert decision.serving_state == 'promoted'
    assert len(decision.sources) == 1
    assert decision.sources[0].source == 'fred'
