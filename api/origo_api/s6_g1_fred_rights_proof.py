from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .rights import RightsGateError, resolve_query_rights


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def _build_matrix(*, legal_artifact_path: str | None, include_fred: bool) -> dict[str, Any]:
    sources: dict[str, Any] = {
        'binance': {
            'rights_state': 'Hosted Allowed',
            'rights_provisional': False,
            'datasets': ['spot_trades', 'spot_agg_trades', 'futures_trades'],
            'legal_signoff_artifact': 'contracts/legal/binance-hosted-allowed.md',
        }
    }
    if include_fred:
        fred_payload: dict[str, Any] = {
            'rights_state': 'Hosted Allowed',
            'rights_provisional': False,
            'datasets': ['fred_series_metrics'],
        }
        if legal_artifact_path is not None:
            fred_payload['legal_signoff_artifact'] = legal_artifact_path
        sources['fred'] = fred_payload

    return {'version': 's6-g1-proof', 'sources': sources}


def run_s6_g1_fred_rights_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s6-g1-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        matrix_path = temp_path / 'rights-matrix.json'
        legal_artifact = temp_path / 'fred-legal-signoff.md'
        legal_artifact.write_text('# fred legal signoff\n', encoding='utf-8')

        original_matrix = os.environ.get('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
        os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(matrix_path)

        try:
            missing_legal_payload = _build_matrix(
                legal_artifact_path='contracts/legal/missing-fred-signoff.md',
                include_fred=True,
            )
            _write_json(matrix_path, missing_legal_payload)

            query_missing_legal_code: str | None = None
            try:
                resolve_query_rights(dataset='fred_series_metrics', auth_token=None)
            except RightsGateError as exc:
                query_missing_legal_code = exc.code

            valid_payload = _build_matrix(
                legal_artifact_path=str(legal_artifact),
                include_fred=True,
            )
            _write_json(matrix_path, valid_payload)

            query_ok = True
            try:
                decision = resolve_query_rights(
                    dataset='fred_series_metrics', auth_token=None
                )
                if len(decision.sources) != 1 or decision.sources[0].source != 'fred':
                    raise RuntimeError('S6-G1 proof expected exactly one source decision for fred')
            except RightsGateError:
                query_ok = False

            missing_state_payload = _build_matrix(
                legal_artifact_path=str(legal_artifact),
                include_fred=False,
            )
            _write_json(matrix_path, missing_state_payload)

            query_missing_state_code: str | None = None
            try:
                resolve_query_rights(dataset='fred_series_metrics', auth_token=None)
            except RightsGateError as exc:
                query_missing_state_code = exc.code
        finally:
            if original_matrix is None:
                os.environ.pop('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', None)
            else:
                os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = original_matrix

    if query_missing_legal_code != 'QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING':
        raise RuntimeError(
            'S6-G1 proof expected QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING for missing legal signoff'
        )
    if not query_ok:
        raise RuntimeError('S6-G1 proof expected valid fred legal signoff to pass query rights')
    if query_missing_state_code != 'QUERY_RIGHTS_MISSING_STATE':
        raise RuntimeError('S6-G1 proof expected QUERY_RIGHTS_MISSING_STATE when fred classification is missing')

    return {
        'proof_scope': 'Slice 6 S6-G1 rights classification and legal gating for FRED query serving',
        'query_missing_legal_signoff_error_code': query_missing_legal_code,
        'query_valid_legal_signoff_passed': query_ok,
        'query_missing_state_error_code': query_missing_state_code,
    }


def main() -> None:
    payload = run_s6_g1_fred_rights_proof()
    output_path = Path('spec/slices/slice-6-fred-integration/guardrails-proof-s6-g1-fred-rights.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
