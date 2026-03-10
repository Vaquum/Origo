from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .rights import (
    RightsGateError,
    resolve_export_rights,
    resolve_query_rights,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def _build_matrix(*, legal_artifact_path: str | None) -> dict[str, Any]:
    source_payload: dict[str, Any] = {
        'rights_state': 'Hosted Allowed',
        'rights_provisional': False,
        'datasets': ['spot_trades', 'spot_agg_trades', 'futures_trades'],
    }
    if legal_artifact_path is not None:
        source_payload['legal_signoff_artifact'] = legal_artifact_path
    return {
        'version': 's4-g1-proof',
        'sources': {
            'binance': source_payload,
        },
    }


def run_s4_g1_legal_signoff_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s4-g1-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        matrix_path = temp_path / 'rights-matrix.json'
        legal_artifact = temp_path / 'legal-signoff.md'
        legal_artifact.write_text('# legal signoff\n', encoding='utf-8')

        original_matrix = os.environ.get('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')

        missing_payload = _build_matrix(
            legal_artifact_path='contracts/legal/missing-signoff.md'
        )
        _write_json(matrix_path, missing_payload)
        os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(matrix_path)

        query_missing_code = None
        try:
            resolve_query_rights(dataset='spot_trades', auth_token=None)
        except RightsGateError as exc:
            query_missing_code = exc.code

        export_missing_code = None
        try:
            resolve_export_rights(dataset='spot_trades', auth_token=None)
        except RightsGateError as exc:
            export_missing_code = exc.code

        ok_payload = _build_matrix(legal_artifact_path=str(legal_artifact))
        _write_json(matrix_path, ok_payload)
        os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(matrix_path)

        query_ok = True
        try:
            resolve_query_rights(dataset='spot_trades', auth_token=None)
        except RightsGateError:
            query_ok = False

        export_ok = True
        try:
            resolve_export_rights(dataset='spot_trades', auth_token=None)
        except RightsGateError:
            export_ok = False

        if original_matrix is None:
            os.environ.pop('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', None)
        else:
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = original_matrix

    if query_missing_code != 'QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING':
        raise RuntimeError(
            'Query legal-signoff proof failed: expected '
            'QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING'
        )
    if export_missing_code != 'EXPORT_RIGHTS_LEGAL_SIGNOFF_MISSING':
        raise RuntimeError(
            'Export legal-signoff proof failed: expected '
            'EXPORT_RIGHTS_LEGAL_SIGNOFF_MISSING'
        )
    if not query_ok:
        raise RuntimeError('Query legal-signoff proof failed: valid signoff should pass')
    if not export_ok:
        raise RuntimeError('Export legal-signoff proof failed: valid signoff should pass')

    return {
        'proof_scope': 'Slice 4 S4-G1 legal sign-off gating for external serving/export',
        'query_missing_signoff_error_code': query_missing_code,
        'query_valid_signoff_passed': query_ok,
        'export_missing_signoff_error_code': export_missing_code,
        'export_valid_signoff_passed': export_ok,
    }


def main() -> None:
    payload = run_s4_g1_legal_signoff_proof()
    output_path = Path('spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g1.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
