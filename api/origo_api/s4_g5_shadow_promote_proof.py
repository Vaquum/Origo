from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .rights import RightsGateError, resolve_query_rights


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def run_s4_g5_shadow_promote_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s4-g5-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        matrix_path = temp_path / 'rights-matrix.json'
        legal_artifact_path = temp_path / 'legal-signoff.md'
        legal_artifact_path.write_text('# legal signoff\n', encoding='utf-8')

        _write_json(
            matrix_path,
            {
                'version': 's4-g5-proof',
                'sources': {
                    'etf_official': {
                        'rights_state': 'Hosted Allowed',
                        'rights_provisional': False,
                        'datasets': ['etf_daily_metrics'],
                        'legal_signoff_artifact': str(legal_artifact_path),
                    }
                },
            },
        )

        original_env: dict[str, str | None] = {
            'ORIGO_SOURCE_RIGHTS_MATRIX_PATH': os.environ.get(
                'ORIGO_SOURCE_RIGHTS_MATRIX_PATH'
            ),
            'ORIGO_ETF_QUERY_SERVING_STATE': os.environ.get(
                'ORIGO_ETF_QUERY_SERVING_STATE'
            ),
        }

        missing_state_error: str | None = None
        shadow_mode_error_code: str | None = None
        promoted_state_passed = False
        invalid_state_error: str | None = None

        try:
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(matrix_path)

            os.environ.pop('ORIGO_ETF_QUERY_SERVING_STATE', None)
            try:
                resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
            except RuntimeError as exc:
                missing_state_error = str(exc)

            os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'shadow'
            try:
                resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
            except RightsGateError as exc:
                shadow_mode_error_code = exc.code

            os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'promoted'
            promoted_decision = resolve_query_rights(
                dataset='etf_daily_metrics',
                auth_token=None,
            )
            promoted_state_passed = (
                promoted_decision.dataset == 'etf_daily_metrics'
                and promoted_decision.serving_state == 'promoted'
                and len(promoted_decision.sources) == 1
                and promoted_decision.sources[0].source == 'etf_official'
            )

            os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'invalid'
            try:
                resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
            except RuntimeError as exc:
                invalid_state_error = str(exc)
        finally:
            for key, value in original_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    if missing_state_error != 'ORIGO_ETF_QUERY_SERVING_STATE must be set and non-empty':
        raise RuntimeError(
            'S4-G5 proof expected fail-loud missing env error for '
            'ORIGO_ETF_QUERY_SERVING_STATE'
        )
    if shadow_mode_error_code != 'QUERY_SERVING_SHADOW_MODE':
        raise RuntimeError(
            'S4-G5 proof expected QUERY_SERVING_SHADOW_MODE when serving state is shadow'
        )
    if not promoted_state_passed:
        raise RuntimeError(
            'S4-G5 proof expected promoted serving state to allow ETF query rights'
        )
    if invalid_state_error is None or 'must be one of' not in invalid_state_error:
        raise RuntimeError(
            'S4-G5 proof expected fail-loud validation error for invalid '
            'ORIGO_ETF_QUERY_SERVING_STATE value'
        )

    return {
        'proof_scope': 'Slice 4 S4-G5 shadow-then-promote serving gate',
        'missing_state_error': missing_state_error,
        'shadow_mode_error_code': shadow_mode_error_code,
        'promoted_state_passed': promoted_state_passed,
        'invalid_state_error': invalid_state_error,
    }


def main() -> None:
    payload = run_s4_g5_shadow_promote_proof()
    output_path = Path('spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g5.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
