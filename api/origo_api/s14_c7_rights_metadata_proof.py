from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from api.origo_api.rights import resolve_export_rights, resolve_query_rights
from api.origo_api.schemas import RawExportStatusResponse, RawQueryResponse


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def run_s14_c7_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s14-c7-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        legal_signoff_path = temp_path / 'legal-signoff.md'
        legal_signoff_path.write_text('# legal\n', encoding='utf-8')
        rights_matrix_path = temp_path / 'rights-matrix.json'
        _write_json(
            rights_matrix_path,
            {
                'version': 's14-c7-proof',
                'sources': {
                    'binance': {
                        'rights_state': 'Hosted Allowed',
                        'rights_provisional': True,
                        'datasets': ['binance_spot_trades'],
                        'legal_signoff_artifact': str(legal_signoff_path),
                    }
                },
            },
        )

        original_matrix = os.environ.get('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
        os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_matrix_path)
        try:
            query_rights = resolve_query_rights(dataset='binance_spot_trades', auth_token=None)
            export_rights = resolve_export_rights(dataset='binance_spot_trades', auth_token=None)
        finally:
            if original_matrix is None:
                os.environ.pop('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', None)
            else:
                os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = original_matrix

    query_rights_state, query_rights_provisional = query_rights.response_metadata()
    if query_rights_state != 'Hosted Allowed':
        raise RuntimeError('S14-C7 proof expected query rights_state=Hosted Allowed')
    if query_rights_provisional is not True:
        raise RuntimeError('S14-C7 proof expected query rights_provisional=true')
    if export_rights.rights_provisional is not True:
        raise RuntimeError('S14-C7 proof expected export rights_provisional=true')

    query_response = RawQueryResponse.model_validate(
        {
            'mode': 'native',
            'source': 'binance_spot_trades',
            'sources': ['binance_spot_trades'],
            'row_count': 1,
            'schema': [{'name': 'trade_id', 'dtype': 'UInt64'}],
            'rights_state': query_rights_state,
            'rights_provisional': query_rights_provisional,
            'rows': [{'trade_id': '1'}],
        }
    )
    now = datetime.now(UTC)
    export_status_response = RawExportStatusResponse.model_validate(
        {
            'export_id': 'proof-export-id',
            'status': 'queued',
            'mode': 'native',
            'format': 'parquet',
            'dataset': 'binance_spot_trades',
            'source': export_rights.source,
            'rights_state': export_rights.rights_state,
            'rights_provisional': export_rights.rights_provisional,
            'submitted_at': now,
            'updated_at': now,
            'artifact': None,
            'error_code': None,
            'error_message': None,
        }
    )

    return {
        'proof_scope': 'Slice 14 S14-C7 provisional rights metadata contract plumbing',
        'query_response_metadata': {
            'rights_state': query_response.rights_state,
            'rights_provisional': query_response.rights_provisional,
        },
        'export_status_metadata': {
            'source': export_status_response.source,
            'rights_state': export_status_response.rights_state,
            'rights_provisional': export_status_response.rights_provisional,
        },
    }


def main() -> None:
    payload = run_s14_c7_proof()
    repo_root = Path.cwd().resolve()
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c7-rights-metadata.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
