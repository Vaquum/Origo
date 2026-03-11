from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from api.origo_api.export_tags import read_export_tags
from api.origo_api.schemas import RawQueryResponse


def run_s14_g5_proof() -> dict[str, Any]:
    query_missing_error: str | None = None
    try:
        RawQueryResponse.model_validate(
            {
                'mode': 'native',
                'source': 'spot_trades',
                'sources': ['spot_trades'],
                'row_count': 1,
                'schema': [{'name': 'datetime', 'dtype': 'DateTime64(3)'}],
                'rows': [{'datetime': '2024-01-01T00:00:00.000Z'}],
            }
        )
    except ValidationError as exc:
        query_missing_error = str(exc)
    if query_missing_error is None:
        raise RuntimeError(
            'S14-G5 proof expected query response contract to fail when rights metadata is missing'
        )

    export_missing_error: str | None = None
    try:
        read_export_tags(
            {
                'origo.export.mode': 'native',
                'origo.export.format': 'parquet',
                'origo.export.dataset': 'spot_trades',
                'origo.export.source': 'binance',
                'origo.export.rights_state': 'Hosted Allowed',
            }
        )
    except RuntimeError as exc:
        export_missing_error = str(exc)
    if export_missing_error is None:
        raise RuntimeError(
            'S14-G5 proof expected export tag parsing to fail when rights_provisional is missing'
        )

    (
        mode,
        export_format,
        dataset,
        source,
        rights_state,
        rights_provisional,
        view_id,
        view_version,
    ) = read_export_tags(
        {
            'origo.export.mode': 'native',
            'origo.export.format': 'parquet',
            'origo.export.dataset': 'spot_trades',
            'origo.export.source': 'binance',
            'origo.export.rights_state': 'Hosted Allowed',
            'origo.export.rights_provisional': 'false',
        }
    )
    if (
        mode != 'native'
        or export_format != 'parquet'
        or dataset != 'spot_trades'
        or source != 'binance'
        or rights_state != 'Hosted Allowed'
        or rights_provisional is not False
        or view_id is not None
        or view_version is not None
    ):
        raise RuntimeError(
            'S14-G5 proof expected export tag parsing to return complete rights metadata'
        )

    return {
        'proof_scope': (
            'Slice 14 S14-G5 provisional-rights metadata emission guardrail '
            'for query/export responses'
        ),
        'query_missing_rights_error': query_missing_error,
        'export_missing_rights_error': export_missing_error,
        'export_rights_metadata_example': {
            'rights_state': rights_state,
            'rights_provisional': rights_provisional,
        },
        'rights_metadata_guardrail_verified': True,
    }


def main() -> None:
    payload = run_s14_g5_proof()
    repo_root = Path.cwd().resolve()
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'guardrails-proof-s14-g5-rights-metadata.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
