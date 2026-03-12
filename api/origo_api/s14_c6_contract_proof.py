from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from api.origo_api.schemas import RawExportRequest, RawQueryRequest


def run_s14_c6_proof() -> dict[str, Any]:
    query_request = RawQueryRequest(
        mode='native',
        sources=['binance_spot_trades', 'okx_spot_trades'],
        n_rows=10,
        view_id='aligned_1s_raw',
        view_version=1,
    )
    export_request = RawExportRequest(
        mode='native',
        format='parquet',
        dataset='binance_spot_trades',
        n_rows=10,
        view_id='aligned_1s_raw',
        view_version=1,
    )

    query_view_validation_error: str | None = None
    try:
        RawQueryRequest(
            mode='native',
            sources=['binance_spot_trades'],
            n_rows=10,
            view_id='aligned_1s_raw',
        )
    except ValidationError as exc:
        query_view_validation_error = str(exc)

    export_view_validation_error: str | None = None
    try:
        RawExportRequest(
            mode='native',
            format='parquet',
            dataset='binance_spot_trades',
            n_rows=10,
            view_id='aligned_1s_raw',
        )
    except ValidationError as exc:
        export_view_validation_error = str(exc)

    if query_view_validation_error is None:
        raise RuntimeError(
            'S14-C6 proof expected query contract to fail when view_id/view_version pairing is incomplete'
        )
    if export_view_validation_error is None:
        raise RuntimeError(
            'S14-C6 proof expected export contract to fail when view_id/view_version pairing is incomplete'
        )

    return {
        'proof_scope': 'Slice 14 S14-C6 multi-source + view contract schema validation',
        'query_request': query_request.model_dump(mode='json'),
        'export_request': export_request.model_dump(mode='json', exclude={'auth_token'}),
        'query_sources_count': len(query_request.sources),
        'query_view_id': query_request.view_id,
        'query_view_version': query_request.view_version,
        'export_view_id': export_request.view_id,
        'export_view_version': export_request.view_version,
        'query_incomplete_view_pair_error': query_view_validation_error,
        'export_incomplete_view_pair_error': export_view_validation_error,
    }


def main() -> None:
    payload = run_s14_c6_proof()
    repo_root = Path.cwd().resolve()
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c6-contract.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
