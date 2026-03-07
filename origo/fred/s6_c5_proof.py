from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .client import build_fred_client_from_env
from .ingest import run_fred_historical_backfill
from .persistence import (
    build_fred_raw_bundles,
    persist_fred_long_metrics_to_clickhouse,
    persist_fred_raw_bundles_to_object_store,
)
from .registry import load_fred_series_registry

_WINDOW_START = date(2024, 1, 1)
_WINDOW_END = date(2024, 3, 31)
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c5-fred-persistence.json'
)


def run_s6_c5_proof() -> dict[str, Any]:
    run_started_at_utc = datetime.now(UTC)
    run_id = f's6-c5-proof-{run_started_at_utc.strftime("%Y%m%dT%H%M%SZ")}'

    registry_version, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()

    backfill_result = run_fred_historical_backfill(
        client=client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        observation_start=_WINDOW_START,
        observation_end=_WINDOW_END,
    )
    rows = list(backfill_result.rows)
    if len(rows) == 0:
        raise RuntimeError('S6-C5 proof expected non-empty backfill rows')

    inserted_row_count = persist_fred_long_metrics_to_clickhouse(rows=rows)
    if inserted_row_count != len(rows):
        raise RuntimeError(
            f'S6-C5 proof expected inserted_row_count={len(rows)}, got {inserted_row_count}'
        )

    raw_bundles = build_fred_raw_bundles(
        client=client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        observation_start=_WINDOW_START,
        observation_end=_WINDOW_END,
    )
    persisted_artifacts = persist_fred_raw_bundles_to_object_store(
        bundles=raw_bundles,
        run_id=run_id,
        run_started_at_utc=run_started_at_utc,
    )
    if len(persisted_artifacts) != len(raw_bundles):
        raise RuntimeError(
            'S6-C5 proof expected persisted artifact count to match bundle count, '
            f'got {len(persisted_artifacts)} != {len(raw_bundles)}'
        )

    source_ids = sorted(entry.source_id for entry in registry_entries)
    persisted_payload: list[dict[str, Any]] = []
    for artifact in persisted_artifacts:
        persisted_payload.append(
            {
                'artifact_id': artifact.artifact_id,
                'storage_uri': artifact.storage_uri,
                'manifest_uri': artifact.manifest_uri,
                'persisted_at_utc': artifact.persisted_at_utc.isoformat(),
            }
        )
    persisted_payload.sort(key=lambda item: item['artifact_id'])

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C5 persist native FRED data to ClickHouse and raw artifacts to object store',
        'registry_version': registry_version,
        'window': {
            'observation_start': _WINDOW_START.isoformat(),
            'observation_end': _WINDOW_END.isoformat(),
        },
        'source_ids': source_ids,
        'row_count': len(rows),
        'inserted_row_count': inserted_row_count,
        'rows_hash_sha256': backfill_result.rows_hash_sha256,
        'raw_bundle_count': len(raw_bundles),
        'persisted_artifact_count': len(persisted_artifacts),
        'run_id': run_id,
        'persisted_artifacts': persisted_payload,
    }


def main() -> None:
    proof_payload = run_s6_c5_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
