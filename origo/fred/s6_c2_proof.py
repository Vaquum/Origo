from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .client import build_fred_client_from_env, fetch_registry_snapshots
from .normalize import (
    long_metric_rows_to_json_rows,
    normalize_fred_snapshots_to_long_metrics,
)
from .registry import load_fred_series_registry

_OBSERVATION_LIMIT = 8
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c2-fred-normalize.json'
)


def _stable_hash(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def run_s6_c2_proof() -> dict[str, Any]:
    registry_version, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()
    snapshots = fetch_registry_snapshots(
        client=client,
        registry_entries=registry_entries,
        observation_limit=_OBSERVATION_LIMIT,
    )

    long_rows = normalize_fred_snapshots_to_long_metrics(
        snapshots=snapshots,
        registry_version=registry_version,
    )
    json_rows = long_metric_rows_to_json_rows(rows=long_rows)
    if len(json_rows) == 0:
        raise RuntimeError('S6-C2 proof expected non-empty normalized rows')

    expected_row_count = sum(len(snapshot.observations) for snapshot in snapshots)
    if len(json_rows) != expected_row_count:
        raise RuntimeError(
            'S6-C2 proof row count mismatch, expected '
            f'{expected_row_count}, got {len(json_rows)}'
        )

    expected_sources = sorted(entry.source_id for entry in registry_entries)
    actual_sources = sorted({row['source_id'] for row in json_rows})
    if actual_sources != expected_sources:
        raise RuntimeError(
            'S6-C2 proof source coverage mismatch, expected '
            f'{expected_sources}, got {actual_sources}'
        )

    for row in json_rows:
        observed = row['observed_at_utc']
        if not isinstance(observed, str) or not observed.endswith('+00:00'):
            raise RuntimeError(
                'S6-C2 proof expected UTC observed_at_utc in ISO format, got '
                f'{observed!r}'
            )
        if 'T00:00:00+00:00' not in observed:
            raise RuntimeError(
                'S6-C2 proof expected UTC midnight observation timestamps, got '
                f'{observed}'
            )

    per_source_counts: dict[str, int] = {}
    for row in json_rows:
        source_id = row['source_id']
        if not isinstance(source_id, str):
            raise RuntimeError(
                f'S6-C2 proof expected source_id string, got {source_id!r}'
            )
        per_source_counts[source_id] = per_source_counts.get(source_id, 0) + 1

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C2 normalize FRED records into long-metric schema',
        'registry_version': registry_version,
        'series_count': len(registry_entries),
        'observation_limit': _OBSERVATION_LIMIT,
        'row_count': len(json_rows),
        'expected_row_count': expected_row_count,
        'source_count': len(actual_sources),
        'source_ids': actual_sources,
        'rows_per_source': per_source_counts,
        'rows_hash_sha256': _stable_hash(json_rows),
        'first_row': json_rows[0],
        'last_row': json_rows[-1],
    }


def main() -> None:
    proof_payload = run_s6_c2_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
