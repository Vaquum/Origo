from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .client import build_fred_client_from_env
from .ingest import run_fred_historical_backfill
from .registry import load_fred_series_registry

_WINDOW_START = date(2024, 1, 1)
_WINDOW_END = date(2024, 3, 31)
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c3-fred-backfill.json'
)


def run_s6_c3_proof() -> dict[str, Any]:
    registry_version, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()
    result = run_fred_historical_backfill(
        client=client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        observation_start=_WINDOW_START,
        observation_end=_WINDOW_END,
    )

    if len(result.rows) == 0:
        raise RuntimeError('S6-C3 proof expected non-empty backfill rows')

    expected_sources = sorted(entry.source_id for entry in registry_entries)
    actual_sources = sorted({series.source_id for series in result.per_series})
    if actual_sources != expected_sources:
        raise RuntimeError(
            'S6-C3 proof source coverage mismatch, expected '
            f'{expected_sources}, got {actual_sources}'
        )

    for row in result.rows:
        observed_date = row.observed_at_utc.date()
        if observed_date < _WINDOW_START or observed_date > _WINDOW_END:
            raise RuntimeError(
                'S6-C3 proof observed_at_utc out of window, got '
                f'{row.observed_at_utc.isoformat()} window='
                f'{_WINDOW_START.isoformat()}..{_WINDOW_END.isoformat()}'
            )

    per_series_payload: list[dict[str, Any]] = []
    for series in result.per_series:
        per_series_payload.append(
            {
                'series_id': series.series_id,
                'source_id': series.source_id,
                'observation_start': series.observation_start.isoformat(),
                'observation_end': series.observation_end.isoformat(),
                'observation_count': series.observation_count,
                'normalized_row_count': series.normalized_row_count,
            }
        )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C3 historical backfill path',
        'registry_version': result.registry_version,
        'window': {
            'observation_start': _WINDOW_START.isoformat(),
            'observation_end': _WINDOW_END.isoformat(),
        },
        'series_count': len(result.per_series),
        'row_count': len(result.rows),
        'rows_hash_sha256': result.rows_hash_sha256,
        'per_series': per_series_payload,
    }


def main() -> None:
    proof_payload = run_s6_c3_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
