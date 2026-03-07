from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .client import build_fred_client_from_env
from .ingest import run_fred_incremental_update
from .registry import load_fred_series_registry

_AS_OF_DATE = date(2024, 3, 31)
_LAST_OBSERVED_BY_SOURCE = {
    'fred_fedfunds': date(2024, 1, 1),
    'fred_cpiaucsl': date(2024, 1, 1),
    'fred_unrate': date(2024, 1, 1),
    'fred_dgs10': date(2024, 2, 15),
}
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c4-fred-incremental.json'
)


def run_s6_c4_proof() -> dict[str, Any]:
    registry_version, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()
    result = run_fred_incremental_update(
        client=client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        last_observed_by_source=_LAST_OBSERVED_BY_SOURCE,
        as_of_date=_AS_OF_DATE,
    )

    expected_sources = sorted(entry.source_id for entry in registry_entries)
    actual_sources = sorted(series.source_id for series in result.per_series)
    if actual_sources != expected_sources:
        raise RuntimeError(
            'S6-C4 proof source coverage mismatch, expected '
            f'{expected_sources}, got {actual_sources}'
        )

    updated_sources: list[str] = []
    no_new_data_sources: list[str] = []
    per_series_payload: list[dict[str, Any]] = []
    for series in result.per_series:
        if series.status == 'updated':
            updated_sources.append(series.source_id)
        else:
            no_new_data_sources.append(series.source_id)
        per_series_payload.append(
            {
                'series_id': series.series_id,
                'source_id': series.source_id,
                'cursor_date': (
                    series.cursor_date.isoformat()
                    if series.cursor_date is not None
                    else None
                ),
                'effective_start': series.effective_start.isoformat(),
                'effective_end': series.effective_end.isoformat(),
                'status': series.status,
                'observation_count': series.observation_count,
                'normalized_row_count': series.normalized_row_count,
            }
        )

    if len(updated_sources) == 0:
        raise RuntimeError('S6-C4 proof expected at least one updated source')

    for row in result.rows:
        observed_date = row.observed_at_utc.date()
        if observed_date > _AS_OF_DATE:
            raise RuntimeError(
                'S6-C4 proof expected observed_at_utc <= as_of_date, got '
                f'{row.observed_at_utc.isoformat()}'
            )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C4 incremental update path',
        'registry_version': result.registry_version,
        'as_of_date': _AS_OF_DATE.isoformat(),
        'series_count': len(result.per_series),
        'row_count': len(result.rows),
        'rows_hash_sha256': result.rows_hash_sha256,
        'updated_source_count': len(updated_sources),
        'updated_sources': sorted(updated_sources),
        'no_new_data_source_count': len(no_new_data_sources),
        'no_new_data_sources': sorted(no_new_data_sources),
        'per_series': per_series_payload,
    }


def main() -> None:
    proof_payload = run_s6_c4_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
