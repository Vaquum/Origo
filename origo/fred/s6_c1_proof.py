from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .client import build_fred_client_from_env, fetch_registry_snapshots
from .contracts import FREDSeriesSnapshot
from .registry import load_fred_series_registry

_OBSERVATION_LIMIT = 5
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c1-fred-connector.json'
)


def _stable_hash(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _normalize_snapshot(snapshot: FREDSeriesSnapshot) -> dict[str, Any]:
    observation_rows: list[dict[str, Any]] = []
    for observation in snapshot.observations:
        observation_rows.append(
            {
                'observation_date': observation.observation_date.isoformat(),
                'realtime_start': observation.realtime_start.isoformat(),
                'realtime_end': observation.realtime_end.isoformat(),
                'value': observation.value,
                'raw_value': observation.raw_value,
            }
        )

    if len(observation_rows) == 0:
        raise RuntimeError(
            'S6-C1 proof expected non-empty observations for series_id='
            f'{snapshot.registry_entry.series_id}'
        )

    dates_desc = [row['observation_date'] for row in observation_rows]
    if dates_desc != sorted(dates_desc, reverse=True):
        raise RuntimeError(
            'S6-C1 proof expected observations sorted by descending date for series_id='
            f'{snapshot.registry_entry.series_id}'
        )

    value_non_null_count = sum(
        1 for row in observation_rows if row['value'] is not None
    )
    if value_non_null_count == 0:
        raise RuntimeError(
            'S6-C1 proof expected at least one non-null observation value for series_id='
            f'{snapshot.registry_entry.series_id}'
        )

    return {
        'series_id': snapshot.registry_entry.series_id,
        'source_id': snapshot.registry_entry.source_id,
        'metric_name': snapshot.registry_entry.metric_name,
        'metric_unit': snapshot.registry_entry.metric_unit,
        'frequency_hint': snapshot.registry_entry.frequency_hint,
        'metadata': {
            'title': snapshot.metadata.title,
            'units': snapshot.metadata.units,
            'frequency': snapshot.metadata.frequency,
            'seasonal_adjustment': snapshot.metadata.seasonal_adjustment,
            'observation_start': snapshot.metadata.observation_start.isoformat(),
            'observation_end': snapshot.metadata.observation_end.isoformat(),
            'last_updated_utc': snapshot.metadata.last_updated_utc.isoformat(),
            'popularity': snapshot.metadata.popularity,
        },
        'observation_count': len(observation_rows),
        'non_null_value_count': value_non_null_count,
        'latest_observation': observation_rows[0],
        'oldest_observation_in_window': observation_rows[-1],
        'observation_window_hash_sha256': _stable_hash(observation_rows),
    }


def run_s6_c1_proof() -> dict[str, Any]:
    registry_version, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()
    snapshots = fetch_registry_snapshots(
        client=client,
        registry_entries=registry_entries,
        observation_limit=_OBSERVATION_LIMIT,
    )

    normalized_snapshots = [_normalize_snapshot(snapshot) for snapshot in snapshots]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C1 FRED connector and series registry mapping',
        'registry_version': registry_version,
        'series_count': len(normalized_snapshots),
        'observation_limit': _OBSERVATION_LIMIT,
        'series': normalized_snapshots,
        'proof_hash_sha256': _stable_hash(normalized_snapshots),
    }


def main() -> None:
    proof_payload = run_s6_c1_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
