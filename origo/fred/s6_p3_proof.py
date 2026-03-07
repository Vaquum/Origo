from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

from .client import build_fred_client_from_env
from .ingest import FREDBackfillResult, run_fred_historical_backfill
from .normalize import long_metric_rows_to_json_rows
from .registry import default_fred_series_registry_path, load_fred_series_registry

_WINDOW_START = date(2024, 1, 1)
_WINDOW_END = date(2024, 3, 31)
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/proof-s6-p3-metadata-version-reproducibility.json'
)


def _stable_hash(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(8192), b''):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class _RunFingerprint:
    row_count: int
    rows_hash_sha256: str
    metadata_hash_sha256: str
    provenance_hash_sha256: str
    per_series_counts: dict[str, int]


def _extract_metadata_fingerprint(
    rows: list[dict[str, Any]], registry_version: str
) -> tuple[str, str]:
    metadata_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []

    for row in rows:
        provenance_json = row.get('provenance_json')
        if not isinstance(provenance_json, str) or provenance_json.strip() == '':
            raise RuntimeError('S6-P3 proof expected non-empty provenance_json for every row')

        parsed = json.loads(provenance_json)
        if not isinstance(parsed, dict):
            raise RuntimeError('S6-P3 proof expected provenance_json to decode to object')
        provenance = cast(dict[str, Any], parsed)

        parsed_registry_version = provenance.get('registry_version')
        if parsed_registry_version != registry_version:
            raise RuntimeError(
                'S6-P3 proof expected provenance registry_version to match registry contract, '
                f'got {parsed_registry_version!r} expected {registry_version!r}'
            )

        metadata_rows.append(
            {
                'series_id': provenance.get('series_id'),
                'units': provenance.get('units'),
                'frequency': provenance.get('frequency'),
                'seasonal_adjustment': provenance.get('seasonal_adjustment'),
                'last_updated_utc': provenance.get('last_updated_utc'),
                'registry_version': parsed_registry_version,
            }
        )
        provenance_rows.append(
            {
                'metric_id': row.get('metric_id'),
                'source_id': row.get('source_id'),
                'observed_at_utc': row.get('observed_at_utc'),
                'provenance': provenance,
            }
        )

    metadata_rows.sort(
        key=lambda item: (
            str(item['series_id']),
            str(item['last_updated_utc']),
            str(item['frequency']),
            str(item['units']),
            str(item['seasonal_adjustment']),
        )
    )
    provenance_rows.sort(
        key=lambda item: (
            str(item['source_id']),
            str(item['observed_at_utc']),
            str(item['metric_id']),
        )
    )

    return _stable_hash(metadata_rows), _stable_hash(provenance_rows)


def _run_once(registry_version: str) -> _RunFingerprint:
    _, registry_entries = load_fred_series_registry()
    client = build_fred_client_from_env()
    backfill_result: FREDBackfillResult = run_fred_historical_backfill(
        client=client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        observation_start=_WINDOW_START,
        observation_end=_WINDOW_END,
    )

    rows = long_metric_rows_to_json_rows(rows=list(backfill_result.rows))
    if len(rows) == 0:
        raise RuntimeError('S6-P3 proof expected non-empty normalized rows')

    metadata_hash, provenance_hash = _extract_metadata_fingerprint(
        rows=rows,
        registry_version=registry_version,
    )

    per_series_counts: dict[str, int] = {}
    for result in backfill_result.per_series:
        per_series_counts[result.source_id] = result.normalized_row_count

    return _RunFingerprint(
        row_count=len(rows),
        rows_hash_sha256=backfill_result.rows_hash_sha256,
        metadata_hash_sha256=metadata_hash,
        provenance_hash_sha256=provenance_hash,
        per_series_counts=per_series_counts,
    )


def run_s6_p3_proof() -> dict[str, Any]:
    registry_path = default_fred_series_registry_path()
    registry_version, _ = load_fred_series_registry(path=registry_path)

    run_1 = _run_once(registry_version)
    run_2 = _run_once(registry_version)

    deterministic_match = (
        run_1.row_count == run_2.row_count
        and run_1.rows_hash_sha256 == run_2.rows_hash_sha256
        and run_1.metadata_hash_sha256 == run_2.metadata_hash_sha256
        and run_1.provenance_hash_sha256 == run_2.provenance_hash_sha256
        and run_1.per_series_counts == run_2.per_series_counts
    )
    if not deterministic_match:
        raise RuntimeError(
            'S6-P3 proof expected metadata/version reproducibility across replay runs, '
            f'run_1={run_1} run_2={run_2}'
        )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 P3 metadata/version reproducibility for replay',
        'registry_path': registry_path.as_posix(),
        'registry_file_sha256': _sha256_file(registry_path),
        'registry_version': registry_version,
        'window': {
            'start': _WINDOW_START.isoformat(),
            'end': _WINDOW_END.isoformat(),
        },
        'run_1': {
            'row_count': run_1.row_count,
            'rows_hash_sha256': run_1.rows_hash_sha256,
            'metadata_hash_sha256': run_1.metadata_hash_sha256,
            'provenance_hash_sha256': run_1.provenance_hash_sha256,
            'per_series_counts': run_1.per_series_counts,
        },
        'run_2': {
            'row_count': run_2.row_count,
            'rows_hash_sha256': run_2.rows_hash_sha256,
            'metadata_hash_sha256': run_2.metadata_hash_sha256,
            'provenance_hash_sha256': run_2.provenance_hash_sha256,
            'per_series_counts': run_2.per_series_counts,
        },
        'deterministic_match': deterministic_match,
    }


def main() -> None:
    proof_payload = run_s6_p3_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
