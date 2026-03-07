from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from origo.data._internal.generic_endpoints import query_native_wide_rows_envelope

_WINDOW = (
    '2024-01-01T00:00:00Z',
    '2024-04-01T00:00:00Z',
)
_OUTPUT_PATH = Path(
    'spec/slices/slice-6-fred-integration/capability-proof-s6-c6-fred-native-query.json'
)


def _stable_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def run_s6_c6_proof() -> dict[str, Any]:
    envelope = query_native_wide_rows_envelope(
        dataset='fred_series_metrics',
        select_cols=[
            'source_id',
            'metric_name',
            'metric_unit',
            'metric_value_float',
            'metric_value_string',
            'observed_at_utc',
        ],
        time_range=_WINDOW,
        include_datetime_col=True,
        auth_token=None,
    )

    rows_payload_obj = envelope.get('rows')
    if not isinstance(rows_payload_obj, list):
        raise RuntimeError('S6-C6 proof expected list rows payload')
    row_objects = cast(list[object], rows_payload_obj)
    rows_payload: list[dict[str, Any]] = []
    for row_obj in row_objects:
        if not isinstance(row_obj, dict):
            raise RuntimeError('S6-C6 proof expected row payloads to be objects')
        rows_payload.append(cast(dict[str, Any], row_obj))

    if len(rows_payload) == 0:
        raise RuntimeError('S6-C6 proof expected non-empty native FRED rows')

    required_columns = {
        'source_id',
        'metric_name',
        'metric_unit',
        'metric_value_float',
        'metric_value_string',
        'observed_at_utc',
    }
    first_row_columns = set(rows_payload[0].keys())
    missing_columns = sorted(required_columns.difference(first_row_columns))
    if len(missing_columns) != 0:
        raise RuntimeError(
            f'S6-C6 proof missing required columns in native envelope: {missing_columns}'
        )

    source_ids = sorted({str(row['source_id']) for row in rows_payload})
    expected_sources = sorted(
        {
            'fred_cpiaucsl',
            'fred_dgs10',
            'fred_fedfunds',
            'fred_unrate',
        }
    )
    if source_ids != expected_sources:
        raise RuntimeError(
            'S6-C6 proof source coverage mismatch, expected '
            f'{expected_sources}, got {source_ids}'
        )

    observed_values = [str(row['observed_at_utc']) for row in rows_payload]
    if observed_values != sorted(observed_values):
        raise RuntimeError(
            'S6-C6 proof expected observed_at_utc ascending order in native envelope'
        )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C6 expose FRED through native raw query mode',
        'window': {'start': _WINDOW[0], 'end': _WINDOW[1]},
        'row_count': len(rows_payload),
        'source_ids': source_ids,
        'rows_hash_sha256': _stable_hash(rows_payload),
        'first_row': rows_payload[0],
        'last_row': rows_payload[-1],
    }


def main() -> None:
    proof_payload = run_s6_c6_proof()
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(
        json.dumps(proof_payload, indent=2, sort_keys=True), encoding='utf-8'
    )
    print(json.dumps(proof_payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
