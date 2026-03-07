from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from origo.data._internal.generic_endpoints import query_native_wide_rows_envelope


def _stable_hash(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _summarize_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    rows = envelope['rows']
    return {
        'mode': envelope['mode'],
        'source': envelope['source'],
        'row_count': envelope['row_count'],
        'schema': envelope['schema'],
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0] if len(rows) > 0 else None,
        'last_row': rows[-1] if len(rows) > 0 else None,
    }


def run_s4_08_query_proof() -> dict[str, Any]:
    historical_envelope = query_native_wide_rows_envelope(
        dataset='etf_daily_metrics',
        select_cols=(
            'source_id',
            'metric_name',
            'metric_value_float',
            'metric_value_int',
            'metric_value_string',
            'observed_at_utc',
        ),
        time_range=('2025-01-09T00:00:00Z', '2025-01-12T00:00:00Z'),
        include_datetime_col=True,
        auth_token=None,
    )
    latest_envelope = query_native_wide_rows_envelope(
        dataset='etf_daily_metrics',
        select_cols=None,
        n_rows=50,
        include_datetime_col=True,
        auth_token=None,
    )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 4 S4-C8 ETF native raw query exposure',
        'queries': {
            'historical_time_range': {
                'window': {
                    'time_range': ['2025-01-09T00:00:00Z', '2025-01-12T00:00:00Z']
                },
                'summary': _summarize_envelope(historical_envelope),
            },
            'latest_rows': {
                'window': {'n_rows': 50},
                'summary': _summarize_envelope(latest_envelope),
            },
        },
    }


def main() -> None:
    result = run_s4_08_query_proof()
    output_path = Path(
        'spec/slices/slice-4-etf-use-case/capability-proof-s4-c8-native-query.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
