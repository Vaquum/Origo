from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .etf_aligned_1s import query_etf_aligned_1s_data
from .native_core import TimeRangeWindow

_EXPECTED_SOURCE_IDS: frozenset[str] = frozenset(
    {
        'etf_ishares_ibit_daily',
        'etf_invesco_btco_daily',
        'etf_bitwise_bitb_daily',
        'etf_ark_arkb_daily',
        'etf_vaneck_hodl_daily',
        'etf_franklin_ezbc_daily',
        'etf_grayscale_gbtc_daily',
        'etf_fidelity_fbtc_daily',
        'etf_coinshares_brrr_daily',
        'etf_hashdex_defi_daily',
    }
)


def _stable_hash(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _json_ready_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized_row: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                normalized_row[key] = value.isoformat()
            else:
                normalized_row[key] = value
        normalized.append(normalized_row)
    return normalized


def run_s5_02_proof() -> dict[str, Any]:
    frame = query_etf_aligned_1s_data(
        dataset='etf_daily_metrics',
        window=TimeRangeWindow(
            start_iso='2026-03-04T00:00:00Z',
            end_iso='2026-03-07T00:00:00Z',
        ),
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )

    required_columns = {
        'aligned_at_utc',
        'source_id',
        'metric_name',
        'metric_unit',
        'metric_value_string',
        'metric_value_int',
        'metric_value_float',
        'metric_value_bool',
        'dimensions_json',
        'provenance_json',
        'latest_ingested_at_utc',
        'records_in_bucket',
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if len(missing_columns) > 0:
        raise RuntimeError(
            f'S5-C2 proof expected required ETF aligned columns, missing={missing_columns}'
        )

    if frame.height == 0:
        raise RuntimeError('S5-C2 proof expected non-empty ETF aligned rows')

    aligned_ms = frame.select(pl.col('aligned_at_utc').cast(pl.Int64)).to_series().to_list()
    if aligned_ms != sorted(aligned_ms):
        raise RuntimeError('S5-C2 proof expected aligned_at_utc ascending sort')
    if any((value % 1000) != 0 for value in aligned_ms):
        raise RuntimeError('S5-C2 proof expected second-aligned ETF timestamps')

    source_ids = frozenset(frame.select('source_id').to_series().to_list())
    missing_source_ids = sorted(_EXPECTED_SOURCE_IDS.difference(source_ids))
    if len(missing_source_ids) > 0:
        raise RuntimeError(
            f'S5-C2 proof expected all ETF sources in proof window, missing={missing_source_ids}'
        )

    max_records_in_bucket = int(frame.select(pl.col('records_in_bucket').max()).item())
    if max_records_in_bucket <= 0:
        raise RuntimeError('S5-C2 proof expected records_in_bucket > 0')

    rows = _json_ready_rows(frame.to_dicts())
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C2 ETF aligned-1s materialization definitions',
        'window': {
            'time_range': ['2026-03-04T00:00:00Z', '2026-03-07T00:00:00Z']
        },
        'row_count': frame.height,
        'source_count': len(source_ids),
        'max_records_in_bucket': max_records_in_bucket,
        'rows_hash_sha256': _stable_hash(rows),
        'schema': [
            {'name': name, 'dtype': str(dtype)} for name, dtype in frame.schema.items()
        ],
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def main() -> None:
    result = run_s5_02_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c2-etf-aligned.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
