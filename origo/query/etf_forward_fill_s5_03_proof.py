from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .etf_aligned_1s import query_etf_forward_fill_intervals
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
_WINDOW_START = '2026-03-05T12:00:00Z'
_WINDOW_END = '2026-03-07T12:00:00Z'
_DAY_BOUNDARY = '2026-03-06T00:00:00Z'


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def _assert_interval_monotonic_non_overlapping(frame: pl.DataFrame) -> None:
    grouped = frame.partition_by(['source_id', 'metric_name'], maintain_order=True)
    for group in grouped:
        source_id = str(group.select('source_id').to_series().item(0))
        metric_name = str(group.select('metric_name').to_series().item(0))
        valid_from_values = group.select('valid_from_utc').to_series().to_list()
        valid_to_values = group.select('valid_to_utc_exclusive').to_series().to_list()

        for idx in range(len(valid_from_values)):
            if valid_to_values[idx] <= valid_from_values[idx]:
                raise RuntimeError(
                    'S5-C3 proof expected valid_to_utc_exclusive > valid_from_utc '
                    f'for source={source_id} metric={metric_name}'
                )
            if idx == 0:
                continue
            if valid_from_values[idx] < valid_from_values[idx - 1]:
                raise RuntimeError(
                    'S5-C3 proof expected increasing valid_from_utc within each '
                    f'source+metric group (source={source_id}, metric={metric_name})'
                )
            if valid_from_values[idx] < valid_to_values[idx - 1]:
                raise RuntimeError(
                    'S5-C3 proof expected non-overlapping forward-fill intervals '
                    f'for source={source_id} metric={metric_name}'
                )


def run_s5_03_proof() -> dict[str, Any]:
    window = TimeRangeWindow(start_iso=_WINDOW_START, end_iso=_WINDOW_END)
    frame = query_etf_forward_fill_intervals(
        dataset='etf_daily_metrics',
        window=window,
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
        'valid_from_utc',
        'valid_to_utc_exclusive',
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if len(missing_columns) > 0:
        raise RuntimeError(
            f'S5-C3 proof expected required forward-fill columns, missing={missing_columns}'
        )

    if frame.height == 0:
        raise RuntimeError('S5-C3 proof expected non-empty ETF forward-fill rows')

    sort_frame = frame.select(
        ['source_id', 'metric_name', 'valid_from_utc', 'valid_to_utc_exclusive']
    )
    sorted_sort_frame = sort_frame.sort(
        ['source_id', 'metric_name', 'valid_from_utc', 'valid_to_utc_exclusive']
    )
    if sort_frame.rows() != sorted_sort_frame.rows():
        raise RuntimeError(
            'S5-C3 proof expected deterministic sorted forward-fill interval ordering'
        )

    start_dt = _parse_iso_datetime(_WINDOW_START)
    end_dt = _parse_iso_datetime(_WINDOW_END)
    day_boundary_dt = _parse_iso_datetime(_DAY_BOUNDARY)

    if frame.filter(pl.col('valid_from_utc') < pl.lit(start_dt)).height > 0:
        raise RuntimeError(
            'S5-C3 proof expected all valid_from_utc values to be >= window_start'
        )
    if frame.filter(pl.col('valid_to_utc_exclusive') > pl.lit(end_dt)).height > 0:
        raise RuntimeError(
            'S5-C3 proof expected all valid_to_utc_exclusive values to be <= window_end'
        )
    if frame.filter(
        pl.col('valid_to_utc_exclusive') <= pl.col('valid_from_utc')
    ).height > 0:
        raise RuntimeError(
            'S5-C3 proof expected valid_to_utc_exclusive > valid_from_utc on all rows'
        )

    _assert_interval_monotonic_non_overlapping(frame)

    aligned_ms = frame.select(pl.col('aligned_at_utc').cast(pl.Int64)).to_series().to_list()
    valid_from_ms = frame.select(pl.col('valid_from_utc').cast(pl.Int64)).to_series().to_list()
    valid_to_ms = frame.select(
        pl.col('valid_to_utc_exclusive').cast(pl.Int64)
    ).to_series().to_list()

    if any((value % 1000) != 0 for value in aligned_ms):
        raise RuntimeError('S5-C3 proof expected second-aligned aligned_at_utc values')
    if any((value % 1000) != 0 for value in valid_from_ms):
        raise RuntimeError('S5-C3 proof expected second-aligned valid_from_utc values')
    if any((value % 1000) != 0 for value in valid_to_ms):
        raise RuntimeError(
            'S5-C3 proof expected second-aligned valid_to_utc_exclusive values'
        )

    source_ids = frozenset(frame.select('source_id').to_series().to_list())
    missing_source_ids = sorted(_EXPECTED_SOURCE_IDS.difference(source_ids))
    if len(missing_source_ids) > 0:
        raise RuntimeError(
            f'S5-C3 proof expected all ETF sources in forward-fill output, missing={missing_source_ids}'
        )

    window_start_carry_count = frame.filter(
        pl.col('valid_from_utc') == pl.lit(start_dt)
    ).height
    if window_start_carry_count == 0:
        raise RuntimeError(
            'S5-C3 proof expected prior-state carry rows anchored at window_start'
        )

    window_end_clip_count = frame.filter(
        pl.col('valid_to_utc_exclusive') == pl.lit(end_dt)
    ).height
    if window_end_clip_count == 0:
        raise RuntimeError(
            'S5-C3 proof expected trailing rows clipped at window_end'
        )

    boundary_start_count = frame.filter(
        pl.col('valid_from_utc') == pl.lit(day_boundary_dt)
    ).height
    boundary_end_count = frame.filter(
        pl.col('valid_to_utc_exclusive') == pl.lit(day_boundary_dt)
    ).height
    if boundary_start_count == 0 or boundary_end_count == 0:
        raise RuntimeError(
            'S5-C3 proof expected UTC day-boundary transition rows in forward-fill output'
        )

    rows = _json_ready_rows(frame.to_dicts())
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C3 ETF aligned forward-fill semantics',
        'window': {'time_range': [_WINDOW_START, _WINDOW_END]},
        'row_count': frame.height,
        'source_count': len(source_ids),
        'window_start_carry_count': window_start_carry_count,
        'window_end_clip_count': window_end_clip_count,
        'utc_day_boundary_start_count': boundary_start_count,
        'utc_day_boundary_end_count': boundary_end_count,
        'rows_hash_sha256': _stable_hash(rows),
        'schema': [
            {'name': name, 'dtype': str(dtype)} for name, dtype in frame.schema.items()
        ],
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def main() -> None:
    result = run_s5_03_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c3-etf-forward-fill.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
