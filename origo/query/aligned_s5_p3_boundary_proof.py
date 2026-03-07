from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from origo.query.etf_aligned_1s import query_etf_forward_fill_intervals
from origo.query.native_core import TimeRangeWindow

_WINDOW_START = '2026-03-05T12:00:00Z'
_WINDOW_END = '2026-03-07T12:00:00Z'
_BOUNDARY = '2026-03-06T00:00:00Z'


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def run_s5_p3_boundary_proof() -> dict[str, Any]:
    boundary_dt = _parse_iso_datetime(_BOUNDARY)
    frame = query_etf_forward_fill_intervals(
        dataset='etf_daily_metrics',
        window=TimeRangeWindow(start_iso=_WINDOW_START, end_iso=_WINDOW_END),
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )
    if frame.height == 0:
        raise RuntimeError('S5-P3 expected non-empty ETF forward-fill intervals')

    before_boundary = frame.filter(
        pl.col('valid_to_utc_exclusive') == pl.lit(boundary_dt)
    ).select(['source_id', 'metric_name', 'aligned_at_utc', 'metric_value_string'])
    after_boundary = frame.filter(pl.col('valid_from_utc') == pl.lit(boundary_dt)).select(
        ['source_id', 'metric_name', 'aligned_at_utc', 'metric_value_string']
    )

    if before_boundary.height == 0 or after_boundary.height == 0:
        raise RuntimeError(
            'S5-P3 expected rows both ending and starting exactly at UTC day boundary'
        )

    before_keys = before_boundary.select(['source_id', 'metric_name'])
    after_keys = after_boundary.select(['source_id', 'metric_name'])
    if before_keys.sort(['source_id', 'metric_name']).rows() != after_keys.sort(
        ['source_id', 'metric_name']
    ).rows():
        raise RuntimeError(
            'S5-P3 expected matching source+metric keys on both sides of UTC boundary'
        )

    if before_boundary.filter(pl.col('aligned_at_utc') >= pl.lit(boundary_dt)).height > 0:
        raise RuntimeError('S5-P3 expected pre-boundary rows to have aligned_at_utc < boundary')
    if after_boundary.filter(pl.col('aligned_at_utc') != pl.lit(boundary_dt)).height > 0:
        raise RuntimeError(
            'S5-P3 expected post-boundary rows to have aligned_at_utc == boundary'
        )

    boundary_join = before_boundary.join(
        after_boundary,
        on=['source_id', 'metric_name'],
        how='inner',
        suffix='_after',
    ).sort(['source_id', 'metric_name'])

    if boundary_join.height != before_boundary.height:
        raise RuntimeError(
            'S5-P3 expected one-to-one boundary transition rows per source+metric'
        )

    transition_samples = _json_ready_rows(boundary_join.head(10).to_dicts())
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 P3 forward-fill UTC day-boundary semantics',
        'window': {'time_range': [_WINDOW_START, _WINDOW_END], 'boundary': _BOUNDARY},
        'row_count': frame.height,
        'boundary_end_count': before_boundary.height,
        'boundary_start_count': after_boundary.height,
        'boundary_transition_count': boundary_join.height,
        'transition_samples': transition_samples,
    }


def main() -> None:
    result = run_s5_p3_boundary_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p3-boundary.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
