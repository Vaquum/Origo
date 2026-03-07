from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .aligned_core import AlignedDataset, build_aligned_query_plan, query_aligned_data
from .native_core import LatestRowsWindow, TimeRangeWindow


@dataclass(frozen=True)
class _PlannerProofCase:
    name: str
    dataset: AlignedDataset
    window: TimeRangeWindow | LatestRowsWindow
    selected_columns: tuple[str, ...]


_CASES: tuple[_PlannerProofCase, ...] = (
    _PlannerProofCase(
        name='binance_spot_time_range',
        dataset='spot_trades',
        window=TimeRangeWindow(
            start_iso='2017-08-17T12:00:00Z',
            end_iso='2017-08-17T13:00:00Z',
        ),
        selected_columns=(
            'aligned_at_utc',
            'open_price',
            'close_price',
            'trade_count',
        ),
    ),
    _PlannerProofCase(
        name='binance_futures_latest_rows',
        dataset='futures_trades',
        window=LatestRowsWindow(rows=15),
        selected_columns=(
            'aligned_at_utc',
            'high_price',
            'low_price',
            'quote_volume_sum',
        ),
    ),
    _PlannerProofCase(
        name='etf_time_range_forward_fill',
        dataset='etf_daily_metrics',
        window=TimeRangeWindow(
            start_iso='2026-03-05T12:00:00Z',
            end_iso='2026-03-07T12:00:00Z',
        ),
        selected_columns=(
            'source_id',
            'metric_name',
            'valid_from_utc',
            'valid_to_utc_exclusive',
            'metric_value_string',
        ),
    ),
    _PlannerProofCase(
        name='etf_latest_rows_observation',
        dataset='etf_daily_metrics',
        window=LatestRowsWindow(rows=25),
        selected_columns=(
            'aligned_at_utc',
            'source_id',
            'metric_name',
            'metric_value_string',
        ),
    ),
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


def _assert_sorted(frame: pl.DataFrame) -> None:
    if 'valid_from_utc' in frame.columns:
        sort_keys = ['source_id', 'metric_name', 'valid_from_utc']
    elif 'source_id' in frame.columns and 'metric_name' in frame.columns:
        sort_keys = ['aligned_at_utc', 'source_id', 'metric_name']
    else:
        sort_keys = ['aligned_at_utc']

    actual = frame.select(sort_keys).rows()
    expected = frame.sort(sort_keys).select(sort_keys).rows()
    if actual != expected:
        raise RuntimeError(
            f'S5-C4 proof expected deterministic sort order by keys={sort_keys}'
        )


def _assert_second_alignment(frame: pl.DataFrame) -> None:
    for column in (
        'aligned_at_utc',
        'valid_from_utc',
        'valid_to_utc_exclusive',
    ):
        if column not in frame.columns:
            continue
        values = frame.select(pl.col(column).cast(pl.Int64)).to_series().to_list()
        if any((value % 1000) != 0 for value in values):
            raise RuntimeError(
                f'S5-C4 proof expected second-aligned datetime values in column={column}'
            )


def _run_case(case: _PlannerProofCase) -> dict[str, Any]:
    plan = build_aligned_query_plan(dataset=case.dataset, window=case.window)
    frame = query_aligned_data(
        dataset=case.dataset,
        window=case.window,
        selected_columns=case.selected_columns,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )

    if frame.height == 0:
        raise RuntimeError(f'S5-C4 proof expected non-empty rows for case={case.name}')
    if tuple(frame.columns) != case.selected_columns:
        raise RuntimeError(
            f'S5-C4 proof expected projected columns={case.selected_columns}, '
            f'got={tuple(frame.columns)} for case={case.name}'
        )

    _assert_sorted(frame)
    _assert_second_alignment(frame)

    rows = _json_ready_rows(frame.to_dicts())
    window_payload: dict[str, Any]
    if isinstance(case.window, TimeRangeWindow):
        window_payload = {
            'kind': 'time_range',
            'start_iso': case.window.start_iso,
            'end_iso': case.window.end_iso,
        }
    else:
        window_payload = {'kind': 'latest_rows', 'rows': case.window.rows}

    return {
        'case': case.name,
        'dataset': case.dataset,
        'execution_path': plan.execution_path,
        'window': window_payload,
        'selected_columns': list(case.selected_columns),
        'row_count': frame.height,
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def run_s5_04_proof() -> dict[str, Any]:
    case_summaries = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C4 unified aligned query planner path',
        'cases': case_summaries,
    }


def main() -> None:
    result = run_s5_04_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c4-aligned-planner.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
