from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from origo.data._internal.generic_endpoints import query_aligned_wide_rows_envelope
from origo.query.aligned_core import (
    AlignedDataset,
    build_aligned_query_plan,
    query_aligned_data,
)
from origo.query.native_core import LatestRowsWindow, TimeRangeWindow


@dataclass(frozen=True)
class _Case:
    case_id: str
    dataset: AlignedDataset
    fields: tuple[str, ...]
    window: TimeRangeWindow | LatestRowsWindow


_CASES: tuple[_Case, ...] = (
    _Case(
        case_id='fred_forward_fill_time_range',
        dataset='fred_series_metrics',
        fields=(
            'source_id',
            'metric_name',
            'valid_from_utc',
            'valid_to_utc_exclusive',
            'metric_value_string',
        ),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-04-01T00:00:00Z',
        ),
    ),
    _Case(
        case_id='fred_observation_latest_rows',
        dataset='fred_series_metrics',
        fields=('aligned_at_utc', 'source_id', 'metric_name', 'metric_value_string'),
        window=LatestRowsWindow(rows=25),
    ),
)
_EXPECTED_SOURCES = {
    'fred_cpiaucsl',
    'fred_dgs10',
    'fred_fedfunds',
    'fred_unrate',
}


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


def _assert_second_alignment(frame: pl.DataFrame) -> None:
    for column in ('aligned_at_utc', 'valid_from_utc', 'valid_to_utc_exclusive'):
        if column not in frame.columns:
            continue
        values = frame.select(pl.col(column).cast(pl.Int64)).to_series().to_list()
        if any((value % 1000) != 0 for value in values):
            raise RuntimeError(
                f'S6-C7 proof expected second-aligned datetime values in column={column}'
            )


def _assert_ordering(case: _Case, frame: pl.DataFrame) -> None:
    if case.case_id == 'fred_forward_fill_time_range':
        sort_keys = ['source_id', 'metric_name', 'valid_from_utc']
    else:
        sort_keys = ['aligned_at_utc', 'source_id', 'metric_name']

    actual = frame.select(sort_keys).rows()
    expected = frame.sort(sort_keys).select(sort_keys).rows()
    if actual != expected:
        raise RuntimeError(
            f'S6-C7 proof expected deterministic sort order by keys={sort_keys}'
        )


def _assert_sources(frame: pl.DataFrame) -> None:
    source_ids = set(frame.select('source_id').to_series().to_list())
    if source_ids != _EXPECTED_SOURCES:
        raise RuntimeError(
            f'S6-C7 proof source coverage mismatch: expected={sorted(_EXPECTED_SOURCES)} got={sorted(source_ids)}'
        )


def _assert_forward_fill_intervals(frame: pl.DataFrame) -> None:
    if 'valid_from_utc' not in frame.columns or 'valid_to_utc_exclusive' not in frame.columns:
        raise RuntimeError('S6-C7 proof expected forward-fill interval columns')

    invalid = frame.filter(pl.col('valid_to_utc_exclusive') <= pl.col('valid_from_utc'))
    if invalid.height > 0:
        raise RuntimeError('S6-C7 proof found non-positive forward-fill interval width')


def _run_case(case: _Case) -> dict[str, Any]:
    plan = build_aligned_query_plan(dataset=case.dataset, window=case.window)
    frame = query_aligned_data(
        dataset=case.dataset,
        window=case.window,
        selected_columns=case.fields,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )

    if frame.height == 0:
        raise RuntimeError(f'S6-C7 proof expected non-empty frame for case={case.case_id}')
    if tuple(frame.columns) != case.fields:
        raise RuntimeError(
            f'S6-C7 proof expected projected columns={case.fields}, '
            f'got={tuple(frame.columns)} for case={case.case_id}'
        )

    _assert_sources(frame)
    _assert_ordering(case, frame)
    _assert_second_alignment(frame)

    if isinstance(case.window, TimeRangeWindow):
        _assert_forward_fill_intervals(frame)
        envelope = query_aligned_wide_rows_envelope(
            dataset=case.dataset,
            select_cols=list(case.fields),
            time_range=(case.window.start_iso, case.window.end_iso),
            auth_token=None,
        )
    else:
        envelope = query_aligned_wide_rows_envelope(
            dataset=case.dataset,
            select_cols=list(case.fields),
            n_rows=case.window.rows,
            auth_token=None,
        )

    if envelope.get('mode') != 'aligned_1s':
        raise RuntimeError(f'S6-C7 proof expected envelope mode=aligned_1s for {case.case_id}')
    if envelope.get('source') != case.dataset:
        raise RuntimeError(
            f'S6-C7 proof expected envelope source={case.dataset} for {case.case_id}'
        )
    row_count = envelope.get('row_count')
    if not isinstance(row_count, int) or row_count <= 0:
        raise RuntimeError(
            f'S6-C7 proof expected envelope row_count>0 for case={case.case_id}'
        )

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
        'case_id': case.case_id,
        'dataset': case.dataset,
        'execution_path': plan.execution_path,
        'window': window_payload,
        'row_count': frame.height,
        'columns': frame.columns,
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def run_s6_c7_proof() -> dict[str, Any]:
    case_results = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 S6-C7 integrate FRED into aligned-1s materialization path',
        'case_count': len(case_results),
        'cases': case_results,
    }


def main() -> None:
    result = run_s6_c7_proof()
    output_path = Path(
        'spec/slices/slice-6-fred-integration/capability-proof-s6-c7-fred-aligned-query.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
