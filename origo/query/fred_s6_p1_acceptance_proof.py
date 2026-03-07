from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from api.origo_api.schemas import RawQueryResponse
from origo.data._internal.generic_endpoints import (
    query_aligned_wide_rows_envelope,
    query_native_wide_rows_envelope,
)
from origo.query.aligned_core import (
    AlignedDataset,
    build_aligned_query_plan,
    query_aligned_data,
)
from origo.query.fred_native import query_fred_native_data
from origo.query.native_core import (
    TimeRangeWindow,
)


@dataclass(frozen=True)
class _NativeCase:
    case_id: str
    fields: tuple[str, ...]
    window: TimeRangeWindow


@dataclass(frozen=True)
class _AlignedCase:
    case_id: str
    fields: tuple[str, ...]
    dataset: AlignedDataset
    window: TimeRangeWindow


_NATIVE_CASES: tuple[_NativeCase, ...] = (
    _NativeCase(
        case_id='native_time_range_window',
        fields=(
            'source_id',
            'metric_name',
            'metric_unit',
            'metric_value_float',
            'metric_value_string',
            'observed_at_utc',
        ),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-04-01T00:00:00Z',
        ),
    ),
)

_ALIGNED_CASES: tuple[_AlignedCase, ...] = (
    _AlignedCase(
        case_id='aligned_forward_fill_time_range_window',
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


def _ensure_sources(rows: list[dict[str, Any]], label: str) -> None:
    source_ids = {str(row['source_id']) for row in rows}
    if source_ids != _EXPECTED_SOURCES:
        raise RuntimeError(
            f'S6-P1 {label} source coverage mismatch: '
            f'expected={sorted(_EXPECTED_SOURCES)} got={sorted(source_ids)}'
        )


def _ensure_rows_non_empty(rows: list[dict[str, Any]], label: str) -> None:
    if len(rows) == 0:
        raise RuntimeError(f'S6-P1 expected non-empty rows for {label}')


def _ensure_required_columns(
    rows: list[dict[str, Any]], required_columns: tuple[str, ...], label: str
) -> None:
    _ensure_rows_non_empty(rows, label)
    missing = sorted(set(required_columns).difference(rows[0].keys()))
    if len(missing) > 0:
        raise RuntimeError(f'S6-P1 {label} missing required columns: {missing}')


def _parse_rows(envelope: dict[str, Any], label: str) -> list[dict[str, Any]]:
    rows_obj = envelope.get('rows')
    if not isinstance(rows_obj, list):
        raise RuntimeError(f'S6-P1 {label} expected rows list payload')
    row_objects = cast(list[object], rows_obj)
    rows: list[dict[str, Any]] = []
    for row_obj in row_objects:
        if not isinstance(row_obj, dict):
            raise RuntimeError(f'S6-P1 {label} expected row payload objects')
        rows.append(cast(dict[str, Any], row_obj))
    return rows


def _run_native_case(case: _NativeCase) -> dict[str, Any]:
    internal_frame = query_fred_native_data(
        dataset='fred_series_metrics',
        select_columns=case.fields,
        window=case.window,
        include_datetime=True,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )

    if internal_frame.height == 0:
        raise RuntimeError(f'S6-P1 expected non-empty internal native frame for {case.case_id}')
    if tuple(internal_frame.columns) != case.fields:
        raise RuntimeError(
            f'S6-P1 native projection mismatch for {case.case_id}: '
            f'expected={case.fields} got={tuple(internal_frame.columns)}'
        )

    envelope = query_native_wide_rows_envelope(
        dataset='fred_series_metrics',
        select_cols=list(case.fields),
        time_range=(case.window.start_iso, case.window.end_iso),
        include_datetime_col=True,
        auth_token=None,
    )

    if envelope.get('mode') != 'native':
        raise RuntimeError(f'S6-P1 expected native envelope mode for {case.case_id}')
    if envelope.get('source') != 'fred_series_metrics':
        raise RuntimeError(f'S6-P1 expected native envelope source for {case.case_id}')

    response = RawQueryResponse.model_validate(envelope)
    rows = _parse_rows(response.model_dump(mode='python'), case.case_id)
    _ensure_required_columns(rows, case.fields, case.case_id)
    _ensure_sources(rows, case.case_id)

    observed_values = [str(row['observed_at_utc']) for row in rows]
    if observed_values != sorted(observed_values):
        raise RuntimeError(
            f'S6-P1 expected observed_at_utc ascending ordering for {case.case_id}'
        )

    return {
        'case_id': case.case_id,
        'mode': 'native',
        'window': {'start_iso': case.window.start_iso, 'end_iso': case.window.end_iso},
        'row_count': len(rows),
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def _run_aligned_case(case: _AlignedCase) -> dict[str, Any]:
    plan = build_aligned_query_plan(dataset=case.dataset, window=case.window)
    internal_frame = query_aligned_data(
        dataset=case.dataset,
        window=case.window,
        selected_columns=case.fields,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )

    if internal_frame.height == 0:
        raise RuntimeError(f'S6-P1 expected non-empty internal aligned frame for {case.case_id}')
    if tuple(internal_frame.columns) != case.fields:
        raise RuntimeError(
            f'S6-P1 aligned projection mismatch for {case.case_id}: '
            f'expected={case.fields} got={tuple(internal_frame.columns)}'
        )

    envelope = query_aligned_wide_rows_envelope(
        dataset=case.dataset,
        select_cols=list(case.fields),
        time_range=(case.window.start_iso, case.window.end_iso),
        auth_token=None,
    )

    if envelope.get('mode') != 'aligned_1s':
        raise RuntimeError(f'S6-P1 expected aligned envelope mode for {case.case_id}')
    if envelope.get('source') != case.dataset:
        raise RuntimeError(f'S6-P1 expected aligned envelope source for {case.case_id}')

    response = RawQueryResponse.model_validate(envelope)
    rows = _parse_rows(response.model_dump(mode='python'), case.case_id)
    _ensure_required_columns(rows, case.fields, case.case_id)
    _ensure_sources(rows, case.case_id)

    if 'valid_from_utc' not in rows[0] or 'valid_to_utc_exclusive' not in rows[0]:
        raise RuntimeError(
            f'S6-P1 expected forward-fill interval columns for {case.case_id}'
        )

    return {
        'case_id': case.case_id,
        'mode': 'aligned_1s',
        'execution_path': plan.execution_path,
        'window': {'start_iso': case.window.start_iso, 'end_iso': case.window.end_iso},
        'row_count': len(rows),
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def run_s6_p1_acceptance_proof() -> dict[str, Any]:
    native_results = [_run_native_case(case) for case in _NATIVE_CASES]
    aligned_results = [_run_aligned_case(case) for case in _ALIGNED_CASES]

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 P1 acceptance scenarios for FRED native and aligned paths',
        'native_case_count': len(native_results),
        'aligned_case_count': len(aligned_results),
        'native_cases': native_results,
        'aligned_cases': aligned_results,
    }


def main() -> None:
    result = run_s6_p1_acceptance_proof()
    output_path = Path('spec/slices/slice-6-fred-integration/proof-s6-p1-acceptance.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
