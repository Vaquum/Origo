from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from origo.data._internal.generic_endpoints import (
    query_aligned_wide_rows_envelope,
    query_native_wide_rows_envelope,
)
from origo.query.native_core import TimeRangeWindow

type QueryMode = Literal['native', 'aligned_1s']


@dataclass(frozen=True)
class _DeterminismCase:
    case_id: str
    mode: QueryMode
    fields: tuple[str, ...]
    window: TimeRangeWindow


_CASES: tuple[_DeterminismCase, ...] = (
    _DeterminismCase(
        case_id='fred_native_fixed_window',
        mode='native',
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
    _DeterminismCase(
        case_id='fred_aligned_fixed_window',
        mode='aligned_1s',
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


def _stable_hash(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _parse_rows(envelope: dict[str, Any], label: str) -> list[dict[str, Any]]:
    rows_obj = envelope.get('rows')
    if not isinstance(rows_obj, list):
        raise RuntimeError(f'S6-P2 {label} expected rows list')
    row_objects = cast(list[object], rows_obj)

    rows: list[dict[str, Any]] = []
    for row_obj in row_objects:
        if not isinstance(row_obj, dict):
            raise RuntimeError(f'S6-P2 {label} expected row payload objects')
        rows.append(cast(dict[str, Any], row_obj))

    if len(rows) == 0:
        raise RuntimeError(f'S6-P2 {label} expected non-empty rows')
    return rows


def _parse_schema(envelope: dict[str, Any], label: str) -> list[dict[str, str]]:
    schema_obj = envelope.get('schema')
    if not isinstance(schema_obj, list):
        raise RuntimeError(f'S6-P2 {label} expected schema list')
    schema_objects = cast(list[object], schema_obj)

    schema_items: list[dict[str, str]] = []
    for item_obj in schema_objects:
        if not isinstance(item_obj, dict):
            raise RuntimeError(f'S6-P2 {label} expected schema object items')
        item = cast(dict[str, Any], item_obj)
        name = item.get('name')
        dtype = item.get('dtype')
        if not isinstance(name, str) or not isinstance(dtype, str):
            raise RuntimeError(
                f'S6-P2 {label} expected schema items with string name/dtype'
            )
        schema_items.append({'name': name, 'dtype': dtype})

    if len(schema_items) == 0:
        raise RuntimeError(f'S6-P2 {label} expected non-empty schema')
    return schema_items


def _validate_sources(rows: list[dict[str, Any]], label: str) -> None:
    source_ids = {str(row['source_id']) for row in rows}
    if source_ids != _EXPECTED_SOURCES:
        raise RuntimeError(
            f'S6-P2 {label} source coverage mismatch: '
            f'expected={sorted(_EXPECTED_SOURCES)} got={sorted(source_ids)}'
        )


def _run_once(case: _DeterminismCase) -> dict[str, Any]:
    if case.mode == 'native':
        envelope = query_native_wide_rows_envelope(
            dataset='fred_series_metrics',
            select_cols=list(case.fields),
            time_range=(case.window.start_iso, case.window.end_iso),
            include_datetime_col=True,
            auth_token=None,
        )
    else:
        envelope = query_aligned_wide_rows_envelope(
            dataset='fred_series_metrics',
            select_cols=list(case.fields),
            time_range=(case.window.start_iso, case.window.end_iso),
            auth_token=None,
        )

    envelope_mode = envelope.get('mode')
    if envelope_mode != case.mode:
        raise RuntimeError(
            f'S6-P2 {case.case_id} expected envelope mode={case.mode} got={envelope_mode}'
        )

    rows = _parse_rows(envelope, case.case_id)
    schema = _parse_schema(envelope, case.case_id)

    row_count_obj = envelope.get('row_count')
    if not isinstance(row_count_obj, int):
        raise RuntimeError(f'S6-P2 {case.case_id} expected integer row_count')
    if row_count_obj != len(rows):
        raise RuntimeError(
            f'S6-P2 {case.case_id} row_count mismatch: '
            f'envelope={row_count_obj} rows_len={len(rows)}'
        )

    _validate_sources(rows, case.case_id)

    return {
        'row_count': row_count_obj,
        'rows_hash_sha256': _stable_hash(rows),
        'schema_hash_sha256': _stable_hash(schema),
    }


def _run_case(case: _DeterminismCase) -> dict[str, Any]:
    run_1 = _run_once(case)
    run_2 = _run_once(case)

    deterministic_match = (
        run_1['row_count'] == run_2['row_count']
        and run_1['rows_hash_sha256'] == run_2['rows_hash_sha256']
        and run_1['schema_hash_sha256'] == run_2['schema_hash_sha256']
    )
    if not deterministic_match:
        raise RuntimeError(
            f'S6-P2 determinism mismatch for case={case.case_id}: '
            f'run_1={run_1}, run_2={run_2}'
        )

    return {
        'case_id': case.case_id,
        'mode': case.mode,
        'window': {
            'start_iso': case.window.start_iso,
            'end_iso': case.window.end_iso,
        },
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': deterministic_match,
    }


def run_s6_p2_determinism_proof() -> dict[str, Any]:
    cases = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 6 P2 replay fixed windows and verify deterministic outputs',
        'case_count': len(cases),
        'deterministic_match_all': all(bool(case['deterministic_match']) for case in cases),
        'cases': cases,
    }


def main() -> None:
    result = run_s6_p2_determinism_proof()
    output_path = Path('spec/slices/slice-6-fred-integration/proof-s6-p2-determinism.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
