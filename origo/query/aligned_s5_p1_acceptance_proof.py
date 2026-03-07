from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from origo.data._internal.generic_endpoints import query_aligned_wide_rows_envelope
from origo.query.aligned_core import (
    AlignedDataset,
    build_aligned_query_plan,
    query_aligned_data,
)
from origo.query.native_core import LatestRowsWindow, TimeRangeWindow


@dataclass(frozen=True)
class _AcceptanceCase:
    case_id: str
    dataset: AlignedDataset
    fields: tuple[str, ...]
    window: TimeRangeWindow | LatestRowsWindow


_CASES: tuple[_AcceptanceCase, ...] = (
    _AcceptanceCase(
        case_id='binance_spot_time_range',
        dataset='spot_trades',
        fields=('aligned_at_utc', 'open_price', 'close_price', 'trade_count'),
        window=TimeRangeWindow(
            start_iso='2017-08-17T12:00:00Z',
            end_iso='2017-08-17T13:00:00Z',
        ),
    ),
    _AcceptanceCase(
        case_id='binance_futures_latest_rows',
        dataset='futures_trades',
        fields=('aligned_at_utc', 'high_price', 'low_price', 'quote_volume_sum'),
        window=LatestRowsWindow(rows=25),
    ),
    _AcceptanceCase(
        case_id='etf_forward_fill_time_range',
        dataset='etf_daily_metrics',
        fields=(
            'source_id',
            'metric_name',
            'valid_from_utc',
            'valid_to_utc_exclusive',
            'metric_value_string',
        ),
        window=TimeRangeWindow(
            start_iso='2026-03-05T12:00:00Z',
            end_iso='2026-03-07T12:00:00Z',
        ),
    ),
    _AcceptanceCase(
        case_id='etf_observation_latest_rows',
        dataset='etf_daily_metrics',
        fields=('aligned_at_utc', 'source_id', 'metric_name', 'metric_value_string'),
        window=LatestRowsWindow(rows=25),
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


def _run_case(case: _AcceptanceCase) -> dict[str, Any]:
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
        raise RuntimeError(f'S5-P1 expected non-empty frame for case={case.case_id}')
    if tuple(frame.columns) != case.fields:
        raise RuntimeError(
            f'S5-P1 expected projected columns={case.fields}, '
            f'got={tuple(frame.columns)} for case={case.case_id}'
        )

    if isinstance(case.window, TimeRangeWindow):
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
        raise RuntimeError(f'S5-P1 expected envelope mode=aligned_1s for {case.case_id}')
    if envelope.get('source') != case.dataset:
        raise RuntimeError(
            f'S5-P1 expected envelope source={case.dataset} for {case.case_id}'
        )
    row_count = envelope.get('row_count')
    if not isinstance(row_count, int) or row_count <= 0:
        raise RuntimeError(
            f'S5-P1 expected envelope row_count>0 for case={case.case_id}'
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


def run_s5_p1_acceptance_proof() -> dict[str, Any]:
    case_results = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 P1 aligned acceptance scenarios across Binance + ETF windows',
        'case_count': len(case_results),
        'cases': case_results,
    }


def main() -> None:
    result = run_s5_p1_acceptance_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p1-acceptance.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
