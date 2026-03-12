from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from origo.query.aligned_core import AlignedDataset, query_aligned_data
from origo.query.native_core import TimeRangeWindow


@dataclass(frozen=True)
class _DeterminismCase:
    case_id: str
    dataset: AlignedDataset
    fields: tuple[str, ...]
    window: TimeRangeWindow


_CASES: tuple[_DeterminismCase, ...] = (
    _DeterminismCase(
        case_id='spot_trades_fixed_window',
        dataset='binance_spot_trades',
        fields=('aligned_at_utc', 'open_price', 'close_price', 'trade_count'),
        window=TimeRangeWindow(
            start_iso='2017-08-17T12:00:00Z',
            end_iso='2017-08-17T13:00:00Z',
        ),
    ),
    _DeterminismCase(
        case_id='etf_forward_fill_fixed_window',
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


def _run_once(case: _DeterminismCase) -> tuple[int, str]:
    frame = query_aligned_data(
        dataset=case.dataset,
        window=case.window,
        selected_columns=case.fields,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )
    rows = _json_ready_rows(frame.to_dicts())
    return frame.height, _stable_hash(rows)


def _run_case(case: _DeterminismCase) -> dict[str, Any]:
    run1_count, run1_hash = _run_once(case)
    run2_count, run2_hash = _run_once(case)
    deterministic_match = run1_count == run2_count and run1_hash == run2_hash
    if not deterministic_match:
        raise RuntimeError(
            f'S5-P2 determinism mismatch for case={case.case_id}: '
            f'run1_count={run1_count}, run2_count={run2_count}, '
            f'run1_hash={run1_hash}, run2_hash={run2_hash}'
        )

    return {
        'case_id': case.case_id,
        'dataset': case.dataset,
        'window': {
            'start_iso': case.window.start_iso,
            'end_iso': case.window.end_iso,
        },
        'run_1': {'row_count': run1_count, 'rows_hash_sha256': run1_hash},
        'run_2': {'row_count': run2_count, 'rows_hash_sha256': run2_hash},
        'deterministic_match': deterministic_match,
    }


def run_s5_p2_determinism_proof() -> dict[str, Any]:
    case_results = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 P2 aligned replay determinism on fixed windows',
        'case_count': len(case_results),
        'deterministic_match_all': all(
            bool(case['deterministic_match']) for case in case_results
        ),
        'cases': case_results,
    }


def main() -> None:
    result = run_s5_p2_determinism_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p2-determinism.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
