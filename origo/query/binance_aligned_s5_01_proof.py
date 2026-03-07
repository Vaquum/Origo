from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .binance_aligned_1s import BinanceAlignedDataset, query_binance_aligned_1s_data
from .native_core import LatestRowsWindow

_DATASETS: tuple[BinanceAlignedDataset, ...] = (
    'spot_trades',
    'spot_agg_trades',
    'futures_trades',
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


def _validate_aligned_frame(*, dataset: str, frame: pl.DataFrame) -> dict[str, Any]:
    required_columns = [
        'aligned_at_utc',
        'open_price',
        'high_price',
        'low_price',
        'close_price',
        'quantity_sum',
        'quote_volume_sum',
        'trade_count',
    ]
    missing_columns = sorted(set(required_columns).difference(frame.columns))
    if len(missing_columns) > 0:
        raise RuntimeError(
            f'S5-C1 proof expected required aligned columns for {dataset}, '
            f'missing={missing_columns}'
        )

    row_count = frame.height
    if row_count == 0:
        raise RuntimeError(f'S5-C1 proof expected non-empty aligned rows for {dataset}')

    aligned_ms = frame.select(pl.col('aligned_at_utc').cast(pl.Int64)).to_series().to_list()
    if aligned_ms != sorted(aligned_ms):
        raise RuntimeError(f'S5-C1 proof expected aligned_at_utc ascending sort for {dataset}')

    if any((value % 1000) != 0 for value in aligned_ms):
        raise RuntimeError(
            f'S5-C1 proof expected second-aligned timestamps for {dataset}'
        )

    rows = _json_ready_rows(frame.to_dicts())
    return {
        'dataset': dataset,
        'row_count': row_count,
        'schema': [
            {'name': name, 'dtype': str(dtype)} for name, dtype in frame.schema.items()
        ],
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def run_s5_01_proof() -> dict[str, Any]:
    dataset_summaries: list[dict[str, Any]] = []
    for dataset in _DATASETS:
        frame = query_binance_aligned_1s_data(
            dataset=dataset,
            window=LatestRowsWindow(rows=25),
            auth_token=None,
            show_summary=False,
            datetime_iso_output=False,
        )
        dataset_summaries.append(_validate_aligned_frame(dataset=dataset, frame=frame))

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C1 Binance aligned-1s materialization definitions',
        'window': {'latest_rows': 25},
        'datasets': dataset_summaries,
    }


def main() -> None:
    result = run_s5_01_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c1-binance-aligned.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
