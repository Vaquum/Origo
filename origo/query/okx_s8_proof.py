from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

import polars as pl

from origo.data._internal.generic_endpoints import (
    query_aligned,
    query_aligned_wide_rows_envelope,
    query_native,
    query_native_wide_rows_envelope,
)

SLICE_DIR = Path('spec/slices/slice-8-okx-spot-trades-aligned')
INGEST_RESULTS_PATH = SLICE_DIR / 'ingest-results.json'

DAY_WINDOWS: dict[str, tuple[str, str]] = {
    # OKX daily trade files are grouped by source day in UTC+8.
    '2024-01-01': ('2023-12-31T16:00:00Z', '2024-01-01T16:00:00Z'),
    '2024-01-02': ('2024-01-01T16:00:00Z', '2024-01-02T16:00:00Z'),
}


class IngestResult(TypedDict):
    partition_day: str
    rows_inserted: int
    zip_sha256: str
    csv_sha256: str
    source_content_md5: str


def _parse_iso_utc_to_ms(value: str) -> int:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def _canonical_hash(records: list[dict[str, Any]]) -> str:
    canonical = json.dumps(records, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _float(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f'{label} must be numeric, got={value}')
    return float(value)


def _int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got={value}')
    return value


def _as_datetime_ms(value: Any, *, label: str) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.astimezone(UTC).timestamp() * 1000)
    if isinstance(value, str):
        return _parse_iso_utc_to_ms(value)
    raise RuntimeError(f'{label} must be datetime or ISO string, got={type(value)}')


def _expect_dict(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_str(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be str')
    return value


def _expect_int(value: Any, *, label: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f'{label} must be int')
    return value


def _load_ingest_results() -> list[IngestResult]:
    if not INGEST_RESULTS_PATH.exists():
        raise RuntimeError(f'Missing ingest results artifact: {INGEST_RESULTS_PATH}')
    payload = json.loads(INGEST_RESULTS_PATH.read_text(encoding='utf-8'))
    payload_obj = _expect_dict(payload, label='ingest-results payload')
    raw_results = _expect_list(
        payload_obj.get('ingest_results'),
        label='ingest-results ingest_results',
    )
    if len(raw_results) == 0:
        raise RuntimeError('ingest_results must be a non-empty list')

    results: list[IngestResult] = []
    for idx, item in enumerate(raw_results):
        item_obj = _expect_dict(item, label=f'ingest_results[{idx}]')
        partition_day = _expect_str(
            item_obj.get('partition_day'),
            label=f'ingest_results[{idx}].partition_day',
        )
        rows_inserted = _expect_int(
            item_obj.get('rows_inserted'),
            label=f'ingest_results[{idx}].rows_inserted',
        )
        zip_sha256 = _expect_str(
            item_obj.get('zip_sha256'),
            label=f'ingest_results[{idx}].zip_sha256',
        )
        csv_sha256 = _expect_str(
            item_obj.get('csv_sha256'),
            label=f'ingest_results[{idx}].csv_sha256',
        )
        source_content_md5 = _expect_str(
            item_obj.get('source_content_md5'),
            label=f'ingest_results[{idx}].source_content_md5',
        )

        results.append(
            IngestResult(
                partition_day=partition_day,
                rows_inserted=rows_inserted,
                zip_sha256=zip_sha256,
                csv_sha256=csv_sha256,
                source_content_md5=source_content_md5,
            )
        )

    return sorted(results, key=lambda result: result['partition_day'])


def _native_frame_for_window(*, start_iso: str, end_iso: str) -> pl.DataFrame:
    return query_native(
        dataset='okx_spot_trades',
        select_cols=(
            'instrument_name',
            'trade_id',
            'side',
            'price',
            'size',
            'quote_quantity',
            'timestamp',
            'datetime',
        ),
        time_range=(start_iso, end_iso),
        include_datetime_col=True,
        datetime_iso_output=False,
        show_summary=False,
        auth_token=None,
    )


def _aligned_frame_for_window(*, start_iso: str, end_iso: str) -> pl.DataFrame:
    return query_aligned(
        dataset='okx_spot_trades',
        select_cols=(
            'aligned_at_utc',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'quantity_sum',
            'quote_volume_sum',
            'trade_count',
        ),
        time_range=(start_iso, end_iso),
        datetime_iso_output=False,
        show_summary=False,
        auth_token=None,
    )


def _fingerprint_native(*, frame: pl.DataFrame, start_ms: int, end_ms: int) -> dict[str, Any]:
    if frame.height == 0:
        raise RuntimeError('native frame must be non-empty for fingerprinting')

    rows = frame.to_dicts()
    first_timestamp = _int(rows[0]['timestamp'], label='native.timestamp')
    last_timestamp = _int(rows[-1]['timestamp'], label='native.timestamp')
    first_trade_id = _int(rows[0]['trade_id'], label='native.trade_id')
    last_trade_id = _int(rows[-1]['trade_id'], label='native.trade_id')

    canonical_rows: list[dict[str, Any]] = []
    for row in rows:
        canonical_rows.append(
            {
                'timestamp': _int(row['timestamp'], label='native.timestamp'),
                'trade_id': _int(row['trade_id'], label='native.trade_id'),
                'price': _float(row['price'], label='native.price'),
                'size': _float(row['size'], label='native.size'),
                'quote_quantity': _float(
                    row['quote_quantity'], label='native.quote_quantity'
                ),
                'side': cast(str, row['side']),
            }
        )

    return {
        'row_count': frame.height,
        'first_trade_id': first_trade_id,
        'last_trade_id': last_trade_id,
        'first_event_offset_ms': first_timestamp - start_ms,
        'last_event_offset_ms': end_ms - last_timestamp,
        'size_sum': float(frame['size'].sum()),
        'quote_quantity_sum': float(frame['quote_quantity'].sum()),
        'rows_hash_sha256': _canonical_hash(canonical_rows),
    }


def _fingerprint_aligned(*, frame: pl.DataFrame, start_ms: int, end_ms: int) -> dict[str, Any]:
    if frame.height == 0:
        raise RuntimeError('aligned frame must be non-empty for fingerprinting')

    rows = frame.to_dicts()
    first_aligned_ms = _as_datetime_ms(
        rows[0]['aligned_at_utc'],
        label='aligned.aligned_at_utc',
    )
    last_aligned_ms = _as_datetime_ms(
        rows[-1]['aligned_at_utc'],
        label='aligned.aligned_at_utc',
    )

    canonical_rows: list[dict[str, Any]] = []
    for row in rows:
        canonical_rows.append(
            {
                'aligned_at_utc_ms': _as_datetime_ms(
                    row['aligned_at_utc'], label='aligned.aligned_at_utc'
                ),
                'open_price': _float(row['open_price'], label='aligned.open_price'),
                'high_price': _float(row['high_price'], label='aligned.high_price'),
                'low_price': _float(row['low_price'], label='aligned.low_price'),
                'close_price': _float(row['close_price'], label='aligned.close_price'),
                'quantity_sum': _float(row['quantity_sum'], label='aligned.quantity_sum'),
                'quote_volume_sum': _float(
                    row['quote_volume_sum'], label='aligned.quote_volume_sum'
                ),
                'trade_count': _int(row['trade_count'], label='aligned.trade_count'),
            }
        )

    return {
        'row_count': frame.height,
        'first_event_offset_ms': first_aligned_ms - start_ms,
        'last_event_offset_ms': end_ms - last_aligned_ms,
        'quantity_sum_total': float(frame['quantity_sum'].sum()),
        'quote_volume_sum_total': float(frame['quote_volume_sum'].sum()),
        'trade_count_total': int(frame['trade_count'].sum()),
        'rows_hash_sha256': _canonical_hash(canonical_rows),
    }


def _run_fingerprint_pass() -> dict[str, dict[str, dict[str, Any]]]:
    native_by_day: dict[str, dict[str, Any]] = {}
    aligned_by_day: dict[str, dict[str, Any]] = {}
    for day, (start_iso, end_iso) in DAY_WINDOWS.items():
        start_ms = _parse_iso_utc_to_ms(start_iso)
        end_ms = _parse_iso_utc_to_ms(end_iso)

        native_frame = _native_frame_for_window(start_iso=start_iso, end_iso=end_iso)
        aligned_frame = _aligned_frame_for_window(start_iso=start_iso, end_iso=end_iso)

        native_by_day[day] = _fingerprint_native(
            frame=native_frame,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        aligned_by_day[day] = _fingerprint_aligned(
            frame=aligned_frame,
            start_ms=start_ms,
            end_ms=end_ms,
        )

    return {
        'native': native_by_day,
        'aligned_1s': aligned_by_day,
    }


def _build_acceptance_proof() -> tuple[dict[str, Any], dict[str, Any]]:
    day = '2024-01-02'
    start_iso, end_iso = DAY_WINDOWS[day]

    native_envelope = query_native_wide_rows_envelope(
        dataset='okx_spot_trades',
        select_cols=['trade_id', 'timestamp', 'price', 'size', 'side'],
        time_range=(start_iso, end_iso),
        include_datetime_col=True,
        auth_token=None,
    )
    if native_envelope['mode'] != 'native' or native_envelope['source'] != 'okx_spot_trades':
        raise RuntimeError('S8-P1 native envelope mode/source mismatch')
    if native_envelope['row_count'] <= 0:
        raise RuntimeError('S8-P1 native envelope row_count must be > 0')

    aligned_envelope = query_aligned_wide_rows_envelope(
        dataset='okx_spot_trades',
        select_cols=[
            'aligned_at_utc',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'quantity_sum',
            'quote_volume_sum',
            'trade_count',
        ],
        time_range=(start_iso, end_iso),
        auth_token=None,
    )
    if aligned_envelope['mode'] != 'aligned_1s' or aligned_envelope['source'] != 'okx_spot_trades':
        raise RuntimeError('S8-P2 aligned envelope mode/source mismatch')
    if aligned_envelope['row_count'] <= 0:
        raise RuntimeError('S8-P2 aligned envelope row_count must be > 0')

    aligned_rows = cast(list[dict[str, Any]], aligned_envelope['rows'])
    if len(aligned_rows) == 0:
        raise RuntimeError('S8-P2 aligned rows must be non-empty')
    trade_count_total = 0
    for idx, row in enumerate(aligned_rows):
        trade_count = row.get('trade_count')
        if not isinstance(trade_count, int) or trade_count <= 0:
            raise RuntimeError(
                f'S8-P2 aligned trade_count must be positive int at row={idx}'
            )
        trade_count_total += trade_count

    acceptance_native = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 8 P1 native acceptance',
        'dataset': 'okx_spot_trades',
        'window': {'day': day, 'start_utc': start_iso, 'end_utc': end_iso},
        'row_count': native_envelope['row_count'],
        'schema': native_envelope['schema'],
        'rows_hash_sha256': _canonical_hash(cast(list[dict[str, Any]], native_envelope['rows'])),
    }

    acceptance_aligned = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 8 P2 aligned acceptance',
        'dataset': 'okx_spot_trades',
        'window': {'day': day, 'start_utc': start_iso, 'end_utc': end_iso},
        'row_count': aligned_envelope['row_count'],
        'trade_count_total': trade_count_total,
        'schema': aligned_envelope['schema'],
        'rows_hash_sha256': _canonical_hash(aligned_rows),
    }

    return acceptance_native, acceptance_aligned


def main() -> None:
    SLICE_DIR.mkdir(parents=True, exist_ok=True)

    ingest_results = _load_ingest_results()
    run_1 = _run_fingerprint_pass()
    run_2 = _run_fingerprint_pass()

    deterministic_match = run_1 == run_2

    acceptance_native, acceptance_aligned = _build_acceptance_proof()

    determinism_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 8 P3 native + aligned determinism replay',
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': deterministic_match,
    }

    source_checksum_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 8 P4 source checksum and row stats',
        'source_checksums': ingest_results,
        'row_stats_by_day': {
            day: {
                'native_row_count': run_1['native'][day]['row_count'],
                'aligned_row_count': run_1['aligned_1s'][day]['row_count'],
                'native_rows_hash_sha256': run_1['native'][day]['rows_hash_sha256'],
                'aligned_rows_hash_sha256': run_1['aligned_1s'][day]['rows_hash_sha256'],
            }
            for day in DAY_WINDOWS
        },
    }

    baseline_fixture_payload = {
        'fixture_window': {
            'days': list(DAY_WINDOWS.keys()),
            'windows_utc': {
                day: {
                    'start_utc': DAY_WINDOWS[day][0],
                    'end_utc': DAY_WINDOWS[day][1],
                }
                for day in DAY_WINDOWS
            },
        },
        'source_checksums': ingest_results,
        'run_1_fingerprints': run_1,
        'run_2_fingerprints': run_2,
        'deterministic_match': deterministic_match,
        'column_key': {
            'first_event_offset_ms': 'First event timestamp offset in milliseconds from window start.',
            'last_event_offset_ms': 'Milliseconds from last event timestamp to window end.',
            'first_trade_id': 'Minimum trade_id in native rows for the window.',
            'last_trade_id': 'Maximum trade_id in native rows for the window.',
            'size_sum': 'Sum of native size across rows in the window.',
            'quote_quantity_sum': 'Sum of native quote_quantity across rows in the window.',
            'quantity_sum_total': 'Sum of aligned quantity_sum across rows in the window.',
            'quote_volume_sum_total': 'Sum of aligned quote_volume_sum across rows in the window.',
            'trade_count_total': 'Sum of aligned trade_count across rows in the window.',
            'row_count': 'Number of rows returned for the exact window.',
            'rows_hash_sha256': 'SHA256 of canonicalized rows for the window.',
        },
    }

    (SLICE_DIR / 'proof-s8-p1-acceptance.json').write_text(
        json.dumps(acceptance_native, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    (SLICE_DIR / 'proof-s8-p2-aligned-acceptance.json').write_text(
        json.dumps(acceptance_aligned, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    (SLICE_DIR / 'proof-s8-p3-determinism.json').write_text(
        json.dumps(determinism_payload, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    (SLICE_DIR / 'proof-s8-p4-source-checksums.json').write_text(
        json.dumps(source_checksum_payload, indent=2, sort_keys=True),
        encoding='utf-8',
    )
    (SLICE_DIR / 'baseline-fixture-2024-01-01_2024-01-02.json').write_text(
        json.dumps(baseline_fixture_payload, indent=2, sort_keys=True),
        encoding='utf-8',
    )

    print(json.dumps({'deterministic_match': deterministic_match}, sort_keys=True))


if __name__ == '__main__':
    main()
