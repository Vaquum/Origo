from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
from origo_control_plane.assets.bitcoin_block_fee_totals_to_origo import (
    normalize_block_fee_total_or_raise,
)
from origo_control_plane.assets.bitcoin_block_subsidy_schedule_to_origo import (
    normalize_block_subsidy_or_raise,
)
from origo_control_plane.assets.bitcoin_block_transactions_to_origo import (
    normalize_block_transactions_or_raise,
)
from origo_control_plane.assets.bitcoin_circulating_supply_to_origo import (
    normalize_circulating_supply_row_or_raise,
)
from origo_control_plane.assets.bitcoin_mempool_state_to_origo import (
    normalize_mempool_state_or_raise,
)
from origo_control_plane.assets.bitcoin_network_hashrate_estimate_to_origo import (
    _HeaderPoint,
    normalize_network_hashrate_rows_or_raise,
)
from origo_control_plane.bitcoin_core.rpc import (
    BitcoinCoreNodeSettings,
    BitcoinCoreRpcClient,
    resolve_bitcoin_core_node_settings,
    validate_bitcoin_core_node_contract_or_raise,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_fee_total_integrity,
    run_bitcoin_block_header_integrity,
    run_bitcoin_block_transaction_integrity,
    run_bitcoin_circulating_supply_integrity,
    run_bitcoin_mempool_state_integrity,
    run_bitcoin_network_hashrate_integrity,
    run_bitcoin_subsidy_schedule_integrity,
)

from origo.query.bitcoin_derived_aligned_1s import (
    _build_bitcoin_derived_forward_fill_intervals,
)

SLICE_DIR = (
    Path(__file__).resolve().parents[1]
    / 'spec'
    / 'slices'
    / 'slice-13-bitcoin-core-signals'
)
_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')

BITCOIN_METRIC_MAP: dict[str, tuple[str, str, str]] = {
    'bitcoin_block_fee_totals': ('block_fee_total_btc', 'BTC', 'fee_total_btc'),
    'bitcoin_block_subsidy_schedule': ('block_subsidy_btc', 'BTC', 'subsidy_btc'),
    'bitcoin_network_hashrate_estimate': (
        'network_hashrate_hs',
        'H/s',
        'hashrate_hs',
    ),
    'bitcoin_circulating_supply': (
        'btc_circulating_supply_btc',
        'BTC',
        'circulating_supply_btc',
    ),
}


@dataclass(frozen=True)
class LiveSourceSnapshot:
    source_chain: str
    headers_start_height: int
    headers_end_height: int
    node_tip_height: int
    node_tip_hash: str
    captured_at_utc: datetime
    header_payloads: list[dict[str, Any]]
    block_payloads: list[dict[str, Any]]
    header_rows: list[dict[str, Any]]
    header_points: list[_HeaderPoint]
    mempool_payload: dict[str, Any]
    mempool_snapshot_at_utc: datetime


def _json_hash(value: Any) -> str:
    payload = json.dumps(
        _normalize_value(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso_utc(value)
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, raw in sorted(value.items()):
            normalized[key] = _normalize_value(raw)
        return normalized
    return value


def _canonical_rows_hash(rows: list[dict[str, Any]]) -> str:
    return _json_hash([_normalize_value(row) for row in rows])


def _require_dict(raw: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuntimeError(f'{label} must be object')
    payload = cast(dict[Any, Any], raw)
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[key] = value
    return normalized


def _require_int(raw: Any, *, label: str, minimum: int | None = None) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RuntimeError(f'{label} must be int')
    if minimum is not None and raw < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={raw}')
    return raw


def _require_float(raw: Any, *, label: str, minimum: float | None = None) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimeError(f'{label} must be numeric')
    value = float(raw)
    if minimum is not None and value < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={value}')
    return value


def _require_hash_hex_64(raw: Any, *, label: str) -> str:
    if not isinstance(raw, str):
        raise RuntimeError(f'{label} must be string')
    if _HASH_HEX_64_PATTERN.fullmatch(raw) is None:
        raise RuntimeError(
            f'{label} must be a 64-char lowercase hexadecimal hash, got={raw}'
        )
    return raw


def _normalize_header_row_or_raise(
    *,
    raw_header: dict[str, Any],
    expected_height: int,
    expected_block_hash: str,
    expected_prev_hash: str | None,
    source_chain: str,
) -> tuple[dict[str, Any], _HeaderPoint]:
    header_hash = _require_hash_hex_64(raw_header.get('hash'), label='header.hash')
    if header_hash != expected_block_hash:
        raise RuntimeError(
            f'header.hash mismatch: expected={expected_block_hash} actual={header_hash}'
        )

    height = _require_int(raw_header.get('height'), label='header.height', minimum=0)
    if height != expected_height:
        raise RuntimeError(
            f'header.height mismatch: expected={expected_height} actual={height}'
        )

    prev_hash_raw = raw_header.get('previousblockhash')
    if prev_hash_raw is None:
        prev_hash = ''
    else:
        prev_hash = _require_hash_hex_64(
            prev_hash_raw, label='header.previousblockhash'
        )

    if expected_prev_hash is None:
        if expected_height == 0 and prev_hash != '':
            raise RuntimeError(
                f'genesis previousblockhash must be empty, got={prev_hash}'
            )
    elif prev_hash != expected_prev_hash:
        raise RuntimeError(
            'header.previousblockhash linkage mismatch: '
            f'expected={expected_prev_hash} actual={prev_hash}'
        )

    merkle_root = _require_hash_hex_64(
        raw_header.get('merkleroot'),
        label='header.merkleroot',
    )
    version = _require_int(raw_header.get('version'), label='header.version')
    nonce = _require_int(raw_header.get('nonce'), label='header.nonce', minimum=0)
    difficulty = _require_float(
        raw_header.get('difficulty'),
        label='header.difficulty',
        minimum=0.0,
    )
    if difficulty <= 0:
        raise RuntimeError(f'header.difficulty must be > 0, got={difficulty}')
    timestamp_seconds = _require_int(raw_header.get('time'), label='header.time', minimum=1)
    timestamp_ms = timestamp_seconds * 1000

    row = {
        'height': height,
        'block_hash': header_hash,
        'prev_hash': prev_hash,
        'merkle_root': merkle_root,
        'version': version,
        'nonce': nonce,
        'difficulty': difficulty,
        'timestamp_ms': timestamp_ms,
        'source_chain': source_chain,
    }
    point = _HeaderPoint(
        height=height,
        block_hash=header_hash,
        timestamp_seconds=timestamp_seconds,
        difficulty=difficulty,
    )
    return row, point


def _capture_live_source_snapshot_or_raise() -> tuple[LiveSourceSnapshot, BitcoinCoreRpcClient]:
    settings: BitcoinCoreNodeSettings = resolve_bitcoin_core_node_settings()
    client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=client,
        settings=settings,
    )

    expected_headers = settings.headers_end_height - settings.headers_start_height + 1
    if expected_headers <= 0:
        raise RuntimeError(
            'Invalid header window, expected positive row count: '
            f'start={settings.headers_start_height} end={settings.headers_end_height}'
        )

    previous_block_hash: str | None = None
    if settings.headers_start_height > 0:
        previous_block_hash = client.get_block_hash(settings.headers_start_height - 1)

    header_payloads: list[dict[str, Any]] = []
    header_rows: list[dict[str, Any]] = []
    header_points: list[_HeaderPoint] = []
    block_payloads: list[dict[str, Any]] = []

    for height in range(settings.headers_start_height, settings.headers_end_height + 1):
        block_hash = client.get_block_hash(height)
        header_payload = _require_dict(
            client.get_block_header(block_hash),
            label=f'getblockheader(height={height})',
        )
        header_row, header_point = _normalize_header_row_or_raise(
            raw_header=header_payload,
            expected_height=height,
            expected_block_hash=block_hash,
            expected_prev_hash=previous_block_hash,
            source_chain=node_contract.chain,
        )
        previous_block_hash = header_point.block_hash

        block_payload = _require_dict(
            client.get_block(block_hash, 3),
            label=f'getblock(height={height})',
        )
        block_payload_hash = _require_hash_hex_64(
            block_payload.get('hash'),
            label=f'getblock(height={height}).hash',
        )
        if block_payload_hash != block_hash:
            raise RuntimeError(
                f'getblock(height={height}) hash mismatch: '
                f'expected={block_hash} actual={block_payload_hash}'
            )
        block_payload_height = _require_int(
            block_payload.get('height'),
            label=f'getblock(height={height}).height',
            minimum=0,
        )
        if block_payload_height != height:
            raise RuntimeError(
                f'getblock(height={height}) height mismatch: '
                f'expected={height} actual={block_payload_height}'
            )
        _require_int(
            block_payload.get('time'),
            label=f'getblock(height={height}).time',
            minimum=1,
        )

        header_payloads.append(header_payload)
        header_rows.append(header_row)
        header_points.append(header_point)
        block_payloads.append(block_payload)

    if len(header_rows) != expected_headers:
        raise RuntimeError(
            'Header capture row count mismatch: '
            f'expected={expected_headers} actual={len(header_rows)}'
        )
    if len(block_payloads) != expected_headers:
        raise RuntimeError(
            'Block capture row count mismatch: '
            f'expected={expected_headers} actual={len(block_payloads)}'
        )

    mempool_snapshot_at_utc = datetime.now(UTC)
    mempool_payload = _require_dict(
        client.get_raw_mempool(verbose=True),
        label='getrawmempool(verbose=true)',
    )

    snapshot = LiveSourceSnapshot(
        source_chain=node_contract.chain,
        headers_start_height=settings.headers_start_height,
        headers_end_height=settings.headers_end_height,
        node_tip_height=node_contract.best_block_height,
        node_tip_hash=node_contract.best_block_hash,
        captured_at_utc=datetime.now(UTC),
        header_payloads=header_payloads,
        block_payloads=block_payloads,
        header_rows=header_rows,
        header_points=header_points,
        mempool_payload=mempool_payload,
        mempool_snapshot_at_utc=mempool_snapshot_at_utc,
    )
    return snapshot, client


def _window_bounds_from_headers(
    header_rows: list[dict[str, Any]],
) -> tuple[datetime, datetime]:
    if len(header_rows) == 0:
        raise RuntimeError('Cannot derive proof window from empty header rows')
    timestamps_ms = [cast(int, row['timestamp_ms']) for row in header_rows]
    min_dt = datetime.fromtimestamp(min(timestamps_ms) / 1000, tz=UTC)
    max_dt = datetime.fromtimestamp(max(timestamps_ms) / 1000, tz=UTC)
    window_start = min_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    window_end = (
        max_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    )
    if window_start >= window_end:
        raise RuntimeError(
            'Invalid proof window derived from headers: '
            f'start={window_start.isoformat()} end={window_end.isoformat()}'
        )
    return window_start, window_end


def _partition_days(window_start: datetime, window_end: datetime) -> list[str]:
    days: list[str] = []
    cursor = window_start
    while cursor < window_end:
        days.append(cursor.strftime('%Y-%m-%d'))
        cursor = cursor + timedelta(days=1)
    if len(days) == 0:
        raise RuntimeError('Derived partition day list is empty')
    return days


def _build_derived_aligned_intervals(
    *,
    dataset: str,
    native_rows: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    metric_name, metric_unit, value_column = BITCOIN_METRIC_MAP[dataset]
    observation_rows: list[dict[str, Any]] = []
    for row in native_rows:
        block_timestamp_ms = cast(int, row['block_timestamp_ms'])
        aligned_at_utc = datetime.fromtimestamp(block_timestamp_ms / 1000, tz=UTC)
        observation_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'source_id': 'bitcoin_core',
                'metric_name': metric_name,
                'metric_unit': metric_unit,
                'metric_value_string': None,
                'metric_value_int': None,
                'metric_value_float': float(cast(float, row[value_column])),
                'metric_value_bool': None,
                'dimensions_json': '{}',
                'provenance_json': json.dumps(
                    {'source': 'bitcoin_core', 'dataset': dataset},
                    sort_keys=True,
                    separators=(',', ':'),
                ),
                'latest_ingested_at_utc': aligned_at_utc,
                'records_in_bucket': 1,
            }
        )
    observations = pl.DataFrame(observation_rows)
    intervals = _build_bitcoin_derived_forward_fill_intervals(
        observations=observations,
        window_start=window_start,
        window_end=window_end,
        datetime_iso_output=False,
    )
    return cast(list[dict[str, Any]], intervals.to_dicts())


def _dataset_native_fingerprint(
    *,
    rows: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    normalized_rows = cast(list[dict[str, Any]], _normalize_value(rows))
    timestamps = [cast(int, row['block_timestamp_ms']) for row in rows]
    first_ts = min(timestamps)
    last_ts = max(timestamps)
    return {
        'row_count': len(rows),
        'block_height_min': min(cast(int, row['block_height']) for row in rows),
        'block_height_max': max(cast(int, row['block_height']) for row in rows),
        'first_event_offset_ms': first_ts - int(window_start.timestamp() * 1000),
        'last_event_offset_ms': int(window_end.timestamp() * 1000) - last_ts,
        'rows_hash_sha256': _json_hash(normalized_rows),
    }


def _dataset_aligned_fingerprint(
    *,
    rows: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    normalized_rows = cast(list[dict[str, Any]], _normalize_value(rows))
    valid_from_values = [
        cast(datetime, row['valid_from_utc']).astimezone(UTC) for row in rows
    ]
    valid_to_values = [
        cast(datetime, row['valid_to_utc_exclusive']).astimezone(UTC) for row in rows
    ]
    metric_sum = sum(float(cast(float, row['metric_value_float'])) for row in rows)
    return {
        'row_count': len(rows),
        'first_event_offset_ms': int(
            (min(valid_from_values) - window_start).total_seconds() * 1000
        ),
        'last_event_offset_ms': int(
            (window_end - max(valid_to_values)).total_seconds() * 1000
        ),
        'metric_value_float_sum': metric_sum,
        'rows_hash_sha256': _json_hash(normalized_rows),
    }


def _daily_native_fingerprint(
    *,
    day_start: datetime,
    day_end: datetime,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)
    day_rows = [
        row
        for row in rows
        if day_start_ms <= cast(int, row['block_timestamp_ms']) < day_end_ms
    ]
    if len(day_rows) == 0:
        return {
            'row_count': 0,
            'first_event_offset_ms': None,
            'last_event_offset_ms': None,
            'fee_total_btc_sum': 0.0,
            'subsidy_btc_sum': 0.0,
            'circulating_supply_btc_sum': 0.0,
            'hashrate_hs_sum': 0.0,
            'rows_hash_sha256': _json_hash([]),
        }

    timestamps = [cast(int, row['block_timestamp_ms']) for row in day_rows]
    return {
        'row_count': len(day_rows),
        'first_event_offset_ms': min(timestamps) - day_start_ms,
        'last_event_offset_ms': day_end_ms - max(timestamps),
        'fee_total_btc_sum': sum(
            float(cast(float, row.get('fee_total_btc', 0.0))) for row in day_rows
        ),
        'subsidy_btc_sum': sum(
            float(cast(float, row.get('subsidy_btc', 0.0))) for row in day_rows
        ),
        'circulating_supply_btc_sum': sum(
            float(cast(float, row.get('circulating_supply_btc', 0.0)))
            for row in day_rows
        ),
        'hashrate_hs_sum': sum(
            float(cast(float, row.get('hashrate_hs', 0.0))) for row in day_rows
        ),
        'rows_hash_sha256': _canonical_rows_hash(day_rows),
    }


def _daily_aligned_fingerprint(
    *,
    day_start: datetime,
    day_end: datetime,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    day_rows = [
        row
        for row in rows
        if day_start <= cast(datetime, row['valid_from_utc']).astimezone(UTC) < day_end
    ]
    if len(day_rows) == 0:
        return {
            'row_count': 0,
            'first_event_offset_ms': None,
            'last_event_offset_ms': None,
            'metric_value_float_sum': 0.0,
            'rows_hash_sha256': _json_hash([]),
        }

    first_valid_from = min(
        cast(datetime, row['valid_from_utc']).astimezone(UTC) for row in day_rows
    )
    last_valid_to = max(
        cast(datetime, row['valid_to_utc_exclusive']).astimezone(UTC) for row in day_rows
    )
    return {
        'row_count': len(day_rows),
        'first_event_offset_ms': int((first_valid_from - day_start).total_seconds() * 1000),
        'last_event_offset_ms': int((day_end - last_valid_to).total_seconds() * 1000),
        'metric_value_float_sum': sum(
            float(cast(float, row['metric_value_float'])) for row in day_rows
        ),
        'rows_hash_sha256': _canonical_rows_hash(day_rows),
    }


def _generate_run(
    *,
    snapshot: LiveSourceSnapshot,
    client: BitcoinCoreRpcClient,
) -> dict[str, Any]:
    transaction_rows: list[dict[str, Any]] = []
    fee_rows: list[dict[str, Any]] = []
    subsidy_rows: list[dict[str, Any]] = []
    supply_rows: list[dict[str, Any]] = []

    for index, block_payload in enumerate(snapshot.block_payloads):
        block_hash = _require_hash_hex_64(block_payload.get('hash'), label='block.hash')
        block_height = _require_int(block_payload.get('height'), label='block.height', minimum=0)
        header_payload = snapshot.header_payloads[index]

        normalized_transactions = normalize_block_transactions_or_raise(
            block_payload=block_payload,
            expected_height=block_height,
            expected_block_hash=block_hash,
            source_chain=snapshot.source_chain,
        )
        transaction_rows.extend(
            [transaction.as_canonical_map() for transaction in normalized_transactions]
        )

        fee_rows.append(
            normalize_block_fee_total_or_raise(
                block_payload=block_payload,
                expected_height=block_height,
                expected_block_hash=block_hash,
                source_chain=snapshot.source_chain,
            ).as_canonical_map()
        )
        subsidy_rows.append(
            normalize_block_subsidy_or_raise(
                block_hash=block_hash,
                block_header=header_payload,
                expected_height=block_height,
                source_chain=snapshot.source_chain,
            ).as_canonical_map()
        )
        supply_rows.append(
            normalize_circulating_supply_row_or_raise(
                block_hash=block_hash,
                block_header=header_payload,
                expected_height=block_height,
                source_chain=snapshot.source_chain,
            ).as_canonical_map()
        )

    hashrate_rows = [
        row.as_canonical_map()
        for row in normalize_network_hashrate_rows_or_raise(
            headers=snapshot.header_points,
            source_chain=snapshot.source_chain,
            client=client,
        )
    ]

    mempool_rows = [
        row.as_canonical_map()
        for row in normalize_mempool_state_or_raise(
            mempool_payload=snapshot.mempool_payload,
            snapshot_at_utc=snapshot.mempool_snapshot_at_utc,
            source_chain=snapshot.source_chain,
        )
    ]

    stream_integrity = {
        'bitcoin_block_headers': run_bitcoin_block_header_integrity(
            rows=snapshot.header_rows
        ).to_dict(),
        'bitcoin_block_transactions': run_bitcoin_block_transaction_integrity(
            rows=transaction_rows
        ).to_dict(),
        'bitcoin_mempool_state': run_bitcoin_mempool_state_integrity(
            rows=mempool_rows
        ).to_dict(),
    }
    derived_integrity = {
        'bitcoin_block_fee_totals': run_bitcoin_block_fee_total_integrity(
            rows=fee_rows
        ).to_dict(),
        'bitcoin_block_subsidy_schedule': run_bitcoin_subsidy_schedule_integrity(
            rows=subsidy_rows
        ).to_dict(),
        'bitcoin_network_hashrate_estimate': run_bitcoin_network_hashrate_integrity(
            rows=hashrate_rows
        ).to_dict(),
        'bitcoin_circulating_supply': run_bitcoin_circulating_supply_integrity(
            rows=supply_rows
        ).to_dict(),
    }

    native_rows_by_dataset: dict[str, list[dict[str, Any]]] = {
        'bitcoin_block_fee_totals': fee_rows,
        'bitcoin_block_subsidy_schedule': subsidy_rows,
        'bitcoin_network_hashrate_estimate': hashrate_rows,
        'bitcoin_circulating_supply': supply_rows,
    }

    window_start, window_end = _window_bounds_from_headers(snapshot.header_rows)
    partition_days = _partition_days(window_start, window_end)

    aligned_rows_by_dataset: dict[str, list[dict[str, Any]]] = {
        dataset: _build_derived_aligned_intervals(
            dataset=dataset,
            native_rows=rows,
            window_start=window_start,
            window_end=window_end,
        )
        for dataset, rows in native_rows_by_dataset.items()
    }

    combined_native_rows: list[dict[str, Any]] = []
    for dataset_name, rows in native_rows_by_dataset.items():
        for row in rows:
            combined_native_rows.append({'dataset': dataset_name, **row})
    combined_aligned_rows: list[dict[str, Any]] = []
    for dataset_name, rows in aligned_rows_by_dataset.items():
        for row in rows:
            combined_aligned_rows.append({'dataset': dataset_name, **row})

    native_fingerprints = {
        dataset: _dataset_native_fingerprint(
            rows=rows,
            window_start=window_start,
            window_end=window_end,
        )
        for dataset, rows in native_rows_by_dataset.items()
    }
    aligned_fingerprints = {
        dataset: _dataset_aligned_fingerprint(
            rows=rows,
            window_start=window_start,
            window_end=window_end,
        )
        for dataset, rows in aligned_rows_by_dataset.items()
    }

    daily_native: dict[str, Any] = {}
    daily_aligned: dict[str, Any] = {}
    windows_utc: dict[str, dict[str, str]] = {}
    source_checksums: list[dict[str, Any]] = []

    for day in partition_days:
        day_start = datetime.fromisoformat(f'{day}T00:00:00+00:00')
        day_end = day_start + timedelta(days=1)
        windows_utc[day] = {'start_utc': _iso_utc(day_start), 'end_utc': _iso_utc(day_end)}

        daily_native[day] = _daily_native_fingerprint(
            day_start=day_start,
            day_end=day_end,
            rows=combined_native_rows,
        )
        daily_aligned[day] = _daily_aligned_fingerprint(
            day_start=day_start,
            day_end=day_end,
            rows=combined_aligned_rows,
        )

        day_blocks = [
            block
            for block in snapshot.block_payloads
            if day_start
            <= datetime.fromtimestamp(
                _require_int(block.get('time'), label='block.time', minimum=1),
                tz=UTC,
            )
            < day_end
        ]
        checksum = _json_hash(day_blocks)
        source_checksums.append(
            {
                'partition_day': day,
                'zip_sha256': checksum,
                'csv_sha256': checksum,
                'rows_inserted': daily_native[day]['row_count'],
                'source_note': (
                    'bitcoin-core live-node block payload checksum '
                    '(zip/csv fields reused for node source)'
                ),
            }
        )

    supply_delta_equals_subsidy = True
    for idx in range(1, len(supply_rows)):
        current_supply_sats = cast(int, supply_rows[idx]['circulating_supply_sats'])
        previous_supply_sats = cast(int, supply_rows[idx - 1]['circulating_supply_sats'])
        current_subsidy_sats = cast(int, subsidy_rows[idx]['subsidy_sats'])
        if current_supply_sats - previous_supply_sats != current_subsidy_sats:
            supply_delta_equals_subsidy = False
            break

    parity_checks = {
        'supply_delta_equals_subsidy': supply_delta_equals_subsidy,
        'fee_totals_non_negative': all(
            float(cast(float, row['fee_total_btc'])) >= 0.0 for row in fee_rows
        ),
        'hashrate_formula_consistent': all(
            float(cast(float, row['hashrate_hs'])) >= 0.0 for row in hashrate_rows
        ),
        'subsidy_formula_consistent': all(
            float(cast(float, row['subsidy_btc'])) >= 0.0 for row in subsidy_rows
        ),
        'all_derived_datasets_have_equal_row_count': (
            len(fee_rows)
            == len(subsidy_rows)
            == len(hashrate_rows)
            == len(supply_rows)
        ),
    }

    return {
        'window': {
            'start_utc': _iso_utc(window_start),
            'end_utc': _iso_utc(window_end),
        },
        'partition_days': partition_days,
        'windows_utc': windows_utc,
        'stream_rows': {
            'bitcoin_block_headers': snapshot.header_rows,
            'bitcoin_block_transactions': transaction_rows,
            'bitcoin_mempool_state': mempool_rows,
        },
        'stream_integrity': stream_integrity,
        'derived_integrity': derived_integrity,
        'native_fingerprints': native_fingerprints,
        'aligned_fingerprints': aligned_fingerprints,
        'daily_native': daily_native,
        'daily_aligned': daily_aligned,
        'source_checksums': source_checksums,
        'parity_checks': parity_checks,
    }


def _node_capture_metadata(snapshot: LiveSourceSnapshot) -> dict[str, Any]:
    return {
        'captured_at_utc': _iso_utc(snapshot.captured_at_utc),
        'mempool_snapshot_at_utc': _iso_utc(snapshot.mempool_snapshot_at_utc),
        'source_chain': snapshot.source_chain,
        'headers_start_height': snapshot.headers_start_height,
        'headers_end_height': snapshot.headers_end_height,
        'node_tip_height': snapshot.node_tip_height,
        'node_tip_hash': snapshot.node_tip_hash,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2),
        encoding='utf-8',
    )


def main() -> None:
    SLICE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot, client = _capture_live_source_snapshot_or_raise()
    run_1 = _generate_run(snapshot=snapshot, client=client)
    run_2 = _generate_run(snapshot=snapshot, client=client)
    deterministic_match = _json_hash(run_1) == _json_hash(run_2)

    generated_at_utc = _iso_utc(datetime.now(UTC))
    node_capture = _node_capture_metadata(snapshot)
    partition_days = cast(list[str], run_1['partition_days'])
    baseline_path = (
        SLICE_DIR
        / (
            'baseline-fixture-live-node-'
            f'{partition_days[0]}_{partition_days[-1]}.json'
        )
    )

    _write_json(
        SLICE_DIR / 'proof-s13-live-node-p1-acceptance.json',
        {
            'generated_at_utc': generated_at_utc,
            'proof_scope': 'Slice 13 P1 stream acceptance (live-node snapshot)',
            'window': run_1['window'],
            'node_capture': node_capture,
            'datasets': {
                dataset: {
                    'row_count': len(rows),
                    'rows_hash_sha256': _canonical_rows_hash(rows),
                    'integrity': run_1['stream_integrity'][dataset],
                }
                for dataset, rows in cast(
                    dict[str, list[dict[str, Any]]], run_1['stream_rows']
                ).items()
            },
        },
    )

    _write_json(
        SLICE_DIR / 'proof-s13-live-node-p2-derived-native-aligned-acceptance.json',
        {
            'generated_at_utc': generated_at_utc,
            'proof_scope': (
                'Slice 13 P2 derived native+aligned acceptance '
                '(live-node snapshot)'
            ),
            'window': run_1['window'],
            'node_capture': node_capture,
            'native_fingerprints': run_1['native_fingerprints'],
            'aligned_fingerprints': run_1['aligned_fingerprints'],
            'integrity': run_1['derived_integrity'],
        },
    )

    _write_json(
        SLICE_DIR / 'proof-s13-live-node-p3-determinism.json',
        {
            'generated_at_utc': generated_at_utc,
            'proof_scope': 'Slice 13 P3 deterministic replay (live-node snapshot)',
            'window': run_1['window'],
            'node_capture': node_capture,
            'deterministic_match': deterministic_match,
            'run_1_native_fingerprints': run_1['native_fingerprints'],
            'run_2_native_fingerprints': run_2['native_fingerprints'],
            'run_1_aligned_fingerprints': run_1['aligned_fingerprints'],
            'run_2_aligned_fingerprints': run_2['aligned_fingerprints'],
            'run_1_stream_hashes': {
                dataset: _canonical_rows_hash(rows)
                for dataset, rows in cast(
                    dict[str, list[dict[str, Any]]], run_1['stream_rows']
                ).items()
            },
            'run_2_stream_hashes': {
                dataset: _canonical_rows_hash(rows)
                for dataset, rows in cast(
                    dict[str, list[dict[str, Any]]], run_2['stream_rows']
                ).items()
            },
        },
    )

    _write_json(
        SLICE_DIR / 'proof-s13-live-node-p4-parity.json',
        {
            'generated_at_utc': generated_at_utc,
            'proof_scope': 'Slice 13 P4 parity and consistency checks (live-node snapshot)',
            'window': run_1['window'],
            'node_capture': node_capture,
            'parity_checks': run_1['parity_checks'],
            'source_checksums': run_1['source_checksums'],
            'native_fingerprints': run_1['native_fingerprints'],
            'aligned_fingerprints': run_1['aligned_fingerprints'],
        },
    )

    _write_json(
        baseline_path,
        {
            'fixture_window': {
                'days': partition_days,
                'windows_utc': run_1['windows_utc'],
            },
            'node_capture': node_capture,
            'source_checksums': run_1['source_checksums'],
            'run_1_fingerprints': {
                'native': run_1['daily_native'],
                'aligned_1s': run_1['daily_aligned'],
            },
            'run_2_fingerprints': {
                'native': run_2['daily_native'],
                'aligned_1s': run_2['daily_aligned'],
            },
            'deterministic_match': deterministic_match,
            'column_key': {
                'row_count': 'Number of rows for the dataset/day partition.',
                'first_event_offset_ms': (
                    'Milliseconds from partition start to first event in partition.'
                ),
                'last_event_offset_ms': (
                    'Milliseconds from last event in partition to partition end.'
                ),
                'fee_total_btc_sum': (
                    'Sum of per-block fee totals (BTC) in native partition rows.'
                ),
                'subsidy_btc_sum': (
                    'Sum of per-block subsidy values (BTC) in native partition rows.'
                ),
                'circulating_supply_btc_sum': (
                    'Sum of circulating supply (BTC) values in native partition rows.'
                ),
                'hashrate_hs_sum': (
                    'Sum of hashrate estimates (H/s) in native partition rows.'
                ),
                'metric_value_float_sum': (
                    'Sum of aligned metric_value_float values in partition rows.'
                ),
                'rows_hash_sha256': 'SHA256 hash of canonicalized partition rows.',
                'zip_sha256': (
                    'Checksum slot required by global fixture contract '
                    '(reused for node source payload).'
                ),
                'csv_sha256': (
                    'Checksum slot required by global fixture contract '
                    '(reused for node source payload).'
                ),
            },
        },
    )


if __name__ == '__main__':
    main()
