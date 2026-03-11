from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import query_aligned, query_native
from origo_control_plane.migrations import MigrationSettings
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    bitcoin_decimal_text,
    canonical_json_string,
)
from origo_control_plane.utils.bitcoin_derived_aligned_projector import (
    project_bitcoin_block_fee_totals_aligned,
    project_bitcoin_block_subsidy_schedule_aligned,
    project_bitcoin_circulating_supply_aligned,
    project_bitcoin_network_hashrate_estimate_aligned,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_fee_totals_native,
    project_bitcoin_block_headers_native,
    project_bitcoin_block_subsidy_schedule_native,
    project_bitcoin_block_transactions_native,
    project_bitcoin_circulating_supply_native,
    project_bitcoin_mempool_state_native,
    project_bitcoin_network_hashrate_estimate_native,
)

WINDOW_START_ISO = '2024-04-20T00:00:00Z'
WINDOW_END_ISO = '2024-04-20T00:00:03Z'
FIXTURE_DAY = '2024-04-20'
INGESTED_AT_UTC = datetime(2026, 3, 10, 23, 30, 0, tzinfo=UTC)
SOURCE_ID = 'bitcoin_core'
SOURCE_CHAIN = 'main'

BITCOIN_NATIVE_DATASETS: tuple[str, ...] = (
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
)

BITCOIN_DERIVED_ALIGNED_DATASETS: tuple[str, ...] = (
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
)

SATS_PER_BTC = 100_000_000
INITIAL_SUBSIDY_SATS = 50 * SATS_PER_BTC
HALVING_INTERVAL_BLOCKS = 210_000
MAX_HALVINGS = 64
HASHRATE_CONSTANT = Decimal(2**32)

_NATIVE_QUERY_COLUMNS: dict[str, tuple[str, ...]] = {
    'bitcoin_block_headers': (
        'height',
        'block_hash',
        'prev_hash',
        'merkle_root',
        'version',
        'nonce',
        'difficulty',
        'timestamp',
        'datetime',
        'source_chain',
    ),
    'bitcoin_block_transactions': (
        'block_height',
        'block_hash',
        'block_timestamp',
        'transaction_index',
        'txid',
        'inputs',
        'outputs',
        'values',
        'scripts',
        'witness_data',
        'coinbase',
        'datetime',
        'source_chain',
    ),
    'bitcoin_mempool_state': (
        'snapshot_at',
        'snapshot_at_unix_ms',
        'txid',
        'fee_rate_sat_vb',
        'vsize',
        'first_seen_timestamp',
        'rbf_flag',
        'source_chain',
    ),
    'bitcoin_block_fee_totals': (
        'block_height',
        'block_hash',
        'block_timestamp',
        'fee_total_btc',
        'datetime',
        'source_chain',
    ),
    'bitcoin_block_subsidy_schedule': (
        'block_height',
        'block_hash',
        'block_timestamp',
        'halving_interval',
        'subsidy_sats',
        'subsidy_btc',
        'datetime',
        'source_chain',
    ),
    'bitcoin_network_hashrate_estimate': (
        'block_height',
        'block_hash',
        'block_timestamp',
        'difficulty',
        'observed_interval_seconds',
        'hashrate_hs',
        'datetime',
        'source_chain',
    ),
    'bitcoin_circulating_supply': (
        'block_height',
        'block_hash',
        'block_timestamp',
        'circulating_supply_sats',
        'circulating_supply_btc',
        'datetime',
        'source_chain',
    ),
}

_ALIGNED_QUERY_COLUMNS: tuple[str, ...] = (
    'aligned_at_utc',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'dimensions_json',
    'provenance_json',
    'latest_ingested_at_utc',
    'records_in_bucket',
    'valid_from_utc',
    'valid_to_utc_exclusive',
)

_NATIVE_NUMERIC_FIELDS: dict[str, tuple[str, ...]] = {
    'bitcoin_block_headers': ('difficulty',),
    'bitcoin_block_transactions': ('transaction_index',),
    'bitcoin_mempool_state': ('fee_rate_sat_vb', 'vsize'),
    'bitcoin_block_fee_totals': ('fee_total_btc',),
    'bitcoin_block_subsidy_schedule': ('subsidy_btc', 'subsidy_sats'),
    'bitcoin_network_hashrate_estimate': ('hashrate_hs',),
    'bitcoin_circulating_supply': ('circulating_supply_btc', 'circulating_supply_sats'),
}

_ALIGNED_NUMERIC_FIELDS: tuple[str, ...] = (
    'metric_value_float',
    'records_in_bucket',
)

_LEGACY_TABLE_BY_DATASET: dict[str, str] = {
    dataset: dataset for dataset in BITCOIN_NATIVE_DATASETS
}


@dataclass(frozen=True)
class FingerprintSpec:
    offset_label: str
    day_field: str
    numeric_fields: tuple[str, ...]


_NATIVE_FINGERPRINT_SPECS: dict[str, FingerprintSpec] = {
    'bitcoin_block_headers': FingerprintSpec(
        offset_label='height',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_block_headers'],
    ),
    'bitcoin_block_transactions': FingerprintSpec(
        offset_label='block_height:transaction_index:txid',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_block_transactions'],
    ),
    'bitcoin_mempool_state': FingerprintSpec(
        offset_label='snapshot_at_unix_ms:txid',
        day_field='snapshot_at',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_mempool_state'],
    ),
    'bitcoin_block_fee_totals': FingerprintSpec(
        offset_label='block_height',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_block_fee_totals'],
    ),
    'bitcoin_block_subsidy_schedule': FingerprintSpec(
        offset_label='block_height',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_block_subsidy_schedule'],
    ),
    'bitcoin_network_hashrate_estimate': FingerprintSpec(
        offset_label='block_height',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_network_hashrate_estimate'],
    ),
    'bitcoin_circulating_supply': FingerprintSpec(
        offset_label='block_height',
        day_field='datetime',
        numeric_fields=_NATIVE_NUMERIC_FIELDS['bitcoin_circulating_supply'],
    ),
}

_ALIGNED_FINGERPRINT_SPECS: dict[str, FingerprintSpec] = {
    dataset: FingerprintSpec(
        offset_label='metric_name:aligned_at_utc',
        day_field='aligned_at_utc',
        numeric_fields=_ALIGNED_NUMERIC_FIELDS,
    )
    for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
}


def _hex64(index: int) -> str:
    return f'{index:064x}'


def _json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _parse_iso_utc(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_clickhouse_datetime64_literal(value: str) -> str:
    parsed = _parse_iso_utc(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _subsidy_sats_for_height(height: int) -> tuple[int, int]:
    halving_interval = height // HALVING_INTERVAL_BLOCKS
    if halving_interval >= MAX_HALVINGS:
        return halving_interval, 0
    return halving_interval, INITIAL_SUBSIDY_SATS >> halving_interval


def _circulating_supply_sats_at_height(height: int) -> int:
    remaining_blocks = height + 1
    total_sats = 0
    for halving_interval in range(MAX_HALVINGS):
        subsidy_sats = INITIAL_SUBSIDY_SATS >> halving_interval
        if subsidy_sats <= 0:
            break
        blocks_in_era = min(remaining_blocks, HALVING_INTERVAL_BLOCKS)
        total_sats += blocks_in_era * subsidy_sats
        remaining_blocks -= blocks_in_era
        if remaining_blocks == 0:
            break
    return total_sats


def _build_block_header_rows() -> list[dict[str, Any]]:
    height_0 = 840_000
    height_1 = 840_001
    dt_0 = _parse_iso_utc('2024-04-20T00:00:00Z')
    dt_1 = _parse_iso_utc('2024-04-20T00:00:01Z')
    return [
        {
            'height': height_0,
            'block_hash': _hex64(100),
            'prev_hash': _hex64(99),
            'merkle_root': _hex64(200),
            'version': 805_306_368,
            'nonce': 1_234_567,
            'difficulty': float(Decimal('86000000000000.123456')),
            'timestamp': int(dt_0.timestamp() * 1000),
            'datetime': dt_0,
            'source_chain': SOURCE_CHAIN,
        },
        {
            'height': height_1,
            'block_hash': _hex64(101),
            'prev_hash': _hex64(100),
            'merkle_root': _hex64(201),
            'version': 805_306_368,
            'nonce': 1_234_568,
            'difficulty': float(Decimal('86000010000000.223456')),
            'timestamp': int(dt_1.timestamp() * 1000),
            'datetime': dt_1,
            'source_chain': SOURCE_CHAIN,
        },
    ]


def _build_block_transaction_rows(
    *,
    headers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    block_0 = headers[0]
    block_1 = headers[1]
    return [
        {
            'block_height': block_0['height'],
            'block_hash': block_0['block_hash'],
            'block_timestamp': block_0['timestamp'],
            'transaction_index': 0,
            'txid': _hex64(300),
            'inputs': _json_compact([{'coinbase': '0404ffff001d0104'}]),
            'outputs': _json_compact([{'n': 0, 'value': '3.12500000'}]),
            'values': _json_compact(
                {
                    'input_value_btc_sum': 0,
                    'output_value_btc_sum': 3.125,
                }
            ),
            'scripts': _json_compact({'asm': 'OP_PUSHBYTES_2 0x04ffff001d'}),
            'witness_data': _json_compact([]),
            'coinbase': 1,
            'datetime': block_0['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
        {
            'block_height': block_0['height'],
            'block_hash': block_0['block_hash'],
            'block_timestamp': block_0['timestamp'],
            'transaction_index': 1,
            'txid': _hex64(301),
            'inputs': _json_compact([{'txid': _hex64(999), 'vout': 0}]),
            'outputs': _json_compact([{'n': 0, 'value': '0.99900000'}]),
            'values': _json_compact(
                {
                    'input_value_btc_sum': 1,
                    'output_value_btc_sum': 0.999,
                }
            ),
            'scripts': _json_compact({'asm': 'OP_CHECKSIG'}),
            'witness_data': _json_compact(['3045022100deadbeef']),
            'coinbase': 0,
            'datetime': block_0['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
        {
            'block_height': block_1['height'],
            'block_hash': block_1['block_hash'],
            'block_timestamp': block_1['timestamp'],
            'transaction_index': 0,
            'txid': _hex64(302),
            'inputs': _json_compact([{'coinbase': '0404ffff001d0104'}]),
            'outputs': _json_compact([{'n': 0, 'value': '3.12500000'}]),
            'values': _json_compact(
                {
                    'input_value_btc_sum': 0,
                    'output_value_btc_sum': 3.125,
                }
            ),
            'scripts': _json_compact({'asm': 'OP_PUSHBYTES_2 0x04ffff001d'}),
            'witness_data': _json_compact([]),
            'coinbase': 1,
            'datetime': block_1['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
        {
            'block_height': block_1['height'],
            'block_hash': block_1['block_hash'],
            'block_timestamp': block_1['timestamp'],
            'transaction_index': 1,
            'txid': _hex64(303),
            'inputs': _json_compact([{'txid': _hex64(998), 'vout': 1}]),
            'outputs': _json_compact([{'n': 0, 'value': '1.99850000'}]),
            'values': _json_compact(
                {
                    'input_value_btc_sum': 2,
                    'output_value_btc_sum': 1.9985,
                }
            ),
            'scripts': _json_compact({'asm': 'OP_CHECKMULTISIG'}),
            'witness_data': _json_compact(['3045022100feedface']),
            'coinbase': 0,
            'datetime': block_1['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
    ]


def _build_mempool_rows() -> list[dict[str, Any]]:
    snapshot_at = _parse_iso_utc('2024-04-20T00:00:02Z')
    snapshot_at_unix_ms = int(snapshot_at.timestamp() * 1000)
    return [
        {
            'snapshot_at': snapshot_at,
            'snapshot_at_unix_ms': snapshot_at_unix_ms,
            'txid': _hex64(400),
            'fee_rate_sat_vb': float(Decimal('12.500000000000000000')),
            'vsize': 140,
            'first_seen_timestamp': int(snapshot_at.timestamp()) - 30,
            'rbf_flag': 1,
            'source_chain': SOURCE_CHAIN,
        },
        {
            'snapshot_at': snapshot_at,
            'snapshot_at_unix_ms': snapshot_at_unix_ms,
            'txid': _hex64(401),
            'fee_rate_sat_vb': float(Decimal('20.250000000000000000')),
            'vsize': 220,
            'first_seen_timestamp': int(snapshot_at.timestamp()) - 20,
            'rbf_flag': 0,
            'source_chain': SOURCE_CHAIN,
        },
    ]


def _build_block_fee_total_rows(
    *,
    headers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            'block_height': headers[0]['height'],
            'block_hash': headers[0]['block_hash'],
            'block_timestamp': headers[0]['timestamp'],
            'fee_total_btc': float(Decimal('0.001000000000000000')),
            'datetime': headers[0]['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
        {
            'block_height': headers[1]['height'],
            'block_hash': headers[1]['block_hash'],
            'block_timestamp': headers[1]['timestamp'],
            'fee_total_btc': float(Decimal('0.001500000000000000')),
            'datetime': headers[1]['datetime'],
            'source_chain': SOURCE_CHAIN,
        },
    ]


def _build_block_subsidy_rows(
    *,
    headers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for header in headers:
        height = cast(int, header['height'])
        halving_interval, subsidy_sats = _subsidy_sats_for_height(height)
        rows.append(
            {
                'block_height': height,
                'block_hash': header['block_hash'],
                'block_timestamp': header['timestamp'],
                'halving_interval': halving_interval,
                'subsidy_sats': subsidy_sats,
                'subsidy_btc': float(Decimal(subsidy_sats) / Decimal(SATS_PER_BTC)),
                'datetime': header['datetime'],
                'source_chain': SOURCE_CHAIN,
            }
        )
    return rows


def _build_hashrate_rows(
    *,
    headers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observed_intervals = [600, 590]
    rows: list[dict[str, Any]] = []
    for index, header in enumerate(headers):
        difficulty_decimal = Decimal(str(cast(float, header['difficulty'])))
        observed_interval_seconds = observed_intervals[index]
        hashrate_hs = float(
            (difficulty_decimal * HASHRATE_CONSTANT)
            / Decimal(observed_interval_seconds)
        )
        rows.append(
            {
                'block_height': header['height'],
                'block_hash': header['block_hash'],
                'block_timestamp': header['timestamp'],
                'difficulty': float(difficulty_decimal),
                'observed_interval_seconds': observed_interval_seconds,
                'hashrate_hs': hashrate_hs,
                'datetime': header['datetime'],
                'source_chain': SOURCE_CHAIN,
            }
        )
    return rows


def _build_circulating_supply_rows(
    *,
    headers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for header in headers:
        height = cast(int, header['height'])
        supply_sats = _circulating_supply_sats_at_height(height)
        rows.append(
            {
                'block_height': height,
                'block_hash': header['block_hash'],
                'block_timestamp': header['timestamp'],
                'circulating_supply_sats': supply_sats,
                'circulating_supply_btc': float(
                    Decimal(supply_sats) / Decimal(SATS_PER_BTC)
                ),
                'datetime': header['datetime'],
                'source_chain': SOURCE_CHAIN,
            }
        )
    return rows


def build_fixture_rows_by_dataset() -> dict[str, list[dict[str, Any]]]:
    headers = _build_block_header_rows()
    transactions = _build_block_transaction_rows(headers=headers)
    mempool = _build_mempool_rows()
    fee_totals = _build_block_fee_total_rows(headers=headers)
    subsidy = _build_block_subsidy_rows(headers=headers)
    hashrate = _build_hashrate_rows(headers=headers)
    supply = _build_circulating_supply_rows(headers=headers)
    return {
        'bitcoin_block_headers': headers,
        'bitcoin_block_transactions': transactions,
        'bitcoin_mempool_state': mempool,
        'bitcoin_block_fee_totals': fee_totals,
        'bitcoin_block_subsidy_schedule': subsidy,
        'bitcoin_network_hashrate_estimate': hashrate,
        'bitcoin_circulating_supply': supply,
    }


def _derive_metric_metadata(dataset: str) -> tuple[str, str, str]:
    if dataset == 'bitcoin_block_fee_totals':
        return ('block_fee_total_btc', 'BTC', 'fee_total_btc')
    if dataset == 'bitcoin_block_subsidy_schedule':
        return ('block_subsidy_btc', 'BTC', 'subsidy_btc')
    if dataset == 'bitcoin_network_hashrate_estimate':
        return ('network_hashrate_hs', 'H/s', 'hashrate_hs')
    if dataset == 'bitcoin_circulating_supply':
        return ('btc_circulating_supply_btc', 'BTC', 'circulating_supply_btc')
    raise RuntimeError(f'Unsupported derived dataset: {dataset}')


def build_fixture_canonical_events(
    *,
    rows_by_dataset: dict[str, list[dict[str, Any]]],
) -> list[BitcoinCanonicalEvent]:
    events: list[BitcoinCanonicalEvent] = []

    for row in rows_by_dataset['bitcoin_block_headers']:
        event_time = cast(datetime, row['datetime']).astimezone(UTC)
        events.append(
            BitcoinCanonicalEvent(
                stream_id='bitcoin_block_headers',
                partition_id=event_time.date().isoformat(),
                source_offset_or_equivalent=str(cast(int, row['height'])),
                source_event_time_utc=event_time,
                payload={
                    'height': row['height'],
                    'block_hash': row['block_hash'],
                    'prev_hash': row['prev_hash'],
                    'merkle_root': row['merkle_root'],
                    'version': row['version'],
                    'nonce': row['nonce'],
                    'difficulty': bitcoin_decimal_text(
                        row['difficulty'],
                        label='bitcoin_block_headers.difficulty',
                    ),
                    'timestamp_ms': row['timestamp'],
                    'source_chain': row['source_chain'],
                },
            )
        )

    for row in rows_by_dataset['bitcoin_block_transactions']:
        event_time = cast(datetime, row['datetime']).astimezone(UTC)
        source_offset = (
            f"{cast(int, row['block_height'])}:"
            f"{cast(int, row['transaction_index'])}:"
            f"{cast(str, row['txid'])}"
        )
        events.append(
            BitcoinCanonicalEvent(
                stream_id='bitcoin_block_transactions',
                partition_id=event_time.date().isoformat(),
                source_offset_or_equivalent=source_offset,
                source_event_time_utc=event_time,
                payload={
                    'block_height': row['block_height'],
                    'block_hash': row['block_hash'],
                    'block_timestamp_ms': row['block_timestamp'],
                    'transaction_index': row['transaction_index'],
                    'txid': row['txid'],
                    'inputs_json': row['inputs'],
                    'outputs_json': row['outputs'],
                    'values_json': row['values'],
                    'scripts_json': row['scripts'],
                    'witness_data_json': row['witness_data'],
                    'coinbase': bool(cast(int, row['coinbase']) == 1),
                    'source_chain': row['source_chain'],
                },
            )
        )

    for row in rows_by_dataset['bitcoin_mempool_state']:
        snapshot_at_unix_ms = cast(int, row['snapshot_at_unix_ms'])
        txid = cast(str, row['txid'])
        event_time = cast(datetime, row['snapshot_at']).astimezone(UTC)
        events.append(
            BitcoinCanonicalEvent(
                stream_id='bitcoin_mempool_state',
                partition_id=event_time.date().isoformat(),
                source_offset_or_equivalent=f'{snapshot_at_unix_ms}:{txid}',
                source_event_time_utc=event_time,
                payload={
                    'snapshot_at_unix_ms': snapshot_at_unix_ms,
                    'txid': txid,
                    'fee_rate_sat_vb': bitcoin_decimal_text(
                        row['fee_rate_sat_vb'],
                        label='bitcoin_mempool_state.fee_rate_sat_vb',
                    ),
                    'vsize': row['vsize'],
                    'first_seen_timestamp': row['first_seen_timestamp'],
                    'rbf_flag': bool(cast(int, row['rbf_flag']) == 1),
                    'source_chain': row['source_chain'],
                },
            )
        )

    for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS:
        metric_name, metric_unit, metric_value_key = _derive_metric_metadata(dataset)
        for row in rows_by_dataset[dataset]:
            event_time = cast(datetime, row['datetime']).astimezone(UTC)
            source_offset = str(cast(int, row['block_height']))
            metric_value_text = bitcoin_decimal_text(
                row[metric_value_key],
                label=f'{dataset}.{metric_value_key}',
            )
            payload: dict[str, Any] = {
                'block_height': row['block_height'],
                'block_hash': row['block_hash'],
                'block_timestamp_ms': row['block_timestamp'],
                'source_chain': row['source_chain'],
                'metric_name': metric_name,
                'metric_unit': metric_unit,
                'metric_value': metric_value_text,
                'provenance': {
                    'source_id': SOURCE_ID,
                    'stream_id': 'bitcoin_block_headers',
                    'source_offset_or_equivalent': source_offset,
                    'block_hash': row['block_hash'],
                },
            }
            if dataset == 'bitcoin_block_fee_totals':
                payload['fee_total_btc'] = bitcoin_decimal_text(
                    row['fee_total_btc'],
                    label='bitcoin_block_fee_totals.fee_total_btc',
                )
            elif dataset == 'bitcoin_block_subsidy_schedule':
                payload['halving_interval'] = row['halving_interval']
                payload['subsidy_sats'] = row['subsidy_sats']
                payload['subsidy_btc'] = bitcoin_decimal_text(
                    row['subsidy_btc'],
                    label='bitcoin_block_subsidy_schedule.subsidy_btc',
                )
            elif dataset == 'bitcoin_network_hashrate_estimate':
                payload['difficulty'] = bitcoin_decimal_text(
                    row['difficulty'],
                    label='bitcoin_network_hashrate_estimate.difficulty',
                )
                payload['observed_interval_seconds'] = row['observed_interval_seconds']
                payload['hashrate_hs'] = bitcoin_decimal_text(
                    row['hashrate_hs'],
                    label='bitcoin_network_hashrate_estimate.hashrate_hs',
                )
            elif dataset == 'bitcoin_circulating_supply':
                payload['circulating_supply_sats'] = row['circulating_supply_sats']
                payload['circulating_supply_btc'] = bitcoin_decimal_text(
                    row['circulating_supply_btc'],
                    label='bitcoin_circulating_supply.circulating_supply_btc',
                )
            else:
                raise RuntimeError(f'Unsupported derived dataset: {dataset}')
            events.append(
                BitcoinCanonicalEvent(
                    stream_id=dataset,
                    partition_id=event_time.date().isoformat(),
                    source_offset_or_equivalent=source_offset,
                    source_event_time_utc=event_time,
                    payload=payload,
                )
            )

    return sorted(
        events,
        key=lambda event: (
            event.stream_id,
            event.partition_id,
            event.source_offset_or_equivalent,
        ),
    )


def fixture_events_by_stream(
    *,
    rows_by_dataset: dict[str, list[dict[str, Any]]],
) -> dict[str, list[BitcoinCanonicalEvent]]:
    grouped: dict[str, list[BitcoinCanonicalEvent]] = {
        stream_id: [] for stream_id in BITCOIN_NATIVE_DATASETS
    }
    for event in build_fixture_canonical_events(rows_by_dataset=rows_by_dataset):
        grouped[event.stream_id].append(event)
    return grouped


def source_checksums_from_events(
    *, events: list[BitcoinCanonicalEvent]
) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[bytes]] = {}
    for event in events:
        payload_raw = canonical_json_string(event.payload).encode('utf-8')
        day_payloads = grouped.get(event.partition_id)
        if day_payloads is None:
            grouped[event.partition_id] = [payload_raw]
        else:
            day_payloads.append(payload_raw)

    checksums: dict[str, dict[str, str]] = {}
    for day in sorted(grouped):
        digest = hashlib.sha256(b'\n'.join(sorted(grouped[day]))).hexdigest()
        checksums[day] = {
            'zip_sha256': digest,
            'csv_sha256': digest,
        }
    return checksums


def build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


@contextmanager
def override_clickhouse_database(database: str):
    previous_value = os.environ.get('CLICKHOUSE_DATABASE')
    os.environ['CLICKHOUSE_DATABASE'] = database
    try:
        yield
    finally:
        if previous_value is None:
            os.environ.pop('CLICKHOUSE_DATABASE', None)
        else:
            os.environ['CLICKHOUSE_DATABASE'] = previous_value


def seed_legacy_tables(
    *,
    client: ClickHouseClient,
    database: str,
    rows_by_dataset: dict[str, list[dict[str, Any]]],
) -> None:
    for dataset in BITCOIN_NATIVE_DATASETS:
        table_name = _LEGACY_TABLE_BY_DATASET[dataset]
        rows = rows_by_dataset[dataset]
        if rows == []:
            continue
        columns = _NATIVE_QUERY_COLUMNS[dataset]
        insert_rows: list[tuple[object, ...]] = []
        for row in rows:
            insert_rows.append(tuple(row[column] for column in columns))
        client.execute(
            f'''
            INSERT INTO {database}.{table_name}
            ({', '.join(columns)})
            VALUES
            ''',
            insert_rows,
        )


def run_native_and_aligned_projectors(
    *,
    client: ClickHouseClient,
    database: str,
    events_by_stream: dict[str, list[BitcoinCanonicalEvent]],
    run_id_prefix: str,
    projected_at_utc: datetime,
) -> dict[str, dict[str, dict[str, int]]]:
    def _partitions(stream_id: str) -> set[str]:
        return {event.partition_id for event in events_by_stream[stream_id]}

    native_summaries = {
        'bitcoin_block_headers': project_bitcoin_block_headers_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_headers'),
            run_id=f'{run_id_prefix}-headers-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_block_transactions': project_bitcoin_block_transactions_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_transactions'),
            run_id=f'{run_id_prefix}-transactions-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_mempool_state': project_bitcoin_mempool_state_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_mempool_state'),
            run_id=f'{run_id_prefix}-mempool-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_block_fee_totals': project_bitcoin_block_fee_totals_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_fee_totals'),
            run_id=f'{run_id_prefix}-fee-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_block_subsidy_schedule': project_bitcoin_block_subsidy_schedule_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_subsidy_schedule'),
            run_id=f'{run_id_prefix}-subsidy-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_network_hashrate_estimate': project_bitcoin_network_hashrate_estimate_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_network_hashrate_estimate'),
            run_id=f'{run_id_prefix}-hashrate-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
        'bitcoin_circulating_supply': project_bitcoin_circulating_supply_native(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_circulating_supply'),
            run_id=f'{run_id_prefix}-supply-native',
            projected_at_utc=projected_at_utc,
        ).to_dict(),
    }

    aligned_summaries = {
        'bitcoin_block_fee_totals': project_bitcoin_block_fee_totals_aligned(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_fee_totals'),
            run_id=f'{run_id_prefix}-fee-aligned',
            projected_at_utc=projected_at_utc + timedelta(seconds=1),
        ).to_dict(),
        'bitcoin_block_subsidy_schedule': project_bitcoin_block_subsidy_schedule_aligned(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_block_subsidy_schedule'),
            run_id=f'{run_id_prefix}-subsidy-aligned',
            projected_at_utc=projected_at_utc + timedelta(seconds=2),
        ).to_dict(),
        'bitcoin_network_hashrate_estimate': project_bitcoin_network_hashrate_estimate_aligned(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_network_hashrate_estimate'),
            run_id=f'{run_id_prefix}-hashrate-aligned',
            projected_at_utc=projected_at_utc + timedelta(seconds=3),
        ).to_dict(),
        'bitcoin_circulating_supply': project_bitcoin_circulating_supply_aligned(
            client=client,
            database=database,
            partition_ids=_partitions('bitcoin_circulating_supply'),
            run_id=f'{run_id_prefix}-supply-aligned',
            projected_at_utc=projected_at_utc + timedelta(seconds=4),
        ).to_dict(),
    }

    return {
        'native': native_summaries,
        'aligned': aligned_summaries,
    }


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat()
    if isinstance(value, float):
        with localcontext() as decimal_context:
            decimal_context.prec = max(decimal_context.prec, 80)
            quantized = Decimal(str(value)).quantize(Decimal('0.000000000001'))
        return float(quantized)
    if isinstance(value, dict):
        value_map = cast(dict[str, Any], value)
        return {key: _normalize_scalar(value_map[key]) for key in sorted(value_map)}
    if isinstance(value, list):
        return [_normalize_scalar(item) for item in cast(list[Any], value)]
    return value


def normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_rows.append({key: _normalize_scalar(row[key]) for key in sorted(row)})
    return normalized_rows


def rows_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def hash_rows_per_day(*, rows: list[dict[str, Any]], day_field: str) -> dict[str, str]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        day_value = row.get(day_field)
        if not isinstance(day_value, str):
            raise RuntimeError(f'Expected string day field {day_field}')
        day = day_value[:10]
        day_rows = grouped.get(day)
        if day_rows is None:
            grouped[day] = [row]
        else:
            day_rows.append(row)
    return {day: rows_hash(grouped[day]) for day in sorted(grouped)}


def _offset_from_row(*, dataset: str, row: dict[str, Any], aligned: bool) -> str:
    if aligned:
        metric_name = cast(str, row['metric_name'])
        aligned_at = cast(str, row['aligned_at_utc'])
        return f'{metric_name}:{aligned_at}'

    if dataset == 'bitcoin_block_headers':
        return str(row['height'])
    if dataset == 'bitcoin_block_transactions':
        return (
            f"{row['block_height']}:"
            f"{row['transaction_index']}:"
            f"{row['txid']}"
        )
    if dataset == 'bitcoin_mempool_state':
        return f"{row['snapshot_at_unix_ms']}:{row['txid']}"
    return str(row['block_height'])


def numeric_sum(*, rows: list[dict[str, Any]], fields: tuple[str, ...]) -> str:
    running = Decimal('0')
    for row in rows:
        for field in fields:
            value = row.get(field)
            if value is None:
                continue
            if isinstance(value, bool):
                raise RuntimeError(f'Boolean value not supported for numeric_sum field={field}')
            if isinstance(value, (int, float, str, Decimal)):
                running += Decimal(str(value))
            else:
                raise RuntimeError(
                    f'Unsupported numeric field type for {field}: {type(value).__name__}'
                )
    return format(running, 'f')


def fingerprint_rows(
    *,
    dataset: str,
    rows: list[dict[str, Any]],
    aligned: bool,
) -> dict[str, Any]:
    if aligned:
        spec = _ALIGNED_FINGERPRINT_SPECS[dataset]
    else:
        spec = _NATIVE_FINGERPRINT_SPECS[dataset]

    offsets = sorted(_offset_from_row(dataset=dataset, row=row, aligned=aligned) for row in rows)
    return {
        'row_count': len(rows),
        'first_offset': offsets[0] if offsets else None,
        'last_offset': offsets[-1] if offsets else None,
        'value_sum': numeric_sum(rows=rows, fields=spec.numeric_fields),
        'hash_per_day': hash_rows_per_day(rows=rows, day_field=spec.day_field),
        'rows_hash_sha256': rows_hash(rows),
    }


def query_native_rows(
    *,
    dataset: str,
    database: str,
) -> list[dict[str, Any]]:
    with override_clickhouse_database(database):
        frame = query_native(
            dataset=cast(Any, dataset),
            select_cols=list(_NATIVE_QUERY_COLUMNS[dataset]),
            time_range=(WINDOW_START_ISO, WINDOW_END_ISO),
            datetime_iso_output=False,
        )
    return normalize_rows(frame.to_dicts())


def query_aligned_rows(
    *,
    dataset: str,
    database: str,
) -> list[dict[str, Any]]:
    with override_clickhouse_database(database):
        frame = query_aligned(
            dataset=cast(Any, dataset),
            select_cols=list(_ALIGNED_QUERY_COLUMNS),
            time_range=(WINDOW_START_ISO, WINDOW_END_ISO),
            datetime_iso_output=False,
        )
    return normalize_rows(frame.to_dicts())


def _query_legacy_native_rows(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: str,
) -> list[dict[str, Any]]:
    columns = _NATIVE_QUERY_COLUMNS[dataset]
    datetime_field = 'snapshot_at' if dataset == 'bitcoin_mempool_state' else 'datetime'
    start_ch = _to_clickhouse_datetime64_literal(WINDOW_START_ISO)
    end_ch = _to_clickhouse_datetime64_literal(WINDOW_END_ISO)

    order_by = {
        'bitcoin_block_headers': 'datetime ASC, height ASC',
        'bitcoin_block_transactions': 'datetime ASC, block_height ASC, transaction_index ASC, txid ASC',
        'bitcoin_mempool_state': 'snapshot_at ASC, snapshot_at_unix_ms ASC, txid ASC',
        'bitcoin_block_fee_totals': 'datetime ASC, block_height ASC',
        'bitcoin_block_subsidy_schedule': 'datetime ASC, block_height ASC',
        'bitcoin_network_hashrate_estimate': 'datetime ASC, block_height ASC',
        'bitcoin_circulating_supply': 'datetime ASC, block_height ASC',
    }[dataset]

    rows = client.execute(
        f'''
        SELECT {', '.join(columns)}
        FROM {database}.{_LEGACY_TABLE_BY_DATASET[dataset]}
        WHERE {datetime_field} >= toDateTime64(%(start)s, 3, 'UTC')
            AND {datetime_field} < toDateTime64(%(end)s, 3, 'UTC')
        ORDER BY {order_by}
        ''',
        {'start': start_ch, 'end': end_ch},
    )

    output: list[dict[str, Any]] = []
    for row in rows:
        row_dict: dict[str, Any] = {}
        for index, column in enumerate(columns):
            row_dict[column] = row[index]
        output.append(row_dict)
    return normalize_rows(output)


def query_all_legacy_native_rows(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[str, list[dict[str, Any]]]:
    return {
        dataset: _query_legacy_native_rows(
            client=client,
            database=database,
            dataset=dataset,
        )
        for dataset in BITCOIN_NATIVE_DATASETS
    }


def _legacy_aligned_expected_row(
    *,
    dataset: str,
    row: dict[str, Any],
    valid_from_utc: datetime,
    valid_to_utc_exclusive: datetime,
) -> dict[str, Any]:
    metric_name, metric_unit, metric_value_key = _derive_metric_metadata(dataset)
    block_height = cast(int, row['block_height'])
    block_hash = cast(str, row['block_hash'])
    provenance_json = _json_compact(
        {
            'source_id': SOURCE_ID,
            'stream_id': 'bitcoin_block_headers',
            'source_offset_or_equivalent': str(block_height),
            'block_hash': block_hash,
        }
    )
    metric_value = cast(float, row[metric_value_key])
    return {
        'aligned_at_utc': cast(datetime, row['datetime']),
        'source_id': SOURCE_ID,
        'metric_name': metric_name,
        'metric_unit': metric_unit,
        'metric_value_string': None,
        'metric_value_int': None,
        'metric_value_float': metric_value,
        'metric_value_bool': None,
        'dimensions_json': '{}',
        'provenance_json': provenance_json,
        'latest_ingested_at_utc': INGESTED_AT_UTC,
        'records_in_bucket': 1,
        'valid_from_utc': valid_from_utc,
        'valid_to_utc_exclusive': valid_to_utc_exclusive,
    }


def legacy_aligned_expected_rows(
    *,
    rows_by_dataset: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    window_start = _parse_iso_utc(WINDOW_START_ISO)
    window_end = _parse_iso_utc(WINDOW_END_ISO)
    expected: dict[str, list[dict[str, Any]]] = {}

    for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS:
        dataset_rows = sorted(
            rows_by_dataset[dataset],
            key=lambda row: cast(datetime, row['datetime']),
        )
        rows: list[dict[str, Any]] = []
        for index, row in enumerate(dataset_rows):
            aligned_at_utc = cast(datetime, row['datetime'])
            next_aligned = (
                cast(datetime, dataset_rows[index + 1]['datetime'])
                if index + 1 < len(dataset_rows)
                else window_end
            )
            valid_from = max(aligned_at_utc, window_start)
            valid_to = min(next_aligned, window_end)
            if valid_to <= valid_from:
                continue
            rows.append(
                _legacy_aligned_expected_row(
                    dataset=dataset,
                    row=row,
                    valid_from_utc=valid_from,
                    valid_to_utc_exclusive=valid_to,
                )
            )
        expected[dataset] = normalize_rows(rows)

    return expected


def write_json_file(*, path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
