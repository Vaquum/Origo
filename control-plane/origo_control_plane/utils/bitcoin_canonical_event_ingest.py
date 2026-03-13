from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from clickhouse_driver import Client as ClickhouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter

_SOURCE_ID = 'bitcoin_core'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_ALLOWED_STREAM_IDS: frozenset[str] = frozenset(
    {
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)


def _require_non_empty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be set and non-empty')
    return normalized


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def bitcoin_decimal_text(value: Any, *, label: str) -> str:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be bool')
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float, str)):
        try:
            parsed = Decimal(str(value))
        except InvalidOperation as exc:
            raise RuntimeError(f'{label} must be decimal-compatible') from exc
    else:
        raise RuntimeError(
            f'{label} must be decimal-compatible, got={type(value).__name__}'
        )
    return format(parsed, 'f')


def canonical_json_string(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


@dataclass(frozen=True)
class BitcoinCanonicalEvent:
    stream_id: str
    partition_id: str
    source_offset_or_equivalent: str
    source_event_time_utc: datetime | None
    payload: dict[str, Any]


def write_bitcoin_events_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[BitcoinCanonicalEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    if len(events) == 0:
        return {
            'rows_processed': 0,
            'rows_inserted': 0,
            'rows_duplicate': 0,
        }

    writer = CanonicalEventWriter(client=client, database=database)
    normalized_ingested_at_utc = _require_utc(
        ingested_at_utc,
        label='ingested_at_utc',
    )

    write_inputs: list[CanonicalEventWriteInput] = []
    for event in events:
        stream_id = _require_non_empty(event.stream_id, label='stream_id')
        if stream_id not in _ALLOWED_STREAM_IDS:
            raise RuntimeError(
                f'Unsupported bitcoin stream_id={stream_id} for canonical event ingest'
            )
        partition_id = _require_non_empty(event.partition_id, label='partition_id')
        source_offset = _require_non_empty(
            event.source_offset_or_equivalent,
            label='source_offset_or_equivalent',
        )
        source_event_time_utc: datetime | None = None
        if event.source_event_time_utc is not None:
            source_event_time_utc = _require_utc(
                event.source_event_time_utc,
                label='source_event_time_utc',
            )
        payload_json = canonical_json_string(event.payload)
        payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
        payload_sha256_raw = hashlib.sha256(payload_raw).hexdigest()
        write_inputs.append(
            CanonicalEventWriteInput(
                source_id=_SOURCE_ID,
                stream_id=stream_id,
                partition_id=partition_id,
                source_offset_or_equivalent=source_offset,
                source_event_time_utc=source_event_time_utc,
                ingested_at_utc=normalized_ingested_at_utc,
                payload_content_type=_PAYLOAD_CONTENT_TYPE,
                payload_encoding=_PAYLOAD_ENCODING,
                payload_raw=payload_raw,
                payload_json_precanonical=payload_json,
                payload_sha256_raw_precomputed=payload_sha256_raw,
                run_id=run_id,
            )
        )
    write_results = writer.write_events(write_inputs)

    rows_inserted = 0
    rows_duplicate = 0
    for write_result in write_results:
        if write_result.status == 'inserted':
            rows_inserted += 1
        elif write_result.status == 'duplicate':
            rows_duplicate += 1
        else:
            raise RuntimeError(
                f'Unexpected canonical writer status: {write_result.status}'
            )

    rows_processed = len(events)
    if rows_inserted + rows_duplicate != rows_processed:
        raise RuntimeError(
            'Bitcoin canonical writer summary mismatch: '
            f'rows_processed={rows_processed} rows_inserted={rows_inserted} '
            f'rows_duplicate={rows_duplicate}'
        )

    return {
        'rows_processed': rows_processed,
        'rows_inserted': rows_inserted,
        'rows_duplicate': rows_duplicate,
    }
