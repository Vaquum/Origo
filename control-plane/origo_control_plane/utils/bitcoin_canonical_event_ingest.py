from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from clickhouse_driver import Client as ClickhouseClient

from origo.events import (
    CanonicalBackfillStateStore,
    CanonicalStreamKey,
    PartitionSourceProof,
    SourceIdentityMaterial,
    build_partition_source_proof,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo.events.errors import ReconciliationError
from origo.events.quarantine import NoopStreamQuarantineRegistry
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


@dataclass(frozen=True)
class BitcoinPartitionBackfillSummary:
    rows_processed: int
    rows_inserted: int
    rows_duplicate: int
    write_path: Literal['writer', 'reconcile_proof_only']
    partition_proof_state: str
    partition_proof_digest_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            'rows_processed': self.rows_processed,
            'rows_inserted': self.rows_inserted,
            'rows_duplicate': self.rows_duplicate,
            'write_path': self.write_path,
            'partition_proof_state': self.partition_proof_state,
            'partition_proof_digest_sha256': self.partition_proof_digest_sha256,
        }


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

    writer = CanonicalEventWriter(
        client=client,
        database=database,
        quarantine_registry=NoopStreamQuarantineRegistry(),
    )
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


def build_bitcoin_partition_source_proof_or_raise(
    *,
    stream_id: str,
    partition_id: str,
    offset_ordering: Literal['numeric', 'lexicographic', 'opaque'],
    source_artifact_identity: dict[str, Any],
    events: list[BitcoinCanonicalEvent],
    allow_empty_partition: bool,
) -> PartitionSourceProof:
    normalized_stream_id = _require_non_empty(stream_id, label='stream_id')
    normalized_partition_id = _require_non_empty(partition_id, label='partition_id')

    materials: list[SourceIdentityMaterial] = []
    for event in events:
        if event.stream_id != normalized_stream_id:
            raise RuntimeError(
                'Bitcoin partition source proof stream mismatch: '
                f'expected={normalized_stream_id} actual={event.stream_id}'
            )
        if event.partition_id != normalized_partition_id:
            raise RuntimeError(
                'Bitcoin partition source proof partition mismatch: '
                f'expected={normalized_partition_id} actual={event.partition_id}'
            )
        source_offset = _require_non_empty(
            event.source_offset_or_equivalent,
            label='event.source_offset_or_equivalent',
        )
        payload_json = canonical_json_string(event.payload)
        payload_sha256_raw = hashlib.sha256(payload_json.encode(_PAYLOAD_ENCODING)).hexdigest()
        event_key = canonical_event_idempotency_key(
            source_id=_SOURCE_ID,
            stream_id=normalized_stream_id,
            partition_id=normalized_partition_id,
            source_offset_or_equivalent=source_offset,
        )
        materials.append(
            SourceIdentityMaterial(
                source_offset_or_equivalent=source_offset,
                event_id=str(canonical_event_id_from_key(event_key)),
                payload_sha256_raw=payload_sha256_raw,
            )
        )

    return build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=normalized_stream_id,
            partition_id=normalized_partition_id,
        ),
        offset_ordering=offset_ordering,
        source_artifact_identity=source_artifact_identity,
        materials=materials,
        allow_empty_partition=allow_empty_partition,
        allow_duplicate_offsets=False,
    )


def execute_bitcoin_partition_backfill_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    source_proof: PartitionSourceProof,
    events: list[BitcoinCanonicalEvent],
    run_id: str,
    ingested_at_utc: datetime,
    execution_mode: Literal['backfill', 'reconcile'],
) -> BitcoinPartitionBackfillSummary:
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    try:
        state_store.assert_partition_can_execute_or_raise(
            stream_key=source_proof.stream_key,
            execution_mode=execution_mode,
        )
    except ReconciliationError as exc:
        if execution_mode == 'backfill' and exc.code == 'RECONCILE_REQUIRED':
            state_store.record_partition_state(
                source_proof=source_proof,
                state='reconcile_required',
                reason='backfill_execution_requires_reconcile',
                run_id=run_id,
                recorded_at_utc=datetime.now(UTC),
                proof_details={'trigger_message': exc.message},
            )
        raise

    execution_assessment = state_store.assess_partition_execution(
        stream_key=source_proof.stream_key
    )
    prove_existing_canonical_without_write = (
        execution_mode == 'reconcile' and execution_assessment.canonical_row_count > 0
    )

    source_manifested_at_utc = datetime.now(UTC)
    state_store.record_source_manifest(
        source_proof=source_proof,
        run_id=run_id,
        manifested_at_utc=source_manifested_at_utc,
    )
    state_store.record_partition_state(
        source_proof=source_proof,
        state='source_manifested',
        reason='source_manifest_recorded',
        run_id=run_id,
        recorded_at_utc=source_manifested_at_utc,
    )

    if prove_existing_canonical_without_write:
        write_summary = {
            'rows_processed': source_proof.source_row_count,
            'rows_inserted': 0,
            'rows_duplicate': source_proof.source_row_count,
        }
        proof_reason = 'reconcile_existing_canonical_rows_detected'
        write_path: Literal['writer', 'reconcile_proof_only'] = 'reconcile_proof_only'
    else:
        write_summary = write_bitcoin_events_to_canonical(
            client=client,
            database=database,
            events=events,
            run_id=run_id,
            ingested_at_utc=ingested_at_utc,
        )
        proof_reason = 'canonical_write_completed'
        write_path = 'writer'

    rows_processed = int(write_summary['rows_processed'])
    rows_inserted = int(write_summary['rows_inserted'])
    rows_duplicate = int(write_summary['rows_duplicate'])
    if rows_processed != len(events):
        raise RuntimeError(
            'Bitcoin partition canonical writer summary mismatch: '
            f'rows_processed={rows_processed} expected={len(events)}'
        )
    if rows_inserted + rows_duplicate != rows_processed:
        raise RuntimeError(
            'Bitcoin partition canonical writer summary mismatch: '
            f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
            f'rows_processed={rows_processed}'
        )

    proof_recorded_at_utc = datetime.now(UTC)
    state_store.record_partition_state(
        source_proof=source_proof,
        state='canonical_written_unproved',
        reason=proof_reason,
        run_id=run_id,
        recorded_at_utc=proof_recorded_at_utc,
    )
    partition_proof = state_store.prove_partition_or_quarantine(
        source_proof=source_proof,
        run_id=run_id,
        recorded_at_utc=proof_recorded_at_utc,
    )
    return BitcoinPartitionBackfillSummary(
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        rows_duplicate=rows_duplicate,
        write_path=write_path,
        partition_proof_state=partition_proof.state,
        partition_proof_digest_sha256=partition_proof.proof_digest_sha256,
    )
