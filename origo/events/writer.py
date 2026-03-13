from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient

from .envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from .errors import EventWriterError, StreamQuarantineError
from .ingest_state import CanonicalStreamKey
from .precision import (
    assert_payload_json_has_no_float_values,
    canonicalize_payload_json_with_precision,
)
from .quarantine import StreamQuarantineRegistry, load_stream_quarantine_state_path
from .runtime_audit import (
    CanonicalRuntimeIngestEvent,
    get_canonical_runtime_audit_log,
)

_CANONICAL_EVENT_NAMESPACE: Final[UUID] = UUID(
    'f1d1ef17-95ce-4f9f-bd81-f9958cdf8ee5'
)
_CANONICAL_EVENT_NAMESPACE_BYTES: Final[bytes] = _CANONICAL_EVENT_NAMESPACE.bytes
_CANONICAL_EVENT_LOG_COLUMNS: Final[tuple[str, ...]] = (
    'envelope_version',
    'event_id',
    'source_id',
    'stream_id',
    'partition_id',
    'source_offset_or_equivalent',
    'source_event_time_utc',
    'ingested_at_utc',
    'payload_content_type',
    'payload_encoding',
    'payload_raw',
    'payload_sha256_raw',
    'payload_json',
)
_OFFSET_LOOKUP_CHUNK_SIZE: Final[int] = 20000
_INSERT_CHUNK_SIZE: Final[int] = 100000
_RUNTIME_AUDIT_MODE_ENV: Final[str] = 'ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'
_RUNTIME_AUDIT_MODE_EVENT: Final[str] = 'event'
_RUNTIME_AUDIT_MODE_SUMMARY: Final[str] = 'summary'


def _canonicalize_content_type(content_type: str) -> str:
    normalized = content_type.strip().lower()
    if normalized == '':
        raise EventWriterError(
            code='WRITER_INVALID_CONTENT_TYPE',
            message='payload_content_type must be set and non-empty',
        )
    return normalized


def _canonicalize_encoding(encoding: str) -> str:
    normalized = encoding.strip().lower()
    if normalized == '':
        raise EventWriterError(
            code='WRITER_INVALID_ENCODING',
            message='payload_encoding must be set and non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise EventWriterError(
            code='WRITER_INVALID_TIMESTAMP',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


def _payload_json_from_raw(
    *,
    source_id: str,
    stream_id: str,
    payload_content_type: str,
    payload_encoding: str,
    payload_raw: bytes,
) -> str:
    media_type = payload_content_type.split(';')[0].strip()
    if media_type != 'application/json':
        raise EventWriterError(
            code='WRITER_UNSUPPORTED_PAYLOAD_MEDIA_TYPE',
            message='Only application/json payloads are currently supported by canonical event writer',
        )
    payload_json = canonicalize_payload_json_with_precision(
        source_id=source_id,
        stream_id=stream_id,
        payload_raw=payload_raw,
        payload_encoding=payload_encoding,
    )
    assert_payload_json_has_no_float_values(
        source_id=source_id,
        stream_id=stream_id,
        payload_json=payload_json,
    )
    return payload_json


def canonical_event_idempotency_key(
    *,
    source_id: str,
    stream_id: str,
    partition_id: str,
    source_offset_or_equivalent: str,
) -> str:
    source_key = source_id.strip()
    stream_key = stream_id.strip()
    partition_key = partition_id.strip()
    offset_key = source_offset_or_equivalent.strip()
    if source_key == '':
        raise EventWriterError(
            code='WRITER_INVALID_IDENTITY',
            message='source_id must be set and non-empty',
        )
    if stream_key == '':
        raise EventWriterError(
            code='WRITER_INVALID_IDENTITY',
            message='stream_id must be set and non-empty',
        )
    if partition_key == '':
        raise EventWriterError(
            code='WRITER_INVALID_IDENTITY',
            message='partition_id must be set and non-empty',
        )
    if offset_key == '':
        raise EventWriterError(
            code='WRITER_INVALID_IDENTITY',
            message='source_offset_or_equivalent must be set and non-empty',
        )
    return (
        f'{CANONICAL_EVENT_ENVELOPE_VERSION}|'
        f'{source_key}|{stream_key}|{partition_key}|{offset_key}'
    )


def canonical_event_id_from_key(idempotency_key: str) -> UUID:
    digest = hashlib.sha1(
        _CANONICAL_EVENT_NAMESPACE_BYTES + idempotency_key.encode('utf-8')
    ).digest()
    uuid_bytes = bytearray(digest[:16])
    uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x50
    uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80
    return UUID(bytes=bytes(uuid_bytes))


@dataclass(frozen=True)
class CanonicalEventWriteInput:
    source_id: str
    stream_id: str
    partition_id: str
    source_offset_or_equivalent: str
    source_event_time_utc: datetime | None
    ingested_at_utc: datetime
    payload_content_type: str
    payload_encoding: str
    payload_raw: bytes
    payload_json_precanonical: str | None = None
    payload_sha256_raw_precomputed: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class CanonicalEventRow:
    envelope_version: int
    event_id: UUID
    source_id: str
    stream_id: str
    partition_id: str
    source_offset_or_equivalent: str
    source_event_time_utc: datetime | None
    ingested_at_utc: datetime
    payload_content_type: str
    payload_encoding: str
    payload_raw: bytes
    payload_sha256_raw: str
    payload_json: str

    @property
    def idempotency_key(self) -> str:
        return canonical_event_idempotency_key(
            source_id=self.source_id,
            stream_id=self.stream_id,
            partition_id=self.partition_id,
            source_offset_or_equivalent=self.source_offset_or_equivalent,
        )

    def to_insert_tuple(self) -> tuple[object, ...]:
        return (
            self.envelope_version,
            self.event_id,
            self.source_id,
            self.stream_id,
            self.partition_id,
            self.source_offset_or_equivalent,
            self.source_event_time_utc,
            self.ingested_at_utc,
            self.payload_content_type,
            self.payload_encoding,
            self.payload_raw,
            self.payload_sha256_raw,
            self.payload_json,
        )


@dataclass(frozen=True)
class CanonicalEventWriteResult:
    status: Literal['inserted', 'duplicate']
    row: CanonicalEventRow


def build_canonical_event_row(event_input: CanonicalEventWriteInput) -> CanonicalEventRow:
    if event_input.payload_raw == b'':
        raise EventWriterError(
            code='WRITER_EMPTY_PAYLOAD_RAW',
            message='payload_raw must be non-empty bytes',
        )

    payload_content_type = _canonicalize_content_type(event_input.payload_content_type)
    payload_encoding = _canonicalize_encoding(event_input.payload_encoding)
    source_event_time_utc: datetime | None = None
    if event_input.source_event_time_utc is not None:
        source_event_time_utc = _ensure_utc(
            event_input.source_event_time_utc, field_name='source_event_time_utc'
        )
    ingested_at_utc = _ensure_utc(event_input.ingested_at_utc, field_name='ingested_at_utc')

    idempotency_key = canonical_event_idempotency_key(
        source_id=event_input.source_id,
        stream_id=event_input.stream_id,
        partition_id=event_input.partition_id,
        source_offset_or_equivalent=event_input.source_offset_or_equivalent,
    )
    event_id = canonical_event_id_from_key(idempotency_key)
    payload_sha256_raw_precomputed = event_input.payload_sha256_raw_precomputed
    if payload_sha256_raw_precomputed is None:
        payload_sha256_raw = hashlib.sha256(event_input.payload_raw).hexdigest()
    else:
        payload_sha256_raw = payload_sha256_raw_precomputed.strip().lower()
        if len(payload_sha256_raw) != 64:
            raise EventWriterError(
                code='WRITER_INVALID_PRECOMPUTED_PAYLOAD_SHA256',
                message='payload_sha256_raw_precomputed must be 64 hex characters',
            )
        try:
            int(payload_sha256_raw, 16)
        except ValueError as exc:
            raise EventWriterError(
                code='WRITER_INVALID_PRECOMPUTED_PAYLOAD_SHA256',
                message='payload_sha256_raw_precomputed must be lowercase hex',
            ) from exc

    payload_json_precanonical = event_input.payload_json_precanonical
    if payload_json_precanonical is None:
        payload_json = _payload_json_from_raw(
            source_id=event_input.source_id,
            stream_id=event_input.stream_id,
            payload_content_type=payload_content_type,
            payload_encoding=payload_encoding,
            payload_raw=event_input.payload_raw,
        )
    else:
        media_type = payload_content_type.split(';')[0].strip()
        if media_type != 'application/json':
            raise EventWriterError(
                code='WRITER_UNSUPPORTED_PAYLOAD_MEDIA_TYPE',
                message=(
                    'payload_json_precanonical is only supported for '
                    'application/json payloads'
                ),
            )
        payload_json = payload_json_precanonical
        if payload_json.strip() == '':
            raise EventWriterError(
                code='WRITER_INVALID_PRECANONICAL_PAYLOAD_JSON',
                message='payload_json_precanonical must be non-empty JSON text',
            )

    return CanonicalEventRow(
        envelope_version=CANONICAL_EVENT_ENVELOPE_VERSION,
        event_id=event_id,
        source_id=event_input.source_id.strip(),
        stream_id=event_input.stream_id.strip(),
        partition_id=event_input.partition_id.strip(),
        source_offset_or_equivalent=event_input.source_offset_or_equivalent.strip(),
        source_event_time_utc=source_event_time_utc,
        ingested_at_utc=ingested_at_utc,
        payload_content_type=payload_content_type,
        payload_encoding=payload_encoding,
        payload_raw=event_input.payload_raw,
        payload_sha256_raw=payload_sha256_raw,
        payload_json=payload_json,
    )


class CanonicalEventWriter:
    def __init__(
        self,
        *,
        client: ClickHouseClient,
        database: str,
        table: str = 'canonical_event_log',
        quarantine_registry: StreamQuarantineRegistry | None = None,
    ) -> None:
        self._client = client
        self._database = database
        self._table = table
        self._quarantine_registry = (
            quarantine_registry
            if quarantine_registry is not None
            else StreamQuarantineRegistry(path=load_stream_quarantine_state_path())
        )

    def write_event(self, event_input: CanonicalEventWriteInput) -> CanonicalEventWriteResult:
        return self.write_events([event_input])[0]

    def write_events(
        self,
        event_inputs: list[CanonicalEventWriteInput],
    ) -> list[CanonicalEventWriteResult]:
        if event_inputs == []:
            raise EventWriterError(
                code='WRITER_EMPTY_BATCH',
                message='event_inputs must contain at least one event',
            )

        prepared: list[tuple[CanonicalEventRow, str | None]] = []
        for event_input in event_inputs:
            prepared.append(
                (
                    build_canonical_event_row(event_input),
                    self._normalize_run_id_or_raise(event_input.run_id),
                )
            )

        grouped_indexes: dict[CanonicalStreamKey, list[int]] = {}
        for index, (row, _run_id) in enumerate(prepared):
            stream_key = CanonicalStreamKey(
                source_id=row.source_id,
                stream_id=row.stream_id,
                partition_id=row.partition_id,
            )
            existing_indexes = grouped_indexes.get(stream_key)
            if existing_indexes is None:
                grouped_indexes[stream_key] = [index]
            else:
                existing_indexes.append(index)

        ordered_results: list[CanonicalEventWriteResult | None] = [None] * len(prepared)
        for stream_key, indexes in grouped_indexes.items():
            try:
                self._quarantine_registry.assert_not_quarantined(stream_key=stream_key)
            except StreamQuarantineError as exc:
                raise EventWriterError(
                    code=exc.code,
                    message=exc.message,
                    context=exc.context,
                ) from exc

            rows_for_stream = [prepared[index][0] for index in indexes]
            offsets = {row.source_offset_or_equivalent for row in rows_for_stream}
            existing_rows: dict[str, tuple[UUID, str]] = {}
            if self._partition_has_existing_rows(stream_key=stream_key):
                existing_rows = self._fetch_existing_for_offsets(
                    stream_key=stream_key,
                    offsets=offsets,
                )

            inserted_rows: list[CanonicalEventRow] = []
            seen_offsets: dict[str, CanonicalEventRow] = {}
            stream_results: list[tuple[int, CanonicalEventWriteResult, str | None]] = []
            for index in indexes:
                row, normalized_run_id = prepared[index]
                offset = row.source_offset_or_equivalent
                seen_row = seen_offsets.get(offset)
                if seen_row is not None:
                    if seen_row.event_id != row.event_id:
                        raise EventWriterError(
                            code='WRITER_IDENTITY_EVENT_ID_CONFLICT',
                            message=(
                                'Canonical identity conflict inside write batch: '
                                'event_id mismatch for duplicated source-event identity'
                            ),
                        )
                    if seen_row.payload_sha256_raw != row.payload_sha256_raw:
                        raise EventWriterError(
                            code='WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
                            message=(
                                'Canonical identity conflict inside write batch: '
                                'payload hash mismatch for duplicated source-event identity'
                            ),
                        )
                    write_result = CanonicalEventWriteResult(status='duplicate', row=row)
                    stream_results.append((index, write_result, normalized_run_id))
                    continue

                existing = existing_rows.get(offset)
                if existing is None:
                    inserted_rows.append(row)
                    seen_offsets[offset] = row
                    write_result = CanonicalEventWriteResult(status='inserted', row=row)
                    stream_results.append((index, write_result, normalized_run_id))
                    continue

                existing_event_id, existing_payload_sha256_raw = existing
                if existing_event_id != row.event_id:
                    raise EventWriterError(
                        code='WRITER_IDENTITY_EVENT_ID_CONFLICT',
                        message=(
                            'Canonical identity conflict: existing event_id does not match '
                            'deterministic event_id'
                        ),
                    )
                if existing_payload_sha256_raw != row.payload_sha256_raw:
                    raise EventWriterError(
                        code='WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
                        message=(
                            'Canonical identity conflict: payload hash mismatch for '
                            'existing source-event identity'
                        ),
                    )

                seen_offsets[offset] = row
                write_result = CanonicalEventWriteResult(status='duplicate', row=row)
                stream_results.append((index, write_result, normalized_run_id))

            self._insert_rows(inserted_rows)

            for index, result, normalized_run_id in stream_results:
                ordered_results[index] = result
            self._append_runtime_audit_events(
                stream_key=stream_key,
                stream_results=stream_results,
            )

        finalized_results: list[CanonicalEventWriteResult] = []
        for result in ordered_results:
            if result is None:
                raise EventWriterError(
                    code='WRITER_INTERNAL_RESULT_MISSING',
                    message='Internal error: missing batch write result entry',
                )
            finalized_results.append(result)
        return finalized_results

    @staticmethod
    def _normalize_run_id_or_raise(run_id: str | None) -> str | None:
        if run_id is None:
            return None
        normalized = run_id.strip()
        if normalized == '':
            raise EventWriterError(
                code='WRITER_INVALID_RUN_ID',
                message='run_id must be non-empty when set',
            )
        return normalized

    def _fetch_existing(self, row: CanonicalEventRow) -> tuple[UUID, str] | None:
        rows = self._client.execute(
            f'''
            SELECT
                event_id,
                payload_sha256_raw
            FROM {self._database}.{self._table}
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
                AND source_offset_or_equivalent = %(source_offset_or_equivalent)s
            ORDER BY ingested_at_utc ASC
            LIMIT 2
            ''',
            {
                'source_id': row.source_id,
                'stream_id': row.stream_id,
                'partition_id': row.partition_id,
                'source_offset_or_equivalent': row.source_offset_or_equivalent,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1:
            raise EventWriterError(
                code='WRITER_IDENTITY_DUPLICATE_ROW_CONFLICT',
                message='Canonical identity conflict: multiple canonical rows exist for same source-event identity',
            )
        event_id, payload_sha256_raw = rows[0]
        return event_id, payload_sha256_raw

    def _partition_has_existing_rows(self, *, stream_key: CanonicalStreamKey) -> bool:
        rows = self._client.execute(
            f'''
            SELECT 1
            FROM {self._database}.{self._table}
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            LIMIT 1
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        return rows != []

    def _fetch_existing_for_offsets(
        self,
        *,
        stream_key: CanonicalStreamKey,
        offsets: set[str],
    ) -> dict[str, tuple[UUID, str]]:
        if offsets == set():
            return {}
        existing_by_offset: dict[str, tuple[UUID, str]] = {}
        ordered_offsets = sorted(offsets)
        for start in range(0, len(ordered_offsets), _OFFSET_LOOKUP_CHUNK_SIZE):
            chunk_offsets = ordered_offsets[start : start + _OFFSET_LOOKUP_CHUNK_SIZE]
            chunk_tuple = tuple(chunk_offsets)
            rows = self._client.execute(
                f'''
                SELECT
                    source_offset_or_equivalent,
                    event_id,
                    payload_sha256_raw
                FROM {self._database}.{self._table}
                WHERE source_id = %(source_id)s
                    AND stream_id = %(stream_id)s
                    AND partition_id = %(partition_id)s
                    AND source_offset_or_equivalent IN %(source_offsets)s
                ORDER BY source_offset_or_equivalent ASC, ingested_at_utc ASC
                ''',
                {
                    'source_id': stream_key.source_id,
                    'stream_id': stream_key.stream_id,
                    'partition_id': stream_key.partition_id,
                    'source_offsets': chunk_tuple,
                },
            )
            for raw_offset, event_id, payload_sha256_raw in rows:
                offset = str(raw_offset)
                existing = existing_by_offset.get(offset)
                candidate = (event_id, str(payload_sha256_raw))
                if existing is not None:
                    raise EventWriterError(
                        code='WRITER_IDENTITY_DUPLICATE_ROW_CONFLICT',
                        message=(
                            'Canonical identity conflict: multiple canonical rows exist for '
                            'same source-event identity'
                        ),
                    )
                existing_by_offset[offset] = candidate
        return existing_by_offset

    def _insert_row(self, row: CanonicalEventRow) -> None:
        self._client.execute(
            f'''
            INSERT INTO {self._database}.{self._table}
            ({', '.join(_CANONICAL_EVENT_LOG_COLUMNS)})
            VALUES
            ''',
            [row.to_insert_tuple()],
        )

    def _insert_rows(self, rows: list[CanonicalEventRow]) -> None:
        if rows == []:
            return
        for start in range(0, len(rows), _INSERT_CHUNK_SIZE):
            chunk = rows[start : start + _INSERT_CHUNK_SIZE]
            self._client.execute(
                f'''
                INSERT INTO {self._database}.{self._table}
                ({', '.join(_CANONICAL_EVENT_LOG_COLUMNS)})
                VALUES
                ''',
                [row.to_insert_tuple() for row in chunk],
            )

    def _append_runtime_audit_events(
        self,
        *,
        stream_key: CanonicalStreamKey,
        stream_results: list[tuple[int, CanonicalEventWriteResult, str | None]],
    ) -> None:
        runtime_audit_mode = os.environ.get(
            _RUNTIME_AUDIT_MODE_ENV,
            _RUNTIME_AUDIT_MODE_EVENT,
        ).strip().lower()
        if runtime_audit_mode not in {
            _RUNTIME_AUDIT_MODE_EVENT,
            _RUNTIME_AUDIT_MODE_SUMMARY,
        }:
            raise EventWriterError(
                code='WRITER_INVALID_RUNTIME_AUDIT_MODE',
                message=(
                    f'{_RUNTIME_AUDIT_MODE_ENV} must be one of '
                    f'[{_RUNTIME_AUDIT_MODE_EVENT}, {_RUNTIME_AUDIT_MODE_SUMMARY}]'
                ),
            )

        if runtime_audit_mode == _RUNTIME_AUDIT_MODE_SUMMARY:
            inserted_count = 0
            duplicate_count = 0
            first_result = stream_results[0][1]
            last_result = stream_results[-1][1]
            for _index, result, _run_id in stream_results:
                if result.status == 'inserted':
                    inserted_count += 1
                elif result.status == 'duplicate':
                    duplicate_count += 1
                else:
                    raise EventWriterError(
                        code='WRITER_UNKNOWN_RESULT_STATUS',
                        message=f'Unknown write result status: {result.status}',
                    )
            try:
                get_canonical_runtime_audit_log().append_ingest_batch_event(
                    stream_key=stream_key,
                    event_type='canonical_ingest_batch',
                    run_id=self._normalize_run_id_or_raise(stream_results[0][2]),
                    batch_event_count=len(stream_results),
                    inserted_count=inserted_count,
                    duplicate_count=duplicate_count,
                    first_source_offset_or_equivalent=(
                        first_result.row.source_offset_or_equivalent
                    ),
                    last_source_offset_or_equivalent=(
                        last_result.row.source_offset_or_equivalent
                    ),
                    first_event_id=str(first_result.row.event_id),
                    last_event_id=str(last_result.row.event_id),
                )
                return
            except RuntimeError as exc:
                raise EventWriterError(
                    code='WRITER_AUDIT_WRITE_FAILED',
                    message='Failed to write canonical ingest runtime audit event',
                    context={
                        'source_id': stream_key.source_id,
                        'stream_id': stream_key.stream_id,
                        'partition_id': stream_key.partition_id,
                        'event_id': str(first_result.row.event_id),
                        'status': first_result.status,
                        'batch_size': len(stream_results),
                    },
                ) from exc

        ingest_events: list[CanonicalRuntimeIngestEvent] = []
        for _index, result, run_id in stream_results:
            normalized_run_id = self._normalize_run_id_or_raise(run_id)
            event_type = (
                'canonical_ingest_inserted'
                if result.status == 'inserted'
                else 'canonical_ingest_duplicate'
            )
            ingest_events.append(
                CanonicalRuntimeIngestEvent(
                    event_type=event_type,
                    source_offset_or_equivalent=result.row.source_offset_or_equivalent,
                    event_id=str(result.row.event_id),
                    payload_sha256_raw=result.row.payload_sha256_raw,
                    status=result.status,
                    run_id=normalized_run_id,
                )
            )
        try:
            get_canonical_runtime_audit_log().append_ingest_events(
                stream_key=stream_key,
                events=ingest_events,
            )
        except RuntimeError as exc:
            first_result = stream_results[0][1]
            raise EventWriterError(
                code='WRITER_AUDIT_WRITE_FAILED',
                message='Failed to write canonical ingest runtime audit event',
                context={
                    'source_id': stream_key.source_id,
                    'stream_id': stream_key.stream_id,
                    'partition_id': stream_key.partition_id,
                    'event_id': str(first_result.row.event_id),
                    'status': first_result.status,
                    'batch_size': len(stream_results),
                },
            ) from exc
