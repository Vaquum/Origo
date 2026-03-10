from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal
from uuid import UUID, uuid5

from clickhouse_driver import Client as ClickHouseClient

from .envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from .errors import EventWriterError, StreamQuarantineError
from .ingest_state import CanonicalStreamKey
from .precision import (
    assert_payload_json_has_no_float_values,
    canonicalize_payload_json_with_precision,
)
from .quarantine import StreamQuarantineRegistry, load_stream_quarantine_state_path
from .runtime_audit import get_canonical_runtime_audit_log

_CANONICAL_EVENT_NAMESPACE: Final[UUID] = UUID(
    'f1d1ef17-95ce-4f9f-bd81-f9958cdf8ee5'
)
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
    return uuid5(_CANONICAL_EVENT_NAMESPACE, idempotency_key)


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
    payload_sha256_raw = hashlib.sha256(event_input.payload_raw).hexdigest()
    payload_json = _payload_json_from_raw(
        source_id=event_input.source_id,
        stream_id=event_input.stream_id,
        payload_content_type=payload_content_type,
        payload_encoding=payload_encoding,
        payload_raw=event_input.payload_raw,
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
        row = build_canonical_event_row(event_input)
        stream_key = CanonicalStreamKey(
            source_id=row.source_id,
            stream_id=row.stream_id,
            partition_id=row.partition_id,
        )
        try:
            self._quarantine_registry.assert_not_quarantined(stream_key=stream_key)
        except StreamQuarantineError as exc:
            raise EventWriterError(
                code=exc.code,
                message=exc.message,
                context=exc.context,
            ) from exc

        existing = self._fetch_existing(row)
        if existing is None:
            self._insert_row(row)
            result = CanonicalEventWriteResult(status='inserted', row=row)
            self._append_runtime_audit_event(
                stream_key=stream_key,
                result=result,
                run_id=event_input.run_id,
            )
            return result

        existing_event_id, existing_payload_sha256_raw = existing
        if existing_event_id != row.event_id:
            raise EventWriterError(
                code='WRITER_IDENTITY_EVENT_ID_CONFLICT',
                message='Canonical identity conflict: existing event_id does not match deterministic event_id',
            )
        if existing_payload_sha256_raw != row.payload_sha256_raw:
            raise EventWriterError(
                code='WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
                message='Canonical identity conflict: payload hash mismatch for existing source-event identity',
            )

        result = CanonicalEventWriteResult(status='duplicate', row=row)
        self._append_runtime_audit_event(
            stream_key=stream_key,
            result=result,
            run_id=event_input.run_id,
        )
        return result

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

    def _insert_row(self, row: CanonicalEventRow) -> None:
        self._client.execute(
            f'''
            INSERT INTO {self._database}.{self._table}
            ({', '.join(_CANONICAL_EVENT_LOG_COLUMNS)})
            VALUES
            ''',
            [row.to_insert_tuple()],
        )

    def _append_runtime_audit_event(
        self,
        *,
        stream_key: CanonicalStreamKey,
        result: CanonicalEventWriteResult,
        run_id: str | None,
    ) -> None:
        normalized_run_id = None
        if run_id is not None:
            normalized = run_id.strip()
            if normalized == '':
                raise EventWriterError(
                    code='WRITER_INVALID_RUN_ID',
                    message='run_id must be non-empty when set',
                )
            normalized_run_id = normalized
        event_type = (
            'canonical_ingest_inserted'
            if result.status == 'inserted'
            else 'canonical_ingest_duplicate'
        )
        try:
            get_canonical_runtime_audit_log().append_ingest_event(
                event_type=event_type,
                stream_key=stream_key,
                source_offset_or_equivalent=result.row.source_offset_or_equivalent,
                event_id=str(result.row.event_id),
                payload_sha256_raw=result.row.payload_sha256_raw,
                status=result.status,
                run_id=normalized_run_id,
            )
        except RuntimeError as exc:
            raise EventWriterError(
                code='WRITER_AUDIT_WRITE_FAILED',
                message='Failed to write canonical ingest runtime audit event',
                context={
                    'source_id': stream_key.source_id,
                    'stream_id': stream_key.stream_id,
                    'partition_id': stream_key.partition_id,
                    'event_id': str(result.row.event_id),
                    'status': result.status,
                },
            ) from exc
