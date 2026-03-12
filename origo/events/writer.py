from __future__ import annotations

import hashlib
import os
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
# Keep tuple-IN identity lookup well below ClickHouse default max_query_size.
_FETCH_EXISTING_IDENTITY_BATCH_SIZE: Final[int] = 1_500
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


CanonicalEventIdentityKey = tuple[str, str, str, str]


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
        return self.write_events([event_input])[0]

    def write_events(
        self, event_inputs: list[CanonicalEventWriteInput]
    ) -> list[CanonicalEventWriteResult]:
        if event_inputs == []:
            return []

        rows: list[CanonicalEventRow] = []
        stream_keys: list[CanonicalStreamKey] = []
        run_ids: list[str | None] = []
        for event_input in event_inputs:
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
            rows.append(row)
            stream_keys.append(stream_key)
            run_ids.append(self._normalize_run_id(event_input.run_id))

        existing_by_identity = self._fetch_existing_bulk(rows)

        inserted_by_identity: dict[
            CanonicalEventIdentityKey, CanonicalEventWriteResult
        ] = {}
        results: list[CanonicalEventWriteResult] = []
        rows_to_insert: list[CanonicalEventRow] = []

        for row in rows:
            identity = self._identity_key(row)
            existing = existing_by_identity.get(identity)
            if existing is not None:
                existing_event_id, existing_payload_sha256_raw = existing
                self._assert_existing_identity_matches(
                    row=row,
                    existing_event_id=existing_event_id,
                    existing_payload_sha256_raw=existing_payload_sha256_raw,
                )
                results.append(CanonicalEventWriteResult(status='duplicate', row=row))
                continue

            existing_insert = inserted_by_identity.get(identity)
            if existing_insert is not None:
                self._assert_existing_identity_matches(
                    row=row,
                    existing_event_id=existing_insert.row.event_id,
                    existing_payload_sha256_raw=existing_insert.row.payload_sha256_raw,
                )
                results.append(CanonicalEventWriteResult(status='duplicate', row=row))
                continue

            inserted_result = CanonicalEventWriteResult(status='inserted', row=row)
            inserted_by_identity[identity] = inserted_result
            results.append(inserted_result)
            rows_to_insert.append(row)

        if rows_to_insert != []:
            self._insert_rows(rows_to_insert)

        self._append_runtime_audit_events(
            stream_keys=stream_keys,
            results=results,
            run_ids=run_ids,
        )
        return results

    def _identity_key(self, row: CanonicalEventRow) -> CanonicalEventIdentityKey:
        return (
            row.source_id,
            row.stream_id,
            row.partition_id,
            row.source_offset_or_equivalent,
        )

    def _fetch_existing_bulk(
        self, rows: list[CanonicalEventRow]
    ) -> dict[CanonicalEventIdentityKey, tuple[UUID, str]]:
        if rows == []:
            return {}

        identity_keys = sorted({self._identity_key(row) for row in rows})
        existing_by_identity: dict[CanonicalEventIdentityKey, tuple[UUID, str]] = {}

        for offset in range(0, len(identity_keys), _FETCH_EXISTING_IDENTITY_BATCH_SIZE):
            chunk = identity_keys[offset : offset + _FETCH_EXISTING_IDENTITY_BATCH_SIZE]
            existing_rows = self._client.execute(
                f'''
                SELECT
                    source_id,
                    stream_id,
                    partition_id,
                    source_offset_or_equivalent,
                    event_id,
                    payload_sha256_raw
                FROM {self._database}.{self._table}
                WHERE (
                    source_id,
                    stream_id,
                    partition_id,
                    source_offset_or_equivalent
                ) IN %(identity_keys)s
                ORDER BY
                    source_id ASC,
                    stream_id ASC,
                    partition_id ASC,
                    source_offset_or_equivalent ASC,
                    ingested_at_utc ASC
                ''',
                {'identity_keys': chunk},
            )

            for (
                source_id,
                stream_id,
                partition_id,
                source_offset_or_equivalent,
                event_id,
                payload_sha256_raw,
            ) in existing_rows:
                identity = (
                    str(source_id),
                    str(stream_id),
                    str(partition_id),
                    str(source_offset_or_equivalent),
                )
                if identity in existing_by_identity:
                    raise EventWriterError(
                        code='WRITER_IDENTITY_DUPLICATE_ROW_CONFLICT',
                        message='Canonical identity conflict: multiple canonical rows exist for same source-event identity',
                    )
                existing_by_identity[identity] = (event_id, payload_sha256_raw)

        return existing_by_identity

    def _assert_existing_identity_matches(
        self,
        *,
        row: CanonicalEventRow,
        existing_event_id: UUID,
        existing_payload_sha256_raw: str,
    ) -> None:
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

    def _insert_rows(self, rows: list[CanonicalEventRow]) -> None:
        self._client.execute(
            f'''
            INSERT INTO {self._database}.{self._table}
            ({', '.join(_CANONICAL_EVENT_LOG_COLUMNS)})
            VALUES
            ''',
            [row.to_insert_tuple() for row in rows],
        )

    def _normalize_run_id(self, run_id: str | None) -> str | None:
        if run_id is None:
            return None
        normalized = run_id.strip()
        if normalized == '':
            raise EventWriterError(
                code='WRITER_INVALID_RUN_ID',
                message='run_id must be non-empty when set',
            )
        return normalized

    def _append_runtime_audit_events(
        self,
        *,
        stream_keys: list[CanonicalStreamKey],
        results: list[CanonicalEventWriteResult],
        run_ids: list[str | None],
    ) -> None:
        if len(stream_keys) != len(results) or len(results) != len(run_ids):
            raise EventWriterError(
                code='WRITER_AUDIT_BATCH_SIZE_MISMATCH',
                message='Audit append requires equal-length stream_keys, results, and run_ids',
            )
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
            if results == []:
                return
            grouped_indexes: dict[tuple[str, str, str], list[int]] = {}
            for index, stream_key in enumerate(stream_keys):
                key = (
                    stream_key.source_id,
                    stream_key.stream_id,
                    stream_key.partition_id,
                )
                existing = grouped_indexes.get(key)
                if existing is None:
                    grouped_indexes[key] = [index]
                else:
                    existing.append(index)
            try:
                audit_log = get_canonical_runtime_audit_log()
                for grouped in grouped_indexes.values():
                    first_index = grouped[0]
                    last_index = grouped[-1]
                    first_stream_key = stream_keys[first_index]
                    first_result = results[first_index]
                    last_result = results[last_index]
                    inserted_count = 0
                    duplicate_count = 0
                    for index in grouped:
                        result = results[index]
                        if result.status == 'inserted':
                            inserted_count += 1
                        elif result.status == 'duplicate':
                            duplicate_count += 1
                        else:
                            raise EventWriterError(
                                code='WRITER_UNKNOWN_RESULT_STATUS',
                                message=f'Unknown write result status: {result.status}',
                            )
                    audit_log.append_ingest_batch_event(
                        stream_key=first_stream_key,
                        event_type='canonical_ingest_batch',
                        run_id=run_ids[first_index],
                        batch_event_count=len(grouped),
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
            except EventWriterError:
                raise
            except RuntimeError as exc:
                first_stream_key = stream_keys[0]
                first_result = results[0]
                raise EventWriterError(
                    code='WRITER_AUDIT_WRITE_FAILED',
                    message='Failed to write canonical ingest runtime audit event',
                    context={
                        'source_id': first_stream_key.source_id,
                        'stream_id': first_stream_key.stream_id,
                        'partition_id': first_stream_key.partition_id,
                        'event_id': str(first_result.row.event_id),
                        'status': first_result.status,
                        'batch_size': len(results),
                    },
                ) from exc

        ingest_events: list[dict[str, object]] = []
        for stream_key, result, run_id in zip(stream_keys, results, run_ids, strict=True):
            ingest_events.append(
                {
                    'event_type': (
                        'canonical_ingest_inserted'
                        if result.status == 'inserted'
                        else 'canonical_ingest_duplicate'
                    ),
                    'source_id': stream_key.source_id,
                    'stream_id': stream_key.stream_id,
                    'partition_id': stream_key.partition_id,
                    'source_offset_or_equivalent': result.row.source_offset_or_equivalent,
                    'event_id': str(result.row.event_id),
                    'payload_sha256_raw': result.row.payload_sha256_raw,
                    'status': result.status,
                    'run_id': run_id,
                }
            )
        try:
            get_canonical_runtime_audit_log().append_ingest_events(events=ingest_events)
        except RuntimeError as exc:
            context: dict[str, object] = {}
            if results != []:
                first_result = results[0]
                first_stream_key = stream_keys[0]
                context = {
                    'source_id': first_stream_key.source_id,
                    'stream_id': first_stream_key.stream_id,
                    'partition_id': first_stream_key.partition_id,
                    'event_id': str(first_result.row.event_id),
                    'status': first_result.status,
                }
            raise EventWriterError(
                code='WRITER_AUDIT_WRITE_FAILED',
                message='Failed to write canonical ingest runtime audit event',
                context=context,
            ) from exc
