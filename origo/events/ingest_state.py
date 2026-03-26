from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal
from uuid import UUID, uuid5

from clickhouse_driver import Client as ClickHouseClient

from .errors import ReconciliationError, StreamQuarantineError
from .quarantine import StreamQuarantineRegistryProtocol

OffsetOrdering = Literal['numeric', 'lexicographic', 'opaque']
CursorWriteStatus = Literal['advanced', 'duplicate']
CheckpointWriteStatus = Literal['recorded', 'duplicate']
CompletenessStatus = Literal['ok', 'gap_detected']

_CHECKPOINT_NAMESPACE: Final[UUID] = UUID('9f9fc6f0-43f9-4af0-b1d8-c81a5358cff9')


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise ReconciliationError(
            code='RECONCILIATION_INVALID_INPUT',
            message=f'{field_name} must be set and non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ReconciliationError(
            code='RECONCILIATION_INVALID_INPUT',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


def _compare_offsets(
    previous: str, current: str, *, offset_ordering: OffsetOrdering
) -> int:
    if offset_ordering == 'numeric':
        try:
            previous_number = int(previous)
        except ValueError as exc:
            raise ReconciliationError(
                code='RECONCILIATION_INVALID_NUMERIC_OFFSET',
                message=(
                    'Numeric offset ordering requires integer previous offset, '
                    f'got {previous!r}'
                ),
            ) from exc
        try:
            current_number = int(current)
        except ValueError as exc:
            raise ReconciliationError(
                code='RECONCILIATION_INVALID_NUMERIC_OFFSET',
                message=(
                    'Numeric offset ordering requires integer current offset, '
                    f'got {current!r}'
                ),
            ) from exc
        if current_number < previous_number:
            return -1
        if current_number > previous_number:
            return 1
        return 0
    if offset_ordering == 'lexicographic':
        if current < previous:
            return -1
        if current > previous:
            return 1
        return 0
    if current == previous:
        return 0
    return 1


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def completeness_checkpoint_id(
    *,
    source_id: str,
    stream_id: str,
    partition_id: str,
    last_checked_source_offset_or_equivalent: str,
    expected_event_count: int,
    observed_event_count: int,
    gap_count: int,
    status: CompletenessStatus,
    gap_details_json: str,
) -> UUID:
    key = (
        f'{source_id}|{stream_id}|{partition_id}|'
        f'{last_checked_source_offset_or_equivalent}|'
        f'{expected_event_count}|{observed_event_count}|{gap_count}|'
        f'{status}|{gap_details_json}'
    )
    return uuid5(_CHECKPOINT_NAMESPACE, key)


@dataclass(frozen=True)
class CanonicalStreamKey:
    source_id: str
    stream_id: str
    partition_id: str


@dataclass(frozen=True)
class CanonicalCursorState:
    stream_key: CanonicalStreamKey
    cursor_revision: int
    last_source_offset_or_equivalent: str
    last_event_id: UUID
    last_source_event_time_utc: datetime | None
    last_ingested_at_utc: datetime
    offset_ordering: OffsetOrdering
    updated_by_run_id: str
    updated_at_utc: datetime


@dataclass(frozen=True)
class CursorAdvanceInput:
    stream_key: CanonicalStreamKey
    next_source_offset_or_equivalent: str
    event_id: UUID
    offset_ordering: OffsetOrdering
    run_id: str
    change_reason: str
    ingested_at_utc: datetime
    source_event_time_utc: datetime | None
    updated_at_utc: datetime
    expected_previous_source_offset_or_equivalent: str | None = None


@dataclass(frozen=True)
class CursorAdvanceResult:
    status: CursorWriteStatus
    cursor_state: CanonicalCursorState
    previous_source_offset_or_equivalent: str | None


@dataclass(frozen=True)
class CompletenessCheckpointInput:
    stream_key: CanonicalStreamKey
    offset_ordering: OffsetOrdering
    check_scope_start_offset: str | None
    check_scope_end_offset: str | None
    last_checked_source_offset_or_equivalent: str
    expected_event_count: int
    observed_event_count: int
    gap_count: int
    status: CompletenessStatus
    gap_details: dict[str, Any]
    checked_by_run_id: str
    checked_at_utc: datetime


@dataclass(frozen=True)
class CompletenessCheckpointState:
    stream_key: CanonicalStreamKey
    checkpoint_revision: int
    checkpoint_id: UUID
    offset_ordering: OffsetOrdering
    check_scope_start_offset: str | None
    check_scope_end_offset: str | None
    last_checked_source_offset_or_equivalent: str
    expected_event_count: int
    observed_event_count: int
    gap_count: int
    status: CompletenessStatus
    gap_details_json: str
    checked_by_run_id: str
    checked_at_utc: datetime


@dataclass(frozen=True)
class CompletenessCheckpointResult:
    status: CheckpointWriteStatus
    checkpoint_state: CompletenessCheckpointState


class CanonicalIngestStateStore:
    def __init__(
        self,
        *,
        client: ClickHouseClient,
        database: str,
        quarantine_registry: StreamQuarantineRegistryProtocol | None = None,
    ) -> None:
        self._client = client
        self._database = _require_non_empty(database, field_name='database')
        self._quarantine_registry = quarantine_registry

    def fetch_latest_cursor_state(
        self, *, stream_key: CanonicalStreamKey
    ) -> CanonicalCursorState | None:
        rows = self._client.execute(
            f'''
            SELECT
                cursor_revision,
                last_source_offset_or_equivalent,
                last_event_id,
                last_source_event_time_utc,
                last_ingested_at_utc,
                offset_ordering,
                updated_by_run_id,
                updated_at_utc
            FROM {self._database}.canonical_ingest_cursor_state
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY cursor_revision DESC
            LIMIT 2
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1:
            latest_revision = int(rows[0][0])
            second_revision = int(rows[1][0])
            if latest_revision == second_revision:
                raise ReconciliationError(
                    code='CURSOR_DUPLICATE_LATEST_REVISION',
                    message=(
                        'Cursor state conflict: duplicate latest cursor_revision '
                        f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                    ),
                )
        row = rows[0]
        return CanonicalCursorState(
            stream_key=stream_key,
            cursor_revision=int(row[0]),
            last_source_offset_or_equivalent=str(row[1]),
            last_event_id=row[2],
            last_source_event_time_utc=row[3],
            last_ingested_at_utc=row[4],
            offset_ordering=row[5],
            updated_by_run_id=str(row[6]),
            updated_at_utc=row[7],
        )

    def advance_cursor(self, cursor_input: CursorAdvanceInput) -> CursorAdvanceResult:
        source_id = _require_non_empty(
            cursor_input.stream_key.source_id, field_name='stream_key.source_id'
        )
        stream_id = _require_non_empty(
            cursor_input.stream_key.stream_id, field_name='stream_key.stream_id'
        )
        partition_id = _require_non_empty(
            cursor_input.stream_key.partition_id, field_name='stream_key.partition_id'
        )
        next_offset = _require_non_empty(
            cursor_input.next_source_offset_or_equivalent,
            field_name='next_source_offset_or_equivalent',
        )
        run_id = _require_non_empty(cursor_input.run_id, field_name='run_id')
        change_reason = _require_non_empty(
            cursor_input.change_reason, field_name='change_reason'
        )
        ingested_at_utc = _ensure_utc(
            cursor_input.ingested_at_utc, field_name='ingested_at_utc'
        )
        source_event_time_utc: datetime | None = None
        if cursor_input.source_event_time_utc is not None:
            source_event_time_utc = _ensure_utc(
                cursor_input.source_event_time_utc, field_name='source_event_time_utc'
            )
        updated_at_utc = _ensure_utc(
            cursor_input.updated_at_utc, field_name='updated_at_utc'
        )

        stream_key = CanonicalStreamKey(
            source_id=source_id,
            stream_id=stream_id,
            partition_id=partition_id,
        )
        if self._quarantine_registry is not None:
            try:
                self._quarantine_registry.assert_not_quarantined(
                    stream_key=stream_key
                )
            except StreamQuarantineError as exc:
                raise ReconciliationError(
                    code=exc.code,
                    message=exc.message,
                    context=exc.context,
                ) from exc
        current_state = self.fetch_latest_cursor_state(stream_key=stream_key)
        if current_state is None:
            if cursor_input.expected_previous_source_offset_or_equivalent is not None:
                raise ReconciliationError(
                    code='CURSOR_EXPECTED_PREVIOUS_OFFSET_INVALID',
                    message=(
                        'Cursor advance expected_previous_source_offset_or_equivalent '
                        'cannot be set for first revision'
                    ),
                )
            next_revision = 1
            previous_offset: str | None = None
        else:
            if current_state.offset_ordering != cursor_input.offset_ordering:
                raise ReconciliationError(
                    code='CURSOR_OFFSET_ORDERING_MISMATCH',
                    message=(
                        'Cursor advance offset ordering mismatch: '
                        f'stored={current_state.offset_ordering} '
                        f'requested={cursor_input.offset_ordering}'
                    ),
                )
            previous_offset = current_state.last_source_offset_or_equivalent
            if (
                cursor_input.expected_previous_source_offset_or_equivalent
                is not None
                and cursor_input.expected_previous_source_offset_or_equivalent
                != previous_offset
            ):
                raise ReconciliationError(
                    code='CURSOR_EXPECTED_PREVIOUS_OFFSET_MISMATCH',
                    message=(
                        'Cursor advance expected previous offset mismatch: '
                        f'expected={cursor_input.expected_previous_source_offset_or_equivalent} '
                        f'actual={previous_offset}'
                    ),
                )
            if (
                current_state.last_event_id == cursor_input.event_id
                and previous_offset == next_offset
            ):
                return CursorAdvanceResult(
                    status='duplicate',
                    cursor_state=current_state,
                    previous_source_offset_or_equivalent=previous_offset,
                )
            comparison = _compare_offsets(
                previous_offset,
                next_offset,
                offset_ordering=cursor_input.offset_ordering,
            )
            if comparison == 0:
                raise ReconciliationError(
                    code='CURSOR_OFFSET_DID_NOT_ADVANCE',
                    message=(
                        'Cursor advance conflict: source offset did not advance '
                        'and event_id changed'
                    ),
                )
            if comparison < 0:
                raise ReconciliationError(
                    code='CURSOR_NON_MONOTONIC_OFFSET',
                    message=(
                        'Cursor advance conflict: non-monotonic source offset '
                        f'transition from {previous_offset!r} to {next_offset!r}'
                    ),
                )
            next_revision = current_state.cursor_revision + 1

        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_ingest_cursor_state
            (
                source_id,
                stream_id,
                partition_id,
                cursor_revision,
                last_source_offset_or_equivalent,
                last_event_id,
                last_source_event_time_utc,
                last_ingested_at_utc,
                offset_ordering,
                updated_by_run_id,
                updated_at_utc
            )
            VALUES
            ''',
            [
                (
                    source_id,
                    stream_id,
                    partition_id,
                    next_revision,
                    next_offset,
                    cursor_input.event_id,
                    source_event_time_utc,
                    ingested_at_utc,
                    cursor_input.offset_ordering,
                    run_id,
                    updated_at_utc,
                )
            ],
        )
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_ingest_cursor_ledger
            (
                source_id,
                stream_id,
                partition_id,
                ledger_revision,
                previous_source_offset_or_equivalent,
                next_source_offset_or_equivalent,
                event_id,
                offset_ordering,
                change_reason,
                run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    source_id,
                    stream_id,
                    partition_id,
                    next_revision,
                    previous_offset,
                    next_offset,
                    cursor_input.event_id,
                    cursor_input.offset_ordering,
                    change_reason,
                    run_id,
                    updated_at_utc,
                )
            ],
        )

        latest_state = self.fetch_latest_cursor_state(stream_key=stream_key)
        if latest_state is None:
            raise ReconciliationError(
                code='CURSOR_STATE_MISSING_AFTER_INSERT',
                message='Cursor advance failed: no cursor state found after insert',
            )
        if latest_state.cursor_revision != next_revision:
            raise ReconciliationError(
                code='CURSOR_REVISION_MISMATCH_AFTER_INSERT',
                message='Cursor advance failed: latest cursor revision mismatch after insert',
            )
        return CursorAdvanceResult(
            status='advanced',
            cursor_state=latest_state,
            previous_source_offset_or_equivalent=previous_offset,
        )

    def fetch_latest_completeness_checkpoint(
        self, *, stream_key: CanonicalStreamKey
    ) -> CompletenessCheckpointState | None:
        rows = self._client.execute(
            f'''
            SELECT
                checkpoint_revision,
                checkpoint_id,
                offset_ordering,
                check_scope_start_offset,
                check_scope_end_offset,
                last_checked_source_offset_or_equivalent,
                expected_event_count,
                observed_event_count,
                gap_count,
                status,
                gap_details_json,
                checked_by_run_id,
                checked_at_utc
            FROM {self._database}.canonical_source_completeness_checkpoints
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY checkpoint_revision DESC
            LIMIT 2
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1:
            latest_revision = int(rows[0][0])
            second_revision = int(rows[1][0])
            if latest_revision == second_revision:
                raise ReconciliationError(
                    code='RECONCILIATION_DUPLICATE_LATEST_CHECKPOINT_REVISION',
                    message=(
                        'Completeness checkpoint conflict: duplicate latest checkpoint_revision '
                        f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                    ),
                )
        row = rows[0]
        return CompletenessCheckpointState(
            stream_key=stream_key,
            checkpoint_revision=int(row[0]),
            checkpoint_id=row[1],
            offset_ordering=row[2],
            check_scope_start_offset=row[3],
            check_scope_end_offset=row[4],
            last_checked_source_offset_or_equivalent=str(row[5]),
            expected_event_count=int(row[6]),
            observed_event_count=int(row[7]),
            gap_count=int(row[8]),
            status=row[9],
            gap_details_json=str(row[10]),
            checked_by_run_id=str(row[11]),
            checked_at_utc=row[12],
        )

    def record_completeness_checkpoint(
        self, checkpoint_input: CompletenessCheckpointInput
    ) -> CompletenessCheckpointResult:
        source_id = _require_non_empty(
            checkpoint_input.stream_key.source_id, field_name='stream_key.source_id'
        )
        stream_id = _require_non_empty(
            checkpoint_input.stream_key.stream_id, field_name='stream_key.stream_id'
        )
        partition_id = _require_non_empty(
            checkpoint_input.stream_key.partition_id, field_name='stream_key.partition_id'
        )
        checked_by_run_id = _require_non_empty(
            checkpoint_input.checked_by_run_id, field_name='checked_by_run_id'
        )
        last_checked_offset = _require_non_empty(
            checkpoint_input.last_checked_source_offset_or_equivalent,
            field_name='last_checked_source_offset_or_equivalent',
        )
        checked_at_utc = _ensure_utc(
            checkpoint_input.checked_at_utc, field_name='checked_at_utc'
        )

        if checkpoint_input.expected_event_count < 0:
            raise ReconciliationError(
                code='RECONCILIATION_INVALID_COUNTS',
                message='expected_event_count must be >= 0',
            )
        if checkpoint_input.observed_event_count < 0:
            raise ReconciliationError(
                code='RECONCILIATION_INVALID_COUNTS',
                message='observed_event_count must be >= 0',
            )
        if checkpoint_input.gap_count < 0:
            raise ReconciliationError(
                code='RECONCILIATION_INVALID_COUNTS',
                message='gap_count must be >= 0',
            )

        if checkpoint_input.status == 'ok':
            if checkpoint_input.gap_count != 0:
                raise ReconciliationError(
                    code='RECONCILIATION_STATUS_CONFLICT',
                    message='status=ok requires gap_count=0',
                )
        if checkpoint_input.status == 'gap_detected':
            if checkpoint_input.gap_count == 0:
                raise ReconciliationError(
                    code='RECONCILIATION_STATUS_CONFLICT',
                    message='status=gap_detected requires gap_count > 0',
                )

        gap_details_json = _canonical_json(checkpoint_input.gap_details)
        checkpoint_id = completeness_checkpoint_id(
            source_id=source_id,
            stream_id=stream_id,
            partition_id=partition_id,
            last_checked_source_offset_or_equivalent=last_checked_offset,
            expected_event_count=checkpoint_input.expected_event_count,
            observed_event_count=checkpoint_input.observed_event_count,
            gap_count=checkpoint_input.gap_count,
            status=checkpoint_input.status,
            gap_details_json=gap_details_json,
        )
        stream_key = CanonicalStreamKey(
            source_id=source_id,
            stream_id=stream_id,
            partition_id=partition_id,
        )
        latest = self.fetch_latest_completeness_checkpoint(stream_key=stream_key)
        if latest is None:
            next_revision = 1
        else:
            if latest.offset_ordering != checkpoint_input.offset_ordering:
                raise ReconciliationError(
                    code='RECONCILIATION_OFFSET_ORDERING_MISMATCH',
                    message=(
                        'Completeness checkpoint offset ordering mismatch: '
                        f'stored={latest.offset_ordering} '
                        f'requested={checkpoint_input.offset_ordering}'
                    ),
                )
            if latest.checkpoint_id == checkpoint_id:
                return CompletenessCheckpointResult(
                    status='duplicate',
                    checkpoint_state=latest,
                )
            next_revision = latest.checkpoint_revision + 1

        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_source_completeness_checkpoints
            (
                source_id,
                stream_id,
                partition_id,
                checkpoint_revision,
                checkpoint_id,
                offset_ordering,
                check_scope_start_offset,
                check_scope_end_offset,
                last_checked_source_offset_or_equivalent,
                expected_event_count,
                observed_event_count,
                gap_count,
                status,
                gap_details_json,
                checked_by_run_id,
                checked_at_utc
            )
            VALUES
            ''',
            [
                (
                    source_id,
                    stream_id,
                    partition_id,
                    next_revision,
                    checkpoint_id,
                    checkpoint_input.offset_ordering,
                    checkpoint_input.check_scope_start_offset,
                    checkpoint_input.check_scope_end_offset,
                    last_checked_offset,
                    checkpoint_input.expected_event_count,
                    checkpoint_input.observed_event_count,
                    checkpoint_input.gap_count,
                    checkpoint_input.status,
                    gap_details_json,
                    checked_by_run_id,
                    checked_at_utc,
                )
            ],
        )

        latest_after_insert = self.fetch_latest_completeness_checkpoint(
            stream_key=stream_key
        )
        if latest_after_insert is None:
            raise ReconciliationError(
                code='RECONCILIATION_CHECKPOINT_MISSING_AFTER_INSERT',
                message='Completeness checkpoint record failed: no checkpoint found after insert',
            )
        if latest_after_insert.checkpoint_revision != next_revision:
            raise ReconciliationError(
                code='RECONCILIATION_CHECKPOINT_REVISION_MISMATCH',
                message='Completeness checkpoint record failed: latest revision mismatch after insert',
            )

        if latest_after_insert.status == 'gap_detected':
            if self._quarantine_registry is None:
                raise ReconciliationError(
                    code='RECONCILIATION_QUARANTINE_REGISTRY_REQUIRED',
                    message=(
                        'gap_detected completeness checkpoint requires '
                        'configured stream quarantine registry'
                    ),
                )
            try:
                self._quarantine_registry.quarantine(
                    stream_key=stream_key,
                    run_id=checked_by_run_id,
                    reason='gap_detected_from_completeness_reconciliation',
                    gap_details=checkpoint_input.gap_details,
                    quarantined_at_utc=checked_at_utc,
                )
            except StreamQuarantineError as exc:
                raise ReconciliationError(
                    code=exc.code,
                    message=exc.message,
                    context=exc.context,
                ) from exc
        return CompletenessCheckpointResult(
            status='recorded',
            checkpoint_state=latest_after_insert,
        )
