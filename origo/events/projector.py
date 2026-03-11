from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient

from .errors import ProjectorRuntimeError
from .ingest_state import CanonicalStreamKey
from .runtime_audit import get_canonical_runtime_audit_log

ProjectorCommitStatus = Literal['checkpointed', 'duplicate']


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise ProjectorRuntimeError(
            code='PROJECTOR_INVALID_INPUT',
            message=f'{field_name} must be set and non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ProjectorRuntimeError(
            code='PROJECTOR_INVALID_INPUT',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _format_datetime64_ms(value: datetime) -> str:
    utc_value = _ensure_utc(value, field_name='datetime64_value')
    return utc_value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _parse_state_json_or_raise(state_json: str) -> dict[str, Any]:
    try:
        parsed = json.loads(state_json)
    except json.JSONDecodeError as exc:
        raise ProjectorRuntimeError(
            code='PROJECTOR_INVALID_STATE_JSON',
            message=f'Projector checkpoint state_json is invalid JSON: {exc.msg}',
        ) from exc
    if not isinstance(parsed, dict):
        raise ProjectorRuntimeError(
            code='PROJECTOR_INVALID_STATE_JSON',
            message='Projector checkpoint state_json must decode to JSON object',
        )
    output: dict[str, Any] = {}
    for key, value in cast(dict[Any, Any], parsed).items():
        if not isinstance(key, str):
            raise ProjectorRuntimeError(
                code='PROJECTOR_INVALID_STATE_JSON',
                message='Projector checkpoint state_json keys must be strings',
            )
        output[key] = value
    return output


@dataclass(frozen=True)
class ProjectorEvent:
    event_id: UUID
    source_offset_or_equivalent: str
    source_event_time_utc: datetime | None
    ingested_at_utc: datetime
    payload_json: str
    payload_sha256_raw: str


@dataclass(frozen=True)
class ProjectorCheckpointState:
    projector_id: str
    stream_key: CanonicalStreamKey
    checkpoint_revision: int
    last_event_id: UUID
    last_source_offset_or_equivalent: str
    last_source_event_time_utc: datetime | None
    last_ingested_at_utc: datetime
    run_id: str
    state_json: str
    checkpointed_at_utc: datetime

    @property
    def parsed_state(self) -> dict[str, Any]:
        return _parse_state_json_or_raise(self.state_json)


@dataclass(frozen=True)
class ProjectorWatermarkState:
    projector_id: str
    stream_key: CanonicalStreamKey
    watermark_revision: int
    watermark_event_id: UUID
    watermark_source_offset_or_equivalent: str
    watermark_source_event_time_utc: datetime | None
    watermark_ingested_at_utc: datetime
    run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class ProjectorStartState:
    projector_id: str
    stream_key: CanonicalStreamKey
    resumed: bool
    checkpoint: ProjectorCheckpointState | None
    watermark: ProjectorWatermarkState | None


@dataclass(frozen=True)
class ProjectorCommitResult:
    status: ProjectorCommitStatus
    checkpoint: ProjectorCheckpointState
    watermark: ProjectorWatermarkState


class CanonicalProjectorRuntime:
    def __init__(
        self,
        *,
        client: ClickHouseClient,
        database: str,
        projector_id: str,
        stream_key: CanonicalStreamKey,
        batch_size: int,
    ) -> None:
        if batch_size <= 0:
            raise ProjectorRuntimeError(
                code='PROJECTOR_INVALID_BATCH_SIZE',
                message='batch_size must be > 0',
            )
        self._client = client
        self._database = _require_non_empty(database, field_name='database')
        self._projector_id = _require_non_empty(projector_id, field_name='projector_id')
        self._stream_key = CanonicalStreamKey(
            source_id=_require_non_empty(
                stream_key.source_id, field_name='stream_key.source_id'
            ),
            stream_id=_require_non_empty(
                stream_key.stream_id, field_name='stream_key.stream_id'
            ),
            partition_id=_require_non_empty(
                stream_key.partition_id, field_name='stream_key.partition_id'
            ),
        )
        self._batch_size = batch_size
        self._started = False
        self._cursor: ProjectorCheckpointState | None = None

    @property
    def projector_id(self) -> str:
        return self._projector_id

    @property
    def stream_key(self) -> CanonicalStreamKey:
        return self._stream_key

    @property
    def started(self) -> bool:
        return self._started

    def start(self) -> ProjectorStartState:
        if self._started:
            raise ProjectorRuntimeError(
                code='PROJECTOR_ALREADY_STARTED',
                message='Projector runtime already started',
            )
        checkpoint = self.fetch_latest_checkpoint()
        watermark = self.fetch_latest_watermark()
        if checkpoint is not None and watermark is not None:
            if checkpoint.checkpoint_revision != watermark.watermark_revision:
                raise ProjectorRuntimeError(
                    code='PROJECTOR_CHECKPOINT_WATERMARK_MISMATCH',
                    message=(
                        'Projector checkpoint/watermark revision mismatch: '
                        f'checkpoint_revision={checkpoint.checkpoint_revision} '
                        f'watermark_revision={watermark.watermark_revision}'
                    ),
                )
        self._cursor = checkpoint
        self._started = True
        return ProjectorStartState(
            projector_id=self._projector_id,
            stream_key=self._stream_key,
            resumed=checkpoint is not None,
            checkpoint=checkpoint,
            watermark=watermark,
        )

    def stop(self) -> None:
        if not self._started:
            raise ProjectorRuntimeError(
                code='PROJECTOR_NOT_STARTED',
                message='Projector runtime is not started',
            )
        self._started = False

    def resume(self) -> ProjectorStartState:
        return self.start()

    def fetch_next_batch(self) -> list[ProjectorEvent]:
        if not self._started:
            raise ProjectorRuntimeError(
                code='PROJECTOR_NOT_STARTED',
                message='Projector runtime is not started',
            )
        if self._cursor is None:
            rows = self._client.execute(
                f'''
                SELECT
                    event_id,
                    source_offset_or_equivalent,
                    source_event_time_utc,
                    ingested_at_utc,
                    payload_json,
                    payload_sha256_raw
                FROM {self._database}.canonical_event_log
                WHERE source_id = %(source_id)s
                    AND stream_id = %(stream_id)s
                    AND partition_id = %(partition_id)s
                ORDER BY ingested_at_utc ASC, event_id ASC
                LIMIT %(limit)s
                ''',
                {
                    'source_id': self._stream_key.source_id,
                    'stream_id': self._stream_key.stream_id,
                    'partition_id': self._stream_key.partition_id,
                    'limit': self._batch_size,
                },
            )
        else:
            last_ingested_at_utc_text = _format_datetime64_ms(
                self._cursor.last_ingested_at_utc
            )
            rows = self._client.execute(
                f'''
                SELECT
                    event_id,
                    source_offset_or_equivalent,
                    source_event_time_utc,
                    ingested_at_utc,
                    payload_json,
                    payload_sha256_raw
                FROM {self._database}.canonical_event_log
                WHERE source_id = %(source_id)s
                    AND stream_id = %(stream_id)s
                    AND partition_id = %(partition_id)s
                    AND (
                        ingested_at_utc > toDateTime64(%(last_ingested_at_utc)s, 3, 'UTC')
                        OR (
                            ingested_at_utc = toDateTime64(%(last_ingested_at_utc)s, 3, 'UTC')
                            AND event_id > %(last_event_id)s
                        )
                    )
                ORDER BY ingested_at_utc ASC, event_id ASC
                LIMIT %(limit)s
                ''',
                {
                    'source_id': self._stream_key.source_id,
                    'stream_id': self._stream_key.stream_id,
                    'partition_id': self._stream_key.partition_id,
                    'last_ingested_at_utc': last_ingested_at_utc_text,
                    'last_event_id': self._cursor.last_event_id,
                    'limit': self._batch_size,
                },
            )
        return [
            ProjectorEvent(
                event_id=row[0],
                source_offset_or_equivalent=str(row[1]),
                source_event_time_utc=row[2],
                ingested_at_utc=row[3],
                payload_json=str(row[4]),
                payload_sha256_raw=str(row[5]),
            )
            for row in rows
        ]

    def commit_checkpoint(
        self,
        *,
        last_event: ProjectorEvent,
        run_id: str,
        checkpointed_at_utc: datetime,
        state: dict[str, Any] | None = None,
    ) -> ProjectorCommitResult:
        if not self._started:
            raise ProjectorRuntimeError(
                code='PROJECTOR_NOT_STARTED',
                message='Projector runtime is not started',
            )
        run_id_value = _require_non_empty(run_id, field_name='run_id')
        checkpointed_at = _ensure_utc(
            checkpointed_at_utc, field_name='checkpointed_at_utc'
        )
        latest_checkpoint = self.fetch_latest_checkpoint()
        if latest_checkpoint is None:
            next_revision = 1
        else:
            if (
                latest_checkpoint.last_event_id == last_event.event_id
                and latest_checkpoint.last_source_offset_or_equivalent
                == last_event.source_offset_or_equivalent
                and latest_checkpoint.last_ingested_at_utc == last_event.ingested_at_utc
            ):
                latest_watermark = self.fetch_latest_watermark()
                if latest_watermark is None:
                    raise ProjectorRuntimeError(
                        code='PROJECTOR_DUPLICATE_WATERMARK_MISSING',
                        message='Projector checkpoint duplicate detected but watermark is missing',
                    )
                self._cursor = latest_checkpoint
                duplicate_result = ProjectorCommitResult(
                    status='duplicate',
                    checkpoint=latest_checkpoint,
                    watermark=latest_watermark,
                )
                self._append_checkpoint_audit_event(
                    result=duplicate_result,
                    run_id=run_id_value,
                    state=state or {},
                )
                return duplicate_result
            if last_event.ingested_at_utc < latest_checkpoint.last_ingested_at_utc:
                raise ProjectorRuntimeError(
                    code='PROJECTOR_NON_MONOTONIC_INGESTED_AT',
                    message='Projector checkpoint conflict: non-monotonic ingested_at_utc',
                )
            if (
                last_event.ingested_at_utc == latest_checkpoint.last_ingested_at_utc
                and last_event.event_id <= latest_checkpoint.last_event_id
            ):
                raise ProjectorRuntimeError(
                    code='PROJECTOR_EVENT_ORDER_NOT_ADVANCED',
                    message='Projector checkpoint conflict: event order did not advance',
                )
            next_revision = latest_checkpoint.checkpoint_revision + 1

        state_json = _canonical_json(state or {})
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_projector_checkpoints
            (
                projector_id,
                source_id,
                stream_id,
                partition_id,
                checkpoint_revision,
                last_event_id,
                last_source_offset_or_equivalent,
                last_source_event_time_utc,
                last_ingested_at_utc,
                run_id,
                state_json,
                checkpointed_at_utc
            )
            VALUES
            ''',
            [
                (
                    self._projector_id,
                    self._stream_key.source_id,
                    self._stream_key.stream_id,
                    self._stream_key.partition_id,
                    next_revision,
                    last_event.event_id,
                    last_event.source_offset_or_equivalent,
                    last_event.source_event_time_utc,
                    last_event.ingested_at_utc,
                    run_id_value,
                    state_json,
                    checkpointed_at,
                )
            ],
        )
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_projector_watermarks
            (
                projector_id,
                source_id,
                stream_id,
                partition_id,
                watermark_revision,
                watermark_event_id,
                watermark_source_offset_or_equivalent,
                watermark_source_event_time_utc,
                watermark_ingested_at_utc,
                run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    self._projector_id,
                    self._stream_key.source_id,
                    self._stream_key.stream_id,
                    self._stream_key.partition_id,
                    next_revision,
                    last_event.event_id,
                    last_event.source_offset_or_equivalent,
                    last_event.source_event_time_utc,
                    last_event.ingested_at_utc,
                    run_id_value,
                    checkpointed_at,
                )
            ],
        )

        checkpoint = self.fetch_latest_checkpoint()
        watermark = self.fetch_latest_watermark()
        if checkpoint is None:
            raise ProjectorRuntimeError(
                code='PROJECTOR_CHECKPOINT_MISSING_AFTER_COMMIT',
                message='Projector checkpoint commit failed: checkpoint missing',
            )
        if watermark is None:
            raise ProjectorRuntimeError(
                code='PROJECTOR_WATERMARK_MISSING_AFTER_COMMIT',
                message='Projector checkpoint commit failed: watermark missing',
            )
        if checkpoint.checkpoint_revision != next_revision:
            raise ProjectorRuntimeError(
                code='PROJECTOR_CHECKPOINT_REVISION_MISMATCH',
                message='Projector checkpoint commit failed: revision mismatch in checkpoint',
            )
        if watermark.watermark_revision != next_revision:
            raise ProjectorRuntimeError(
                code='PROJECTOR_WATERMARK_REVISION_MISMATCH',
                message='Projector checkpoint commit failed: revision mismatch in watermark',
            )
        self._cursor = checkpoint
        committed_result = ProjectorCommitResult(
            status='checkpointed',
            checkpoint=checkpoint,
            watermark=watermark,
        )
        self._append_checkpoint_audit_event(
            result=committed_result,
            run_id=run_id_value,
            state=state or {},
        )
        return committed_result

    def fetch_latest_checkpoint(self) -> ProjectorCheckpointState | None:
        rows = self._client.execute(
            f'''
            SELECT
                checkpoint_revision,
                last_event_id,
                last_source_offset_or_equivalent,
                last_source_event_time_utc,
                last_ingested_at_utc,
                run_id,
                state_json,
                checkpointed_at_utc
            FROM {self._database}.canonical_projector_checkpoints
            WHERE projector_id = %(projector_id)s
                AND source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY checkpoint_revision DESC
            LIMIT 2
            ''',
            {
                'projector_id': self._projector_id,
                'source_id': self._stream_key.source_id,
                'stream_id': self._stream_key.stream_id,
                'partition_id': self._stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1 and int(rows[0][0]) == int(rows[1][0]):
            raise ProjectorRuntimeError(
                code='PROJECTOR_DUPLICATE_CHECKPOINT_REVISION',
                message='Projector checkpoint conflict: duplicate latest checkpoint_revision',
            )
        row = rows[0]
        return ProjectorCheckpointState(
            projector_id=self._projector_id,
            stream_key=self._stream_key,
            checkpoint_revision=int(row[0]),
            last_event_id=row[1],
            last_source_offset_or_equivalent=str(row[2]),
            last_source_event_time_utc=row[3],
            last_ingested_at_utc=row[4],
            run_id=str(row[5]),
            state_json=str(row[6]),
            checkpointed_at_utc=row[7],
        )

    def fetch_latest_watermark(self) -> ProjectorWatermarkState | None:
        rows = self._client.execute(
            f'''
            SELECT
                watermark_revision,
                watermark_event_id,
                watermark_source_offset_or_equivalent,
                watermark_source_event_time_utc,
                watermark_ingested_at_utc,
                run_id,
                recorded_at_utc
            FROM {self._database}.canonical_projector_watermarks
            WHERE projector_id = %(projector_id)s
                AND source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY watermark_revision DESC
            LIMIT 2
            ''',
            {
                'projector_id': self._projector_id,
                'source_id': self._stream_key.source_id,
                'stream_id': self._stream_key.stream_id,
                'partition_id': self._stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1 and int(rows[0][0]) == int(rows[1][0]):
            raise ProjectorRuntimeError(
                code='PROJECTOR_DUPLICATE_WATERMARK_REVISION',
                message='Projector watermark conflict: duplicate latest watermark_revision',
            )
        row = rows[0]
        return ProjectorWatermarkState(
            projector_id=self._projector_id,
            stream_key=self._stream_key,
            watermark_revision=int(row[0]),
            watermark_event_id=row[1],
            watermark_source_offset_or_equivalent=str(row[2]),
            watermark_source_event_time_utc=row[3],
            watermark_ingested_at_utc=row[4],
            run_id=str(row[5]),
            recorded_at_utc=row[6],
        )

    def _append_checkpoint_audit_event(
        self,
        *,
        result: ProjectorCommitResult,
        run_id: str,
        state: dict[str, Any],
    ) -> None:
        event_type = (
            'projector_checkpointed'
            if result.status == 'checkpointed'
            else 'projector_checkpoint_duplicate'
        )
        try:
            get_canonical_runtime_audit_log().append_projector_checkpoint_event(
                event_type=event_type,
                projector_id=self._projector_id,
                stream_key=self._stream_key,
                run_id=run_id,
                checkpoint_revision=result.checkpoint.checkpoint_revision,
                last_event_id=str(result.checkpoint.last_event_id),
                last_source_offset_or_equivalent=result.checkpoint.last_source_offset_or_equivalent,
                status=result.status,
                state=state,
            )
        except RuntimeError as exc:
            raise ProjectorRuntimeError(
                code='PROJECTOR_AUDIT_WRITE_FAILED',
                message='Failed to write canonical projector checkpoint runtime audit event',
                context={
                    'projector_id': self._projector_id,
                    'source_id': self._stream_key.source_id,
                    'stream_id': self._stream_key.stream_id,
                    'partition_id': self._stream_key.partition_id,
                    'checkpoint_revision': result.checkpoint.checkpoint_revision,
                    'status': result.status,
                },
            ) from exc
