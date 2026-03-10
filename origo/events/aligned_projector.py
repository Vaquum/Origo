from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal
from uuid import UUID, uuid5

from clickhouse_driver import Client as ClickHouseClient

from .errors import ProjectorRuntimeError
from .ingest_state import CanonicalStreamKey
from .projector import CanonicalProjectorRuntime, ProjectorEvent

AlignedPolicyStatus = Literal['recorded', 'duplicate']

_ALIGNED_POLICY_NAMESPACE: Final[UUID] = UUID(
    'cebeb0d2-e8fe-4c03-9eed-4dc8de260260'
)


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise ProjectorRuntimeError(
            code='ALIGNED_PROJECTOR_INVALID_INPUT',
            message=f'{field_name} must be set and non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ProjectorRuntimeError(
            code='ALIGNED_PROJECTOR_INVALID_INPUT',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _bucket_floor(ingested_at_utc: datetime, *, bucket_size_seconds: int) -> datetime:
    seconds = int(ingested_at_utc.timestamp())
    floored = seconds - (seconds % bucket_size_seconds)
    return datetime.fromtimestamp(floored, tz=UTC).replace(microsecond=0)


def _require_source_event_time(event: ProjectorEvent) -> datetime:
    if event.source_event_time_utc is None:
        raise ProjectorRuntimeError(
            code='ALIGNED_PROJECTOR_SOURCE_EVENT_TIME_MISSING',
            message=(
                'Aligned projection requires source_event_time_utc for every event; '
                f'event_id={event.event_id}'
            ),
        )
    return _ensure_utc(
        event.source_event_time_utc,
        field_name='source_event_time_utc',
    )


def aligned_projection_policy_id(
    *,
    view_id: str,
    view_version: int,
    source_id: str,
    stream_id: str,
    partition_id: str,
    bucket_size_seconds: int,
    tier_policy: str,
    retention_hot_days: int,
    retention_warm_days: int,
) -> UUID:
    key = (
        f'{view_id}|{view_version}|{source_id}|{stream_id}|{partition_id}|'
        f'{bucket_size_seconds}|{tier_policy}|{retention_hot_days}|{retention_warm_days}'
    )
    return uuid5(_ALIGNED_POLICY_NAMESPACE, key)


@dataclass(frozen=True)
class AlignedProjectionPolicyInput:
    view_id: str
    view_version: int
    stream_key: CanonicalStreamKey
    bucket_size_seconds: int
    tier_policy: str
    retention_hot_days: int
    retention_warm_days: int
    recorded_by_run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class AlignedProjectionPolicyState:
    view_id: str
    view_version: int
    stream_key: CanonicalStreamKey
    policy_revision: int
    policy_id: UUID
    bucket_size_seconds: int
    tier_policy: str
    retention_hot_days: int
    retention_warm_days: int
    recorded_by_run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class AlignedPolicyRecordResult:
    status: AlignedPolicyStatus
    policy_state: AlignedProjectionPolicyState


@dataclass(frozen=True)
class Aligned1sAggregateRow:
    view_id: str
    view_version: int
    stream_key: CanonicalStreamKey
    aligned_at_utc: datetime
    bucket_event_count: int
    first_event_id: UUID
    last_event_id: UUID
    first_source_offset_or_equivalent: str
    last_source_offset_or_equivalent: str
    latest_source_event_time_utc: datetime | None
    latest_ingested_at_utc: datetime
    payload_rows_json: str
    bucket_sha256: str
    projector_id: str
    projected_at_utc: datetime

    def to_insert_tuple(self) -> tuple[object, ...]:
        return (
            self.view_id,
            self.view_version,
            self.stream_key.source_id,
            self.stream_key.stream_id,
            self.stream_key.partition_id,
            self.aligned_at_utc,
            self.bucket_event_count,
            self.first_event_id,
            self.last_event_id,
            self.first_source_offset_or_equivalent,
            self.last_source_offset_or_equivalent,
            self.latest_source_event_time_utc,
            self.latest_ingested_at_utc,
            self.payload_rows_json,
            self.bucket_sha256,
            self.projector_id,
            self.projected_at_utc,
        )


@dataclass(frozen=True)
class AlignedBackfillResult:
    batches_processed: int
    events_processed: int
    rows_written: int
    last_checkpoint_status: str | None


class CanonicalAlignedPolicyStore:
    def __init__(self, *, client: ClickHouseClient, database: str) -> None:
        self._client = client
        self._database = _require_non_empty(database, field_name='database')

    def fetch_latest_policy(
        self, *, view_id: str, view_version: int, stream_key: CanonicalStreamKey
    ) -> AlignedProjectionPolicyState | None:
        rows = self._client.execute(
            f'''
            SELECT
                policy_revision,
                policy_id,
                bucket_size_seconds,
                tier_policy,
                retention_hot_days,
                retention_warm_days,
                recorded_by_run_id,
                recorded_at_utc
            FROM {self._database}.canonical_aligned_projection_policies
            WHERE view_id = %(view_id)s
                AND view_version = %(view_version)s
                AND source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY policy_revision DESC
            LIMIT 2
            ''',
            {
                'view_id': view_id,
                'view_version': view_version,
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        if len(rows) > 1 and int(rows[0][0]) == int(rows[1][0]):
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_DUPLICATE_REVISION',
                message=(
                    'Aligned policy conflict: duplicate latest policy_revision for '
                    f'{view_id}/{view_version}/{stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        row = rows[0]
        return AlignedProjectionPolicyState(
            view_id=view_id,
            view_version=view_version,
            stream_key=stream_key,
            policy_revision=int(row[0]),
            policy_id=row[1],
            bucket_size_seconds=int(row[2]),
            tier_policy=str(row[3]),
            retention_hot_days=int(row[4]),
            retention_warm_days=int(row[5]),
            recorded_by_run_id=str(row[6]),
            recorded_at_utc=row[7],
        )

    def record_policy(
        self, policy_input: AlignedProjectionPolicyInput
    ) -> AlignedPolicyRecordResult:
        view_id = _require_non_empty(policy_input.view_id, field_name='view_id')
        if policy_input.view_version <= 0:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_INVALID_INPUT',
                message='view_version must be > 0',
            )
        if policy_input.bucket_size_seconds <= 0:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_INVALID_INPUT',
                message='bucket_size_seconds must be > 0',
            )
        if policy_input.retention_hot_days <= 0:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_INVALID_INPUT',
                message='retention_hot_days must be > 0',
            )
        if policy_input.retention_warm_days <= 0:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_INVALID_INPUT',
                message='retention_warm_days must be > 0',
            )
        tier_policy = _require_non_empty(policy_input.tier_policy, field_name='tier_policy')
        run_id = _require_non_empty(
            policy_input.recorded_by_run_id, field_name='recorded_by_run_id'
        )
        recorded_at = _ensure_utc(policy_input.recorded_at_utc, field_name='recorded_at_utc')
        stream_key = CanonicalStreamKey(
            source_id=_require_non_empty(
                policy_input.stream_key.source_id, field_name='stream_key.source_id'
            ),
            stream_id=_require_non_empty(
                policy_input.stream_key.stream_id, field_name='stream_key.stream_id'
            ),
            partition_id=_require_non_empty(
                policy_input.stream_key.partition_id,
                field_name='stream_key.partition_id',
            ),
        )
        policy_id = aligned_projection_policy_id(
            view_id=view_id,
            view_version=policy_input.view_version,
            source_id=stream_key.source_id,
            stream_id=stream_key.stream_id,
            partition_id=stream_key.partition_id,
            bucket_size_seconds=policy_input.bucket_size_seconds,
            tier_policy=tier_policy,
            retention_hot_days=policy_input.retention_hot_days,
            retention_warm_days=policy_input.retention_warm_days,
        )
        latest_policy = self.fetch_latest_policy(
            view_id=view_id,
            view_version=policy_input.view_version,
            stream_key=stream_key,
        )
        if latest_policy is None:
            next_revision = 1
        else:
            if latest_policy.policy_id == policy_id:
                return AlignedPolicyRecordResult(
                    status='duplicate',
                    policy_state=latest_policy,
                )
            next_revision = latest_policy.policy_revision + 1
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_aligned_projection_policies
            (
                view_id,
                view_version,
                source_id,
                stream_id,
                partition_id,
                policy_revision,
                policy_id,
                bucket_size_seconds,
                tier_policy,
                retention_hot_days,
                retention_warm_days,
                recorded_by_run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    view_id,
                    policy_input.view_version,
                    stream_key.source_id,
                    stream_key.stream_id,
                    stream_key.partition_id,
                    next_revision,
                    policy_id,
                    policy_input.bucket_size_seconds,
                    tier_policy,
                    policy_input.retention_hot_days,
                    policy_input.retention_warm_days,
                    run_id,
                    recorded_at,
                )
            ],
        )
        latest_after_insert = self.fetch_latest_policy(
            view_id=view_id,
            view_version=policy_input.view_version,
            stream_key=stream_key,
        )
        if latest_after_insert is None:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_MISSING_AFTER_INSERT',
                message='Aligned policy insert failed: latest policy missing',
            )
        if latest_after_insert.policy_revision != next_revision:
            raise ProjectorRuntimeError(
                code='ALIGNED_POLICY_REVISION_MISMATCH',
                message='Aligned policy insert failed: policy revision mismatch',
            )
        return AlignedPolicyRecordResult(
            status='recorded',
            policy_state=latest_after_insert,
        )


class CanonicalAligned1sProjector:
    def __init__(self, *, client: ClickHouseClient, database: str) -> None:
        self._client = client
        self._database = _require_non_empty(database, field_name='database')

    def project_batch(
        self,
        *,
        events: list[ProjectorEvent],
        policy: AlignedProjectionPolicyState,
        projector_id: str,
        projected_at_utc: datetime,
    ) -> list[Aligned1sAggregateRow]:
        if events == []:
            return []
        projector_id_value = _require_non_empty(projector_id, field_name='projector_id')
        projected_at = _ensure_utc(projected_at_utc, field_name='projected_at_utc')
        ordered_events = sorted(
            events,
            key=lambda event: (_require_source_event_time(event), event.event_id),
        )
        buckets: dict[datetime, list[ProjectorEvent]] = {}
        for event in ordered_events:
            bucket_key = _bucket_floor(
                _require_source_event_time(event),
                bucket_size_seconds=policy.bucket_size_seconds,
            )
            existing = buckets.get(bucket_key)
            if existing is None:
                buckets[bucket_key] = [event]
            else:
                existing.append(event)

        output: list[Aligned1sAggregateRow] = []
        for aligned_at in sorted(buckets):
            bucket_events = buckets[aligned_at]
            first_event = bucket_events[0]
            last_event = bucket_events[-1]
            latest_source_event_time_utc = None
            for event in reversed(bucket_events):
                if event.source_event_time_utc is not None:
                    latest_source_event_time_utc = event.source_event_time_utc
                    break
            payload_rows = [
                {
                    'event_id': str(event.event_id),
                    'source_offset_or_equivalent': event.source_offset_or_equivalent,
                    'source_event_time_utc': (
                        None
                        if event.source_event_time_utc is None
                        else event.source_event_time_utc.isoformat()
                    ),
                    'ingested_at_utc': event.ingested_at_utc.isoformat(),
                    'payload_json': event.payload_json,
                    'payload_sha256_raw': event.payload_sha256_raw,
                }
                for event in bucket_events
            ]
            payload_rows_json = _canonical_json({'rows': payload_rows})
            bucket_sha256 = hashlib.sha256(payload_rows_json.encode()).hexdigest()
            output.append(
                Aligned1sAggregateRow(
                    view_id=policy.view_id,
                    view_version=policy.view_version,
                    stream_key=policy.stream_key,
                    aligned_at_utc=aligned_at,
                    bucket_event_count=len(bucket_events),
                    first_event_id=first_event.event_id,
                    last_event_id=last_event.event_id,
                    first_source_offset_or_equivalent=first_event.source_offset_or_equivalent,
                    last_source_offset_or_equivalent=last_event.source_offset_or_equivalent,
                    latest_source_event_time_utc=latest_source_event_time_utc,
                    latest_ingested_at_utc=last_event.ingested_at_utc,
                    payload_rows_json=payload_rows_json,
                    bucket_sha256=bucket_sha256,
                    projector_id=projector_id_value,
                    projected_at_utc=projected_at,
                )
            )
        return output

    def insert_aggregate_rows(self, *, rows: list[Aligned1sAggregateRow]) -> None:
        if rows == []:
            return
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_aligned_1s_aggregates
            (
                view_id,
                view_version,
                source_id,
                stream_id,
                partition_id,
                aligned_at_utc,
                bucket_event_count,
                first_event_id,
                last_event_id,
                first_source_offset_or_equivalent,
                last_source_offset_or_equivalent,
                latest_source_event_time_utc,
                latest_ingested_at_utc,
                payload_rows_json,
                bucket_sha256,
                projector_id,
                projected_at_utc
            )
            VALUES
            ''',
            [row.to_insert_tuple() for row in rows],
        )

    def backfill_from_runtime(
        self,
        *,
        runtime: CanonicalProjectorRuntime,
        policy: AlignedProjectionPolicyState,
        run_id: str,
        projector_id: str,
        projected_at_utc: datetime,
    ) -> AlignedBackfillResult:
        run_id_value = _require_non_empty(run_id, field_name='run_id')
        projected_at = _ensure_utc(projected_at_utc, field_name='projected_at_utc')
        if runtime.projector_id != projector_id:
            raise ProjectorRuntimeError(
                code='ALIGNED_RUNTIME_PROJECTOR_ID_MISMATCH',
                message=(
                    'Runtime projector_id mismatch: '
                    f'runtime={runtime.projector_id} requested={projector_id}'
                ),
            )
        if runtime.stream_key != policy.stream_key:
            raise ProjectorRuntimeError(
                code='ALIGNED_RUNTIME_STREAM_KEY_MISMATCH',
                message='Runtime stream_key does not match aligned projection policy',
            )
        if not runtime.started:
            runtime.start()

        batches_processed = 0
        events_processed = 0
        rows_written = 0
        last_checkpoint_status: str | None = None
        while True:
            batch = runtime.fetch_next_batch()
            if batch == []:
                break
            batches_processed += 1
            events_processed += len(batch)
            projected_rows = self.project_batch(
                events=batch,
                policy=policy,
                projector_id=projector_id,
                projected_at_utc=projected_at,
            )
            self.insert_aggregate_rows(rows=projected_rows)
            rows_written += len(projected_rows)
            checkpoint_result = runtime.commit_checkpoint(
                last_event=batch[-1],
                run_id=run_id_value,
                checkpointed_at_utc=projected_at,
                state={
                    'aligned_view_id': policy.view_id,
                    'aligned_view_version': policy.view_version,
                    'bucket_size_seconds': policy.bucket_size_seconds,
                    'projected_rows': len(projected_rows),
                },
            )
            last_checkpoint_status = checkpoint_result.status
        return AlignedBackfillResult(
            batches_processed=batches_processed,
            events_processed=events_processed,
            rows_written=rows_written,
            last_checkpoint_status=last_checkpoint_status,
        )

    def fetch_aligned_rows(
        self,
        *,
        view_id: str,
        view_version: int,
        stream_key: CanonicalStreamKey,
    ) -> list[Aligned1sAggregateRow]:
        rows = self._client.execute(
            f'''
            SELECT
                aligned_at_utc,
                bucket_event_count,
                first_event_id,
                last_event_id,
                first_source_offset_or_equivalent,
                last_source_offset_or_equivalent,
                latest_source_event_time_utc,
                latest_ingested_at_utc,
                payload_rows_json,
                bucket_sha256,
                projector_id,
                projected_at_utc
            FROM {self._database}.canonical_aligned_1s_aggregates
            WHERE view_id = %(view_id)s
                AND view_version = %(view_version)s
                AND source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY aligned_at_utc ASC
            ''',
            {
                'view_id': view_id,
                'view_version': view_version,
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        return [
            Aligned1sAggregateRow(
                view_id=view_id,
                view_version=view_version,
                stream_key=stream_key,
                aligned_at_utc=row[0],
                bucket_event_count=int(row[1]),
                first_event_id=row[2],
                last_event_id=row[3],
                first_source_offset_or_equivalent=str(row[4]),
                last_source_offset_or_equivalent=str(row[5]),
                latest_source_event_time_utc=row[6],
                latest_ingested_at_utc=row[7],
                payload_rows_json=str(row[8]),
                bucket_sha256=str(row[9]),
                projector_id=str(row[10]),
                projected_at_utc=row[11],
            )
            for row in rows
        ]
