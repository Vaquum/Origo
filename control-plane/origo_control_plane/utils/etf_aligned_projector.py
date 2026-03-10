from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from clickhouse_driver import Client as ClickhouseClient

from origo.events.aligned_projector import (
    Aligned1sAggregateRow,
    AlignedProjectionPolicyInput,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime, ProjectorEvent

_CANONICAL_SOURCE_ID = 'etf'
_CANONICAL_STREAM_ID = 'etf_daily_metrics'
_ALIGNED_VIEW_ID = 'aligned_1s_raw'
_ALIGNED_VIEW_VERSION = 1
_ALIGNED_BUCKET_SECONDS = 1
_ALIGNED_TIER_POLICY = 'hot_1s_warm_1m'
_ALIGNED_RETENTION_HOT_DAYS = 7
_ALIGNED_RETENTION_WARM_DAYS = 90
_PROJECTOR_ID = 'etf_daily_metrics_aligned_1s_v1'


def _require_non_empty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be set and non-empty')
    return normalized


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def _bucket_floor_source_event(source_event_time_utc: datetime) -> datetime:
    normalized = _require_utc(
        source_event_time_utc,
        label='source_event_time_utc',
    )
    seconds = int(normalized.timestamp())
    return datetime.fromtimestamp(seconds, tz=UTC)


def _canonical_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _require_source_event_time(event: ProjectorEvent) -> datetime:
    if event.source_event_time_utc is None:
        raise RuntimeError(
            'ETF aligned projector requires source_event_time_utc for every event'
        )
    return _require_utc(
        event.source_event_time_utc,
        label='event.source_event_time_utc',
    )


def _project_batch(
    *,
    events: list[ProjectorEvent],
    stream_key: CanonicalStreamKey,
    projected_at_utc: datetime,
) -> list[Aligned1sAggregateRow]:
    if events == []:
        return []

    buckets: dict[datetime, list[ProjectorEvent]] = {}
    for event in sorted(
        events,
        key=lambda item: (
            _require_source_event_time(item),
            item.event_id,
        ),
    ):
        aligned_at_utc = _bucket_floor_source_event(_require_source_event_time(event))
        existing = buckets.get(aligned_at_utc)
        if existing is None:
            buckets[aligned_at_utc] = [event]
        else:
            existing.append(event)

    output: list[Aligned1sAggregateRow] = []
    for aligned_at_utc in sorted(buckets):
        bucket_events = buckets[aligned_at_utc]
        first_event = bucket_events[0]
        last_event = bucket_events[-1]
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
        bucket_sha256 = hashlib.sha256(payload_rows_json.encode('utf-8')).hexdigest()
        latest_source_event_time_utc = _require_source_event_time(last_event)

        output.append(
            Aligned1sAggregateRow(
                view_id=_ALIGNED_VIEW_ID,
                view_version=_ALIGNED_VIEW_VERSION,
                stream_key=stream_key,
                aligned_at_utc=aligned_at_utc,
                bucket_event_count=len(bucket_events),
                first_event_id=first_event.event_id,
                last_event_id=last_event.event_id,
                first_source_offset_or_equivalent=first_event.source_offset_or_equivalent,
                last_source_offset_or_equivalent=last_event.source_offset_or_equivalent,
                latest_source_event_time_utc=latest_source_event_time_utc,
                latest_ingested_at_utc=_require_utc(
                    last_event.ingested_at_utc,
                    label='last_event.ingested_at_utc',
                ),
                payload_rows_json=payload_rows_json,
                bucket_sha256=bucket_sha256,
                projector_id=_PROJECTOR_ID,
                projected_at_utc=_require_utc(
                    projected_at_utc,
                    label='projected_at_utc',
                ),
            )
        )
    return output


@dataclass(frozen=True)
class ETFAlignedProjectorSummary:
    partitions_processed: int
    policies_recorded: int
    policies_duplicate: int
    batches_processed: int
    events_processed: int
    rows_written: int

    def to_dict(self) -> dict[str, int]:
        return {
            'partitions_processed': self.partitions_processed,
            'policies_recorded': self.policies_recorded,
            'policies_duplicate': self.policies_duplicate,
            'batches_processed': self.batches_processed,
            'events_processed': self.events_processed,
            'rows_written': self.rows_written,
        }


def _aggregate_summaries(
    summaries: list[ETFAlignedProjectorSummary],
) -> ETFAlignedProjectorSummary:
    return ETFAlignedProjectorSummary(
        partitions_processed=sum(summary.partitions_processed for summary in summaries),
        policies_recorded=sum(summary.policies_recorded for summary in summaries),
        policies_duplicate=sum(summary.policies_duplicate for summary in summaries),
        batches_processed=sum(summary.batches_processed for summary in summaries),
        events_processed=sum(summary.events_processed for summary in summaries),
        rows_written=sum(summary.rows_written for summary in summaries),
    )


def _project_partition(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
) -> ETFAlignedProjectorSummary:
    normalized_partition_id = _require_non_empty(partition_id, label='partition_id')
    normalized_run_id = _require_non_empty(run_id, label='run_id')
    normalized_projected_at_utc = _require_utc(
        projected_at_utc,
        label='projected_at_utc',
    )
    stream_key = CanonicalStreamKey(
        source_id=_CANONICAL_SOURCE_ID,
        stream_id=_CANONICAL_STREAM_ID,
        partition_id=normalized_partition_id,
    )
    policy_store = CanonicalAlignedPolicyStore(client=client, database=database)
    policy_result = policy_store.record_policy(
        AlignedProjectionPolicyInput(
            view_id=_ALIGNED_VIEW_ID,
            view_version=_ALIGNED_VIEW_VERSION,
            stream_key=stream_key,
            bucket_size_seconds=_ALIGNED_BUCKET_SECONDS,
            tier_policy=_ALIGNED_TIER_POLICY,
            retention_hot_days=_ALIGNED_RETENTION_HOT_DAYS,
            retention_warm_days=_ALIGNED_RETENTION_WARM_DAYS,
            recorded_by_run_id=normalized_run_id,
            recorded_at_utc=normalized_projected_at_utc,
        )
    )

    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=batch_size,
    )
    aligned_projector = CanonicalAligned1sProjector(client=client, database=database)

    runtime.start()
    batches_processed = 0
    events_processed = 0
    rows_written = 0
    try:
        while True:
            batch = runtime.fetch_next_batch()
            if batch == []:
                break
            batches_processed += 1
            events_processed += len(batch)
            projected_rows = _project_batch(
                events=batch,
                stream_key=stream_key,
                projected_at_utc=normalized_projected_at_utc,
            )
            aligned_projector.insert_aggregate_rows(rows=projected_rows)
            rows_written += len(projected_rows)
            runtime.commit_checkpoint(
                last_event=batch[-1],
                run_id=normalized_run_id,
                checkpointed_at_utc=normalized_projected_at_utc,
                state={
                    'aligned_view_id': _ALIGNED_VIEW_ID,
                    'aligned_view_version': _ALIGNED_VIEW_VERSION,
                    'bucket_size_seconds': _ALIGNED_BUCKET_SECONDS,
                    'projected_rows': len(projected_rows),
                },
            )
    finally:
        runtime.stop()

    return ETFAlignedProjectorSummary(
        partitions_processed=1,
        policies_recorded=1 if policy_result.status == 'recorded' else 0,
        policies_duplicate=1 if policy_result.status == 'duplicate' else 0,
        batches_processed=batches_processed,
        events_processed=events_processed,
        rows_written=rows_written,
    )


def project_etf_daily_metrics_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ETFAlignedProjectorSummary:
    normalized_database = _require_non_empty(database, label='database')
    normalized_partitions = sorted(
        {_require_non_empty(partition_id, label='partition_id') for partition_id in partition_ids}
    )
    if normalized_partitions == []:
        raise RuntimeError('partition_ids must contain at least one partition_id')

    summaries: list[ETFAlignedProjectorSummary] = []
    for partition_id in normalized_partitions:
        summaries.append(
            _project_partition(
                client=client,
                database=normalized_database,
                partition_id=partition_id,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
                batch_size=batch_size,
            )
        )
    return _aggregate_summaries(summaries)
