from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from clickhouse_driver import Client as ClickhouseClient

from origo.events.aligned_projector import (
    AlignedProjectionPolicyInput,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime

BitcoinDerivedStreamId = Literal[
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]

_ALIGNED_VIEW_ID = 'aligned_1s_raw'
_ALIGNED_VIEW_VERSION = 1
_ALIGNED_BUCKET_SECONDS = 1
_ALIGNED_TIER_POLICY = 'hot_1s_warm_1m'
_ALIGNED_RETENTION_HOT_DAYS = 7
_ALIGNED_RETENTION_WARM_DAYS = 90


def _require_non_empty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be set and non-empty')
    return normalized


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware')
    return value.astimezone(UTC)


@dataclass(frozen=True)
class AlignedProjectorSummary:
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
    summaries: list[AlignedProjectorSummary],
) -> AlignedProjectorSummary:
    return AlignedProjectorSummary(
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
    stream_key: CanonicalStreamKey,
    projector_id: str,
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
) -> AlignedProjectorSummary:
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
            recorded_by_run_id=run_id,
            recorded_at_utc=projected_at_utc,
        )
    )

    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=projector_id,
        stream_key=stream_key,
        batch_size=batch_size,
        require_terminal_partition_proof=True,
    )
    aligned_projector = CanonicalAligned1sProjector(client=client, database=database)

    runtime.start()
    try:
        backfill_result = aligned_projector.backfill_from_runtime(
            runtime=runtime,
            policy=policy_result.policy_state,
            run_id=run_id,
            projector_id=projector_id,
            projected_at_utc=projected_at_utc,
        )
    finally:
        runtime.stop()

    return AlignedProjectorSummary(
        partitions_processed=1,
        policies_recorded=1 if policy_result.status == 'recorded' else 0,
        policies_duplicate=1 if policy_result.status == 'duplicate' else 0,
        batches_processed=backfill_result.batches_processed,
        events_processed=backfill_result.events_processed,
        rows_written=backfill_result.rows_written,
    )


def _project_stream_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    stream_id: BitcoinDerivedStreamId,
    projector_id: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
) -> AlignedProjectorSummary:
    normalized_database = _require_non_empty(database, label='database')
    normalized_run_id = _require_non_empty(run_id, label='run_id')
    normalized_projector_id = _require_non_empty(projector_id, label='projector_id')
    normalized_projected_at_utc = _require_utc(
        projected_at_utc,
        label='projected_at_utc',
    )

    normalized_partitions = sorted(
        {_require_non_empty(partition_id, label='partition_id') for partition_id in partition_ids}
    )
    if normalized_partitions == []:
        raise RuntimeError('partition_ids must contain at least one partition_id')

    summaries: list[AlignedProjectorSummary] = []
    for partition_id in normalized_partitions:
        summaries.append(
            _project_partition(
                client=client,
                database=normalized_database,
                stream_key=CanonicalStreamKey(
                    source_id='bitcoin_core',
                    stream_id=stream_id,
                    partition_id=partition_id,
                ),
                projector_id=normalized_projector_id,
                run_id=normalized_run_id,
                projected_at_utc=normalized_projected_at_utc,
                batch_size=batch_size,
            )
        )

    return _aggregate_summaries(summaries)


def project_bitcoin_block_fee_totals_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> AlignedProjectorSummary:
    return _project_stream_aligned(
        client=client,
        database=database,
        stream_id='bitcoin_block_fee_totals',
        projector_id='bitcoin_block_fee_totals_aligned_1s_v1',
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
    )


def project_bitcoin_block_subsidy_schedule_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> AlignedProjectorSummary:
    return _project_stream_aligned(
        client=client,
        database=database,
        stream_id='bitcoin_block_subsidy_schedule',
        projector_id='bitcoin_block_subsidy_schedule_aligned_1s_v1',
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
    )


def project_bitcoin_network_hashrate_estimate_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> AlignedProjectorSummary:
    return _project_stream_aligned(
        client=client,
        database=database,
        stream_id='bitcoin_network_hashrate_estimate',
        projector_id='bitcoin_network_hashrate_estimate_aligned_1s_v1',
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
    )


def project_bitcoin_circulating_supply_aligned(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> AlignedProjectorSummary:
    return _project_stream_aligned(
        client=client,
        database=database,
        stream_id='bitcoin_circulating_supply',
        projector_id='bitcoin_circulating_supply_aligned_1s_v1',
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
    )
