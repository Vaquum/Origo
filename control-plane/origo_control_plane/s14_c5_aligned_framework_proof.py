from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.aligned_projector import (
    AlignedProjectionPolicyInput,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c5_proof'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def run_s14_c5_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        stream_key = CanonicalStreamKey(
            source_id='binance',
            stream_id='spot_trades',
            partition_id='btcusdt',
        )
        writer = CanonicalEventWriter(client=admin_client, database=proof_database)
        write_statuses: list[str] = []
        for offset, second, ms in [
            ('1', 1, 100000),
            ('2', 1, 800000),
            ('3', 2, 200000),
            ('4', 2, 900000),
        ]:
            result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id=stream_key.source_id,
                    stream_id=stream_key.stream_id,
                    partition_id=stream_key.partition_id,
                    source_offset_or_equivalent=offset,
                    source_event_time_utc=datetime(2024, 1, 1, 0, 0, second, ms, tzinfo=UTC),
                    ingested_at_utc=datetime(2024, 1, 1, 0, 0, second, ms, tzinfo=UTC),
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=f'{{"trade_id":"{offset}","qty":"0.{offset}"}}'.encode(),
                )
            )
            write_statuses.append(result.status)
        if write_statuses != ['inserted', 'inserted', 'inserted', 'inserted']:
            raise RuntimeError(
                'S14-C5 proof expected four inserted canonical events, '
                f'got {write_statuses}'
            )

        policy_store = CanonicalAlignedPolicyStore(
            client=admin_client, database=proof_database
        )
        policy_result = policy_store.record_policy(
            AlignedProjectionPolicyInput(
                view_id='aligned_1s_raw',
                view_version=1,
                stream_key=stream_key,
                bucket_size_seconds=1,
                tier_policy='hot_1s_warm_1m',
                retention_hot_days=7,
                retention_warm_days=90,
                recorded_by_run_id='s14-c5-proof-policy',
                recorded_at_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=UTC),
            )
        )
        duplicate_policy_result = policy_store.record_policy(
            AlignedProjectionPolicyInput(
                view_id='aligned_1s_raw',
                view_version=1,
                stream_key=stream_key,
                bucket_size_seconds=1,
                tier_policy='hot_1s_warm_1m',
                retention_hot_days=7,
                retention_warm_days=90,
                recorded_by_run_id='s14-c5-proof-policy',
                recorded_at_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=UTC),
            )
        )
        if policy_result.status != 'recorded':
            raise RuntimeError('S14-C5 proof expected first policy write status=recorded')
        if duplicate_policy_result.status != 'duplicate':
            raise RuntimeError('S14-C5 proof expected duplicate policy write status=duplicate')

        aligned_projector = CanonicalAligned1sProjector(
            client=admin_client,
            database=proof_database,
        )
        runtime_1 = CanonicalProjectorRuntime(
            client=admin_client,
            database=proof_database,
            projector_id='aligned_1s_projection_v1',
            stream_key=stream_key,
            batch_size=10,
        )
        backfill_1 = aligned_projector.backfill_from_runtime(
            runtime=runtime_1,
            policy=policy_result.policy_state,
            run_id='s14-c5-proof-run-1',
            projector_id='aligned_1s_projection_v1',
            projected_at_utc=datetime(2024, 1, 1, 0, 6, 0, tzinfo=UTC),
        )
        runtime_1.stop()

        runtime_2 = CanonicalProjectorRuntime(
            client=admin_client,
            database=proof_database,
            projector_id='aligned_1s_projection_v1',
            stream_key=stream_key,
            batch_size=10,
        )
        backfill_2 = aligned_projector.backfill_from_runtime(
            runtime=runtime_2,
            policy=policy_result.policy_state,
            run_id='s14-c5-proof-run-2',
            projector_id='aligned_1s_projection_v1',
            projected_at_utc=datetime(2024, 1, 1, 0, 7, 0, tzinfo=UTC),
        )
        runtime_2.stop()

        aligned_rows = aligned_projector.fetch_aligned_rows(
            view_id='aligned_1s_raw',
            view_version=1,
            stream_key=stream_key,
        )
        if len(aligned_rows) != 2:
            raise RuntimeError(
                f'S14-C5 proof expected 2 aligned rows, got {len(aligned_rows)}'
            )
        bucket_event_counts = [row.bucket_event_count for row in aligned_rows]
        if bucket_event_counts != [2, 2]:
            raise RuntimeError(
                'S14-C5 proof expected bucket event counts [2, 2], '
                f'got {bucket_event_counts}'
            )
        if any(len(row.bucket_sha256) != 64 for row in aligned_rows):
            raise RuntimeError('S14-C5 proof expected 64-char bucket_sha256 values')
        if backfill_1.events_processed != 4 or backfill_1.rows_written != 2:
            raise RuntimeError(
                'S14-C5 proof expected first backfill to process 4 events into 2 rows'
            )
        if backfill_2.events_processed != 0 or backfill_2.rows_written != 0:
            raise RuntimeError(
                'S14-C5 proof expected resume backfill to process no additional rows'
            )

        return {
            'proof_scope': 'Slice 14 S14-C5 persistent aligned framework (policy + projector backfill)',
            'proof_database': proof_database,
            'canonical_event_write_statuses': write_statuses,
            'policy_record_statuses': {
                'first': policy_result.status,
                'duplicate': duplicate_policy_result.status,
                'policy_revision': policy_result.policy_state.policy_revision,
                'bucket_size_seconds': policy_result.policy_state.bucket_size_seconds,
                'tier_policy': policy_result.policy_state.tier_policy,
            },
            'backfill_runs': {
                'run_1': {
                    'batches_processed': backfill_1.batches_processed,
                    'events_processed': backfill_1.events_processed,
                    'rows_written': backfill_1.rows_written,
                    'last_checkpoint_status': backfill_1.last_checkpoint_status,
                },
                'run_2_resume': {
                    'batches_processed': backfill_2.batches_processed,
                    'events_processed': backfill_2.events_processed,
                    'rows_written': backfill_2.rows_written,
                    'last_checkpoint_status': backfill_2.last_checkpoint_status,
                },
            },
            'aligned_rows': [
                {
                    'aligned_at_utc': row.aligned_at_utc.isoformat(),
                    'bucket_event_count': row.bucket_event_count,
                    'first_offset': row.first_source_offset_or_equivalent,
                    'last_offset': row.last_source_offset_or_equivalent,
                    'bucket_sha256': row.bucket_sha256,
                }
                for row in aligned_rows
            ],
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_c5_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c5-aligned-framework.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
