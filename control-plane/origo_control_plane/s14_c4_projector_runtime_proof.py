from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c4_proof'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def run_s14_c4_proof() -> dict[str, Any]:
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

        writer = CanonicalEventWriter(client=admin_client, database=proof_database)
        stream_key = CanonicalStreamKey(
            source_id='binance',
            stream_id='spot_trades',
            partition_id='btcusdt',
        )

        write_results: list[str] = []
        for offset, second in [('1', 1), ('2', 2), ('3', 3)]:
            result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id=stream_key.source_id,
                    stream_id=stream_key.stream_id,
                    partition_id=stream_key.partition_id,
                    source_offset_or_equivalent=offset,
                    source_event_time_utc=datetime(2024, 1, 1, 0, 0, second, tzinfo=UTC),
                    ingested_at_utc=datetime(2024, 1, 1, 0, 0, second, tzinfo=UTC),
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=f'{{"trade_id":"{offset}","price":"30000.{offset}"}}'.encode(),
                )
            )
            write_results.append(result.status)
        if write_results != ['inserted', 'inserted', 'inserted']:
            raise RuntimeError(
                'S14-C4 proof expected three inserted canonical events, '
                f'got {write_results}'
            )

        runtime_1 = CanonicalProjectorRuntime(
            client=admin_client,
            database=proof_database,
            projector_id='native_spot_projection_v1',
            stream_key=stream_key,
            batch_size=1,
        )
        start_1 = runtime_1.start()
        batch_1 = runtime_1.fetch_next_batch()
        if len(batch_1) != 1:
            raise RuntimeError(
                f'S14-C4 proof expected first runtime batch size=1, got {len(batch_1)}'
            )
        commit_1 = runtime_1.commit_checkpoint(
            last_event=batch_1[-1],
            run_id='s14-c4-proof-run-1',
            checkpointed_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
            state={'processed_offsets': [batch_1[-1].source_offset_or_equivalent]},
        )
        runtime_1.stop()

        runtime_2 = CanonicalProjectorRuntime(
            client=admin_client,
            database=proof_database,
            projector_id='native_spot_projection_v1',
            stream_key=stream_key,
            batch_size=2,
        )
        start_2 = runtime_2.start()
        batch_2 = runtime_2.fetch_next_batch()
        batch_2_offsets = [event.source_offset_or_equivalent for event in batch_2]
        if batch_2_offsets != ['2', '3']:
            raise RuntimeError(
                'S14-C4 proof expected second runtime batch offsets [2, 3], '
                f'got {batch_2_offsets}'
            )
        commit_2 = runtime_2.commit_checkpoint(
            last_event=batch_2[-1],
            run_id='s14-c4-proof-run-2',
            checkpointed_at_utc=datetime(2024, 1, 1, 0, 2, 0, tzinfo=UTC),
            state={'processed_offsets': batch_2_offsets},
        )
        runtime_2.stop()

        runtime_3 = CanonicalProjectorRuntime(
            client=admin_client,
            database=proof_database,
            projector_id='native_spot_projection_v1',
            stream_key=stream_key,
            batch_size=2,
        )
        start_3 = runtime_3.start()
        duplicate_commit = runtime_3.commit_checkpoint(
            last_event=batch_2[-1],
            run_id='s14-c4-proof-run-3',
            checkpointed_at_utc=datetime(2024, 1, 1, 0, 3, 0, tzinfo=UTC),
            state={'processed_offsets': batch_2_offsets},
        )
        batch_3 = runtime_3.fetch_next_batch()
        runtime_3.stop()

        if batch_3 != []:
            raise RuntimeError(
                f'S14-C4 proof expected resumed runtime batch to be empty, got {len(batch_3)} rows'
            )
        if commit_1.status != 'checkpointed':
            raise RuntimeError(
                f'S14-C4 proof expected first commit status=checkpointed, got {commit_1.status}'
            )
        if commit_2.status != 'checkpointed':
            raise RuntimeError(
                f'S14-C4 proof expected second commit status=checkpointed, got {commit_2.status}'
            )
        if duplicate_commit.status != 'duplicate':
            raise RuntimeError(
                'S14-C4 proof expected duplicate commit on already checkpointed event'
            )

        latest_checkpoint = runtime_3.fetch_latest_checkpoint()
        latest_watermark = runtime_3.fetch_latest_watermark()
        if latest_checkpoint is None:
            raise RuntimeError('S14-C4 proof expected latest checkpoint')
        if latest_watermark is None:
            raise RuntimeError('S14-C4 proof expected latest watermark')
        if latest_checkpoint.checkpoint_revision != 2:
            raise RuntimeError(
                'S14-C4 proof expected latest checkpoint revision=2, '
                f'got {latest_checkpoint.checkpoint_revision}'
            )
        if latest_watermark.watermark_revision != 2:
            raise RuntimeError(
                'S14-C4 proof expected latest watermark revision=2, '
                f'got {latest_watermark.watermark_revision}'
            )
        if latest_checkpoint.last_source_offset_or_equivalent != '3':
            raise RuntimeError(
                'S14-C4 proof expected latest checkpoint offset=3, '
                f'got {latest_checkpoint.last_source_offset_or_equivalent}'
            )

        return {
            'proof_scope': 'Slice 14 S14-C4 projector runtime checkpoint/watermark resume',
            'proof_database': proof_database,
            'canonical_event_write_statuses': write_results,
            'runtime_starts': [
                {
                    'run': 1,
                    'resumed': start_1.resumed,
                    'checkpoint_revision': (
                        None
                        if start_1.checkpoint is None
                        else start_1.checkpoint.checkpoint_revision
                    ),
                },
                {
                    'run': 2,
                    'resumed': start_2.resumed,
                    'checkpoint_revision': (
                        None
                        if start_2.checkpoint is None
                        else start_2.checkpoint.checkpoint_revision
                    ),
                },
                {
                    'run': 3,
                    'resumed': start_3.resumed,
                    'checkpoint_revision': (
                        None
                        if start_3.checkpoint is None
                        else start_3.checkpoint.checkpoint_revision
                    ),
                },
            ],
            'batch_offsets': {
                'run_1': [event.source_offset_or_equivalent for event in batch_1],
                'run_2': batch_2_offsets,
                'run_3': [event.source_offset_or_equivalent for event in batch_3],
            },
            'commit_statuses': {
                'run_1': commit_1.status,
                'run_2': commit_2.status,
                'run_3_duplicate_commit': duplicate_commit.status,
            },
            'latest_checkpoint': {
                'revision': latest_checkpoint.checkpoint_revision,
                'offset': latest_checkpoint.last_source_offset_or_equivalent,
                'event_id': str(latest_checkpoint.last_event_id),
            },
            'latest_watermark': {
                'revision': latest_watermark.watermark_revision,
                'offset': latest_watermark.watermark_source_offset_or_equivalent,
                'event_id': str(latest_watermark.watermark_event_id),
            },
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_c4_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c4-projector-runtime.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
