from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient

from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
    CursorAdvanceInput,
)
from origo.events.quarantine import StreamQuarantineRegistry
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c3_proof'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def run_s14_c3_proof() -> dict[str, Any]:
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
    quarantine_state_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-quarantine.json'
    )
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        store = CanonicalIngestStateStore(
            client=admin_client,
            database=proof_database,
            quarantine_registry=StreamQuarantineRegistry(path=quarantine_state_path),
        )
        stream_key = CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        )

        first_advance = store.advance_cursor(
            CursorAdvanceInput(
                stream_key=stream_key,
                next_source_offset_or_equivalent='1700000000001',
                event_id=UUID('4af960d2-68a4-41ca-8b49-6fc7b1f0700f'),
                offset_ordering='numeric',
                run_id='s14-c3-proof-run',
                change_reason='ingest_batch_append',
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                updated_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
            )
        )
        duplicate_advance = store.advance_cursor(
            CursorAdvanceInput(
                stream_key=stream_key,
                next_source_offset_or_equivalent='1700000000001',
                event_id=UUID('4af960d2-68a4-41ca-8b49-6fc7b1f0700f'),
                offset_ordering='numeric',
                run_id='s14-c3-proof-run',
                change_reason='ingest_batch_append',
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                updated_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
            )
        )
        second_advance = store.advance_cursor(
            CursorAdvanceInput(
                stream_key=stream_key,
                next_source_offset_or_equivalent='1700000000002',
                event_id=UUID('866655f8-65c3-4889-b31e-ad8f9f82a98f'),
                offset_ordering='numeric',
                run_id='s14-c3-proof-run',
                change_reason='ingest_batch_append',
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 3, tzinfo=UTC),
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
                updated_at_utc=datetime(2024, 1, 1, 0, 0, 4, tzinfo=UTC),
                expected_previous_source_offset_or_equivalent='1700000000001',
            )
        )

        non_monotonic_error: str | None = None
        try:
            store.advance_cursor(
                CursorAdvanceInput(
                    stream_key=stream_key,
                    next_source_offset_or_equivalent='1700000000001',
                    event_id=UUID('fbe3ec95-91ec-4922-8f49-b3b6ebdc24da'),
                    offset_ordering='numeric',
                    run_id='s14-c3-proof-run',
                    change_reason='ingest_batch_append',
                    ingested_at_utc=datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC),
                    source_event_time_utc=datetime(2024, 1, 1, 0, 0, 3, tzinfo=UTC),
                    updated_at_utc=datetime(2024, 1, 1, 0, 0, 6, tzinfo=UTC),
                    expected_previous_source_offset_or_equivalent='1700000000002',
                )
            )
        except RuntimeError as exc:
            non_monotonic_error = str(exc)

        if non_monotonic_error is None:
            raise RuntimeError(
                'S14-C3 proof expected non-monotonic cursor advance to fail loudly'
            )

        first_checkpoint = store.record_completeness_checkpoint(
            CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='numeric',
                check_scope_start_offset='1700000000001',
                check_scope_end_offset='1700000000002',
                last_checked_source_offset_or_equivalent='1700000000002',
                expected_event_count=2,
                observed_event_count=2,
                gap_count=0,
                status='ok',
                gap_details={},
                checked_by_run_id='s14-c3-proof-run',
                checked_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
            )
        )
        duplicate_checkpoint = store.record_completeness_checkpoint(
            CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='numeric',
                check_scope_start_offset='1700000000001',
                check_scope_end_offset='1700000000002',
                last_checked_source_offset_or_equivalent='1700000000002',
                expected_event_count=2,
                observed_event_count=2,
                gap_count=0,
                status='ok',
                gap_details={},
                checked_by_run_id='s14-c3-proof-run',
                checked_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
            )
        )
        gap_checkpoint = store.record_completeness_checkpoint(
            CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='numeric',
                check_scope_start_offset='1700000000001',
                check_scope_end_offset='1700000000004',
                last_checked_source_offset_or_equivalent='1700000000004',
                expected_event_count=4,
                observed_event_count=3,
                gap_count=1,
                status='gap_detected',
                gap_details={'missing_offsets': ['1700000000003']},
                checked_by_run_id='s14-c3-proof-run',
                checked_at_utc=datetime(2024, 1, 1, 0, 2, 0, tzinfo=UTC),
            )
        )

        latest_cursor = store.fetch_latest_cursor_state(stream_key=stream_key)
        latest_checkpoint = store.fetch_latest_completeness_checkpoint(
            stream_key=stream_key
        )
        if latest_cursor is None:
            raise RuntimeError('S14-C3 proof expected cursor state to exist')
        if latest_checkpoint is None:
            raise RuntimeError('S14-C3 proof expected completeness checkpoint to exist')
        if latest_cursor.cursor_revision != 2:
            raise RuntimeError(
                f'S14-C3 proof expected cursor revision=2, got {latest_cursor.cursor_revision}'
            )
        if latest_checkpoint.checkpoint_revision != 2:
            raise RuntimeError(
                'S14-C3 proof expected completeness checkpoint revision=2, '
                f'got {latest_checkpoint.checkpoint_revision}'
            )
        if latest_checkpoint.status != 'gap_detected':
            raise RuntimeError(
                'S14-C3 proof expected latest completeness status=gap_detected'
            )

        return {
            'proof_scope': 'Slice 14 S14-C3 ingest cursor/ledger and completeness checkpoints',
            'proof_database': proof_database,
            'cursor_advances': [
                {
                    'status': first_advance.status,
                    'cursor_revision': first_advance.cursor_state.cursor_revision,
                    'offset': first_advance.cursor_state.last_source_offset_or_equivalent,
                    'event_id': str(first_advance.cursor_state.last_event_id),
                },
                {
                    'status': duplicate_advance.status,
                    'cursor_revision': duplicate_advance.cursor_state.cursor_revision,
                    'offset': duplicate_advance.cursor_state.last_source_offset_or_equivalent,
                    'event_id': str(duplicate_advance.cursor_state.last_event_id),
                },
                {
                    'status': second_advance.status,
                    'cursor_revision': second_advance.cursor_state.cursor_revision,
                    'offset': second_advance.cursor_state.last_source_offset_or_equivalent,
                    'event_id': str(second_advance.cursor_state.last_event_id),
                },
            ],
            'non_monotonic_error': non_monotonic_error,
            'completeness_checkpoints': [
                {
                    'status': first_checkpoint.status,
                    'checkpoint_revision': first_checkpoint.checkpoint_state.checkpoint_revision,
                    'checkpoint_id': str(first_checkpoint.checkpoint_state.checkpoint_id),
                    'state': first_checkpoint.checkpoint_state.status,
                },
                {
                    'status': duplicate_checkpoint.status,
                    'checkpoint_revision': duplicate_checkpoint.checkpoint_state.checkpoint_revision,
                    'checkpoint_id': str(duplicate_checkpoint.checkpoint_state.checkpoint_id),
                    'state': duplicate_checkpoint.checkpoint_state.status,
                },
                {
                    'status': gap_checkpoint.status,
                    'checkpoint_revision': gap_checkpoint.checkpoint_state.checkpoint_revision,
                    'checkpoint_id': str(gap_checkpoint.checkpoint_state.checkpoint_id),
                    'state': gap_checkpoint.checkpoint_state.status,
                },
            ],
            'latest_cursor_revision': latest_cursor.cursor_revision,
            'latest_checkpoint_revision': latest_checkpoint.checkpoint_revision,
            'latest_checkpoint_status': latest_checkpoint.status,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()
        if quarantine_state_path.exists():
            quarantine_state_path.unlink()


def main() -> None:
    payload = run_s14_c3_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c3-ingest-reconciliation.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
