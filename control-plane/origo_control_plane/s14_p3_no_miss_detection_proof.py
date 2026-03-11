from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
)
from origo.events.quarantine import StreamQuarantineRegistry
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_p3_proof'


@dataclass(frozen=True)
class _FixtureEvent:
    source_offset_or_equivalent: str
    source_event_time_utc: datetime
    payload_raw: bytes


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_events() -> tuple[_FixtureEvent, ...]:
    return (
        _FixtureEvent(
            source_offset_or_equivalent='1001',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43000.10000000","qty":"0.01000000",'
                b'"quote_qty":"430.00100000","trade_id":"1001"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1002',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 300000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"43000.20000000","qty":"0.02000000",'
                b'"quote_qty":"860.00400000","trade_id":"1002"}'
            ),
        ),
        # 1003 intentionally omitted to inject a deterministic source gap.
        _FixtureEvent(
            source_offset_or_equivalent='1004',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43000.30000000","qty":"0.03000000",'
                b'"quote_qty":"1290.00900000","trade_id":"1004"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1005',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, 400000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"43000.40000000","qty":"0.04000000",'
                b'"quote_qty":"1720.01600000","trade_id":"1005"}'
            ),
        ),
    )


def _parse_numeric_offset(offset: str) -> int:
    try:
        return int(offset)
    except ValueError as exc:
        raise RuntimeError(
            f'Offset must be numeric for S14-P3 no-miss proof, got {offset!r}'
        ) from exc


def _detect_missing_offsets(observed_offsets: list[str]) -> list[str]:
    if observed_offsets == []:
        return []
    ordered = sorted(_parse_numeric_offset(offset) for offset in observed_offsets)
    missing: list[str] = []
    expected = ordered[0]
    for value in ordered:
        while expected < value:
            missing.append(str(expected))
            expected += 1
        expected = value + 1
    return missing


def _enforce_no_miss_or_raise(
    *, stream_key: CanonicalStreamKey, missing_offsets: list[str]
) -> None:
    if missing_offsets == []:
        return
    raise RuntimeError(
        'No-miss reconciliation failed: '
        f'source={stream_key.source_id} stream={stream_key.stream_id} '
        f'partition={stream_key.partition_id} missing_offsets={missing_offsets}'
    )


def run_s14_p3_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    fixture_events = _fixture_events()
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    quarantine_state_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-quarantine.json'
    )
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        writer = CanonicalEventWriter(client=admin_client, database=proof_database)
        ingest_statuses: list[str] = []
        for event in fixture_events:
            result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id=stream_key.source_id,
                    stream_id=stream_key.stream_id,
                    partition_id=stream_key.partition_id,
                    source_offset_or_equivalent=event.source_offset_or_equivalent,
                    source_event_time_utc=event.source_event_time_utc,
                    ingested_at_utc=event.source_event_time_utc,
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=event.payload_raw,
                )
            )
            ingest_statuses.append(result.status)
        if ingest_statuses != ['inserted', 'inserted', 'inserted', 'inserted']:
            raise RuntimeError(
                'S14-P3 proof expected all fixture writes to be inserted, '
                f'got statuses={ingest_statuses}'
            )

        observed_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY toInt64(source_offset_or_equivalent) ASC
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        observed_offsets = [str(row[0]) for row in observed_rows]
        missing_offsets = _detect_missing_offsets(observed_offsets)
        expected_missing_offsets = ['1003']
        if missing_offsets != expected_missing_offsets:
            raise RuntimeError(
                'S14-P3 proof expected injected missing offsets to be '
                f'{expected_missing_offsets}, got {missing_offsets}'
            )

        expected_event_count = (
            _parse_numeric_offset(observed_offsets[-1])
            - _parse_numeric_offset(observed_offsets[0])
            + 1
        )
        observed_event_count = len(observed_offsets)

        state_store = CanonicalIngestStateStore(
            client=admin_client,
            database=proof_database,
            quarantine_registry=StreamQuarantineRegistry(path=quarantine_state_path),
        )
        checkpoint = state_store.record_completeness_checkpoint(
            CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='numeric',
                check_scope_start_offset=observed_offsets[0],
                check_scope_end_offset=observed_offsets[-1],
                last_checked_source_offset_or_equivalent=observed_offsets[-1],
                expected_event_count=expected_event_count,
                observed_event_count=observed_event_count,
                gap_count=len(missing_offsets),
                status='gap_detected',
                gap_details={'missing_offsets': missing_offsets},
                checked_by_run_id='s14-p3-proof-run',
                checked_at_utc=datetime(2024, 1, 1, 0, 2, 0, tzinfo=UTC),
            )
        )
        if checkpoint.status != 'recorded':
            raise RuntimeError(
                f'S14-P3 proof expected checkpoint status=recorded, got {checkpoint.status}'
            )
        if checkpoint.checkpoint_state.status != 'gap_detected':
            raise RuntimeError(
                'S14-P3 proof expected checkpoint state status=gap_detected'
            )

        duplicate_checkpoint = state_store.record_completeness_checkpoint(
            CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='numeric',
                check_scope_start_offset=observed_offsets[0],
                check_scope_end_offset=observed_offsets[-1],
                last_checked_source_offset_or_equivalent=observed_offsets[-1],
                expected_event_count=expected_event_count,
                observed_event_count=observed_event_count,
                gap_count=len(missing_offsets),
                status='gap_detected',
                gap_details={'missing_offsets': missing_offsets},
                checked_by_run_id='s14-p3-proof-run',
                checked_at_utc=datetime(2024, 1, 1, 0, 2, 0, tzinfo=UTC),
            )
        )
        if duplicate_checkpoint.status != 'duplicate':
            raise RuntimeError(
                'S14-P3 proof expected duplicate checkpoint record to return status=duplicate'
            )

        quarantine_error: str | None = None
        try:
            _enforce_no_miss_or_raise(
                stream_key=stream_key,
                missing_offsets=missing_offsets,
            )
        except RuntimeError as exc:
            quarantine_error = str(exc)

        if quarantine_error is None:
            raise RuntimeError(
                'S14-P3 proof expected fail-loud no-miss reconciliation gate to raise'
            )

        return {
            'proof_scope': (
                'Slice 14 S14-P3 no-miss detection with injected gaps and '
                'reconciliation checks'
            ),
            'proof_database': proof_database,
            'stream_key': {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
            'ingest_statuses': ingest_statuses,
            'observed_offsets': observed_offsets,
            'missing_offsets': missing_offsets,
            'expected_event_count': expected_event_count,
            'observed_event_count': observed_event_count,
            'checkpoint': {
                'status': checkpoint.status,
                'checkpoint_revision': checkpoint.checkpoint_state.checkpoint_revision,
                'checkpoint_state': checkpoint.checkpoint_state.status,
                'checkpoint_id': str(checkpoint.checkpoint_state.checkpoint_id),
            },
            'duplicate_checkpoint_status': duplicate_checkpoint.status,
            'quarantine_triggered': True,
            'quarantine_error': quarantine_error,
            'no_miss_detection_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()
        if quarantine_state_path.exists():
            quarantine_state_path.unlink()


def main() -> None:
    payload = run_s14_p3_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p3-no-miss-detection.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
