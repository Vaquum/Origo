from __future__ import annotations

import importlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.errors import EventWriterError
from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
)
from origo.events.quarantine import StreamQuarantineRegistry
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_g3_proof'


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
            source_offset_or_equivalent='5001',
            source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43400.10000000","qty":"0.01000000",'
                b'"quote_qty":"434.00100000","trade_id":"5001"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='5003',
            source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 900000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"43400.30000000","qty":"0.03000000",'
                b'"quote_qty":"1302.00900000","trade_id":"5003"}'
            ),
        ),
    )


@contextmanager
def _temporary_env(env: dict[str, str]) -> Iterator[None]:
    previous: dict[str, str | None] = {key: os.environ.get(key) for key in env}
    try:
        for key, value in env.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run_s14_g3_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
    )
    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    fixture = _fixture_events()

    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        runtime_audit_log_path = temp_path / 'audit' / 'canonical-runtime-events.jsonl'
        quarantine_state_path = temp_path / 'audit' / 'stream-quarantine-state.json'
        with _temporary_env(
            {
                'ORIGO_AUDIT_LOG_RETENTION_DAYS': '365',
                'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH': str(runtime_audit_log_path),
                'ORIGO_STREAM_QUARANTINE_STATE_PATH': str(quarantine_state_path),
            }
        ):
            runtime_audit_module = importlib.import_module('origo.events.runtime_audit')
            setattr(runtime_audit_module, '_runtime_audit_singleton', None)

            quarantine_registry = StreamQuarantineRegistry(path=quarantine_state_path)
            try:
                admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
                runner.migrate()

                writer = CanonicalEventWriter(
                    client=admin_client,
                    database=proof_database,
                    quarantine_registry=quarantine_registry,
                )
                write_statuses: list[str] = []
                for event in fixture:
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
                            run_id='s14-g3-proof',
                        )
                    )
                    write_statuses.append(result.status)
                if write_statuses != ['inserted', 'inserted']:
                    raise RuntimeError(
                        'S14-G3 proof expected fixture writes to be inserted, '
                        f'got statuses={write_statuses}'
                    )

                store = CanonicalIngestStateStore(
                    client=admin_client,
                    database=proof_database,
                    quarantine_registry=quarantine_registry,
                )
                checkpoint_result = store.record_completeness_checkpoint(
                    CompletenessCheckpointInput(
                        stream_key=stream_key,
                        offset_ordering='numeric',
                        check_scope_start_offset='5001',
                        check_scope_end_offset='5003',
                        last_checked_source_offset_or_equivalent='5003',
                        expected_event_count=3,
                        observed_event_count=2,
                        gap_count=1,
                        status='gap_detected',
                        gap_details={'missing_offsets': ['5002']},
                        checked_by_run_id='s14-g3-proof',
                        checked_at_utc=datetime(2024, 1, 5, 0, 1, 0, tzinfo=UTC),
                    )
                )
                if checkpoint_result.status != 'recorded':
                    raise RuntimeError(
                        'S14-G3 proof expected gap checkpoint status=recorded'
                    )

                quarantine_state = quarantine_registry.get(stream_key=stream_key)
                if quarantine_state is None:
                    raise RuntimeError(
                        'S14-G3 proof expected stream quarantine state to exist'
                    )

                blocked_error_code: str | None = None
                try:
                    writer.write_event(
                        CanonicalEventWriteInput(
                            source_id=stream_key.source_id,
                            stream_id=stream_key.stream_id,
                            partition_id=stream_key.partition_id,
                            source_offset_or_equivalent='5004',
                            source_event_time_utc=datetime(
                                2024,
                                1,
                                5,
                                0,
                                0,
                                1,
                                tzinfo=UTC,
                            ),
                            ingested_at_utc=datetime(
                                2024,
                                1,
                                5,
                                0,
                                0,
                                1,
                                tzinfo=UTC,
                            ),
                            payload_content_type='application/json',
                            payload_encoding='utf-8',
                            payload_raw=(
                                b'{"is_best_match":true,"is_buyer_maker":false,'
                                b'"price":"43400.40000000","qty":"0.04000000",'
                                b'"quote_qty":"1736.01600000","trade_id":"5004"}'
                            ),
                            run_id='s14-g3-proof',
                        )
                    )
                except EventWriterError as exc:
                    blocked_error_code = exc.code

                if blocked_error_code != 'STREAM_QUARANTINED':
                    raise RuntimeError(
                        'S14-G3 proof expected post-quarantine write to fail with '
                        f'STREAM_QUARANTINED, got {blocked_error_code}'
                    )

                return {
                    'proof_scope': (
                        'Slice 14 S14-G3 continuous reconciliation and fail-loud '
                        'stream quarantine on detected gaps'
                    ),
                    'proof_database': proof_database,
                    'write_statuses': write_statuses,
                    'checkpoint_status': checkpoint_result.status,
                    'quarantine_reason': quarantine_state.reason,
                    'blocked_error_code': blocked_error_code,
                    'quarantine_state_path': str(quarantine_state_path),
                    'quarantine_verified': True,
                }
            finally:
                runner.close()
                admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
                admin_client.disconnect()


def main() -> None:
    payload = run_s14_g3_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'guardrails-proof-s14-g3-quarantine.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
