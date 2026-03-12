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
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.quarantine import StreamQuarantineRegistry
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_g2_proof'


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


def _fixture_event() -> _FixtureEvent:
    return _FixtureEvent(
        source_offset_or_equivalent='4001',
        source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 100000, tzinfo=UTC),
        payload_raw=(
            b'{"is_best_match":true,"is_buyer_maker":false,'
            b'"price":"43300.10000000","qty":"0.01000000",'
            b'"quote_qty":"433.00100000","trade_id":"4001"}'
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


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    lines = [line for line in path.read_text(encoding='utf-8').splitlines() if line != '']
    output: list[dict[str, Any]] = []
    for line in lines:
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise RuntimeError('S14-G2 proof expected JSON object in runtime audit log')
        output.append(cast(dict[str, Any], raw))
    return output


def run_s14_g2_proof() -> dict[str, Any]:
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
    fixture = _fixture_event()
    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)

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

            try:
                admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
                runner.migrate()

                writer = CanonicalEventWriter(
                    client=admin_client,
                    database=proof_database,
                    quarantine_registry=StreamQuarantineRegistry(
                        path=quarantine_state_path
                    ),
                )
                write_result = writer.write_event(
                    CanonicalEventWriteInput(
                        source_id=stream_key.source_id,
                        stream_id=stream_key.stream_id,
                        partition_id=stream_key.partition_id,
                        source_offset_or_equivalent=fixture.source_offset_or_equivalent,
                        source_event_time_utc=fixture.source_event_time_utc,
                        ingested_at_utc=fixture.source_event_time_utc,
                        payload_content_type='application/json',
                        payload_encoding='utf-8',
                        payload_raw=fixture.payload_raw,
                        run_id='s14-g2-proof',
                    )
                )
                if write_result.status != 'inserted':
                    raise RuntimeError(
                        'S14-G2 proof expected initial writer status=inserted'
                    )

                runtime = CanonicalProjectorRuntime(
                    client=admin_client,
                    database=proof_database,
                    projector_id='s14-g2-projector',
                    stream_key=stream_key,
                    batch_size=10,
                )
                runtime.start()
                batch = runtime.fetch_next_batch()
                if len(batch) != 1:
                    raise RuntimeError(
                        f'S14-G2 proof expected projector batch size=1, got {len(batch)}'
                    )
                commit_result = runtime.commit_checkpoint(
                    last_event=batch[-1],
                    run_id='s14-g2-proof',
                    checkpointed_at_utc=datetime(2024, 1, 4, 0, 1, 0, tzinfo=UTC),
                    state={'projected_rows': 1},
                )
                runtime.stop()
                if commit_result.status != 'checkpointed':
                    raise RuntimeError(
                        'S14-G2 proof expected checkpoint commit status=checkpointed'
                    )

                if not runtime_audit_log_path.exists():
                    raise RuntimeError(
                        'S14-G2 proof expected canonical runtime audit log file to exist'
                    )
                audit_events = _read_jsonl(runtime_audit_log_path)
                event_types = [str(event.get('event_type')) for event in audit_events]
                if event_types != ['canonical_ingest_inserted', 'projector_checkpointed']:
                    raise RuntimeError(
                        'S14-G2 proof expected canonical runtime audit event sequence '
                        "['canonical_ingest_inserted', 'projector_checkpointed'], "
                        f'got {event_types}'
                    )

                return {
                    'proof_scope': (
                        'Slice 14 S14-G2 immutable runtime audit events for '
                        'event ingestion and projector checkpoint transitions'
                    ),
                    'proof_database': proof_database,
                    'runtime_audit_log_path': str(runtime_audit_log_path),
                    'runtime_audit_event_count': len(audit_events),
                    'runtime_audit_event_types': event_types,
                    'audit_verified': True,
                }
            finally:
                runner.close()
                admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
                admin_client.disconnect()


def main() -> None:
    payload = run_s14_g2_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'guardrails-proof-s14-g2-runtime-audit.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
