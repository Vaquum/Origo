from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import (
    CanonicalEventWriteInput,
    CanonicalEventWriter,
    build_canonical_event_row,
)
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c2_proof'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def run_s14_c2_proof() -> dict[str, Any]:
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

        event_input = CanonicalEventWriteInput(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
            source_offset_or_equivalent='1700000000000',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
            payload_content_type='application/json',
            payload_encoding='utf-8',
            payload_raw=b'{"price":"30000.0","qty":"0.5","trade_id":"1"}',
        )
        first_result = writer.write_event(event_input)
        second_result = writer.write_event(event_input)

        if first_result.status != 'inserted':
            raise RuntimeError(
                f'S14-C2 proof expected first write status=inserted, got {first_result.status}'
            )
        if second_result.status != 'duplicate':
            raise RuntimeError(
                f'S14-C2 proof expected second write status=duplicate, got {second_result.status}'
            )
        if first_result.row.event_id != second_result.row.event_id:
            raise RuntimeError(
                'S14-C2 proof expected deterministic event_id equality across repeated writes'
            )
        if first_result.row.payload_sha256_raw != second_result.row.payload_sha256_raw:
            raise RuntimeError(
                'S14-C2 proof expected deterministic payload hash equality across repeated writes'
            )

        persisted_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
                AND source_offset_or_equivalent = %(source_offset_or_equivalent)s
            ''',
            {
                'source_id': event_input.source_id,
                'stream_id': event_input.stream_id,
                'partition_id': event_input.partition_id,
                'source_offset_or_equivalent': event_input.source_offset_or_equivalent,
            },
        )
        persisted_count = int(persisted_rows[0][0])
        if persisted_count != 1:
            raise RuntimeError(
                f'S14-C2 proof expected exactly one persisted canonical event row, got {persisted_count}'
            )

        conflict_error: str | None = None
        conflicting_input = CanonicalEventWriteInput(
            source_id=event_input.source_id,
            stream_id=event_input.stream_id,
            partition_id=event_input.partition_id,
            source_offset_or_equivalent=event_input.source_offset_or_equivalent,
            source_event_time_utc=event_input.source_event_time_utc,
            ingested_at_utc=event_input.ingested_at_utc,
            payload_content_type=event_input.payload_content_type,
            payload_encoding=event_input.payload_encoding,
            payload_raw=b'{"price":"30100.0","qty":"0.5","trade_id":"1"}',
        )
        try:
            writer.write_event(conflicting_input)
        except RuntimeError as exc:
            conflict_error = str(exc)

        if conflict_error is None:
            raise RuntimeError(
                'S14-C2 proof expected payload hash conflict error for same source-event identity'
            )
        if 'payload hash mismatch' not in conflict_error:
            raise RuntimeError(
                'S14-C2 proof expected payload hash mismatch message, '
                f'got {conflict_error}'
            )

        canonical_row = build_canonical_event_row(event_input)
        return {
            'proof_scope': 'Slice 14 S14-C2 deterministic canonical event writer',
            'proof_database': proof_database,
            'first_write_status': first_result.status,
            'second_write_status': second_result.status,
            'persisted_row_count_after_repeat': persisted_count,
            'event_id': str(canonical_row.event_id),
            'payload_sha256_raw': canonical_row.payload_sha256_raw,
            'payload_json': canonical_row.payload_json,
            'idempotency_key': canonical_row.idempotency_key,
            'conflicting_payload_error': conflict_error,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_c2_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c2-writer.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
