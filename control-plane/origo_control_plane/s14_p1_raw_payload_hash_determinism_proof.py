from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX_RUN_1 = '_s14_p1_run_1'
_PROOF_DB_SUFFIX_RUN_2 = '_s14_p1_run_2'


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
            source_offset_or_equivalent='1704067200100-101',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"41000.10000000","qty":"0.01000000",'
                b'"quote_qty":"410.00100000","trade_id":"101"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067200800-102',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 800000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"41000.20000000","qty":"0.02000000",'
                b'"quote_qty":"820.00400000","trade_id":"102"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067201200-103',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, 200000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"41000.30000000","qty":"0.01500000",'
                b'"quote_qty":"615.00450000","trade_id":"103"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067201900-104',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, 900000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"40999.90000000","qty":"0.02500000",'
                b'"quote_qty":"1024.99750000","trade_id":"104"}'
            ),
        ),
    )


def _compute_fixture_checksum(events: tuple[_FixtureEvent, ...]) -> str:
    payload = b''.join(event.payload_raw for event in events)
    return hashlib.sha256(payload).hexdigest()


def _ingest_fixture(
    *,
    admin_client: ClickHouseClient,
    settings: MigrationSettings,
    proof_database: str,
    events: tuple[_FixtureEvent, ...],
) -> dict[str, Any]:
    runner = MigrationRunner(settings=settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()
        writer = CanonicalEventWriter(client=admin_client, database=proof_database)

        statuses: list[str] = []
        for event in events:
            write_result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id='binance',
                    stream_id='binance_spot_trades',
                    partition_id='btcusdt',
                    source_offset_or_equivalent=event.source_offset_or_equivalent,
                    source_event_time_utc=event.source_event_time_utc,
                    ingested_at_utc=event.source_event_time_utc,
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=event.payload_raw,
                )
            )
            statuses.append(write_result.status)

        if statuses != ['inserted', 'inserted', 'inserted', 'inserted']:
            raise RuntimeError(
                'S14-P1 proof expected all fixture rows to be inserted on first ingest, '
                f'got statuses={statuses}'
            )

        rows = admin_client.execute(
            f'''
            SELECT
                source_offset_or_equivalent,
                payload_sha256_raw,
                event_id
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY source_offset_or_equivalent ASC
            ''',
            {
                'source_id': 'binance',
                'stream_id': 'binance_spot_trades',
                'partition_id': 'btcusdt',
            },
        )
        fingerprint_rows = [
            {
                'source_offset_or_equivalent': str(row[0]),
                'payload_sha256_raw': str(row[1]),
                'event_id': str(row[2]),
            }
            for row in rows
        ]
        fingerprint_hash = hashlib.sha256(
            json.dumps(
                fingerprint_rows,
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=True,
            ).encode('utf-8')
        ).hexdigest()

        return {
            'proof_database': proof_database,
            'statuses': statuses,
            'row_count': len(fingerprint_rows),
            'fingerprint_rows': fingerprint_rows,
            'fingerprint_hash_sha256': fingerprint_hash,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')


def run_s14_p1_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database_run_1 = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_1}'
    proof_database_run_2 = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_2}'

    run_1_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database_run_1,
    )
    run_2_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database_run_2,
    )

    fixture_events = _fixture_events()
    fixture_checksum = _compute_fixture_checksum(fixture_events)

    admin_client = _build_clickhouse_client(base_settings)
    try:
        run_1 = _ingest_fixture(
            admin_client=admin_client,
            settings=run_1_settings,
            proof_database=proof_database_run_1,
            events=fixture_events,
        )
        run_2 = _ingest_fixture(
            admin_client=admin_client,
            settings=run_2_settings,
            proof_database=proof_database_run_2,
            events=fixture_events,
        )
    finally:
        admin_client.disconnect()

    if run_1['fingerprint_hash_sha256'] != run_2['fingerprint_hash_sha256']:
        raise RuntimeError(
            'S14-P1 proof failed: canonical payload hash fingerprint differs across '
            'repeated fixed-fixture ingests'
        )

    if run_1['fingerprint_rows'] != run_2['fingerprint_rows']:
        raise RuntimeError(
            'S14-P1 proof failed: canonical payload hash rows differ across '
            'repeated fixed-fixture ingests'
        )

    return {
        'proof_scope': (
            'Slice 14 S14-P1 canonical raw-payload hash determinism across '
            'repeated fixed-fixture ingests'
        ),
        'fixture_window_utc': {
            'start': '2024-01-01T00:00:00.100Z',
            'end': '2024-01-01T00:00:01.900Z',
        },
        'fixture_event_count': len(fixture_events),
        'fixture_payload_checksum_sha256': fixture_checksum,
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': True,
    }


def main() -> None:
    payload = run_s14_p1_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p1-raw-payload-hash-determinism.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
