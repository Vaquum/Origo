from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_p2_proof'


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
            source_offset_or_equivalent='1704067200100-201',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"42000.10000000","qty":"0.01000000",'
                b'"quote_qty":"420.00100000","trade_id":"201"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067200300-202',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 300000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"42000.20000000","qty":"0.01500000",'
                b'"quote_qty":"630.00300000","trade_id":"202"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067200600-203',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 600000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"42000.30000000","qty":"0.02000000",'
                b'"quote_qty":"840.00600000","trade_id":"203"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='1704067201000-204',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, 0, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"42000.40000000","qty":"0.01200000",'
                b'"quote_qty":"504.00480000","trade_id":"204"}'
            ),
        ),
    )


def _write_events(
    *, writer: CanonicalEventWriter, events: tuple[_FixtureEvent, ...]
) -> list[str]:
    statuses: list[str] = []
    for event in events:
        result = writer.write_event(
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
        statuses.append(result.status)
    return statuses


def run_s14_p2_proof() -> dict[str, Any]:
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

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        writer_first_process = CanonicalEventWriter(
            client=admin_client,
            database=proof_database,
        )
        phase_1_statuses = _write_events(
            writer=writer_first_process,
            events=fixture_events[:2],
        )
        if phase_1_statuses != ['inserted', 'inserted']:
            raise RuntimeError(
                'S14-P2 proof expected phase-1 pre-crash writes to be inserted, '
                f'got {phase_1_statuses}'
            )

        # Simulated crash/restart: new writer instance replays full fixture.
        writer_after_restart = CanonicalEventWriter(
            client=admin_client,
            database=proof_database,
        )
        phase_2_statuses = _write_events(
            writer=writer_after_restart,
            events=fixture_events,
        )
        expected_phase_2 = ['duplicate', 'duplicate', 'inserted', 'inserted']
        if phase_2_statuses != expected_phase_2:
            raise RuntimeError(
                'S14-P2 proof expected crash/restart replay statuses '
                f'{expected_phase_2}, got {phase_2_statuses}'
            )

        # Full duplicate replay after successful recovery.
        writer_duplicate_replay = CanonicalEventWriter(
            client=admin_client,
            database=proof_database,
        )
        phase_3_statuses = _write_events(
            writer=writer_duplicate_replay,
            events=fixture_events,
        )
        expected_phase_3 = ['duplicate', 'duplicate', 'duplicate', 'duplicate']
        if phase_3_statuses != expected_phase_3:
            raise RuntimeError(
                'S14-P2 proof expected post-recovery duplicate replay statuses '
                f'{expected_phase_3}, got {phase_3_statuses}'
            )

        persisted_count_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ''',
            {
                'source_id': 'binance',
                'stream_id': 'binance_spot_trades',
                'partition_id': 'btcusdt',
            },
        )
        persisted_count = int(persisted_count_rows[0][0])
        if persisted_count != len(fixture_events):
            raise RuntimeError(
                'S14-P2 proof expected exactly fixture-size persisted rows after '
                f'replay/restart, got {persisted_count}'
            )

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT
                source_offset_or_equivalent,
                count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            GROUP BY source_offset_or_equivalent
            HAVING count() > 1
            ''',
            {
                'source_id': 'binance',
                'stream_id': 'binance_spot_trades',
                'partition_id': 'btcusdt',
            },
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S14-P2 proof expected zero duplicate source-event identities, '
                f'got {duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 14 S14-P2 exactly-once ingest under duplicate replay and '
                'crash/restart scenarios'
            ),
            'proof_database': proof_database,
            'fixture_event_count': len(fixture_events),
            'phases': {
                'phase_1_pre_crash_partial_ingest': phase_1_statuses,
                'phase_2_after_restart_full_replay': phase_2_statuses,
                'phase_3_post_recovery_duplicate_replay': phase_3_statuses,
            },
            'persisted_row_count': persisted_count,
            'duplicate_identity_rows': duplicate_identity_rows,
            'exactly_once_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_p2_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p2-exactly-once-ingest.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
