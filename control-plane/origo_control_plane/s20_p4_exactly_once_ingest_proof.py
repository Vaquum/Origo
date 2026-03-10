from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.s20_bitcoin_proof_common import (
    BITCOIN_NATIVE_DATASETS,
    INGESTED_AT_UTC,
    build_clickhouse_client,
    build_fixture_rows_by_dataset,
    fixture_events_by_stream,
)
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    write_bitcoin_events_to_canonical,
)

_PROOF_DB_SUFFIX = '_s20_p4_proof'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def _phase_1_events(
    *, events_by_stream: dict[str, list[BitcoinCanonicalEvent]]
) -> list[BitcoinCanonicalEvent]:
    phase_1: list[BitcoinCanonicalEvent] = []
    for stream_id in BITCOIN_NATIVE_DATASETS:
        stream_events = events_by_stream[stream_id]
        if stream_events == []:
            raise RuntimeError(f'S20-P4 expected fixture events for stream={stream_id}')
        phase_1.append(stream_events[0])
    return phase_1


def _expected_phase_2_inserted_count(
    *,
    events_by_stream: dict[str, list[BitcoinCanonicalEvent]],
) -> int:
    return sum(len(stream_events) - 1 for stream_events in events_by_stream.values())


def run_s20_p4_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    rows_by_dataset = build_fixture_rows_by_dataset()
    events_by_stream = fixture_events_by_stream(rows_by_dataset=rows_by_dataset)
    all_events = [
        event
        for stream_id in BITCOIN_NATIVE_DATASETS
        for event in events_by_stream[stream_id]
    ]
    phase_1_events = _phase_1_events(events_by_stream=events_by_stream)

    admin_client: ClickHouseClient = build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        phase_1_summary = write_bitcoin_events_to_canonical(
            client=admin_client,
            database=proof_database,
            events=phase_1_events,
            run_id='s20-p4-phase-1',
            ingested_at_utc=INGESTED_AT_UTC,
        )
        expected_phase_1 = {
            'rows_processed': len(phase_1_events),
            'rows_inserted': len(phase_1_events),
            'rows_duplicate': 0,
        }
        if phase_1_summary != expected_phase_1:
            raise RuntimeError(
                'S20-P4 phase-1 summary mismatch: '
                f'observed={phase_1_summary} expected={expected_phase_1}'
            )

        phase_2_summary = write_bitcoin_events_to_canonical(
            client=admin_client,
            database=proof_database,
            events=all_events,
            run_id='s20-p4-phase-2',
            ingested_at_utc=INGESTED_AT_UTC + datetime.resolution,
        )
        expected_phase_2 = {
            'rows_processed': len(all_events),
            'rows_inserted': _expected_phase_2_inserted_count(events_by_stream=events_by_stream),
            'rows_duplicate': len(phase_1_events),
        }
        if phase_2_summary != expected_phase_2:
            raise RuntimeError(
                'S20-P4 phase-2 summary mismatch: '
                f'observed={phase_2_summary} expected={expected_phase_2}'
            )

        phase_3_summary = write_bitcoin_events_to_canonical(
            client=admin_client,
            database=proof_database,
            events=all_events,
            run_id='s20-p4-phase-3',
            ingested_at_utc=INGESTED_AT_UTC + timedelta(seconds=1),
        )
        expected_phase_3 = {
            'rows_processed': len(all_events),
            'rows_inserted': 0,
            'rows_duplicate': len(all_events),
        }
        if phase_3_summary != expected_phase_3:
            raise RuntimeError(
                'S20-P4 phase-3 summary mismatch: '
                f'observed={phase_3_summary} expected={expected_phase_3}'
            )

        persisted_by_stream: dict[str, int] = {}
        for stream_id in BITCOIN_NATIVE_DATASETS:
            persisted_rows = admin_client.execute(
                f'''
                SELECT count()
                FROM {proof_database}.canonical_event_log
                WHERE source_id = 'bitcoin_core'
                    AND stream_id = %(stream_id)s
                ''',
                {'stream_id': stream_id},
            )
            persisted_count = _require_int(
                persisted_rows[0][0],
                label=f'persisted_count[{stream_id}]',
            )
            expected_count = len(events_by_stream[stream_id])
            if persisted_count != expected_count:
                raise RuntimeError(
                    'S20-P4 persisted count mismatch: '
                    f'stream={stream_id} expected={expected_count} observed={persisted_count}'
                )
            persisted_by_stream[stream_id] = persisted_count

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT stream_id, partition_id, source_offset_or_equivalent, count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'bitcoin_core'
            GROUP BY stream_id, partition_id, source_offset_or_equivalent
            HAVING count() > 1
            ORDER BY stream_id, partition_id, source_offset_or_equivalent
            '''
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S20-P4 duplicate identity rows found: '
                f'{duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 20 S20-P4 exactly-once ingest for Bitcoin canonical events '
                'under duplicate replay and crash/restart simulation across all seven datasets'
            ),
            'proof_database': proof_database,
            'fixture_event_count': len(all_events),
            'phase_1_pre_crash_partial_ingest': phase_1_summary,
            'phase_2_after_restart_full_replay': phase_2_summary,
            'phase_3_post_recovery_duplicate_replay': phase_3_summary,
            'persisted_rows_by_stream': persisted_by_stream,
            'duplicate_identity_rows': duplicate_identity_rows,
            'exactly_once_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s20_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
