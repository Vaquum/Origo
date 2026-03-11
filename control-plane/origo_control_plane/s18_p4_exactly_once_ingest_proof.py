from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.okx_canonical_event_ingest import (
    parse_okx_spot_trade_csv,
    write_okx_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s18_p4_proof'
_SLICE_DIR = Path('spec/slices/slice-18-okx-event-sourcing-port')


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_csv_bytes() -> bytes:
    return (
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,4001,buy,42600.10000000,0.01000000,1704326400100\n'
        b'BTC-USDT,4002,sell,42600.20000000,0.02000000,1704326400400\n'
        b'BTC-USDT,4003,buy,42600.30000000,0.03000000,1704326400900\n'
        b'BTC-USDT,4004,sell,42600.40000000,0.04000000,1704326401200\n'
    )


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def run_s18_p4_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    events = parse_okx_spot_trade_csv(_fixture_csv_bytes())

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        phase_1_summary = write_okx_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events[:2],
            run_id='s18-p4-phase-1',
            ingested_at_utc=datetime(2026, 3, 10, 22, 10, 0, tzinfo=UTC),
        )
        if phase_1_summary != {
            'rows_processed': 2,
            'rows_inserted': 2,
            'rows_duplicate': 0,
        }:
            raise RuntimeError(
                'S18-P4 phase-1 summary mismatch: '
                f'observed={phase_1_summary}'
            )

        phase_2_summary = write_okx_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s18-p4-phase-2',
            ingested_at_utc=datetime(2026, 3, 10, 22, 10, 30, tzinfo=UTC),
        )
        if phase_2_summary != {
            'rows_processed': 4,
            'rows_inserted': 2,
            'rows_duplicate': 2,
        }:
            raise RuntimeError(
                'S18-P4 phase-2 summary mismatch: '
                f'observed={phase_2_summary}'
            )

        phase_3_summary = write_okx_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s18-p4-phase-3',
            ingested_at_utc=datetime(2026, 3, 10, 22, 11, 0, tzinfo=UTC),
        )
        if phase_3_summary != {
            'rows_processed': 4,
            'rows_inserted': 0,
            'rows_duplicate': 4,
        }:
            raise RuntimeError(
                'S18-P4 phase-3 summary mismatch: '
                f'observed={phase_3_summary}'
            )

        persisted_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'okx'
                AND stream_id = 'okx_spot_trades'
            '''
        )
        persisted_count = _require_int(
            persisted_rows[0][0],
            label='persisted_count',
        )
        if persisted_count != len(events):
            raise RuntimeError(
                f'S18-P4 persisted count mismatch expected={len(events)} observed={persisted_count}'
            )

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent, count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'okx'
                AND stream_id = 'okx_spot_trades'
            GROUP BY source_offset_or_equivalent
            HAVING count() > 1
            '''
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S18-P4 duplicate identity rows found: '
                f'{duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 18 S18-P4 exactly-once ingest for OKX canonical events '
                'under duplicate replay and crash/restart simulation'
            ),
            'proof_database': proof_database,
            'fixture_event_count': len(events),
            'phase_1_pre_crash_partial_ingest': phase_1_summary,
            'phase_2_after_restart_full_replay': phase_2_summary,
            'phase_3_post_recovery_duplicate_replay': phase_3_summary,
            'persisted_row_count': persisted_count,
            'duplicate_identity_rows': duplicate_identity_rows,
            'exactly_once_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s18_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s18-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
