from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s19_p4_proof'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-19-bybit-event-sourcing-port'
)
_FIXTURE_DATE = '2024-01-04'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_csv_bytes() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1704326400.1000,BTCUSDT,Buy,0.010,42600.10,PlusTick,m-4001,4.26001e+11,0.010,426.001\n'
        b'1704326400.4000,BTCUSDT,Sell,0.020,42600.20,MinusTick,m-4002,8.52004e+11,0.020,852.004\n'
        b'1704326400.9000,BTCUSDT,Buy,0.030,42600.30,PlusTick,m-4003,1.278009e+12,0.030,1278.009\n'
        b'1704326401.2000,BTCUSDT,Sell,0.040,42600.40,MinusTick,m-4004,1.704016e+12,0.040,1704.016\n'
    )


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def run_s19_p4_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(_FIXTURE_DATE)
    events = parse_bybit_spot_trade_csv(
        csv_content=_fixture_csv_bytes(),
        date_str=_FIXTURE_DATE,
        day_start_ts_utc_ms=day_start_ts_utc_ms,
        day_end_ts_utc_ms=day_end_ts_utc_ms,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        phase_1_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events[:2],
            run_id='s19-p4-phase-1',
            ingested_at_utc=datetime(2026, 3, 10, 22, 10, 0, tzinfo=UTC),
        )
        if phase_1_summary != {
            'rows_processed': 2,
            'rows_inserted': 2,
            'rows_duplicate': 0,
        }:
            raise RuntimeError(
                'S19-P4 phase-1 summary mismatch: '
                f'observed={phase_1_summary}'
            )

        phase_2_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s19-p4-phase-2',
            ingested_at_utc=datetime(2026, 3, 10, 22, 10, 30, tzinfo=UTC),
        )
        if phase_2_summary != {
            'rows_processed': 4,
            'rows_inserted': 2,
            'rows_duplicate': 2,
        }:
            raise RuntimeError(
                'S19-P4 phase-2 summary mismatch: '
                f'observed={phase_2_summary}'
            )

        phase_3_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s19-p4-phase-3',
            ingested_at_utc=datetime(2026, 3, 10, 22, 11, 0, tzinfo=UTC),
        )
        if phase_3_summary != {
            'rows_processed': 4,
            'rows_inserted': 0,
            'rows_duplicate': 4,
        }:
            raise RuntimeError(
                'S19-P4 phase-3 summary mismatch: '
                f'observed={phase_3_summary}'
            )

        persisted_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'bybit'
                AND stream_id = 'bybit_spot_trades'
            '''
        )
        persisted_count = _require_int(
            persisted_rows[0][0],
            label='persisted_count',
        )
        if persisted_count != len(events):
            raise RuntimeError(
                f'S19-P4 persisted count mismatch expected={len(events)} observed={persisted_count}'
            )

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent, count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'bybit'
                AND stream_id = 'bybit_spot_trades'
            GROUP BY source_offset_or_equivalent
            HAVING count() > 1
            '''
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S19-P4 duplicate identity rows found: '
                f'{duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 19 S19-P4 exactly-once ingest for Bybit canonical events '
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
    payload = run_s19_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s19-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
