from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    BybitSpotTradeEvent,
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)
from origo_control_plane.utils.bybit_native_projector import (
    project_bybit_spot_trades_native,
)
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

_PROOF_DB_SUFFIX = '_s19_g1_proof'
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


def _set_proof_runtime_env(*, proof_database: str) -> dict[str, str | None]:
    quarantine_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s19-g1-quarantine.json'
    )
    runtime_audit_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s19-g1-runtime-audit.log'
    )
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    previous: dict[str, str | None] = {
        'ORIGO_AUDIT_LOG_RETENTION_DAYS': os.environ.get('ORIGO_AUDIT_LOG_RETENTION_DAYS'),
        'ORIGO_STREAM_QUARANTINE_STATE_PATH': os.environ.get('ORIGO_STREAM_QUARANTINE_STATE_PATH'),
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH': os.environ.get(
            'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'
        ),
    }
    os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
    os.environ['ORIGO_STREAM_QUARANTINE_STATE_PATH'] = str(quarantine_path)
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'] = str(runtime_audit_path)
    return previous


def _restore_proof_runtime_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _build_gap_csv_bytes() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1704326400.1000,BTCUSDT,Buy,0.010,42900.10,PlusTick,m-7001,4.29001e+11,0.010,429.001\n'
        b'1704326400.9000,BTCUSDT,Sell,0.020,42900.30,MinusTick,m-7003,8.58006e+11,0.020,858.006\n'
    )


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _run_ingest_integrity_guardrail_proof(events: list[BybitSpotTradeEvent]) -> dict[str, Any]:
    ingest_error: str | None = None
    try:
        run_exchange_integrity_suite_rows(
            dataset='bybit_spot_trades',
            rows=[event.to_integrity_tuple() for event in events],
        )
    except ValueError as exc:
        ingest_error = str(exc)

    if ingest_error is None:
        raise RuntimeError(
            'S19-G1 ingest integrity proof expected sequence-gap failure but got success'
        )
    if 'sequence-gap' not in ingest_error:
        raise RuntimeError(
            'S19-G1 ingest integrity proof expected sequence-gap error message, '
            f'got: {ingest_error}'
        )

    return {
        'events_parsed': len(events),
        'error': ingest_error,
    }


def _run_projection_integrity_guardrail_proof(
    *,
    admin_client: ClickHouseClient,
    proof_database: str,
    events: list[BybitSpotTradeEvent],
) -> dict[str, Any]:
    write_summary = write_bybit_spot_trades_to_canonical(
        client=admin_client,
        database=proof_database,
        events=events,
        run_id='s19-g1-projection-write',
        ingested_at_utc=datetime(2026, 3, 10, 22, 40, 0, tzinfo=UTC),
    )
    if write_summary['rows_processed'] != len(events):
        raise RuntimeError('S19-G1 projection proof expected full canonical write')

    projection_error: str | None = None
    try:
        project_bybit_spot_trades_native(
            client=admin_client,
            database=proof_database,
            partition_ids={event.partition_id for event in events},
            run_id='s19-g1-projection-run',
            projected_at_utc=datetime(2026, 3, 10, 22, 40, 5, tzinfo=UTC),
        )
    except ValueError as exc:
        projection_error = str(exc)

    if projection_error is None:
        raise RuntimeError(
            'S19-G1 projection integrity proof expected sequence-gap failure but got success'
        )
    if 'sequence-gap' not in projection_error:
        raise RuntimeError(
            'S19-G1 projection integrity proof expected sequence-gap error message, '
            f'got: {projection_error}'
        )

    projected_rows_raw = admin_client.execute(
        f'SELECT count() FROM {proof_database}.canonical_bybit_spot_trades_native_v1'
    )
    projected_rows = int(projected_rows_raw[0][0])
    if projected_rows != 0:
        raise RuntimeError(
            'S19-G1 projection integrity proof expected no projected rows on failure, '
            f'observed={projected_rows}'
        )

    canonical_rows_raw = admin_client.execute(
        f'''
        SELECT count()
        FROM {proof_database}.canonical_event_log
        WHERE source_id = %(source_id)s
            AND stream_id = %(stream_id)s
        ''',
        {'source_id': 'bybit', 'stream_id': 'bybit_spot_trades'},
    )
    canonical_rows = int(canonical_rows_raw[0][0])
    if canonical_rows != len(events):
        raise RuntimeError(
            'S19-G1 projection integrity proof expected canonical rows to remain intact, '
            f'expected={len(events)} observed={canonical_rows}'
        )

    return {
        'write_summary': write_summary,
        'error': projection_error,
        'projected_rows_after_failure': projected_rows,
        'canonical_rows_written': canonical_rows,
    }


def run_s19_g1_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    runner = MigrationRunner(settings=proof_settings)
    admin_client = _build_clickhouse_client(base_settings)
    previous_env = _set_proof_runtime_env(proof_database=proof_database)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(_FIXTURE_DATE)
        events = parse_bybit_spot_trade_csv(
            csv_content=_build_gap_csv_bytes(),
            date_str=_FIXTURE_DATE,
            day_start_ts_utc_ms=day_start_ts_utc_ms,
            day_end_ts_utc_ms=day_end_ts_utc_ms,
        )
        ingest_guardrail = _run_ingest_integrity_guardrail_proof(events)
        projection_guardrail = _run_projection_integrity_guardrail_proof(
            admin_client=admin_client,
            proof_database=proof_database,
            events=events,
        )
        return {
            'proof_scope': (
                'Slice 19 S19-G1 exchange integrity suite enforcement in '
                'Bybit ingest and projection paths'
            ),
            'proof_database': proof_database,
            'ingest_path_guardrail': ingest_guardrail,
            'projection_path_guardrail': projection_guardrail,
            'guardrail_verified': True,
        }
    finally:
        _restore_proof_runtime_env(previous_env)
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s19_g1_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s19-g1-exchange-integrity.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
