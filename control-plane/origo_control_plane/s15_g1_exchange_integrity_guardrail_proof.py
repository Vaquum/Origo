from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.binance_canonical_event_ingest import (
    BinanceSpotTradeEvent,
    parse_binance_spot_trade_csv,
    write_binance_spot_trades_to_canonical,
)
from origo_control_plane.utils.binance_native_projector import (
    project_binance_spot_trades_native,
)
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

_PROOF_DB_SUFFIX = '_s15_g1_proof'
_SLICE_DIR = Path('spec/slices/slice-15-binance-event-sourcing-port')


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int')
    return value


def _set_proof_runtime_env(*, proof_database: str) -> dict[str, str | None]:
    quarantine_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s15-g1-quarantine.json'
    )
    runtime_audit_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s15-g1-runtime-audit.log'
    )
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    previous: dict[str, str | None] = {
        'ORIGO_AUDIT_LOG_RETENTION_DAYS': os.environ.get(
            'ORIGO_AUDIT_LOG_RETENTION_DAYS'
        ),
        'ORIGO_STREAM_QUARANTINE_STATE_PATH': os.environ.get(
            'ORIGO_STREAM_QUARANTINE_STATE_PATH'
        ),
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
        b'trade_id,price,qty,quote_qty,timestamp,is_buyer_maker,is_best_match\n'
        b'2001,43000.10000000,0.01000000,430.00100000,1704412800100,false,true\n'
        b'2003,43000.30000000,0.02000000,860.00600000,1704412800900,true,true\n'
    )


def _run_ingest_integrity_guardrail_proof() -> dict[str, Any]:
    events = parse_binance_spot_trade_csv(_build_gap_csv_bytes())
    ingest_error: str | None = None
    try:
        run_exchange_integrity_suite_rows(
            dataset='binance_spot_trades',
            rows=[event.to_integrity_tuple() for event in events],
        )
    except ValueError as exc:
        ingest_error = str(exc)

    if ingest_error is None:
        raise RuntimeError(
            'S15-G1 ingest integrity proof expected sequence-gap failure but got success'
        )

    if 'sequence-gap' not in ingest_error:
        raise RuntimeError(
            'S15-G1 ingest integrity proof expected sequence-gap error message, '
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
    events: list[BinanceSpotTradeEvent],
) -> dict[str, Any]:
    write_summary = write_binance_spot_trades_to_canonical(
        client=admin_client,
        database=proof_database,
        events=events,
        run_id='s15-g1-projection-write',
        ingested_at_utc=datetime(2024, 1, 5, 0, 0, 2, tzinfo=UTC),
    )
    if write_summary['rows_processed'] != len(events):
        raise RuntimeError('S15-G1 projection proof expected full canonical write')

    partition_ids = {event.partition_id for event in events}
    projection_error: str | None = None
    try:
        project_binance_spot_trades_native(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s15-g1-projection-run',
            projected_at_utc=datetime(2024, 1, 5, 0, 0, 5, tzinfo=UTC),
        )
    except ValueError as exc:
        projection_error = str(exc)

    if projection_error is None:
        raise RuntimeError(
            'S15-G1 projection integrity proof expected sequence-gap failure but got success'
        )
    if 'sequence-gap' not in projection_error:
        raise RuntimeError(
            'S15-G1 projection integrity proof expected sequence-gap error message, '
            f'got: {projection_error}'
        )

    projected_rows_raw = admin_client.execute(
        f'SELECT count() FROM {proof_database}.canonical_binance_spot_trades_native_v1'
    )
    projected_rows = _require_int(
        projected_rows_raw[0][0],
        label='projected_rows',
    )
    if projected_rows != 0:
        raise RuntimeError(
            'S15-G1 projection integrity proof expected no projected rows on failure, '
            f'observed={projected_rows}'
        )

    canonical_rows_raw = admin_client.execute(
        f'''
        SELECT count()
        FROM {proof_database}.canonical_event_log
        WHERE source_id = %(source_id)s
            AND stream_id = %(stream_id)s
        ''',
        {'source_id': 'binance', 'stream_id': 'binance_spot_trades'},
    )
    canonical_rows = _require_int(canonical_rows_raw[0][0], label='canonical_rows')
    if canonical_rows != len(events):
        raise RuntimeError(
            'S15-G1 projection integrity proof expected canonical rows to remain intact, '
            f'expected={len(events)} observed={canonical_rows}'
        )

    return {
        'write_summary': write_summary,
        'error': projection_error,
        'projected_rows_after_failure': projected_rows,
        'canonical_rows_written': canonical_rows,
    }


def run_s15_g1_proof() -> dict[str, Any]:
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

        events = parse_binance_spot_trade_csv(_build_gap_csv_bytes())
        ingest_guardrail = _run_ingest_integrity_guardrail_proof()
        projection_guardrail = _run_projection_integrity_guardrail_proof(
            admin_client=admin_client,
            proof_database=proof_database,
            events=events,
        )

        return {
            'proof_scope': (
                'Slice 15 S15-G1 exchange integrity suite enforcement in '
                'Binance ingest + projection paths'
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
    payload = run_s15_g1_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s15-g1-exchange-integrity.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
