from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient

from origo.events.errors import ReconciliationError
from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
    CursorAdvanceInput,
)
from origo.events.quarantine import StreamQuarantineRegistry
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s19_p5_proof'
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


def _fixture_csv_bytes_with_gap() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1704326400.1000,BTCUSDT,Buy,0.010,42700.10,PlusTick,m-5001,4.27001e+11,0.010,427.001\n'
        b'1704326400.4000,BTCUSDT,Sell,0.020,42700.20,MinusTick,m-5002,8.54004e+11,0.020,854.004\n'
        b'1704326400.9000,BTCUSDT,Buy,0.030,42700.40,PlusTick,m-5004,1.281012e+12,0.030,1281.012\n'
        b'1704326401.2000,BTCUSDT,Sell,0.040,42700.50,MinusTick,m-5005,1.70802e+12,0.040,1708.020\n'
    )


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _parse_numeric_offset(offset: str) -> int:
    try:
        return int(offset)
    except ValueError as exc:
        raise RuntimeError(f'Offset must be numeric, got {offset!r}') from exc


def _detect_missing_offsets(observed_offsets: list[str]) -> list[str]:
    if observed_offsets == []:
        return []
    ordered = sorted(_parse_numeric_offset(offset) for offset in observed_offsets)
    missing: list[str] = []
    expected = ordered[0]
    for value in ordered:
        while expected < value:
            missing.append(str(expected))
            expected += 1
        expected = value + 1
    return missing


def _enforce_no_miss_or_raise(*, stream_key: CanonicalStreamKey, missing_offsets: list[str]) -> None:
    if missing_offsets == []:
        return
    raise RuntimeError(
        'No-miss reconciliation failed: '
        f'source={stream_key.source_id} stream={stream_key.stream_id} '
        f'partition={stream_key.partition_id} missing_offsets={missing_offsets}'
    )


def run_s19_p5_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    quarantine_state_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s19-p5-quarantine.json'
    )
    stream_key = CanonicalStreamKey(
        source_id='bybit',
        stream_id='bybit_spot_trades',
        partition_id='2024-01-04',
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        quarantine_registry = StreamQuarantineRegistry(path=quarantine_state_path)
        state_store = CanonicalIngestStateStore(
            client=admin_client,
            database=proof_database,
            quarantine_registry=quarantine_registry,
        )

        day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(_FIXTURE_DATE)
        events = parse_bybit_spot_trade_csv(
            csv_content=_fixture_csv_bytes_with_gap(),
            date_str=_FIXTURE_DATE,
            day_start_ts_utc_ms=day_start_ts_utc_ms,
            day_end_ts_utc_ms=day_end_ts_utc_ms,
        )
        write_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s19-p5-proof',
            ingested_at_utc=datetime(2026, 3, 10, 22, 20, 0, tzinfo=UTC),
        )
        expected_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_summary:
            raise RuntimeError(
                'S19-P5 expected all fixture writes inserted, '
                f'observed={write_summary}'
            )

        observed_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY toInt64(source_offset_or_equivalent) ASC
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        observed_offsets = [str(row[0]) for row in observed_rows]
        missing_offsets = _detect_missing_offsets(observed_offsets)
        if missing_offsets != ['5003']:
            raise RuntimeError(
                'S19-P5 expected exactly one injected missing offset=5003, '
                f'observed_missing_offsets={missing_offsets}'
            )

        checkpoint_input = CompletenessCheckpointInput(
            stream_key=stream_key,
            offset_ordering='numeric',
            check_scope_start_offset='5001',
            check_scope_end_offset='5005',
            last_checked_source_offset_or_equivalent='5005',
            expected_event_count=5,
            observed_event_count=4,
            gap_count=1,
            status='gap_detected',
            gap_details={'missing_offsets': missing_offsets},
            checked_by_run_id='s19-p5-proof',
            checked_at_utc=datetime(2026, 3, 10, 22, 20, 10, tzinfo=UTC),
        )
        checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
        if checkpoint.status != 'recorded':
            raise RuntimeError(
                f'S19-P5 expected checkpoint status=recorded, observed={checkpoint.status}'
            )
        duplicate_checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
        if duplicate_checkpoint.status != 'duplicate':
            raise RuntimeError(
                'S19-P5 expected duplicate checkpoint status=duplicate, '
                f'observed={duplicate_checkpoint.status}'
            )

        try:
            _enforce_no_miss_or_raise(stream_key=stream_key, missing_offsets=missing_offsets)
        except RuntimeError as exc:
            fail_loud_error = str(exc)
        else:
            raise RuntimeError('S19-P5 expected fail-loud no-miss enforcement error')

        quarantine_state = quarantine_registry.get(stream_key=stream_key)
        if quarantine_state is None:
            raise RuntimeError('S19-P5 expected stream quarantine state to exist')

        blocked_error_code: str
        try:
            state_store.advance_cursor(
                CursorAdvanceInput(
                    stream_key=stream_key,
                    next_source_offset_or_equivalent='5006',
                    event_id=UUID('00000000-0000-0000-0000-000000000001'),
                    offset_ordering='numeric',
                    run_id='s19-p5-proof-post-quarantine',
                    change_reason='post-gap-should-block',
                    ingested_at_utc=datetime(2026, 3, 10, 22, 20, 20, tzinfo=UTC),
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 3, tzinfo=UTC),
                    updated_at_utc=datetime(2026, 3, 10, 22, 20, 20, tzinfo=UTC),
                )
            )
        except ReconciliationError as exc:
            blocked_error_code = exc.code
            if blocked_error_code != 'STREAM_QUARANTINED':
                raise RuntimeError(
                    'S19-P5 expected STREAM_QUARANTINED on post-gap cursor advance, '
                    f'observed={blocked_error_code}'
                ) from exc
        else:
            raise RuntimeError('S19-P5 expected post-gap cursor advance to be blocked')

        return {
            'proof_scope': (
                'Slice 19 S19-P5 no-miss completeness proof with gap detection '
                'and quarantine enforcement for Bybit canonical ingest'
            ),
            'proof_database': proof_database,
            'stream_key': {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
            'observed_offsets': observed_offsets,
            'missing_offsets': missing_offsets,
            'checkpoint_status': checkpoint.status,
            'duplicate_checkpoint_status': duplicate_checkpoint.status,
            'fail_loud_error': fail_loud_error,
            'quarantine_reason': quarantine_state.reason,
            'post_quarantine_block_error_code': blocked_error_code,
            'no_miss_guardrail_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()
        if quarantine_state_path.exists():
            quarantine_state_path.unlink()


def main() -> None:
    payload = run_s19_p5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s19-p5-no-miss-completeness.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
