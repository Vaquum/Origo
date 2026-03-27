from __future__ import annotations

import json
from datetime import UTC, datetime
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
from origo_control_plane.utils.okx_canonical_event_ingest import (
    parse_okx_spot_trade_csv,
    write_okx_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s18_p5_proof'
_SLICE_DIR = Path('spec/slices/slice-18-okx-event-sourcing-port')


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_csv_bytes_with_gap() -> bytes:
    return (
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,5001,buy,42700.10000000,0.01000000,1704326400100\n'
        b'BTC-USDT,5002,sell,42700.20000000,0.02000000,1704326400400\n'
        b'BTC-USDT,5004,buy,42700.40000000,0.03000000,1704326400900\n'
        b'BTC-USDT,5005,sell,42700.50000000,0.04000000,1704326401200\n'
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


def run_s18_p5_proof() -> dict[str, Any]:
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
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s18-p5-quarantine.json'
    )
    stream_key = CanonicalStreamKey(
        source_id='okx',
        stream_id='okx_spot_trades',
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

        events = parse_okx_spot_trade_csv(_fixture_csv_bytes_with_gap())
        write_summary = write_okx_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s18-p5-proof',
            ingested_at_utc=datetime(2026, 3, 10, 22, 20, 0, tzinfo=UTC),
        )
        expected_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_summary:
            raise RuntimeError(
                'S18-P5 expected all fixture writes inserted, '
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
                'S18-P5 expected exactly one injected missing offset=5003, '
                f'observed_missing_offsets={missing_offsets}'
            )

        checkpoint_input = CompletenessCheckpointInput(
            stream_key=stream_key,
            offset_ordering='numeric_monotonic',
            check_scope_start_offset='5001',
            check_scope_end_offset='5005',
            last_checked_source_offset_or_equivalent='5005',
            expected_event_count=5,
            observed_event_count=4,
            gap_count=1,
            status='gap_detected',
            gap_details={'missing_offsets': missing_offsets},
            checked_by_run_id='s18-p5-proof',
            checked_at_utc=datetime(2026, 3, 10, 22, 20, 10, tzinfo=UTC),
        )
        checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
        if checkpoint.status != 'recorded':
            raise RuntimeError(
                f'S18-P5 expected checkpoint status=recorded, observed={checkpoint.status}'
            )
        duplicate_checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
        if duplicate_checkpoint.status != 'duplicate':
            raise RuntimeError(
                'S18-P5 expected duplicate checkpoint status=duplicate, '
                f'observed={duplicate_checkpoint.status}'
            )

        try:
            _enforce_no_miss_or_raise(stream_key=stream_key, missing_offsets=missing_offsets)
        except RuntimeError as exc:
            fail_loud_error = str(exc)
        else:
            raise RuntimeError('S18-P5 expected fail-loud no-miss enforcement error')

        quarantine_state = quarantine_registry.get(stream_key=stream_key)
        if quarantine_state is None:
            raise RuntimeError('S18-P5 expected stream quarantine state to exist')

        blocked_error_code: str
        try:
            state_store.advance_cursor(
                CursorAdvanceInput(
                    stream_key=stream_key,
                    next_source_offset_or_equivalent='5006',
                    event_id=UUID('00000000-0000-0000-0000-000000000001'),
                    offset_ordering='numeric_monotonic',
                    run_id='s18-p5-proof-post-quarantine',
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
                    'S18-P5 expected STREAM_QUARANTINED on post-gap cursor advance, '
                    f'observed={blocked_error_code}'
                ) from exc
        else:
            raise RuntimeError('S18-P5 expected post-gap cursor advance to be blocked')

        return {
            'proof_scope': (
                'Slice 18 S18-P5 no-miss completeness proof with gap detection '
                'and quarantine enforcement for OKX canonical ingest'
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
    payload = run_s18_p5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s18-p5-no-miss-completeness.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
