from __future__ import annotations

import json
from dataclasses import dataclass
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
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s16_p5_proof'
_SLICE_DIR = Path('spec/slices/slice-16-etf-event-sourcing-port')


@dataclass(frozen=True)
class _CadenceEvent:
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


def _events_with_injected_gap() -> tuple[_CadenceEvent, ...]:
    return (
        _CadenceEvent(
            source_offset_or_equivalent='2026-03-08',
            source_event_time_utc=datetime(2026, 3, 8, 0, 0, 0, tzinfo=UTC),
            payload_raw=(
                b'{"record_source_id":"etf_ishares_ibit_daily","metric_id":"ibit:2026-03-08:btc_units",'
                b'"metric_name":"btc_units","metric_unit":"BTC","metric_value_kind":"float",'
                b'"metric_value_text":"285123.125","metric_value_bool":null,'
                b'"observed_at_utc":"2026-03-08T00:00:00+00:00",'
                b'"dimensions":{"issuer":"iShares","ticker":"IBIT"},"provenance":null}'
            ),
        ),
        # 2026-03-09 intentionally omitted
        _CadenceEvent(
            source_offset_or_equivalent='2026-03-10',
            source_event_time_utc=datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC),
            payload_raw=(
                b'{"record_source_id":"etf_ishares_ibit_daily","metric_id":"ibit:2026-03-10:btc_units",'
                b'"metric_name":"btc_units","metric_unit":"BTC","metric_value_kind":"float",'
                b'"metric_value_text":"286500.875","metric_value_bool":null,'
                b'"observed_at_utc":"2026-03-10T00:00:00+00:00",'
                b'"dimensions":{"issuer":"iShares","ticker":"IBIT"},"provenance":null}'
            ),
        ),
    )


def _detect_missing_dates(observed_dates: list[str]) -> list[str]:
    if observed_dates == []:
        return []

    parsed_dates = [datetime.fromisoformat(f'{item}T00:00:00+00:00').date() for item in observed_dates]
    ordered = sorted(parsed_dates)
    missing: list[str] = []
    current = ordered[0]
    last = ordered[-1]
    while current <= last:
        current_iso = current.isoformat()
        if current_iso not in observed_dates:
            missing.append(current_iso)
        current = current.fromordinal(current.toordinal() + 1)
    return missing


def _enforce_no_miss_or_raise(*, stream_key: CanonicalStreamKey, missing_offsets: list[str]) -> None:
    if missing_offsets == []:
        return
    raise RuntimeError(
        'No-miss cadence reconciliation failed: '
        f'source={stream_key.source_id} stream={stream_key.stream_id} '
        f'partition={stream_key.partition_id} missing_offsets={missing_offsets}'
    )


def run_s16_p5_proof() -> dict[str, Any]:
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
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s16-p5-quarantine.json'
    )

    stream_key = CanonicalStreamKey(
        source_id='etf',
        stream_id='etf_daily_metrics',
        partition_id='2026-03',
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
        writer = CanonicalEventWriter(client=admin_client, database=proof_database)

        statuses: list[str] = []
        for event in _events_with_injected_gap():
            write_result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id='etf',
                    stream_id='etf_daily_metrics',
                    partition_id='2026-03',
                    source_offset_or_equivalent=event.source_offset_or_equivalent,
                    source_event_time_utc=event.source_event_time_utc,
                    ingested_at_utc=event.source_event_time_utc,
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=event.payload_raw,
                )
            )
            statuses.append(write_result.status)

        expected_statuses = ['inserted', 'inserted']
        if statuses != expected_statuses:
            raise RuntimeError(
                'S16-P5 expected fixture writes inserted, '
                f'observed={statuses}'
            )

        observed_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY source_offset_or_equivalent ASC
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        observed_offsets = [str(row[0]) for row in observed_rows]
        missing_offsets = _detect_missing_dates(observed_offsets)
        if missing_offsets != ['2026-03-09']:
            raise RuntimeError(
                'S16-P5 expected exactly one injected missing day, '
                f'observed_missing_offsets={missing_offsets}'
            )

        checkpoint_input = CompletenessCheckpointInput(
            stream_key=stream_key,
            offset_ordering='lexicographic',
            check_scope_start_offset='2026-03-08',
            check_scope_end_offset='2026-03-10',
            last_checked_source_offset_or_equivalent='2026-03-10',
            expected_event_count=3,
            observed_event_count=2,
            gap_count=1,
            status='gap_detected',
            gap_details={'missing_offsets': missing_offsets},
            checked_by_run_id='s16-p5-proof',
            checked_at_utc=datetime(2026, 3, 10, 14, 0, 0, tzinfo=UTC),
        )
        checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
        if checkpoint.status != 'recorded':
            raise RuntimeError(
                f'S16-P5 expected checkpoint status=recorded, observed={checkpoint.status}'
            )

        duplicate_checkpoint = state_store.record_completeness_checkpoint(
            checkpoint_input
        )
        if duplicate_checkpoint.status != 'duplicate':
            raise RuntimeError(
                'S16-P5 expected duplicate checkpoint status=duplicate, '
                f'observed={duplicate_checkpoint.status}'
            )

        try:
            _enforce_no_miss_or_raise(stream_key=stream_key, missing_offsets=missing_offsets)
        except RuntimeError as exc:
            fail_loud_error = str(exc)
        else:
            raise RuntimeError('S16-P5 expected fail-loud no-miss enforcement error')

        quarantine_state = quarantine_registry.get(stream_key=stream_key)
        if quarantine_state is None:
            raise RuntimeError('S16-P5 expected stream quarantine state to exist')

        blocked_error_code: str
        try:
            state_store.advance_cursor(
                CursorAdvanceInput(
                    stream_key=stream_key,
                    next_source_offset_or_equivalent='2026-03-11',
                    event_id=UUID('00000000-0000-0000-0000-000000000001'),
                    offset_ordering='lexicographic',
                    run_id='s16-p5-proof-post-quarantine',
                    change_reason='post-gap-should-block',
                    ingested_at_utc=datetime(2026, 3, 10, 14, 5, 0, tzinfo=UTC),
                    source_event_time_utc=datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC),
                    updated_at_utc=datetime(2026, 3, 10, 14, 5, 0, tzinfo=UTC),
                )
            )
        except ReconciliationError as exc:
            blocked_error_code = exc.code
            if blocked_error_code != 'STREAM_QUARANTINED':
                raise RuntimeError(
                    'S16-P5 expected STREAM_QUARANTINED on post-gap cursor advance, '
                    f'observed={blocked_error_code}'
                ) from exc
        else:
            raise RuntimeError('S16-P5 expected post-gap cursor advance to be blocked')

        return {
            'proof_scope': (
                'Slice 16 S16-P5 no-miss cadence completeness proof with '
                'gap detection and quarantine enforcement for ETF canonical ingest'
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
    payload = run_s16_p5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s16-p5-no-miss-completeness.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
