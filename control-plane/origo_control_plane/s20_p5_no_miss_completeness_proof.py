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
from origo_control_plane.s20_bitcoin_proof_common import (
    BITCOIN_NATIVE_DATASETS,
    build_clickhouse_client,
    build_fixture_rows_by_dataset,
    fixture_events_by_stream,
)
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    write_bitcoin_events_to_canonical,
)

_PROOF_DB_SUFFIX = '_s20_p5_proof'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _hex64(index: int) -> str:
    return f'{index:064x}'


def _parse_numeric_offsets(offsets: list[str]) -> list[int]:
    parsed: list[int] = []
    for offset in offsets:
        try:
            parsed.append(int(offset))
        except ValueError as exc:
            raise RuntimeError(f'Numeric offset required, got={offset}') from exc
    return sorted(parsed)


def _detect_missing_numeric_offsets(offsets: list[str]) -> list[str]:
    ordered = _parse_numeric_offsets(offsets)
    if ordered == []:
        return []
    missing: list[str] = []
    expected = ordered[0]
    for value in ordered:
        while expected < value:
            missing.append(str(expected))
            expected += 1
        expected = value + 1
    return missing


def _parse_transaction_offset(offset: str) -> tuple[int, int, str]:
    parts = offset.split(':')
    if len(parts) != 3:
        raise RuntimeError(f'Invalid transaction offset format: {offset}')
    try:
        height = int(parts[0])
        index = int(parts[1])
    except ValueError as exc:
        raise RuntimeError(f'Invalid transaction offset format: {offset}') from exc
    txid = parts[2]
    return (height, index, txid)


def _detect_missing_transaction_offsets(offsets: list[str]) -> list[str]:
    parsed = [_parse_transaction_offset(offset) for offset in offsets]
    if parsed == []:
        return []

    by_height: dict[int, list[int]] = {}
    for height, index, _ in parsed:
        indices = by_height.get(height)
        if indices is None:
            by_height[height] = [index]
        else:
            indices.append(index)

    missing: list[str] = []
    for height in sorted(by_height):
        ordered = sorted(by_height[height])
        expected = ordered[0]
        for index in ordered:
            while expected < index:
                missing.append(f'{height}:{expected}:*')
                expected += 1
            expected = index + 1
    return missing


def _parse_mempool_offset(offset: str) -> tuple[int, int]:
    parts = offset.split(':')
    if len(parts) != 2:
        raise RuntimeError(f'Invalid mempool offset format: {offset}')
    try:
        snapshot = int(parts[0])
        txid_int = int(parts[1], 16)
    except ValueError as exc:
        raise RuntimeError(f'Invalid mempool offset format: {offset}') from exc
    return (snapshot, txid_int)


def _detect_missing_mempool_offsets(offsets: list[str]) -> list[str]:
    parsed = [_parse_mempool_offset(offset) for offset in offsets]
    if parsed == []:
        return []

    snapshots = {snapshot for snapshot, _ in parsed}
    if len(snapshots) != 1:
        raise RuntimeError(
            'Mempool completeness proof expects one snapshot per partition '
            f'but got snapshots={sorted(snapshots)}'
        )
    snapshot = next(iter(snapshots))
    ordered = sorted(txid_int for _, txid_int in parsed)
    missing: list[str] = []
    expected = ordered[0]
    for txid_int in ordered:
        while expected < txid_int:
            missing.append(f'{snapshot}:{_hex64(expected)}')
            expected += 1
        expected = txid_int + 1
    return missing


def _detect_missing_offsets(*, stream_id: str, offsets: list[str]) -> list[str]:
    if stream_id == 'bitcoin_block_transactions':
        return _detect_missing_transaction_offsets(offsets)
    if stream_id == 'bitcoin_mempool_state':
        return _detect_missing_mempool_offsets(offsets)
    return _detect_missing_numeric_offsets(offsets)


def _next_offset_for_quarantine_probe(*, stream_id: str, offsets: list[str]) -> str:
    if stream_id == 'bitcoin_block_transactions':
        height, index, _ = _parse_transaction_offset(sorted(offsets)[-1])
        return f'{height}:{index + 1}:{_hex64(9999)}'
    if stream_id == 'bitcoin_mempool_state':
        snapshot, txid_int = _parse_mempool_offset(sorted(offsets)[-1])
        return f'{snapshot}:{_hex64(txid_int + 1)}'

    ordered = _parse_numeric_offsets(offsets)
    return str(ordered[-1] + 1)


def _enforce_no_miss_or_raise(*, stream_key: CanonicalStreamKey, missing_offsets: list[str]) -> None:
    if missing_offsets == []:
        return
    raise RuntimeError(
        'No-miss reconciliation failed: '
        f'source={stream_key.source_id} stream={stream_key.stream_id} '
        f'partition={stream_key.partition_id} missing_offsets={missing_offsets}'
    )


def _build_gap_events() -> dict[str, list[BitcoinCanonicalEvent]]:
    rows_by_dataset = build_fixture_rows_by_dataset()
    fixture_by_stream = fixture_events_by_stream(rows_by_dataset=rows_by_dataset)

    output: dict[str, list[BitcoinCanonicalEvent]] = {}
    for stream_id in BITCOIN_NATIVE_DATASETS:
        stream_events = fixture_by_stream[stream_id]
        if len(stream_events) < 2:
            raise RuntimeError(f'S20-P5 expected >=2 fixture events for stream={stream_id}')

        first = stream_events[0]
        second = stream_events[1]

        if stream_id == 'bitcoin_block_transactions':
            payload = dict(second.payload)
            payload['transaction_index'] = 2
            txid = _hex64(305)
            payload['txid'] = txid
            output[stream_id] = [
                first,
                BitcoinCanonicalEvent(
                    stream_id=stream_id,
                    partition_id=second.partition_id,
                    source_offset_or_equivalent=f"{payload['block_height']}:2:{txid}",
                    source_event_time_utc=second.source_event_time_utc,
                    payload=payload,
                ),
            ]
            continue

        if stream_id == 'bitcoin_mempool_state':
            payload = dict(second.payload)
            txid = _hex64(402)
            payload['txid'] = txid
            snapshot_at_unix_ms = payload['snapshot_at_unix_ms']
            output[stream_id] = [
                first,
                BitcoinCanonicalEvent(
                    stream_id=stream_id,
                    partition_id=second.partition_id,
                    source_offset_or_equivalent=f'{snapshot_at_unix_ms}:{txid}',
                    source_event_time_utc=second.source_event_time_utc,
                    payload=payload,
                ),
            ]
            continue

        payload = dict(second.payload)
        if stream_id == 'bitcoin_block_headers':
            payload['height'] = 840_002
        else:
            payload['block_height'] = 840_002
        if 'halving_interval' in payload:
            payload['halving_interval'] = 4
            payload['subsidy_sats'] = 312_500_000
            payload['subsidy_btc'] = '3.125000000000000000'
        if 'circulating_supply_sats' in payload:
            payload['circulating_supply_sats'] = payload['circulating_supply_sats'] + 312_500_000
        output[stream_id] = [
            first,
            BitcoinCanonicalEvent(
                stream_id=stream_id,
                partition_id=second.partition_id,
                source_offset_or_equivalent='840002',
                source_event_time_utc=second.source_event_time_utc,
                payload=payload,
            ),
        ]

    return output


def run_s20_p5_proof() -> dict[str, Any]:
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
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s20-p5-quarantine.json'
    )

    gap_events_by_stream = _build_gap_events()

    admin_client: ClickHouseClient = build_clickhouse_client(base_settings)
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

        per_stream_results: dict[str, dict[str, Any]] = {}
        for stream_id in BITCOIN_NATIVE_DATASETS:
            events = gap_events_by_stream[stream_id]
            write_summary = write_bitcoin_events_to_canonical(
                client=admin_client,
                database=proof_database,
                events=events,
                run_id=f's20-p5-{stream_id}',
                ingested_at_utc=datetime(2026, 3, 10, 23, 45, 0, tzinfo=UTC),
            )
            expected_summary = {
                'rows_processed': len(events),
                'rows_inserted': len(events),
                'rows_duplicate': 0,
            }
            if write_summary != expected_summary:
                raise RuntimeError(
                    'S20-P5 expected all gap-fixture rows inserted once: '
                    f'stream={stream_id} observed={write_summary} expected={expected_summary}'
                )

            partition_id = events[0].partition_id
            stream_key = CanonicalStreamKey(
                source_id='bitcoin_core',
                stream_id=stream_id,
                partition_id=partition_id,
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
            missing_offsets = _detect_missing_offsets(
                stream_id=stream_id,
                offsets=observed_offsets,
            )
            if len(missing_offsets) != 1:
                raise RuntimeError(
                    'S20-P5 expected exactly one injected missing offset per stream: '
                    f'stream={stream_id} missing_offsets={missing_offsets}'
                )

            checkpoint_input = CompletenessCheckpointInput(
                stream_key=stream_key,
                offset_ordering='lexicographic',
                check_scope_start_offset=min(observed_offsets),
                check_scope_end_offset=max(observed_offsets),
                last_checked_source_offset_or_equivalent=max(observed_offsets),
                expected_event_count=len(observed_offsets) + len(missing_offsets),
                observed_event_count=len(observed_offsets),
                gap_count=len(missing_offsets),
                status='gap_detected',
                gap_details={'missing_offsets': missing_offsets},
                checked_by_run_id=f's20-p5-{stream_id}',
                checked_at_utc=datetime(2026, 3, 10, 23, 45, 5, tzinfo=UTC),
            )
            checkpoint = state_store.record_completeness_checkpoint(checkpoint_input)
            if checkpoint.status != 'recorded':
                raise RuntimeError(
                    f'S20-P5 expected checkpoint status=recorded for stream={stream_id}, '
                    f'observed={checkpoint.status}'
                )
            duplicate_checkpoint = state_store.record_completeness_checkpoint(
                checkpoint_input
            )
            if duplicate_checkpoint.status != 'duplicate':
                raise RuntimeError(
                    f'S20-P5 expected duplicate checkpoint for stream={stream_id}, '
                    f'observed={duplicate_checkpoint.status}'
                )

            try:
                _enforce_no_miss_or_raise(
                    stream_key=stream_key,
                    missing_offsets=missing_offsets,
                )
            except RuntimeError as exc:
                fail_loud_error = str(exc)
            else:
                raise RuntimeError(
                    f'S20-P5 expected fail-loud no-miss enforcement for stream={stream_id}'
                )

            quarantine_state = quarantine_registry.get(stream_key=stream_key)
            if quarantine_state is None:
                raise RuntimeError(
                    f'S20-P5 expected quarantine state for stream={stream_id}'
                )

            blocked_error_code: str
            try:
                state_store.advance_cursor(
                    CursorAdvanceInput(
                        stream_key=stream_key,
                        next_source_offset_or_equivalent=_next_offset_for_quarantine_probe(
                            stream_id=stream_id,
                            offsets=observed_offsets,
                        ),
                        event_id=UUID('00000000-0000-0000-0000-000000000001'),
                        offset_ordering='lexicographic',
                        run_id=f's20-p5-{stream_id}-post-quarantine',
                        change_reason='post-gap-should-block',
                        ingested_at_utc=datetime(2026, 3, 10, 23, 45, 10, tzinfo=UTC),
                        source_event_time_utc=datetime(2024, 4, 20, 0, 0, 2, tzinfo=UTC),
                        updated_at_utc=datetime(2026, 3, 10, 23, 45, 10, tzinfo=UTC),
                    )
                )
            except ReconciliationError as exc:
                blocked_error_code = exc.code
                if blocked_error_code != 'STREAM_QUARANTINED':
                    raise RuntimeError(
                        'S20-P5 expected STREAM_QUARANTINED after gap detection: '
                        f'stream={stream_id} observed={blocked_error_code}'
                    ) from exc
            else:
                raise RuntimeError(
                    f'S20-P5 expected cursor advance block for stream={stream_id}'
                )

            per_stream_results[stream_id] = {
                'observed_offsets': observed_offsets,
                'missing_offsets': missing_offsets,
                'checkpoint_status': checkpoint.status,
                'duplicate_checkpoint_status': duplicate_checkpoint.status,
                'fail_loud_error': fail_loud_error,
                'quarantine_reason': quarantine_state.reason,
                'post_quarantine_block_error_code': blocked_error_code,
            }

        return {
            'proof_scope': (
                'Slice 20 S20-P5 no-miss completeness proof with gap detection '
                'and quarantine enforcement across all seven Bitcoin datasets'
            ),
            'proof_database': proof_database,
            'stream_results': per_stream_results,
            'no_miss_guardrail_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()
        if quarantine_state_path.exists():
            quarantine_state_path.unlink()


def main() -> None:
    payload = run_s20_p5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-p5-no-miss-completeness.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
