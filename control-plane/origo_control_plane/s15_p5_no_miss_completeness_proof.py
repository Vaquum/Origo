from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from clickhouse_driver import Client as ClickHouseClient

from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
)
from origo.events.quarantine import StreamQuarantineRegistry
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

Dataset = Literal['binance_spot_trades']

_PROOF_DB_SUFFIX = '_s15_p5_proof'
_SLICE_DIR = Path('spec/slices/slice-15-binance-event-sourcing-port')


@dataclass(frozen=True)
class FixtureEvent:
    source_offset_or_equivalent: str
    source_event_time_utc: datetime
    payload_raw: bytes


@dataclass(frozen=True)
class StreamFixture:
    dataset: Dataset
    stream_id: Dataset
    partition_id: str
    events: tuple[FixtureEvent, ...]


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixtures_with_injected_gap() -> tuple[StreamFixture, ...]:
    partition_id = '2024-01-05'
    return (
        StreamFixture(
            dataset='binance_spot_trades',
            stream_id='binance_spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='10101',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10101,"price":"42200.10000000","qty":"0.01000000",'
                        b'"quote_qty":"422.00100000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10102',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 400000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10102,"price":"42200.20000000","qty":"0.02000000",'
                        b'"quote_qty":"844.00400000","is_buyer_maker":true,"is_best_match":true}'
                    ),
                ),
                # 10103 intentionally omitted (gap injection)
                FixtureEvent(
                    source_offset_or_equivalent='10104',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10104,"price":"42200.40000000","qty":"0.04000000",'
                        b'"quote_qty":"1688.01600000","is_buyer_maker":true,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10105',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 400000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10105,"price":"42200.50000000","qty":"0.05000000",'
                        b'"quote_qty":"2110.02500000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='binance_spot_trades',
            stream_id='binance_spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='10201',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 120000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":10201,"price":"42200.05000000","qty":"0.10000000",'
                        b'"first_trade_id":10200,"last_trade_id":10201,"is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10202',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 620000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":10202,"price":"42200.25000000","qty":"0.20000000",'
                        b'"first_trade_id":10202,"last_trade_id":10205,"is_buyer_maker":true}'
                    ),
                ),
                # 10203 intentionally omitted (gap injection)
                FixtureEvent(
                    source_offset_or_equivalent='10204',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 120000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":10204,"price":"42200.45000000","qty":"0.13000000",'
                        b'"first_trade_id":10209,"last_trade_id":10212,"is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10205',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 520000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":10205,"price":"42199.95000000","qty":"0.17000000",'
                        b'"first_trade_id":10213,"last_trade_id":10217,"is_buyer_maker":false}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='binance_spot_trades',
            stream_id='binance_spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='10301',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 160000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10301,"price":"42210.10000000","qty":"0.21000000",'
                        b'"quote_qty":"8864.12100000","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10302',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 0, 760000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10302,"price":"42210.20000000","qty":"0.18000000",'
                        b'"quote_qty":"7597.83600000","is_buyer_maker":true}'
                    ),
                ),
                # 10303 intentionally omitted (gap injection)
                FixtureEvent(
                    source_offset_or_equivalent='10304',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 260000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10304,"price":"42210.40000000","qty":"0.17000000",'
                        b'"quote_qty":"7175.76800000","is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='10305',
                    source_event_time_utc=datetime(2024, 1, 5, 0, 0, 1, 860000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":10305,"price":"42209.80000000","qty":"0.19000000",'
                        b'"quote_qty":"8019.86200000","is_buyer_maker":false}'
                    ),
                ),
            ),
        ),
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


def run_s15_p5_proof() -> dict[str, Any]:
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
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s15-p5-quarantine.json'
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

        results: dict[str, Any] = {}
        for fixture in _fixtures_with_injected_gap():
            stream_key = CanonicalStreamKey(
                source_id='binance',
                stream_id=fixture.stream_id,
                partition_id=fixture.partition_id,
            )
            writer = CanonicalEventWriter(client=admin_client, database=proof_database)

            statuses: list[str] = []
            for event in fixture.events:
                write_result = writer.write_event(
                    CanonicalEventWriteInput(
                        source_id='binance',
                        stream_id=fixture.stream_id,
                        partition_id=fixture.partition_id,
                        source_offset_or_equivalent=event.source_offset_or_equivalent,
                        source_event_time_utc=event.source_event_time_utc,
                        ingested_at_utc=event.source_event_time_utc,
                        payload_content_type='application/json',
                        payload_encoding='utf-8',
                        payload_raw=event.payload_raw,
                    )
                )
                statuses.append(write_result.status)

            expected_statuses = ['inserted'] * len(fixture.events)
            if statuses != expected_statuses:
                raise RuntimeError(
                    f'S15-P5 expected all fixture writes inserted for dataset={fixture.dataset}, '
                    f'observed={statuses}'
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
            if len(missing_offsets) != 1:
                raise RuntimeError(
                    f'S15-P5 expected exactly one injected gap for dataset={fixture.dataset}, '
                    f'observed_missing_offsets={missing_offsets}'
                )

            expected_event_count = (
                _parse_numeric_offset(observed_offsets[-1])
                - _parse_numeric_offset(observed_offsets[0])
                + 1
            )
            observed_event_count = len(observed_offsets)

            checkpoint = state_store.record_completeness_checkpoint(
                CompletenessCheckpointInput(
                    stream_key=stream_key,
                    offset_ordering='numeric',
                    check_scope_start_offset=observed_offsets[0],
                    check_scope_end_offset=observed_offsets[-1],
                    last_checked_source_offset_or_equivalent=observed_offsets[-1],
                    expected_event_count=expected_event_count,
                    observed_event_count=observed_event_count,
                    gap_count=len(missing_offsets),
                    status='gap_detected',
                    gap_details={'missing_offsets': missing_offsets},
                    checked_by_run_id=f's15-p5-{fixture.dataset}',
                    checked_at_utc=datetime(2024, 1, 5, 0, 2, 0, tzinfo=UTC),
                )
            )
            if checkpoint.status != 'recorded':
                raise RuntimeError(
                    f'S15-P5 expected checkpoint status=recorded for dataset={fixture.dataset}'
                )

            duplicate_checkpoint = state_store.record_completeness_checkpoint(
                CompletenessCheckpointInput(
                    stream_key=stream_key,
                    offset_ordering='numeric',
                    check_scope_start_offset=observed_offsets[0],
                    check_scope_end_offset=observed_offsets[-1],
                    last_checked_source_offset_or_equivalent=observed_offsets[-1],
                    expected_event_count=expected_event_count,
                    observed_event_count=observed_event_count,
                    gap_count=len(missing_offsets),
                    status='gap_detected',
                    gap_details={'missing_offsets': missing_offsets},
                    checked_by_run_id=f's15-p5-{fixture.dataset}',
                    checked_at_utc=datetime(2024, 1, 5, 0, 2, 0, tzinfo=UTC),
                )
            )
            if duplicate_checkpoint.status != 'duplicate':
                raise RuntimeError(
                    'S15-P5 expected duplicate checkpoint status=duplicate '
                    f'dataset={fixture.dataset}, observed={duplicate_checkpoint.status}'
                )

            quarantine_state = quarantine_registry.get(stream_key=stream_key)
            if quarantine_state is None:
                raise RuntimeError(
                    f'S15-P5 expected quarantine state for dataset={fixture.dataset}'
                )
            if quarantine_state.reason != 'gap_detected_from_completeness_reconciliation':
                raise RuntimeError(
                    f'S15-P5 unexpected quarantine reason for dataset={fixture.dataset}'
                )

            quarantine_enforced = False
            try:
                quarantine_registry.assert_not_quarantined(stream_key=stream_key)
            except Exception:
                quarantine_enforced = True

            if not quarantine_enforced:
                raise RuntimeError(
                    f'S15-P5 expected quarantine enforcement for dataset={fixture.dataset}'
                )

            no_miss_error: str | None = None
            try:
                _enforce_no_miss_or_raise(
                    stream_key=stream_key,
                    missing_offsets=missing_offsets,
                )
            except RuntimeError as exc:
                no_miss_error = str(exc)

            if no_miss_error is None:
                raise RuntimeError(
                    f'S15-P5 expected fail-loud no-miss error for dataset={fixture.dataset}'
                )

            results[fixture.dataset] = {
                'ingest_statuses': statuses,
                'observed_offsets': observed_offsets,
                'missing_offsets': missing_offsets,
                'expected_event_count': expected_event_count,
                'observed_event_count': observed_event_count,
                'checkpoint_status': checkpoint.status,
                'checkpoint_revision': checkpoint.checkpoint_state.checkpoint_revision,
                'duplicate_checkpoint_status': duplicate_checkpoint.status,
                'quarantine_enforced': quarantine_enforced,
                'quarantine_state': {
                    'run_id': quarantine_state.run_id,
                    'reason': quarantine_state.reason,
                    'quarantined_at_utc': quarantine_state.quarantined_at_utc.isoformat(),
                },
                'no_miss_error': no_miss_error,
            }

        return {
            'proof_scope': (
                'Slice 15 S15-P5 no-miss completeness proof with reconciliation '
                'and gap injection for Binance binance_spot_trades'
            ),
            'proof_database': proof_database,
            'datasets': results,
            'no_miss_detection_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()
        if quarantine_state_path.exists():
            quarantine_state_path.unlink()


def main() -> None:
    payload = run_s15_p5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s15-p5-no-miss-completeness.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
