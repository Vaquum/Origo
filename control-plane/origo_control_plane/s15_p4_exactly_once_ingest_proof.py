from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

Dataset = Literal['binance_spot_trades']

_PROOF_DB_SUFFIX = '_s15_p4_proof'
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


def _fixtures() -> tuple[StreamFixture, ...]:
    partition_id = '2024-01-04'
    return (
        StreamFixture(
            dataset='binance_spot_trades',
            stream_id='binance_spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='9101',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9101,"price":"42100.10000000","qty":"0.01000000",'
                        b'"quote_qty":"421.00100000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9102',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 400000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9102,"price":"42100.20000000","qty":"0.02000000",'
                        b'"quote_qty":"842.00400000","is_buyer_maker":true,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9103',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 900000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9103,"price":"42100.30000000","qty":"0.03000000",'
                        b'"quote_qty":"1263.00900000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9104',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 200000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9104,"price":"42100.40000000","qty":"0.04000000",'
                        b'"quote_qty":"1684.01600000","is_buyer_maker":true,"is_best_match":true}'
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
                    source_offset_or_equivalent='9201',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 120000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":9201,"price":"42100.05000000","qty":"0.10000000",'
                        b'"first_trade_id":9198,"last_trade_id":9201,"is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9202',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 620000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":9202,"price":"42100.25000000","qty":"0.20000000",'
                        b'"first_trade_id":9202,"last_trade_id":9205,"is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9203',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 120000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":9203,"price":"42099.95000000","qty":"0.15000000",'
                        b'"first_trade_id":9206,"last_trade_id":9208,"is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9204',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 620000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":9204,"price":"42100.35000000","qty":"0.13000000",'
                        b'"first_trade_id":9209,"last_trade_id":9212,"is_buyer_maker":true}'
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
                    source_offset_or_equivalent='9301',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 160000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9301,"price":"42110.10000000","qty":"0.21000000",'
                        b'"quote_qty":"8843.12100000","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9302',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 760000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9302,"price":"42110.20000000","qty":"0.18000000",'
                        b'"quote_qty":"7579.83600000","is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9303',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 260000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9303,"price":"42109.90000000","qty":"0.22000000",'
                        b'"quote_qty":"9264.17800000","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9304',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 860000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9304,"price":"42110.40000000","qty":"0.17000000",'
                        b'"quote_qty":"7158.76800000","is_buyer_maker":true}'
                    ),
                ),
            ),
        ),
    )


def _write_events(
    *,
    writer: CanonicalEventWriter,
    fixture: StreamFixture,
    events: tuple[FixtureEvent, ...],
) -> list[str]:
    statuses: list[str] = []
    for event in events:
        result = writer.write_event(
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
        statuses.append(result.status)
    return statuses


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def run_s15_p4_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        results: dict[str, Any] = {}
        for fixture in _fixtures():
            writer_phase_1 = CanonicalEventWriter(client=admin_client, database=proof_database)
            phase_1_statuses = _write_events(
                writer=writer_phase_1,
                fixture=fixture,
                events=fixture.events[:2],
            )
            if phase_1_statuses != ['inserted', 'inserted']:
                raise RuntimeError(
                    f'S15-P4 phase 1 statuses mismatch dataset={fixture.dataset}: {phase_1_statuses}'
                )

            writer_phase_2 = CanonicalEventWriter(client=admin_client, database=proof_database)
            phase_2_statuses = _write_events(
                writer=writer_phase_2,
                fixture=fixture,
                events=fixture.events,
            )
            expected_phase_2 = ['duplicate', 'duplicate', 'inserted', 'inserted']
            if phase_2_statuses != expected_phase_2:
                raise RuntimeError(
                    'S15-P4 phase 2 statuses mismatch '
                    f'dataset={fixture.dataset} expected={expected_phase_2} observed={phase_2_statuses}'
                )

            writer_phase_3 = CanonicalEventWriter(client=admin_client, database=proof_database)
            phase_3_statuses = _write_events(
                writer=writer_phase_3,
                fixture=fixture,
                events=fixture.events,
            )
            expected_phase_3 = ['duplicate', 'duplicate', 'duplicate', 'duplicate']
            if phase_3_statuses != expected_phase_3:
                raise RuntimeError(
                    'S15-P4 phase 3 statuses mismatch '
                    f'dataset={fixture.dataset} expected={expected_phase_3} observed={phase_3_statuses}'
                )

            persisted_rows = admin_client.execute(
                f'''
                SELECT count()
                FROM {proof_database}.canonical_event_log
                WHERE source_id = %(source_id)s
                    AND stream_id = %(stream_id)s
                    AND partition_id = %(partition_id)s
                ''',
                {
                    'source_id': 'binance',
                    'stream_id': fixture.stream_id,
                    'partition_id': fixture.partition_id,
                },
            )
            persisted_count = _require_int(
                persisted_rows[0][0],
                label=f'{fixture.dataset}.persisted_count',
            )
            if persisted_count != len(fixture.events):
                raise RuntimeError(
                    f'S15-P4 persisted count mismatch dataset={fixture.dataset} '
                    f'expected={len(fixture.events)} observed={persisted_count}'
                )

            duplicate_identity_rows = admin_client.execute(
                f'''
                SELECT source_offset_or_equivalent, count()
                FROM {proof_database}.canonical_event_log
                WHERE source_id = %(source_id)s
                    AND stream_id = %(stream_id)s
                    AND partition_id = %(partition_id)s
                GROUP BY source_offset_or_equivalent
                HAVING count() > 1
                ''',
                {
                    'source_id': 'binance',
                    'stream_id': fixture.stream_id,
                    'partition_id': fixture.partition_id,
                },
            )
            if duplicate_identity_rows != []:
                raise RuntimeError(
                    f'S15-P4 duplicate identity rows found dataset={fixture.dataset}: '
                    f'{duplicate_identity_rows}'
                )

            results[fixture.dataset] = {
                'fixture_event_count': len(fixture.events),
                'phase_1_pre_crash_partial_ingest': phase_1_statuses,
                'phase_2_after_restart_full_replay': phase_2_statuses,
                'phase_3_post_recovery_duplicate_replay': phase_3_statuses,
                'persisted_row_count': persisted_count,
                'duplicate_identity_rows': duplicate_identity_rows,
                'exactly_once_verified': True,
            }

        return {
            'proof_scope': (
                'Slice 15 S15-P4 exactly-once ingest under duplicate replay and '
                'crash/restart for Binance binance_spot_trades'
            ),
            'proof_database': proof_database,
            'datasets': results,
            'exactly_once_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s15_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s15-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
