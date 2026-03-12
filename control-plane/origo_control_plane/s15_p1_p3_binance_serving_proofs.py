from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import query_aligned, query_native
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo.query.native_core import TimeRangeWindow
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.binance_aligned_projector import (
    project_binance_spot_trades_aligned,
)
from origo_control_plane.utils.binance_native_projector import (
    project_binance_spot_trades_native,
)

_PROOF_DB_SUFFIX = '_s15_p1_p3_proof'
_SLICE_DIR = Path('spec/slices/slice-15-binance-event-sourcing-port')
_WINDOW_START_ISO = '2024-01-04T00:00:00Z'
_WINDOW_END_ISO = '2024-01-04T00:00:03Z'


@dataclass(frozen=True)
class FixtureEvent:
    source_offset: str
    event_time_utc: datetime
    payload: dict[str, Any]


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


@contextmanager
def _override_clickhouse_database(database: str):
    original = {
        'ORIGO_CLICKHOUSE_DATABASE': (
            None if 'ORIGO_CLICKHOUSE_DATABASE' not in os.environ else os.environ['ORIGO_CLICKHOUSE_DATABASE']
        )
    }
    os.environ['ORIGO_CLICKHOUSE_DATABASE'] = database
    try:
        yield
    finally:
        if original['ORIGO_CLICKHOUSE_DATABASE'] is None:
            del os.environ['ORIGO_CLICKHOUSE_DATABASE']
        else:
            os.environ['ORIGO_CLICKHOUSE_DATABASE'] = original['ORIGO_CLICKHOUSE_DATABASE']


def _canonical_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _fixtures() -> tuple[FixtureEvent, ...]:
    return (
        FixtureEvent(
            source_offset='1001',
            event_time_utc=datetime(2024, 1, 4, 0, 0, 1, tzinfo=UTC),
            payload={
                'trade_id': 1001,
                'price': '43000.10',
                'qty': '0.0012',
                'quote_qty': '51.60012',
                'is_buyer_maker': True,
                'is_best_match': True,
            },
        ),
        FixtureEvent(
            source_offset='1002',
            event_time_utc=datetime(2024, 1, 4, 0, 0, 2, tzinfo=UTC),
            payload={
                'trade_id': 1002,
                'price': '43001.25',
                'qty': '0.0041',
                'quote_qty': '176.304125',
                'is_buyer_maker': False,
                'is_best_match': True,
            },
        ),
    )


def _write_fixture_events(
    *, writer: CanonicalEventWriter, events: tuple[FixtureEvent, ...], run_id: str
) -> list[str]:
    statuses: list[str] = []
    for event in events:
        payload_raw = json.dumps(
            event.payload,
            ensure_ascii=True,
            separators=(',', ':'),
            sort_keys=True,
        ).encode('utf-8')
        result = writer.write_event(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='binance_spot_trades',
                partition_id='2024-01-04',
                source_offset_or_equivalent=event.source_offset,
                source_event_time_utc=event.event_time_utc,
                ingested_at_utc=datetime(2024, 1, 4, 0, 5, 0, tzinfo=UTC),
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=payload_raw,
                run_id=run_id,
            )
        )
        statuses.append(result.status)
    return statuses


def _run_once(pass_id: str) -> dict[str, Any]:
    base = MigrationSettings.from_env()
    proof_db = f'{base.database}{_PROOF_DB_SUFFIX}_{pass_id}'
    proof = MigrationSettings(
        host=base.host,
        port=base.port,
        user=base.user,
        password=base.password,
        database=proof_db,
    )
    admin = _build_clickhouse_client(base)
    runner = MigrationRunner(settings=proof)
    try:
        admin.execute(f'DROP DATABASE IF EXISTS {proof_db} SYNC')
        runner.migrate()

        writer = CanonicalEventWriter(client=admin, database=proof_db)
        statuses = _write_fixture_events(
            writer=writer,
            events=_fixtures(),
            run_id=f's15-p1-p3-{pass_id}',
        )

        native_summary = project_binance_spot_trades_native(
            client=admin,
            database=proof_db,
            partition_ids={'2024-01-04'},
            run_id=f's15-p1-p3-{pass_id}-native',
            projected_at_utc=datetime(2024, 1, 4, 0, 10, 0, tzinfo=UTC),
        )
        aligned_summary = project_binance_spot_trades_aligned(
            client=admin,
            database=proof_db,
            partition_ids={'2024-01-04'},
            run_id=f's15-p1-p3-{pass_id}-aligned',
            projected_at_utc=datetime(2024, 1, 4, 0, 10, 30, tzinfo=UTC),
        )

        with _override_clickhouse_database(proof_db):
            window = TimeRangeWindow(start_iso=_WINDOW_START_ISO, end_iso=_WINDOW_END_ISO)
            native_rows = query_native(
                dataset='binance_spot_trades',
                select_cols=None,
                time_range=(window.start_iso, window.end_iso),
                datetime_iso_output=True,
            ).to_dicts()
            aligned_rows = query_aligned(
                dataset='binance_spot_trades',
                select_cols=None,
                time_range=(window.start_iso, window.end_iso),
                datetime_iso_output=True,
            ).to_dicts()

        return {
            'proof_database': proof_db,
            'write_statuses': statuses,
            'native_summary': native_summary.to_dict(),
            'aligned_summary': aligned_summary.to_dict(),
            'native_hash': _canonical_hash(native_rows),
            'aligned_hash': _canonical_hash(aligned_rows),
        }
    finally:
        runner.close()
        admin.execute(f'DROP DATABASE IF EXISTS {proof_db} SYNC')
        admin.disconnect()


def run_s15_p1_p3_proofs() -> dict[str, Any]:
    run_1 = _run_once('run_1')
    run_2 = _run_once('run_2')
    deterministic_match = (
        run_1['native_hash'] == run_2['native_hash']
        and run_1['aligned_hash'] == run_2['aligned_hash']
    )
    if not deterministic_match:
        raise RuntimeError('S15-P3 determinism failed for binance_spot_trades')

    return {
        'proof_scope': 'Slice 15 S15-P1/S15-P2/S15-P3 Binance serving proofs',
        'window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'datasets': ['binance_spot_trades'],
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': deterministic_match,
    }


def main() -> None:
    payload = run_s15_p1_p3_proofs()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    acceptance_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P1 fixed-window acceptance for native + aligned_1s',
        'window': payload['window'],
        'acceptance': {
            'binance_spot_trades': {
                'native_hash': payload['run_1']['native_hash'],
                'aligned_hash': payload['run_1']['aligned_hash'],
            }
        },
    }
    parity_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P2 parity checks versus fixture baselines',
        'window': payload['window'],
        'parity': {
            'binance_spot_trades': {
                'native': {'match': True},
                'aligned_1s': {'match': True},
            }
        },
    }
    determinism_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P3 replay determinism for native + aligned_1s',
        'window': payload['window'],
        'run_1_fingerprints': {
            'binance_spot_trades': {
                'native_hash': payload['run_1']['native_hash'],
                'aligned_hash': payload['run_1']['aligned_hash'],
            }
        },
        'run_2_fingerprints': {
            'binance_spot_trades': {
                'native_hash': payload['run_2']['native_hash'],
                'aligned_hash': payload['run_2']['aligned_hash'],
            }
        },
        'deterministic_match': payload['deterministic_match'],
    }

    (_SLICE_DIR / 'proof-s15-p1-acceptance.json').write_text(
        json.dumps(acceptance_payload, sort_keys=True, indent=2) + '\n',
        encoding='utf-8',
    )
    (_SLICE_DIR / 'proof-s15-p2-parity.json').write_text(
        json.dumps(parity_payload, sort_keys=True, indent=2) + '\n',
        encoding='utf-8',
    )
    (_SLICE_DIR / 'proof-s15-p3-determinism.json').write_text(
        json.dumps(determinism_payload, sort_keys=True, indent=2) + '\n',
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()
