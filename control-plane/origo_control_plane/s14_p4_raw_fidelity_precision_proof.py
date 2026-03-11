from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_p4_proof'


@dataclass(frozen=True)
class _FixtureEvent:
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


def _fixture_events() -> tuple[_FixtureEvent, ...]:
    return (
        _FixtureEvent(
            source_offset_or_equivalent='2001',
            source_event_time_utc=datetime(2024, 1, 2, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{\n'
                b'  "qty":"0.01000000",\n'
                b'  "price":"43123.12345678",\n'
                b'  "quote_qty":"431.23123457",\n'
                b'  "trade_id":"2001",\n'
                b'  "is_buyer_maker":false,\n'
                b'  "is_best_match":true\n'
                b'}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='2002',
            source_event_time_utc=datetime(2024, 1, 2, 0, 0, 0, 600000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"trade_id":2002,"price":"43123.22345678",'
                b'"qty":"0.02000000","quote_qty":"862.46446914"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='2003',
            source_event_time_utc=datetime(2024, 1, 2, 0, 0, 1, 200000, tzinfo=UTC),
            payload_raw=(
                b'{ "is_best_match": true, "is_buyer_maker": false,'
                b'"trade_id":"2003", "price":"43123.32345678",'
                b'"qty":"0.03000000", "quote_qty":"1293.69970370" }'
            ),
        ),
    )


def _require_payload_object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must decode to object')
    return cast(dict[str, Any], value)


def _require_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be string, got {type(value).__name__}')
    return value


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def _assert_decimal_scale_8(value: str, *, label: str) -> None:
    if '.' not in value:
        raise RuntimeError(f'{label} must contain decimal point')
    fraction = value.split('.')[-1]
    if len(fraction) != 8:
        raise RuntimeError(
            f'{label} must keep scale=8, got value={value!r} scale={len(fraction)}'
        )


def _payload_raw_to_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode('utf-8')
    raise RuntimeError(
        f'{label} must be bytes-compatible payload value, got {type(value).__name__}'
    )


def run_s14_p4_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    fixture_events = _fixture_events()
    fixture_by_offset = {
        event.source_offset_or_equivalent: event for event in fixture_events
    }

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        writer = CanonicalEventWriter(client=admin_client, database=proof_database)
        statuses: list[str] = []
        for event in fixture_events:
            result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id='binance',
                    stream_id='spot_trades',
                    partition_id='btcusdt',
                    source_offset_or_equivalent=event.source_offset_or_equivalent,
                    source_event_time_utc=event.source_event_time_utc,
                    ingested_at_utc=event.source_event_time_utc,
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=event.payload_raw,
                )
            )
            statuses.append(result.status)
        if statuses != ['inserted', 'inserted', 'inserted']:
            raise RuntimeError(
                'S14-P4 proof expected all fixture writes to be inserted, '
                f'got statuses={statuses}'
            )

        rows = admin_client.execute(
            f'''
            SELECT
                source_offset_or_equivalent,
                event_id,
                payload_raw,
                payload_sha256_raw,
                payload_json
            FROM {proof_database}.canonical_event_log
            WHERE source_id = %(source_id)s
                AND stream_id = %(stream_id)s
                AND partition_id = %(partition_id)s
            ORDER BY toInt64(source_offset_or_equivalent) ASC
            ''',
            {
                'source_id': 'binance',
                'stream_id': 'spot_trades',
                'partition_id': 'btcusdt',
            },
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in rows:
            source_offset = str(row[0])
            fixture_event = fixture_by_offset.get(source_offset)
            if fixture_event is None:
                raise RuntimeError(
                    f'S14-P4 proof encountered unknown source offset={source_offset}'
                )

            stored_payload_raw = _payload_raw_to_bytes(
                row[2],
                label='canonical_event_log.payload_raw',
            )
            stored_sha256_raw = str(row[3])
            stored_payload_json = str(row[4])

            expected_sha256_raw = hashlib.sha256(fixture_event.payload_raw).hexdigest()
            if stored_payload_raw != fixture_event.payload_raw:
                raise RuntimeError(
                    f'S14-P4 raw fidelity failure for source offset={source_offset}'
                )
            if stored_sha256_raw != expected_sha256_raw:
                raise RuntimeError(
                    'S14-P4 payload_sha256_raw mismatch for source '
                    f'offset={source_offset}'
                )

            canonicalized_from_raw = canonicalize_payload_json_with_precision(
                source_id='binance',
                stream_id='spot_trades',
                payload_raw=fixture_event.payload_raw,
                payload_encoding='utf-8',
            )
            if stored_payload_json != canonicalized_from_raw:
                raise RuntimeError(
                    'S14-P4 precision round-trip mismatch for source '
                    f'offset={source_offset}'
                )

            payload_object = _require_payload_object(
                json.loads(stored_payload_json),
                label='stored payload_json',
            )
            trade_id = _require_int(payload_object.get('trade_id'), label='trade_id')
            price = _require_string(payload_object.get('price'), label='price')
            qty = _require_string(payload_object.get('qty'), label='qty')
            quote_qty = _require_string(
                payload_object.get('quote_qty'),
                label='quote_qty',
            )
            _assert_decimal_scale_8(price, label='price')
            _assert_decimal_scale_8(qty, label='qty')
            _assert_decimal_scale_8(quote_qty, label='quote_qty')

            fidelity_rows.append(
                {
                    'source_offset_or_equivalent': source_offset,
                    'event_id': str(row[1]),
                    'payload_sha256_raw': stored_sha256_raw,
                    'payload_json_sha256': hashlib.sha256(
                        stored_payload_json.encode('utf-8')
                    ).hexdigest(),
                    'trade_id': trade_id,
                    'price': price,
                    'qty': qty,
                    'quote_qty': quote_qty,
                }
            )

        return {
            'proof_scope': (
                'Slice 14 S14-P4 raw-fidelity and numeric-precision round-trip '
                'proof on fixed fixtures'
            ),
            'proof_database': proof_database,
            'fixture_event_count': len(fixture_events),
            'write_statuses': statuses,
            'fidelity_rows': fidelity_rows,
            'raw_fidelity_verified': True,
            'precision_round_trip_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_p4_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p4-raw-fidelity-precision.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
