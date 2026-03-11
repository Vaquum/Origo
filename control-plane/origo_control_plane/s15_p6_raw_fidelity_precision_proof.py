from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

Dataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']

_PROOF_DB_SUFFIX = '_s15_p6_proof'
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
    partition_id = '2024-01-06'
    return (
        StreamFixture(
            dataset='spot_trades',
            stream_id='spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='11101',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{\n'
                        b'  "trade_id":"11101",\n'
                        b'  "price":"42300.12345678",\n'
                        b'  "qty":"0.01000000",\n'
                        b'  "quote_qty":"423.00123457",\n'
                        b'  "is_buyer_maker":false,\n'
                        b'  "is_best_match":true\n'
                        b'}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='11102',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 600000, tzinfo=UTC),
                    payload_raw=(
                        b'{"is_best_match":true,"is_buyer_maker":true,'
                        b'"trade_id":11102,"price":"42300.22345678",'
                        b'"qty":"0.02000000","quote_qty":"846.00446914"}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='spot_agg_trades',
            stream_id='spot_agg_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='11201',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 200000, tzinfo=UTC),
                    payload_raw=(
                        b'{ "agg_trade_id":"11201", "price":"42300.05000000", '
                        b'"qty":"0.10000000", "first_trade_id":"11200", '
                        b'"last_trade_id":"11201", "is_buyer_maker":false }'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='11202',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 700000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":11202,"price":"42300.25000000",'
                        b'"qty":"0.20000000","first_trade_id":11202,'
                        b'"last_trade_id":11205,"is_buyer_maker":true}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='futures_trades',
            stream_id='futures_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='11301',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 300000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"11301","price":"42310.10000000",'
                        b'"qty":"0.21000000","quote_qty":"8885.12100000",'
                        b'"is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='11302',
                    source_event_time_utc=datetime(2024, 1, 6, 0, 0, 0, 800000, tzinfo=UTC),
                    payload_raw=(
                        b'{"is_buyer_maker":true,"trade_id":11302,'
                        b'"price":"42310.20000000","qty":"0.18000000",'
                        b'"quote_qty":"7615.83600000"}'
                    ),
                ),
            ),
        ),
    )


def _require_dict(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def _require_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be string, got {type(value).__name__}')
    return value


def _assert_decimal_scale_8(value: str, *, label: str) -> None:
    if '.' not in value:
        raise RuntimeError(f'{label} must contain decimal point')
    fraction = value.split('.')[-1]
    if len(fraction) != 8:
        raise RuntimeError(f'{label} must use scale=8, got value={value!r}')


def _payload_raw_to_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode('utf-8')
    raise RuntimeError(f'{label} must be bytes-compatible')


def _write_fixture_events(
    *,
    writer: CanonicalEventWriter,
    fixture: StreamFixture,
) -> list[str]:
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
    return statuses


def _verify_payload_precision(
    *,
    dataset: Dataset,
    payload_object: dict[str, Any],
) -> dict[str, Any]:
    if dataset == 'spot_trades':
        trade_id = _require_int(payload_object.get('trade_id'), label='trade_id')
        price = _require_string(payload_object.get('price'), label='price')
        qty = _require_string(payload_object.get('qty'), label='qty')
        quote_qty = _require_string(payload_object.get('quote_qty'), label='quote_qty')
        _assert_decimal_scale_8(price, label='price')
        _assert_decimal_scale_8(qty, label='qty')
        _assert_decimal_scale_8(quote_qty, label='quote_qty')
        return {
            'trade_id': trade_id,
            'price': price,
            'qty': qty,
            'quote_qty': quote_qty,
        }

    if dataset == 'spot_agg_trades':
        agg_trade_id = _require_int(payload_object.get('agg_trade_id'), label='agg_trade_id')
        first_trade_id = _require_int(payload_object.get('first_trade_id'), label='first_trade_id')
        last_trade_id = _require_int(payload_object.get('last_trade_id'), label='last_trade_id')
        price = _require_string(payload_object.get('price'), label='price')
        qty = _require_string(payload_object.get('qty'), label='qty')
        _assert_decimal_scale_8(price, label='price')
        _assert_decimal_scale_8(qty, label='qty')
        return {
            'agg_trade_id': agg_trade_id,
            'first_trade_id': first_trade_id,
            'last_trade_id': last_trade_id,
            'price': price,
            'qty': qty,
        }

    trade_id = _require_int(payload_object.get('trade_id'), label='trade_id')
    price = _require_string(payload_object.get('price'), label='price')
    qty = _require_string(payload_object.get('qty'), label='qty')
    quote_qty = _require_string(payload_object.get('quote_qty'), label='quote_qty')
    _assert_decimal_scale_8(price, label='price')
    _assert_decimal_scale_8(qty, label='qty')
    _assert_decimal_scale_8(quote_qty, label='quote_qty')
    return {
        'trade_id': trade_id,
        'price': price,
        'qty': qty,
        'quote_qty': quote_qty,
    }


def run_s15_p6_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    fixtures = _fixtures()

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        results: dict[str, Any] = {}
        for fixture in fixtures:
            writer = CanonicalEventWriter(client=admin_client, database=proof_database)
            statuses = _write_fixture_events(writer=writer, fixture=fixture)
            expected_statuses = ['inserted'] * len(fixture.events)
            if statuses != expected_statuses:
                raise RuntimeError(
                    f'S15-P6 expected inserted statuses for dataset={fixture.dataset}, '
                    f'observed={statuses}'
                )

            fixture_by_offset = {
                event.source_offset_or_equivalent: event for event in fixture.events
            }
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
                    'stream_id': fixture.stream_id,
                    'partition_id': fixture.partition_id,
                },
            )

            fidelity_rows: list[dict[str, Any]] = []
            for row in rows:
                source_offset = str(row[0])
                fixture_event = fixture_by_offset.get(source_offset)
                if fixture_event is None:
                    raise RuntimeError(
                        f'S15-P6 unknown source offset in canonical log: {source_offset}'
                    )

                stored_payload_raw = _payload_raw_to_bytes(
                    row[2],
                    label='canonical_event_log.payload_raw',
                )
                stored_payload_sha256_raw = str(row[3])
                stored_payload_json = str(row[4])

                expected_payload_sha256_raw = hashlib.sha256(
                    fixture_event.payload_raw
                ).hexdigest()
                if stored_payload_raw != fixture_event.payload_raw:
                    raise RuntimeError(
                        f'S15-P6 raw fidelity mismatch dataset={fixture.dataset} '
                        f'offset={source_offset}'
                    )
                if stored_payload_sha256_raw != expected_payload_sha256_raw:
                    raise RuntimeError(
                        f'S15-P6 payload_sha256_raw mismatch dataset={fixture.dataset} '
                        f'offset={source_offset}'
                    )

                canonicalized_from_raw = canonicalize_payload_json_with_precision(
                    source_id='binance',
                    stream_id=fixture.stream_id,
                    payload_raw=fixture_event.payload_raw,
                    payload_encoding='utf-8',
                )
                if stored_payload_json != canonicalized_from_raw:
                    raise RuntimeError(
                        f'S15-P6 precision round-trip mismatch dataset={fixture.dataset} '
                        f'offset={source_offset}'
                    )

                payload_object = _require_dict(
                    json.loads(stored_payload_json),
                    label='stored payload_json',
                )
                precision_fields = _verify_payload_precision(
                    dataset=fixture.dataset,
                    payload_object=payload_object,
                )

                fidelity_rows.append(
                    {
                        'source_offset_or_equivalent': source_offset,
                        'event_id': str(row[1]),
                        'payload_sha256_raw': stored_payload_sha256_raw,
                        'payload_json_sha256': hashlib.sha256(
                            stored_payload_json.encode('utf-8')
                        ).hexdigest(),
                        'precision_fields': precision_fields,
                    }
                )

            results[fixture.dataset] = {
                'fixture_event_count': len(fixture.events),
                'write_statuses': statuses,
                'fidelity_rows': fidelity_rows,
                'raw_fidelity_verified': True,
                'precision_round_trip_verified': True,
            }

        return {
            'proof_scope': (
                'Slice 15 S15-P6 raw-fidelity and numeric-precision proof for '
                'Binance spot_trades, spot_agg_trades, futures_trades'
            ),
            'proof_database': proof_database,
            'datasets': results,
            'raw_fidelity_verified': True,
            'precision_round_trip_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s15_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s15-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
