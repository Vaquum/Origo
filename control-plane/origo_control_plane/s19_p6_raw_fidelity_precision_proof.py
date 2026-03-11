from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s19_p6_proof'
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


def _fixture_csv_bytes() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1704326400.1000,BTCUSDT,Buy,0.010,42800.12345678,PlusTick,m-6001,4.280012345678e+11,0.010,428.0012345678\n'
        b'1704326400.6000,BTCUSDT,Sell,0.020,42800.22345678,MinusTick,m-6002,8.560044691356e+11,0.020,856.0044691356\n'
    )


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _payload_raw_for_event(*, event_row: dict[str, Any]) -> bytes:
    payload = {
        'symbol': event_row['symbol'],
        'trade_id': event_row['trade_id'],
        'trd_match_id': event_row['trd_match_id'],
        'side': event_row['side'],
        'price': event_row['price'],
        'size': event_row['size'],
        'quote_quantity': event_row['quote_quantity'],
        'timestamp': event_row['timestamp'],
        'tick_direction': event_row['tick_direction'],
        'gross_value': event_row['gross_value'],
        'home_notional': event_row['home_notional'],
        'foreign_notional': event_row['foreign_notional'],
    }
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    ).encode('utf-8')


def _payload_raw_to_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode('utf-8')
    raise RuntimeError(f'{label} must be bytes-compatible')


def run_s19_p6_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(_FIXTURE_DATE)
    events = parse_bybit_spot_trade_csv(
        csv_content=_fixture_csv_bytes(),
        date_str=_FIXTURE_DATE,
        day_start_ts_utc_ms=day_start_ts_utc_ms,
        day_end_ts_utc_ms=day_end_ts_utc_ms,
    )
    fixture_rows: list[dict[str, Any]] = [
        {
            'symbol': event.symbol,
            'trade_id': event.trade_id,
            'trd_match_id': event.trd_match_id,
            'side': event.side,
            'price': event.price_text,
            'size': event.size_text,
            'quote_quantity': event.quote_quantity_text,
            'timestamp': event.timestamp,
            'tick_direction': event.tick_direction,
            'gross_value': event.gross_value_text,
            'home_notional': event.home_notional_text,
            'foreign_notional': event.foreign_notional_text,
        }
        for event in events
    ]
    fixture_by_offset = {str(row['trade_id']): row for row in fixture_rows}

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s19-p6-proof',
            ingested_at_utc=datetime(2026, 3, 10, 22, 30, 0, tzinfo=UTC),
        )
        expected_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_summary:
            raise RuntimeError(
                'S19-P6 expected all rows inserted once, '
                f'observed={write_summary}'
            )

        stored_rows = admin_client.execute(
            f'''
            SELECT
                source_offset_or_equivalent,
                payload_raw,
                payload_sha256_raw,
                payload_json
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'bybit'
                AND stream_id = 'bybit_spot_trades'
            ORDER BY toInt64(source_offset_or_equivalent) ASC
            '''
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in stored_rows:
            offset = str(row[0])
            fixture_row = fixture_by_offset.get(offset)
            if fixture_row is None:
                raise RuntimeError(f'S19-P6 unknown offset in canonical log: {offset}')

            expected_payload_raw = _payload_raw_for_event(event_row=fixture_row)
            observed_payload_raw = _payload_raw_to_bytes(
                row[1],
                label='canonical_event_log.payload_raw',
            )
            observed_payload_sha256 = str(row[2])
            observed_payload_json = str(row[3])

            expected_payload_sha256 = hashlib.sha256(expected_payload_raw).hexdigest()
            if observed_payload_raw != expected_payload_raw:
                raise RuntimeError(f'S19-P6 payload_raw mismatch for offset={offset}')
            if observed_payload_sha256 != expected_payload_sha256:
                raise RuntimeError(
                    f'S19-P6 payload_sha256_raw mismatch for offset={offset}'
                )

            canonicalized_json = canonicalize_payload_json_with_precision(
                source_id='bybit',
                stream_id='bybit_spot_trades',
                payload_raw=expected_payload_raw,
                payload_encoding='utf-8',
            )
            if observed_payload_json != canonicalized_json:
                raise RuntimeError(
                    f'S19-P6 payload_json precision-canonical mismatch for offset={offset}'
                )

            fidelity_rows.append(
                {
                    'source_offset_or_equivalent': offset,
                    'payload_sha256_raw': observed_payload_sha256,
                    'payload_json_sha256': hashlib.sha256(
                        observed_payload_json.encode('utf-8')
                    ).hexdigest(),
                    'raw_fidelity_match': True,
                    'precision_roundtrip_match': True,
                }
            )

        invalid_precision_error: str
        invalid_payload = (
            b'{"symbol":"BTCUSDT","trade_id":"9999","trd_match_id":"m-9999",'
            b'"side":"buy","price":"42800.1234567891234567891","size":"0.01000000",'
            b'"quote_quantity":"428.0012345678","timestamp":"1704326400100",'
            b'"tick_direction":"PlusTick","gross_value":"4.280012345678e+11",'
            b'"home_notional":"0.010","foreign_notional":"428.0012345678"}'
        )
        try:
            canonicalize_payload_json_with_precision(
                source_id='bybit',
                stream_id='bybit_spot_trades',
                payload_raw=invalid_payload,
                payload_encoding='utf-8',
            )
        except RuntimeError as exc:
            invalid_precision_error = str(exc)
        else:
            raise RuntimeError(
                'S19-P6 expected precision guardrail rejection for scale overflow'
            )

        return {
            'proof_scope': (
                'Slice 19 S19-P6 raw fidelity and precision proof for Bybit canonical ingest'
            ),
            'proof_database': proof_database,
            'write_summary': write_summary,
            'fidelity_rows': fidelity_rows,
            'invalid_precision_guardrail_error': invalid_precision_error,
            'raw_fidelity_and_precision_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s19_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s19-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
