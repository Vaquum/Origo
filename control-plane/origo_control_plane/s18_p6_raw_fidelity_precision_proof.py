from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.okx_canonical_event_ingest import (
    parse_okx_spot_trade_csv,
    write_okx_spot_trades_to_canonical,
)

_PROOF_DB_SUFFIX = '_s18_p6_proof'
_SLICE_DIR = Path('spec/slices/slice-18-okx-event-sourcing-port')


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_csv_bytes() -> bytes:
    return (
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,6001,buy,42800.12345678,0.01000000,1704326400100\n'
        b'BTC-USDT,6002,sell,42800.22345678,0.02000000,1704326400600\n'
    )


def _payload_raw_for_event(*, event_row: dict[str, Any]) -> bytes:
    payload = {
        'instrument_name': event_row['instrument_name'],
        'trade_id': event_row['trade_id'],
        'side': event_row['side'],
        'price': event_row['price'],
        'size': event_row['size'],
        'timestamp': event_row['timestamp'],
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


def run_s18_p6_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    events = parse_okx_spot_trade_csv(_fixture_csv_bytes())
    fixture_rows: list[dict[str, Any]] = [
        {
            'instrument_name': event.instrument_name,
            'trade_id': event.trade_id,
            'side': event.side,
            'price': event.price_text,
            'size': event.size_text,
            'timestamp': event.timestamp,
        }
        for event in events
    ]
    fixture_by_offset = {str(row['trade_id']): row for row in fixture_rows}

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_okx_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s18-p6-proof',
            ingested_at_utc=datetime(2026, 3, 10, 22, 30, 0, tzinfo=UTC),
        )
        expected_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_summary:
            raise RuntimeError(
                'S18-P6 expected all rows inserted once, '
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
            WHERE source_id = 'okx'
                AND stream_id = 'okx_spot_trades'
            ORDER BY toInt64(source_offset_or_equivalent) ASC
            '''
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in stored_rows:
            offset = str(row[0])
            fixture_row = fixture_by_offset.get(offset)
            if fixture_row is None:
                raise RuntimeError(f'S18-P6 unknown offset in canonical log: {offset}')

            expected_payload_raw = _payload_raw_for_event(event_row=fixture_row)
            observed_payload_raw = _payload_raw_to_bytes(
                row[1],
                label='canonical_event_log.payload_raw',
            )
            observed_payload_sha256 = str(row[2])
            observed_payload_json = str(row[3])

            expected_payload_sha256 = hashlib.sha256(expected_payload_raw).hexdigest()
            if observed_payload_raw != expected_payload_raw:
                raise RuntimeError(f'S18-P6 payload_raw mismatch for offset={offset}')
            if observed_payload_sha256 != expected_payload_sha256:
                raise RuntimeError(
                    f'S18-P6 payload_sha256_raw mismatch for offset={offset}'
                )

            canonicalized_json = canonicalize_payload_json_with_precision(
                source_id='okx',
                stream_id='okx_spot_trades',
                payload_raw=expected_payload_raw,
                payload_encoding='utf-8',
            )
            if observed_payload_json != canonicalized_json:
                raise RuntimeError(
                    f'S18-P6 payload_json precision-canonical mismatch for offset={offset}'
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
            b'{"instrument_name":"BTC-USDT","trade_id":"9999","side":"buy",'
            b'"price":"42800.123456789","size":"0.01000000","timestamp":"1704326400100"}'
        )
        try:
            canonicalize_payload_json_with_precision(
                source_id='okx',
                stream_id='okx_spot_trades',
                payload_raw=invalid_payload,
                payload_encoding='utf-8',
            )
        except RuntimeError as exc:
            invalid_precision_error = str(exc)
        else:
            raise RuntimeError(
                'S18-P6 expected precision guardrail rejection for scale overflow'
            )

        return {
            'proof_scope': (
                'Slice 18 S18-P6 raw fidelity and precision proof for OKX canonical ingest'
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
    payload = run_s18_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s18-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
