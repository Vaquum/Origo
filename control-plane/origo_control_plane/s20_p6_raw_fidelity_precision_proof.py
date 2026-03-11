from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.s20_bitcoin_proof_common import (
    BITCOIN_NATIVE_DATASETS,
    INGESTED_AT_UTC,
    build_clickhouse_client,
    build_fixture_canonical_events,
    build_fixture_rows_by_dataset,
)
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    canonical_json_string,
    write_bitcoin_events_to_canonical,
)

_PROOF_DB_SUFFIX = '_s20_p6_proof'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _payload_raw_for_event(event: BitcoinCanonicalEvent) -> bytes:
    return canonical_json_string(event.payload).encode('utf-8')


def _payload_raw_to_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode('utf-8')
    raise RuntimeError(f'{label} must be bytes-compatible')


def _invalid_precision_payload_raw(*, event: BitcoinCanonicalEvent) -> bytes:
    payload = dict(event.payload)
    if event.stream_id == 'bitcoin_block_headers':
        payload['difficulty'] = '1.1234567890123456789'
    elif event.stream_id == 'bitcoin_block_transactions':
        payload['block_height'] = '840000.1'
    elif event.stream_id == 'bitcoin_mempool_state':
        payload['fee_rate_sat_vb'] = '12.1234567890123456789'
    elif event.stream_id == 'bitcoin_block_fee_totals':
        payload['fee_total_btc'] = '0.1234567890123456789'
    elif event.stream_id == 'bitcoin_block_subsidy_schedule':
        payload['subsidy_btc'] = '3.1234567890123456789'
    elif event.stream_id == 'bitcoin_network_hashrate_estimate':
        payload['hashrate_hs'] = '100.1234567890123456789'
    elif event.stream_id == 'bitcoin_circulating_supply':
        payload['circulating_supply_btc'] = '100.1234567890123456789'
    else:
        raise RuntimeError(f'Unsupported stream for precision proof: {event.stream_id}')

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    ).encode('utf-8')


def run_s20_p6_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    rows_by_dataset = build_fixture_rows_by_dataset()
    events = build_fixture_canonical_events(rows_by_dataset=rows_by_dataset)
    events_by_identity = {
        (event.stream_id, event.partition_id, event.source_offset_or_equivalent): event
        for event in events
    }

    admin_client: ClickHouseClient = build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_bitcoin_events_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s20-p6-proof',
            ingested_at_utc=INGESTED_AT_UTC,
        )
        expected_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_summary:
            raise RuntimeError(
                'S20-P6 expected all rows inserted once; '
                f'observed={write_summary} expected={expected_summary}'
            )

        stored_rows = admin_client.execute(
            f'''
            SELECT
                stream_id,
                partition_id,
                source_offset_or_equivalent,
                payload_raw,
                payload_sha256_raw,
                payload_json
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'bitcoin_core'
            ORDER BY stream_id, partition_id, source_offset_or_equivalent
            '''
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in stored_rows:
            stream_id = str(row[0])
            partition_id = str(row[1])
            source_offset = str(row[2])
            identity = (stream_id, partition_id, source_offset)
            fixture_event = events_by_identity.get(identity)
            if fixture_event is None:
                raise RuntimeError(f'S20-P6 unknown event identity in canonical log: {identity}')

            expected_payload_raw = _payload_raw_for_event(fixture_event)
            observed_payload_raw = _payload_raw_to_bytes(
                row[3],
                label='canonical_event_log.payload_raw',
            )
            observed_payload_sha256 = str(row[4])
            observed_payload_json = str(row[5])

            expected_payload_sha256 = hashlib.sha256(expected_payload_raw).hexdigest()
            if observed_payload_raw != expected_payload_raw:
                raise RuntimeError(
                    f'S20-P6 payload_raw mismatch for identity={identity}'
                )
            if observed_payload_sha256 != expected_payload_sha256:
                raise RuntimeError(
                    'S20-P6 payload_sha256_raw mismatch '
                    f'for identity={identity}'
                )

            canonicalized_json = canonicalize_payload_json_with_precision(
                source_id='bitcoin_core',
                stream_id=stream_id,
                payload_raw=expected_payload_raw,
                payload_encoding='utf-8',
            )
            if observed_payload_json != canonicalized_json:
                raise RuntimeError(
                    'S20-P6 payload_json precision-canonical mismatch '
                    f'for identity={identity}'
                )

            fidelity_rows.append(
                {
                    'stream_id': stream_id,
                    'partition_id': partition_id,
                    'source_offset_or_equivalent': source_offset,
                    'payload_sha256_raw': observed_payload_sha256,
                    'payload_json_sha256': hashlib.sha256(
                        observed_payload_json.encode('utf-8')
                    ).hexdigest(),
                    'raw_fidelity_match': True,
                    'precision_roundtrip_match': True,
                }
            )

        invalid_precision_errors: dict[str, str] = {}
        for stream_id in BITCOIN_NATIVE_DATASETS:
            first_stream_event = next(
                event for event in events if event.stream_id == stream_id
            )
            invalid_payload = _invalid_precision_payload_raw(event=first_stream_event)
            try:
                canonicalize_payload_json_with_precision(
                    source_id='bitcoin_core',
                    stream_id=stream_id,
                    payload_raw=invalid_payload,
                    payload_encoding='utf-8',
                )
            except RuntimeError as exc:
                invalid_precision_errors[stream_id] = str(exc)
            else:
                raise RuntimeError(
                    'S20-P6 expected precision guardrail rejection for stream='
                    f'{stream_id}'
                )

        return {
            'proof_scope': (
                'Slice 20 S20-P6 raw fidelity and precision proof '
                'for Bitcoin canonical ingest across all seven datasets'
            ),
            'proof_database': proof_database,
            'write_summary': write_summary,
            'fidelity_rows': fidelity_rows,
            'invalid_precision_guardrail_errors': invalid_precision_errors,
            'raw_fidelity_and_precision_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s20_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
