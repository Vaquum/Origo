from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.bitcoin_core import (
    format_bitcoin_height_range_partition_id_or_raise,
)
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    write_bitcoin_events_to_canonical,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_subsidy_schedule_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_headers_native,
)

_PROOF_DB_SUFFIX = '_s20_g1_proof'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _set_proof_runtime_env(*, proof_database: str) -> dict[str, str | None]:
    quarantine_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s20-g1-quarantine.json'
    )
    runtime_audit_path = (
        Path.cwd() / 'storage' / 'audit' / f'{proof_database}-s20-g1-runtime-audit.log'
    )
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    previous: dict[str, str | None] = {
        'ORIGO_AUDIT_LOG_RETENTION_DAYS': os.environ.get('ORIGO_AUDIT_LOG_RETENTION_DAYS'),
        'ORIGO_STREAM_QUARANTINE_STATE_PATH': os.environ.get('ORIGO_STREAM_QUARANTINE_STATE_PATH'),
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH': os.environ.get(
            'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'
        ),
    }
    os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
    os.environ['ORIGO_STREAM_QUARANTINE_STATE_PATH'] = str(quarantine_path)
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'] = str(runtime_audit_path)
    return previous


def _restore_proof_runtime_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _bad_header_events() -> list[BitcoinCanonicalEvent]:
    event_time_0 = datetime(2024, 4, 20, 0, 0, 0, tzinfo=UTC)
    event_time_1 = datetime(2024, 4, 20, 0, 0, 1, tzinfo=UTC)
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=840000,
        end_height=840002,
    )
    return [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_headers',
            partition_id=partition_id,
            source_offset_or_equivalent='840000',
            source_event_time_utc=event_time_0,
            payload={
                'height': 840000,
                'block_hash': f'{100:064x}',
                'prev_hash': f'{99:064x}',
                'merkle_root': f'{200:064x}',
                'version': 805306368,
                'nonce': 1234567,
                'difficulty': '86000000000000.123456000000000000',
                'timestamp_ms': int(event_time_0.timestamp() * 1000),
                'source_chain': 'main',
            },
        ),
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_headers',
            partition_id=partition_id,
            source_offset_or_equivalent='840002',
            source_event_time_utc=event_time_1,
            payload={
                'height': 840002,
                'block_hash': f'{102:064x}',
                'prev_hash': f'{100:064x}',
                'merkle_root': f'{202:064x}',
                'version': 805306368,
                'nonce': 1234568,
                'difficulty': '86000010000000.223456000000000000',
                'timestamp_ms': int(event_time_1.timestamp() * 1000),
                'source_chain': 'main',
            },
        ),
    ]


def _run_projection_linkage_guardrail(
    *,
    admin_client: ClickHouseClient,
    proof_database: str,
) -> dict[str, Any]:
    events = _bad_header_events()
    write_summary = write_bitcoin_events_to_canonical(
        client=admin_client,
        database=proof_database,
        events=events,
        run_id='s20-g1-linkage-write',
        ingested_at_utc=datetime(2026, 3, 10, 23, 55, 0, tzinfo=UTC),
    )
    if write_summary['rows_processed'] != len(events):
        raise RuntimeError('S20-G1 expected all bad-header events to be written')

    projection_error: str | None = None
    try:
        project_bitcoin_block_headers_native(
            client=admin_client,
            database=proof_database,
            partition_ids={
                format_bitcoin_height_range_partition_id_or_raise(
                    start_height=840000,
                    end_height=840002,
                )
            },
            run_id='s20-g1-linkage-project',
            projected_at_utc=datetime(2026, 3, 10, 23, 55, 5, tzinfo=UTC),
        )
    except ValueError as exc:
        projection_error = str(exc)

    if projection_error is None:
        raise RuntimeError(
            'S20-G1 projection linkage proof expected failure but got success'
        )
    if 'linkage check failed' not in projection_error:
        raise RuntimeError(
            'S20-G1 projection linkage proof expected linkage-check failure message, '
            f'got={projection_error}'
        )

    projected_rows_raw = admin_client.execute(
        f'SELECT count() FROM {proof_database}.canonical_bitcoin_block_headers_native_v1'
    )
    projected_rows = int(projected_rows_raw[0][0])
    if projected_rows != 0:
        raise RuntimeError(
            'S20-G1 expected zero projected rows after linkage failure, '
            f'observed={projected_rows}'
        )

    return {
        'write_summary': write_summary,
        'error': projection_error,
        'projected_rows_after_failure': projected_rows,
    }


def _run_derived_formula_guardrail() -> dict[str, Any]:
    bad_rows = [
        {
            'block_height': 840000,
            'block_hash': f'{100:064x}',
            'block_timestamp_ms': int(datetime(2024, 4, 20, 0, 0, 0, tzinfo=UTC).timestamp() * 1000),
            'halving_interval': 4,
            'subsidy_sats': 312500001,
            'subsidy_btc': 3.12500001,
        },
        {
            'block_height': 840001,
            'block_hash': f'{101:064x}',
            'block_timestamp_ms': int(datetime(2024, 4, 20, 0, 0, 1, tzinfo=UTC).timestamp() * 1000),
            'halving_interval': 4,
            'subsidy_sats': 312500000,
            'subsidy_btc': 3.125,
        },
    ]

    formula_error: str | None = None
    try:
        run_bitcoin_subsidy_schedule_integrity(rows=bad_rows)
    except ValueError as exc:
        formula_error = str(exc)

    if formula_error is None:
        raise RuntimeError(
            'S20-G1 derived formula proof expected subsidy formula failure but got success'
        )
    if 'formula check failed' not in formula_error:
        raise RuntimeError(
            'S20-G1 derived formula proof expected formula-check failure message, '
            f'got={formula_error}'
        )

    return {
        'rows_checked': len(bad_rows),
        'error': formula_error,
    }


def run_s20_g1_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    runner = MigrationRunner(settings=proof_settings)
    admin_client = _build_clickhouse_client(base_settings)
    previous_env = _set_proof_runtime_env(proof_database=proof_database)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        linkage_guardrail = _run_projection_linkage_guardrail(
            admin_client=admin_client,
            proof_database=proof_database,
        )
        derived_formula_guardrail = _run_derived_formula_guardrail()
        return {
            'proof_scope': (
                'Slice 20 S20-G1 stream-linkage and derived-formula integrity suite '
                'enforcement in Bitcoin event/projection paths'
            ),
            'proof_database': proof_database,
            'linkage_guardrail': linkage_guardrail,
            'derived_formula_guardrail': derived_formula_guardrail,
            'guardrail_verified': True,
        }
    finally:
        _restore_proof_runtime_env(previous_env)
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s20_g1_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-g1-bitcoin-integrity.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
