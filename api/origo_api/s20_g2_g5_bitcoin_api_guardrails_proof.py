from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import uuid4

from clickhouse_driver import Client as ClickHouseClient
from fastapi.testclient import TestClient

_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


@dataclass(frozen=True)
class _ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty for S20-G2/G5 proof')
    return value.strip()


def _expect_dict(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    payload = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in payload.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _build_clickhouse_settings_from_env() -> _ClickHouseSettings:
    port_raw = _require_env('CLICKHOUSE_PORT')
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT must be integer') from exc
    return _ClickHouseSettings(
        host=_require_env('CLICKHOUSE_HOST'),
        port=port,
        user=_require_env('CLICKHOUSE_USER'),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=_require_env('CLICKHOUSE_DATABASE'),
    )


def _build_clickhouse_client(settings: _ClickHouseSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _run_migrations(settings: _ClickHouseSettings) -> None:
    migrations_module: Any = importlib.import_module('origo_control_plane.migrations')
    migration_settings = migrations_module.MigrationSettings(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
    )
    runner = migrations_module.MigrationRunner(settings=migration_settings)
    try:
        runner.migrate()
    finally:
        runner.close()


def _seed_bitcoin_native_projection_row(*, client: ClickHouseClient, database: str) -> None:
    observed_at_utc = datetime(2024, 4, 20, 0, 0, 0, tzinfo=UTC)
    client.execute(
        f'''
        INSERT INTO {database}.canonical_bitcoin_block_fee_totals_native_v1
        (
            block_height,
            block_hash,
            block_timestamp,
            fee_total_btc,
            datetime,
            source_chain,
            event_id,
            source_offset_or_equivalent,
            source_event_time_utc,
            ingested_at_utc
        )
        VALUES
        ''',
        [
            (
                840000,
                f'{100:064x}',
                int(observed_at_utc.timestamp() * 1000),
                0.001,
                observed_at_utc,
                'main',
                uuid4(),
                '840000',
                observed_at_utc,
                datetime(2024, 4, 20, 0, 0, 5, tzinfo=UTC),
            )
        ],
    )


def _seed_bitcoin_aligned_projection_row(*, client: ClickHouseClient, database: str) -> None:
    aligned_at_utc = datetime(2024, 4, 20, 0, 0, 0, tzinfo=UTC)
    payload_json = json.dumps(
        {
            'block_height': 840000,
            'block_hash': f'{100:064x}',
            'block_timestamp_ms': int(aligned_at_utc.timestamp() * 1000),
            'fee_total_btc': '0.001000000000000000',
            'source_chain': 'main',
            'metric_name': 'block_fee_total_btc',
            'metric_unit': 'BTC',
            'metric_value': '0.001000000000000000',
            'provenance': {
                'source_id': 'bitcoin_core',
                'stream_id': 'bitcoin_block_headers',
                'source_offset_or_equivalent': '840000',
                'block_hash': f'{100:064x}',
            },
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    payload_rows_json = json.dumps(
        {
            'rows': [
                {
                    'event_id': str(uuid4()),
                    'source_offset_or_equivalent': '840000',
                    'source_event_time_utc': '2024-04-20T00:00:00+00:00',
                    'ingested_at_utc': '2024-04-20T00:00:05+00:00',
                    'payload_json': payload_json,
                    'payload_sha256_raw': hashlib.sha256(
                        payload_json.encode('utf-8')
                    ).hexdigest(),
                }
            ]
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    bucket_sha256 = hashlib.sha256(payload_rows_json.encode('utf-8')).hexdigest()

    client.execute(
        f'''
        INSERT INTO {database}.canonical_aligned_1s_aggregates
        (
            view_id,
            view_version,
            source_id,
            stream_id,
            partition_id,
            aligned_at_utc,
            bucket_event_count,
            first_event_id,
            last_event_id,
            first_source_offset_or_equivalent,
            last_source_offset_or_equivalent,
            latest_source_event_time_utc,
            latest_ingested_at_utc,
            payload_rows_json,
            bucket_sha256,
            projector_id,
            projected_at_utc
        )
        VALUES
        ''',
        [
            (
                'aligned_1s_raw',
                1,
                'bitcoin_core',
                'bitcoin_block_fee_totals',
                '2024-04-20',
                aligned_at_utc,
                1,
                uuid4(),
                uuid4(),
                '840000',
                '840000',
                aligned_at_utc,
                datetime(2024, 4, 20, 0, 0, 5, tzinfo=UTC),
                payload_rows_json,
                bucket_sha256,
                's20-g2-g5-proof-seed',
                datetime(2024, 4, 20, 0, 0, 6, tzinfo=UTC),
            )
        ],
    )


def _build_rights_matrix_hosted_provisional(*, legal_signoff_artifact: str) -> dict[str, Any]:
    return {
        'version': '2026-03-10-s20-g2-g5-proof',
        'sources': {
            'bitcoin_core': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': True,
                'datasets': [
                    'bitcoin_block_headers',
                    'bitcoin_block_transactions',
                    'bitcoin_mempool_state',
                    'bitcoin_block_fee_totals',
                    'bitcoin_block_subsidy_schedule',
                    'bitcoin_network_hashrate_estimate',
                    'bitcoin_circulating_supply',
                ],
                'legal_signoff_artifact': legal_signoff_artifact,
            }
        },
    }


def _build_rights_matrix_missing_bitcoin() -> dict[str, Any]:
    return {
        'version': '2026-03-10-s20-g2-g5-proof-missing',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['spot_trades'],
                'legal_signoff_artifact': 'contracts/legal/binance-hosted-allowed.md',
            }
        },
    }


def _post_json(
    *,
    client: TestClient,
    path: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    response = client.post(path, headers=headers, json=payload)
    status_code = response.status_code
    body = response.json()
    return status_code, _expect_dict(body, label=f'response body for path={path}')


def _extract_warning_codes(warnings_payload: Any) -> list[str]:
    if not isinstance(warnings_payload, list):
        raise RuntimeError('warnings must be list')
    warning_codes: list[str] = []
    for warning_raw in cast(list[Any], warnings_payload):
        if not isinstance(warning_raw, dict):
            continue
        warning = _expect_dict(warning_raw, label='warning')
        code = warning.get('code')
        if isinstance(code, str):
            warning_codes.append(code)
    return sorted(set(warning_codes))


def _load_main_module() -> Any:
    module_name = 'api.origo_api.main'
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def run_s20_g2_g5_proof() -> dict[str, Any]:
    base_settings = _build_clickhouse_settings_from_env()
    proof_database = f'{base_settings.database}_s20_g2_g5_proof'
    proof_settings = _ClickHouseSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    previous_clickhouse_database = os.environ.get('CLICKHOUSE_DATABASE')
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        _run_migrations(proof_settings)
        _seed_bitcoin_native_projection_row(client=admin_client, database=proof_database)
        _seed_bitcoin_aligned_projection_row(client=admin_client, database=proof_database)
        os.environ['CLICKHOUSE_DATABASE'] = proof_database

        with TemporaryDirectory(prefix='origo-s20-g2-g5-proof-') as tmp_dir:
            tmp_path = Path(tmp_dir)
            legal_signoff_path = tmp_path / 'bitcoin-legal-signoff.md'
            legal_signoff_path.write_text(
                '# Bitcoin Core Hosted Allowed Legal Sign-off (Provisional)\n',
                encoding='utf-8',
            )
            rights_hosted_path = tmp_path / 'source-rights-matrix-hosted.json'
            rights_missing_path = tmp_path / 'source-rights-matrix-missing.json'
            rights_hosted_path.write_text(
                json.dumps(
                    _build_rights_matrix_hosted_provisional(
                        legal_signoff_artifact=str(legal_signoff_path)
                    ),
                    indent=2,
                    sort_keys=True,
                )
                + '\n',
                encoding='utf-8',
            )
            rights_missing_path.write_text(
                json.dumps(
                    _build_rights_matrix_missing_bitcoin(),
                    indent=2,
                    sort_keys=True,
                )
                + '\n',
                encoding='utf-8',
            )
            export_audit_log_path = tmp_path / 'export-audit-events.jsonl'

            os.environ['ORIGO_INTERNAL_API_KEY'] = 's20-proof-key'
            os.environ['ORIGO_QUERY_MAX_CONCURRENCY'] = '4'
            os.environ['ORIGO_QUERY_MAX_QUEUE'] = '8'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY'] = '1'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_QUEUE'] = '1'
            os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'] = '2'
            os.environ['ORIGO_EXPORT_MAX_QUEUE'] = '8'
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_hosted_path)
            os.environ['ORIGO_EXPORT_AUDIT_LOG_PATH'] = str(export_audit_log_path)
            os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
            os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'promoted'
            os.environ['ORIGO_FRED_QUERY_SERVING_STATE'] = 'promoted'
            os.environ['ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS'] = '1'
            os.environ['ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS'] = '2'
            os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS'] = '1'

            main_module = _load_main_module()
            client = TestClient(main_module.app)
            headers = {'X-API-Key': 's20-proof-key'}

            native_status, native_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'native',
                    'sources': ['bitcoin_block_fee_totals'],
                    'time_range': ['2024-04-20T00:00:00Z', '2024-04-20T00:00:02Z'],
                    'strict': False,
                },
            )
            if native_status != 200:
                raise RuntimeError(
                    'S20-G2/G5 expected non-strict Bitcoin native query success, '
                    f'got status={native_status} body={native_body}'
                )

            aligned_status, aligned_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'sources': ['bitcoin_block_fee_totals'],
                    'time_range': ['2024-04-20T00:00:00Z', '2024-04-20T00:00:02Z'],
                    'strict': False,
                },
            )
            if aligned_status != 200:
                raise RuntimeError(
                    'S20-G2/G5 expected non-strict Bitcoin aligned query success, '
                    f'got status={aligned_status} body={aligned_body}'
                )

            native_rights_state = native_body.get('rights_state')
            native_rights_provisional = native_body.get('rights_provisional')
            aligned_rights_state = aligned_body.get('rights_state')
            aligned_rights_provisional = aligned_body.get('rights_provisional')
            if native_rights_state != 'Hosted Allowed' or native_rights_provisional is not True:
                raise RuntimeError(
                    'S20-G5 expected Hosted Allowed provisional rights metadata for native, '
                    f'observed rights_state={native_rights_state!r} '
                    f'rights_provisional={native_rights_provisional!r}'
                )
            if aligned_rights_state != 'Hosted Allowed' or aligned_rights_provisional is not True:
                raise RuntimeError(
                    'S20-G5 expected Hosted Allowed provisional rights metadata for aligned, '
                    f'observed rights_state={aligned_rights_state!r} '
                    f'rights_provisional={aligned_rights_provisional!r}'
                )

            warning_codes = _extract_warning_codes(aligned_body.get('warnings'))
            if 'ALIGNED_FRESHNESS_STALE' not in warning_codes:
                raise RuntimeError(
                    'S20-G2 expected ALIGNED_FRESHNESS_STALE warning in aligned query'
                )

            strict_status, strict_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'sources': ['bitcoin_block_fee_totals'],
                    'time_range': ['2024-04-20T00:00:00Z', '2024-04-20T00:00:02Z'],
                    'strict': True,
                },
            )
            strict_detail = _expect_dict(strict_body.get('detail'), label='strict detail')
            strict_code = strict_detail.get('code')
            if strict_status != 409 or strict_code != 'STRICT_MODE_WARNING_FAILURE':
                raise RuntimeError(
                    'S20-G2 expected strict failure on stale aligned warning, '
                    f'observed status={strict_status} code={strict_code!r}'
                )

            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_missing_path)
            missing_rights_status, missing_rights_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'native',
                    'sources': ['bitcoin_block_fee_totals'],
                    'time_range': ['2024-04-20T00:00:00Z', '2024-04-20T00:00:02Z'],
                    'strict': False,
                },
            )
            missing_rights_detail = _expect_dict(
                missing_rights_body.get('detail'),
                label='missing rights detail',
            )
            missing_rights_code = missing_rights_detail.get('code')
            if (
                missing_rights_status != 409
                or missing_rights_code != 'QUERY_RIGHTS_MISSING_STATE'
            ):
                raise RuntimeError(
                    'S20-G5 expected fail-closed rights rejection, '
                    f'observed status={missing_rights_status} code={missing_rights_code!r}'
                )

            return {
                'proof_scope': (
                    'Slice 20 S20-G2/S20-G5 Bitcoin freshness warning semantics and '
                    'rights/legal provisional-metadata behavior with fail-closed rights gate'
                ),
                'proof_database': proof_database,
                'native_status': native_status,
                'aligned_status': aligned_status,
                'strict_status': strict_status,
                'missing_rights_status': missing_rights_status,
                'warning_codes': warning_codes,
                'native_rights_state': native_rights_state,
                'native_rights_provisional': native_rights_provisional,
                'aligned_rights_state': aligned_rights_state,
                'aligned_rights_provisional': aligned_rights_provisional,
                'strict_error_code': strict_code,
                'missing_rights_error_code': missing_rights_code,
                'guardrails_verified': True,
            }
    finally:
        if previous_clickhouse_database is None:
            os.environ.pop('CLICKHOUSE_DATABASE', None)
        else:
            os.environ['CLICKHOUSE_DATABASE'] = previous_clickhouse_database
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s20_g2_g5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-g2-g5-api-guardrails.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
