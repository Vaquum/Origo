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

_SLICE_DIR = Path('spec/slices/slice-18-okx-event-sourcing-port')


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
        raise RuntimeError(f'{name} must be set and non-empty for S18-G2 proof')
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


def _seed_okx_native_projection_row(*, client: ClickHouseClient, database: str) -> None:
    trade_time = datetime(2024, 1, 4, 0, 0, 0, 100000, tzinfo=UTC)
    client.execute(
        f'''
        INSERT INTO {database}.canonical_okx_spot_trades_native_v1
        (
            instrument_name,
            trade_id,
            side,
            price,
            size,
            quote_quantity,
            timestamp,
            datetime,
            event_id,
            source_offset_or_equivalent,
            source_event_time_utc,
            ingested_at_utc
        )
        VALUES
        ''',
        [
            (
                'BTC-USDT',
                8001,
                'buy',
                43000.1,
                0.01,
                430.001,
                1704326400100,
                trade_time,
                uuid4(),
                '8001',
                trade_time,
                datetime(2026, 3, 10, 23, 0, 0, tzinfo=UTC),
            )
        ],
    )


def _seed_okx_aligned_projection_row(*, client: ClickHouseClient, database: str) -> None:
    payload_json = json.dumps(
        {
            'instrument_name': 'BTC-USDT',
            'trade_id': 8001,
            'side': 'buy',
            'price': '43000.10000000',
            'size': '0.01000000',
            'timestamp': 1704326400100,
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    payload_rows_json = json.dumps(
        {
            'rows': [
                {
                    'event_id': str(uuid4()),
                    'source_offset_or_equivalent': '8001',
                    'source_event_time_utc': '2024-01-04T00:00:00.100000+00:00',
                    'ingested_at_utc': '2026-03-10T23:00:00+00:00',
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
    aligned_at_utc = datetime(2024, 1, 4, 0, 0, 0, tzinfo=UTC)
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
                'okx',
                'okx_spot_trades',
                '2024-01-04',
                aligned_at_utc,
                1,
                uuid4(),
                uuid4(),
                '8001',
                '8001',
                aligned_at_utc,
                datetime(2026, 3, 10, 23, 0, 0, tzinfo=UTC),
                payload_rows_json,
                bucket_sha256,
                's18-g2-proof-seed',
                datetime(2026, 3, 10, 23, 0, 1, tzinfo=UTC),
            )
        ],
    )


def _build_rights_matrix_hosted_provisional(*, legal_signoff_artifact: str) -> dict[str, Any]:
    return {
        'version': '2026-03-10-s18-g2-proof',
        'sources': {
            'okx': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': True,
                'datasets': ['okx_spot_trades'],
                'legal_signoff_artifact': legal_signoff_artifact,
            }
        },
    }


def _build_rights_matrix_missing_okx() -> dict[str, Any]:
    return {
        'version': '2026-03-10-s18-g2-proof-missing',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['binance_spot_trades'],
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


def _load_main_module() -> Any:
    module_name = 'api.origo_api.main'
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def run_s18_g2_proof() -> dict[str, Any]:
    base_settings = _build_clickhouse_settings_from_env()
    proof_database = f'{base_settings.database}_s18_g2_proof'
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
        _seed_okx_native_projection_row(client=admin_client, database=proof_database)
        _seed_okx_aligned_projection_row(client=admin_client, database=proof_database)
        os.environ['CLICKHOUSE_DATABASE'] = proof_database

        with TemporaryDirectory(prefix='origo-s18-g2-proof-') as tmp_dir:
            tmp_path = Path(tmp_dir)
            legal_signoff_path = tmp_path / 'okx-legal-signoff.md'
            legal_signoff_path.write_text(
                '# OKX Hosted Allowed Legal Sign-off (Provisional)\n',
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
                    _build_rights_matrix_missing_okx(),
                    indent=2,
                    sort_keys=True,
                )
                + '\n',
                encoding='utf-8',
            )
            export_audit_log_path = tmp_path / 'export-audit-events.jsonl'

            os.environ['ORIGO_INTERNAL_API_KEY'] = 's18-proof-key'
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
            os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS'] = '300'

            main_module = _load_main_module()
            client = TestClient(main_module.app)
            headers = {'X-API-Key': 's18-proof-key'}

            native_status, native_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'native',
                    'sources': ['okx_spot_trades'],
                    'time_range': ['2024-01-04T00:00:00Z', '2024-01-04T00:00:02Z'],
                    'strict': False,
                },
            )
            if native_status != 200:
                raise RuntimeError(
                    'S18-G2 expected non-strict OKX native query success, '
                    f'got status={native_status} body={native_body}'
                )

            aligned_status, aligned_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'sources': ['okx_spot_trades'],
                    'time_range': ['2024-01-04T00:00:00Z', '2024-01-04T00:00:02Z'],
                    'strict': False,
                },
            )
            if aligned_status != 200:
                raise RuntimeError(
                    'S18-G2 expected non-strict OKX aligned query success, '
                    f'got status={aligned_status} body={aligned_body}'
                )

            rights_state = native_body.get('rights_state')
            rights_provisional = native_body.get('rights_provisional')
            aligned_rights_state = aligned_body.get('rights_state')
            aligned_rights_provisional = aligned_body.get('rights_provisional')
            if rights_state != 'Hosted Allowed' or rights_provisional is not True:
                raise RuntimeError(
                    'S18-G2 expected Hosted Allowed provisional rights metadata for native, '
                    f'observed rights_state={rights_state!r} '
                    f'rights_provisional={rights_provisional!r}'
                )
            if (
                aligned_rights_state != 'Hosted Allowed'
                or aligned_rights_provisional is not True
            ):
                raise RuntimeError(
                    'S18-G2 expected Hosted Allowed provisional rights metadata for aligned, '
                    f'observed rights_state={aligned_rights_state!r} '
                    f'rights_provisional={aligned_rights_provisional!r}'
                )

            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_missing_path)
            missing_rights_status, missing_rights_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'native',
                    'sources': ['okx_spot_trades'],
                    'time_range': ['2024-01-04T00:00:00Z', '2024-01-04T00:00:02Z'],
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
                    'S18-G2 expected fail-closed rights rejection, '
                    f'observed status={missing_rights_status} code={missing_rights_code!r}'
                )

            return {
                'proof_scope': (
                    'Slice 18 S18-G2 OKX rights/legal/provisional metadata '
                    'semantics with fail-closed rights behavior'
                ),
                'proof_database': proof_database,
                'native_status': native_status,
                'aligned_status': aligned_status,
                'missing_rights_status': missing_rights_status,
                'missing_rights_error_code': missing_rights_code,
                'rights_state': rights_state,
                'rights_provisional': rights_provisional,
                'aligned_rights_state': aligned_rights_state,
                'aligned_rights_provisional': aligned_rights_provisional,
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
    payload = run_s18_g2_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s18-g2-api-guardrails.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
