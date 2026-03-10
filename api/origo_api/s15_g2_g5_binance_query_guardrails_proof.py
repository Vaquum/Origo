from __future__ import annotations

import hashlib
import importlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import uuid4

from clickhouse_driver import Client as ClickHouseClient
from fastapi.testclient import TestClient

_SLICE_DIR = Path('spec/slices/slice-15-binance-event-sourcing-port')


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
        raise RuntimeError(f'{name} must be set and non-empty for S15-G2/G5 proof')
    return value


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


def _build_rights_matrix(*, legal_signoff_artifact: str) -> dict[str, Any]:
    return {
        'version': '2026-03-10-s15-g2-g5-proof',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['spot_trades', 'spot_agg_trades', 'futures_trades'],
                'legal_signoff_artifact': legal_signoff_artifact,
            }
        },
    }


def _extract_warning_codes(warnings_payload: Any) -> list[str]:
    if not isinstance(warnings_payload, list):
        raise RuntimeError('warnings must be a list')
    warning_codes: list[str] = []
    for warning_raw in cast(list[Any], warnings_payload):
        if not isinstance(warning_raw, dict):
            continue
        warning = _expect_dict(warning_raw, label='warning')
        code = warning.get('code')
        if isinstance(code, str):
            warning_codes.append(code)
    return sorted(set(warning_codes))


def _post_json(
    *,
    client: Any,
    path: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    response = client.post(path, headers=headers, json=payload)
    status_code = response.status_code
    if not isinstance(status_code, int):
        raise RuntimeError(f'status_code must be int for path={path}')
    body = response.json()
    return status_code, _expect_dict(body, label=f'response body for path={path}')


def _build_clickhouse_settings_from_env() -> _ClickHouseSettings:
    host = _require_env('CLICKHOUSE_HOST')
    port_raw = _require_env('CLICKHOUSE_PORT')
    user = _require_env('CLICKHOUSE_USER')
    password = _require_env('CLICKHOUSE_PASSWORD')
    database = _require_env('CLICKHOUSE_DATABASE')
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT must be an integer') from exc
    return _ClickHouseSettings(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
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


def _seed_minimal_aligned_binance_row(
    *,
    client: ClickHouseClient,
    database: str,
) -> None:
    payload_json = json.dumps(
        {
            'trade_id': 7001,
            'price': '42000.10000000',
            'qty': '0.01000000',
            'quote_qty': '420.00100000',
            'is_buyer_maker': False,
            'is_best_match': True,
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    payload_rows_json = json.dumps(
        {
            'rows': [
                {
                    'source_offset_or_equivalent': '7001',
                    'payload_json': payload_json,
                }
            ]
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    bucket_sha256 = hashlib.sha256(payload_rows_json.encode('utf-8')).hexdigest()
    aligned_at_utc = datetime(2017, 8, 17, 12, 0, 0, tzinfo=UTC)

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
                'binance',
                'spot_trades',
                '2017-08-17',
                aligned_at_utc,
                1,
                uuid4(),
                uuid4(),
                '7001',
                '7001',
                aligned_at_utc,
                aligned_at_utc,
                payload_rows_json,
                bucket_sha256,
                's15-g2-g5-proof-seed',
                aligned_at_utc,
            )
        ],
    )


def run_s15_g2_g5_proof() -> dict[str, Any]:
    base_settings = _build_clickhouse_settings_from_env()
    proof_database = f'{base_settings.database}_s15_g2_g5_proof'
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
        _seed_minimal_aligned_binance_row(client=admin_client, database=proof_database)
        os.environ['CLICKHOUSE_DATABASE'] = proof_database

        with TemporaryDirectory(prefix='origo-s15-g2-g5-proof-') as tmp_dir:
            tmp_path = Path(tmp_dir)
            legal_signoff_path = tmp_path / 'binance-legal-signoff.md'
            legal_signoff_path.write_text(
                '# Binance Hosted Allowed Legal Sign-off\n\nS15 guardrail proof harness.\n',
                encoding='utf-8',
            )
            rights_matrix_path = tmp_path / 'source-rights-matrix.json'
            rights_matrix_path.write_text(
                json.dumps(
                    _build_rights_matrix(
                        legal_signoff_artifact=str(legal_signoff_path)
                    ),
                    indent=2,
                    sort_keys=True,
                )
                + '\n',
                encoding='utf-8',
            )
            export_audit_log_path = tmp_path / 'export-audit-events.jsonl'

            os.environ['ORIGO_INTERNAL_API_KEY'] = 's15-proof-key'
            os.environ['ORIGO_QUERY_MAX_CONCURRENCY'] = '4'
            os.environ['ORIGO_QUERY_MAX_QUEUE'] = '8'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY'] = '1'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_QUEUE'] = '1'
            os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'] = '2'
            os.environ['ORIGO_EXPORT_MAX_QUEUE'] = '8'
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_matrix_path)
            os.environ['ORIGO_EXPORT_AUDIT_LOG_PATH'] = str(export_audit_log_path)
            os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
            os.environ['ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS'] = '2'
            os.environ['ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS'] = '2'
            os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS'] = '1'

            main_module = importlib.import_module('api.origo_api.main')
            client = TestClient(main_module.app)
            headers = {'X-API-Key': 's15-proof-key'}

            non_strict_status, non_strict_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'sources': ['spot_trades'],
                    'time_range': ['2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'],
                    'strict': False,
                },
            )
            if non_strict_status != 200:
                raise RuntimeError(
                    'S15-G2 proof expected non-strict query success, '
                    f'got status={non_strict_status} body={non_strict_body}'
                )

            warning_codes = _extract_warning_codes(non_strict_body.get('warnings'))
            if 'ALIGNED_FRESHNESS_STALE' not in warning_codes:
                raise RuntimeError(
                    'S15-G2 proof expected ALIGNED_FRESHNESS_STALE warning in non-strict query'
                )

            freshness = _expect_dict(
                non_strict_body.get('freshness'),
                label='S15-G2 freshness payload',
            )
            lag_seconds = freshness.get('lag_seconds')
            if not isinstance(lag_seconds, int) or lag_seconds <= 0:
                raise RuntimeError(
                    'S15-G2 proof expected freshness.lag_seconds > 0'
                )

            rights_state = non_strict_body.get('rights_state')
            rights_provisional = non_strict_body.get('rights_provisional')
            if rights_state != 'Hosted Allowed':
                raise RuntimeError(
                    f'S15-G5 proof expected rights_state=Hosted Allowed, got {rights_state!r}'
                )
            if rights_provisional is not False:
                raise RuntimeError(
                    'S15-G5 proof expected rights_provisional=false for binance'
                )

            strict_status, strict_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'sources': ['spot_trades'],
                    'time_range': ['2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'],
                    'strict': True,
                },
            )
            strict_detail = _expect_dict(
                strict_body.get('detail'),
                label='S15-G2 strict detail',
            )
            strict_error_code = strict_detail.get('code')
            if strict_status != 409 or strict_error_code != 'STRICT_MODE_WARNING_FAILURE':
                raise RuntimeError(
                    'S15-G2 proof expected strict query rejection with '
                    f'STRICT_MODE_WARNING_FAILURE, got status={strict_status} '
                    f'code={strict_error_code!r}'
                )

            return {
                'proof_scope': (
                    'Slice 15 S15-G2 freshness warning semantics and '
                    'S15-G5 rights metadata behavior for Binance aligned queries'
                ),
                'proof_database': proof_database,
                'generated_at_utc': datetime.now(UTC).isoformat(),
                'non_strict_query': {
                    'status_code': non_strict_status,
                    'warning_codes': warning_codes,
                    'freshness': freshness,
                    'rights_state': rights_state,
                    'rights_provisional': rights_provisional,
                },
                'strict_query': {
                    'status_code': strict_status,
                    'error_code': strict_error_code,
                },
                'guardrail_verified': True,
            }
    finally:
        if previous_clickhouse_database is None:
            os.environ.pop('CLICKHOUSE_DATABASE', None)
        else:
            os.environ['CLICKHOUSE_DATABASE'] = previous_clickhouse_database
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    _require_env('CLICKHOUSE_HOST')
    _require_env('CLICKHOUSE_PORT')
    _require_env('CLICKHOUSE_HTTP_PORT')
    _require_env('CLICKHOUSE_USER')
    _require_env('CLICKHOUSE_PASSWORD')
    _require_env('CLICKHOUSE_DATABASE')

    payload = run_s15_g2_g5_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s15-g2-g5-api-guardrails.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
