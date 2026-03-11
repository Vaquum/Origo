from __future__ import annotations

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

_SLICE_DIR = Path('spec/slices/slice-17-fred-event-sourcing-port')


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
        raise RuntimeError(f'{name} must be set and non-empty for S17-G1/G2 proof')
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


def _seed_fred_native_projection_row(*, client: ClickHouseClient, database: str) -> None:
    observed_at_utc = datetime(2026, 3, 8, 0, 0, 0, tzinfo=UTC)
    ingested_at_utc = datetime(2026, 3, 10, 10, 0, 0, tzinfo=UTC)
    dimensions_json = json.dumps(
        {
            'frequency': 'Daily',
            'observation_date': '2026-03-08',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': 'FEDFUNDS',
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    provenance_json = json.dumps(
        {
            'fetched_from': 'fred_api',
            'frequency': 'Daily',
            'last_updated_utc': '2026-03-08T12:00:00Z',
            'realtime_end': '2026-03-08',
            'realtime_start': '2026-03-08',
            'registry_version': '2026-03-10-s17-fixture',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': 'FEDFUNDS',
            'source_uri': 'fred://series/FEDFUNDS',
            'units': 'Percent',
        },
        sort_keys=True,
        separators=(',', ':'),
    )

    client.execute(
        f'''
        INSERT INTO {database}.canonical_fred_series_metrics_native_v1
        (
            metric_id,
            source_id,
            metric_name,
            metric_unit,
            metric_value_string,
            metric_value_int,
            metric_value_float,
            metric_value_bool,
            observed_at_utc,
            dimensions_json,
            provenance_json,
            ingested_at_utc,
            event_id,
            source_offset_or_equivalent,
            source_event_time_utc
        )
        VALUES
        ''',
        [
            (
                'fred_fedfunds:2026-03-08:effective_federal_funds_rate',
                'fred_fedfunds',
                'effective_federal_funds_rate',
                'Percent',
                '4.33',
                None,
                4.33,
                None,
                observed_at_utc,
                dimensions_json,
                provenance_json,
                ingested_at_utc,
                uuid4(),
                'fred_fedfunds:2026-03-08:effective_federal_funds_rate',
                observed_at_utc,
            )
        ],
    )


def _build_rights_matrix_hosted_provisional(*, legal_signoff_artifact: str) -> dict[str, Any]:
    return {
        'version': '2026-03-10-s17-g1-g2-proof',
        'sources': {
            'fred': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': True,
                'datasets': ['fred_series_metrics'],
                'legal_signoff_artifact': legal_signoff_artifact,
            }
        },
    }


def _build_rights_matrix_missing_fred() -> dict[str, Any]:
    return {
        'version': '2026-03-10-s17-g1-g2-proof-missing',
        'sources': {
            'ishares': {
                'rights_state': 'Hosted Allowed',
                'rights_provisional': False,
                'datasets': ['etf_daily_metrics'],
                'legal_signoff_artifact': 'contracts/legal/etf-hosted-allowed.md',
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


def run_s17_g1_g2_proof() -> dict[str, Any]:
    base_settings = _build_clickhouse_settings_from_env()
    proof_database = f'{base_settings.database}_s17_g1_g2_proof'
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
        _seed_fred_native_projection_row(client=admin_client, database=proof_database)
        os.environ['CLICKHOUSE_DATABASE'] = proof_database

        with TemporaryDirectory(prefix='origo-s17-g1-g2-proof-') as tmp_dir:
            tmp_path = Path(tmp_dir)
            legal_signoff_path = tmp_path / 'fred-legal-signoff.md'
            legal_signoff_path.write_text(
                '# FRED Hosted Allowed Legal Sign-off (Provisional)\n',
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
                    _build_rights_matrix_missing_fred(),
                    indent=2,
                    sort_keys=True,
                )
                + '\n',
                encoding='utf-8',
            )
            export_audit_log_path = tmp_path / 'export-audit-events.jsonl'
            fred_alert_log_path = tmp_path / 'fred-alert-events.jsonl'

            os.environ['ORIGO_INTERNAL_API_KEY'] = 's17-proof-key'
            os.environ['ORIGO_QUERY_MAX_CONCURRENCY'] = '4'
            os.environ['ORIGO_QUERY_MAX_QUEUE'] = '8'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY'] = '1'
            os.environ['ORIGO_ALIGNED_QUERY_MAX_QUEUE'] = '1'
            os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'] = '2'
            os.environ['ORIGO_EXPORT_MAX_QUEUE'] = '8'
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_hosted_path)
            os.environ['ORIGO_EXPORT_AUDIT_LOG_PATH'] = str(export_audit_log_path)
            os.environ['ORIGO_FRED_ALERT_AUDIT_LOG_PATH'] = str(fred_alert_log_path)
            os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
            os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'promoted'
            os.environ['ORIGO_FRED_QUERY_SERVING_STATE'] = 'promoted'
            os.environ['ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS'] = '1'
            os.environ['ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS'] = '1'
            os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS'] = '300'
            os.environ['ORIGO_FRED_DISCORD_WEBHOOK_URL'] = 'https://discord.example/webhook'
            os.environ['ORIGO_FRED_DISCORD_TIMEOUT_SECONDS'] = '10'

            fred_alert_module = cast(
                Any, importlib.import_module('api.origo_api.fred_alert_audit')
            )
            setattr(fred_alert_module, '_fred_alert_audit_singleton', None)
            original_post = fred_alert_module._post_fred_discord_alert

            def _fake_post_fred_discord_alert(
                *, webhook_url: str, timeout_seconds: int, content: str
            ) -> int:
                _ = (webhook_url, timeout_seconds, content)
                return 204

            fred_alert_module._post_fred_discord_alert = _fake_post_fred_discord_alert
            try:
                main_module = _load_main_module()
                client = TestClient(main_module.app)
                headers = {'X-API-Key': 's17-proof-key'}

                non_strict_status, non_strict_body = _post_json(
                    client=client,
                    path='/v1/raw/query',
                    headers=headers,
                    payload={
                        'mode': 'native',
                        'sources': ['fred_series_metrics'],
                        'time_range': ['2026-03-08T00:00:00Z', '2026-03-11T00:00:00Z'],
                        'strict': False,
                    },
                )
                if non_strict_status != 200:
                    raise RuntimeError(
                        'S17-G1/G2 expected non-strict FRED query success, '
                        f'got status={non_strict_status} body={non_strict_body}'
                    )

                rights_state = non_strict_body.get('rights_state')
                rights_provisional = non_strict_body.get('rights_provisional')
                if rights_state != 'Hosted Allowed' or rights_provisional is not True:
                    raise RuntimeError(
                        'S17-G2 expected Hosted Allowed provisional rights metadata, '
                        f'observed rights_state={rights_state!r} '
                        f'rights_provisional={rights_provisional!r}'
                    )

                warning_codes = _extract_warning_codes(non_strict_body.get('warnings'))
                required_warning_codes = {
                    'FRED_SOURCE_PUBLISH_STALE',
                    'FRED_SOURCE_PUBLISH_MISSING',
                }
                missing_warning_codes = sorted(required_warning_codes.difference(warning_codes))
                if missing_warning_codes:
                    raise RuntimeError(
                        'S17-G1 expected FRED warning coverage missing warning codes: '
                        f'{missing_warning_codes} observed={warning_codes}'
                    )

                strict_status, strict_body = _post_json(
                    client=client,
                    path='/v1/raw/query',
                    headers=headers,
                    payload={
                        'mode': 'native',
                        'sources': ['fred_series_metrics'],
                        'time_range': ['2026-03-08T00:00:00Z', '2026-03-11T00:00:00Z'],
                        'strict': True,
                    },
                )
                strict_detail = _expect_dict(strict_body.get('detail'), label='strict detail')
                strict_code = strict_detail.get('code')
                if strict_status != 409 or strict_code != 'STRICT_MODE_WARNING_FAILURE':
                    raise RuntimeError(
                        'S17-G1 expected strict failure on warnings, '
                        f'observed status={strict_status} code={strict_code!r}'
                    )

                os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_missing_path)
                missing_rights_status, missing_rights_body = _post_json(
                    client=client,
                    path='/v1/raw/query',
                    headers=headers,
                    payload={
                        'mode': 'native',
                        'sources': ['fred_series_metrics'],
                        'time_range': ['2026-03-08T00:00:00Z', '2026-03-11T00:00:00Z'],
                        'strict': False,
                    },
                )
                missing_rights_detail = _expect_dict(
                    missing_rights_body.get('detail'),
                    label='missing rights detail',
                )
                if (
                    missing_rights_status != 409
                    or missing_rights_detail.get('code') != 'QUERY_RIGHTS_MISSING_STATE'
                ):
                    raise RuntimeError(
                        'S17-G2 expected QUERY_RIGHTS_MISSING_STATE for missing rights, '
                        f'observed status={missing_rights_status} detail={missing_rights_detail}'
                    )
            finally:
                fred_alert_module._post_fred_discord_alert = original_post

            return {
                'proof_scope': (
                    'Slice 17 S17-G1/G2 FRED publish-freshness warnings and rights/legal '
                    'guardrail behavior on canonical projection serving path'
                ),
                'proof_database': proof_database,
                'non_strict_status': non_strict_status,
                'strict_status': strict_status,
                'missing_rights_status': missing_rights_status,
                'warning_codes': warning_codes,
                'rights_state': rights_state,
                'rights_provisional': rights_provisional,
                'strict_error_code': strict_code,
                'missing_rights_error_code': missing_rights_detail.get('code'),
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
    payload = run_s17_g1_g2_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s17-g1-g2-api-guardrails.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
