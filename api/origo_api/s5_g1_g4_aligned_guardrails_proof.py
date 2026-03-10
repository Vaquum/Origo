from __future__ import annotations

import importlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from fastapi.testclient import TestClient


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty for guardrail proof')
    return value


def _build_rights_matrix(*, legal_signoff_artifact: str) -> dict[str, Any]:
    return {
        'version': '2026-03-06-s5-g1-g4-proof',
        'sources': {
            'binance': {
                'rights_state': 'Hosted Allowed',
                'datasets': [
                    'spot_trades',
                    'spot_agg_trades',
                    'futures_trades',
                ],
                'legal_signoff_artifact': legal_signoff_artifact,
            },
            'ishares': {
                'rights_state': 'Ingest Only',
                'datasets': ['etf_daily_metrics'],
            },
        },
    }


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in cast(dict[Any, Any], value).items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


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
        raise RuntimeError(f'HTTP status_code must be int for path={path}')
    body = response.json()
    return status_code, _expect_dict(body, f'HTTP JSON body for path={path}')


def run_s5_g1_g4_proof() -> dict[str, Any]:
    with TemporaryDirectory(prefix='origo-s5-g1-g4-proof-') as tmp_dir:
        temp_path = Path(tmp_dir)
        legal_signoff_path = temp_path / 'binance-legal-signoff.md'
        legal_signoff_path.write_text(
            '# Binance Hosted Allowed Legal Sign-off\n\nApproved for proof harness only.\n',
            encoding='utf-8',
        )
        rights_matrix_path = temp_path / 'source-rights-matrix.json'
        rights_matrix_path.write_text(
            json.dumps(
                _build_rights_matrix(legal_signoff_artifact=str(legal_signoff_path)),
                indent=2,
                sort_keys=True,
            ),
            encoding='utf-8',
        )
        export_audit_log_path = temp_path / 'export-audit-events.jsonl'

        os.environ['ORIGO_INTERNAL_API_KEY'] = 'proof-internal-key'
        os.environ['ORIGO_QUERY_MAX_CONCURRENCY'] = '4'
        os.environ['ORIGO_QUERY_MAX_QUEUE'] = '8'
        os.environ['ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY'] = '1'
        os.environ['ORIGO_ALIGNED_QUERY_MAX_QUEUE'] = '1'
        os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'] = '2'
        os.environ['ORIGO_EXPORT_MAX_QUEUE'] = '8'
        os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(rights_matrix_path)
        os.environ['ORIGO_EXPORT_AUDIT_LOG_PATH'] = str(export_audit_log_path)
        os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'
        os.environ['ORIGO_ETF_QUERY_SERVING_STATE'] = 'promoted'
        os.environ['ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS'] = '2'
        os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS'] = '1'

        # Reset audit singleton for proof-local log path.
        export_audit_module = importlib.import_module('api.origo_api.export_audit')
        setattr(export_audit_module, '_audit_log_singleton', None)

        main_module = importlib.import_module('api.origo_api.main')
        client = TestClient(main_module.app)
        headers = {'X-API-Key': 'proof-internal-key'}

        strict_status, strict_body = _post_json(
            client=client,
            path='/v1/raw/query',
            headers=headers,
            payload={
                'mode': 'aligned_1s',
                'dataset': 'spot_trades',
                'fields': ['aligned_at_utc', 'open_price', 'close_price'],
                'n_rows': 5,
                'include_datetime': True,
                'strict': True,
            },
        )
        strict_detail = _expect_dict(
            strict_body.get('detail'),
            'S5-G1 strict failure detail',
        )
        strict_code = strict_detail.get('code')
        if strict_status != 409 or strict_code != 'STRICT_MODE_WARNING_FAILURE':
            raise RuntimeError(
                'S5-G1 proof expected strict aligned query to fail with '
                f'STRICT_MODE_WARNING_FAILURE, got status={strict_status} code={strict_code}'
            )

        freshness_status, freshness_body = _post_json(
            client=client,
            path='/v1/raw/query',
            headers=headers,
            payload={
                'mode': 'aligned_1s',
                'dataset': 'spot_trades',
                'fields': ['aligned_at_utc', 'open_price', 'close_price'],
                'time_range': ['2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'],
                'include_datetime': True,
                'strict': False,
            },
        )
        if freshness_status != 200:
            raise RuntimeError(
                f'S5-G2 proof expected aligned query success, got status={freshness_status}'
            )
        warnings_payload = freshness_body.get('warnings')
        if not isinstance(warnings_payload, list):
            raise RuntimeError('S5-G2 proof expected warnings list')
        freshness_warning_codes: list[str] = []
        for warning_obj in cast(list[Any], warnings_payload):
            if not isinstance(warning_obj, dict):
                continue
            warning = _expect_dict(warning_obj, 'S5-G2 warning item')
            warning_code = warning.get('code')
            if isinstance(warning_code, str):
                freshness_warning_codes.append(warning_code)
        freshness_warning_codes = sorted(freshness_warning_codes)
        if 'ALIGNED_FRESHNESS_STALE' not in freshness_warning_codes:
            raise RuntimeError('S5-G2 proof expected ALIGNED_FRESHNESS_STALE warning')
        freshness_payload = _expect_dict(
            freshness_body.get('freshness'),
            'S5-G2 freshness payload',
        )
        lag_seconds = freshness_payload.get('lag_seconds')
        if not isinstance(lag_seconds, int) or lag_seconds <= 0:
            raise RuntimeError('S5-G2 proof expected freshness lag_seconds > 0')

        setattr(main_module, '_pending_aligned_requests', main_module.ALIGNED_QUERY_MAX_QUEUE)
        try:
            queue_status, queue_body = _post_json(
                client=client,
                path='/v1/raw/query',
                headers=headers,
                payload={
                    'mode': 'aligned_1s',
                    'dataset': 'spot_trades',
                    'fields': ['aligned_at_utc', 'open_price'],
                    'time_range': ['2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'],
                    'include_datetime': True,
                    'strict': False,
                },
            )
        finally:
            setattr(main_module, '_pending_aligned_requests', 0)

        queue_detail = _expect_dict(queue_body.get('detail'), 'S5-G3 queue failure detail')
        queue_code = queue_detail.get('code')
        if queue_status != 503 or queue_code != 'ALIGNED_QUERY_QUEUE_LIMIT_REACHED':
            raise RuntimeError(
                'S5-G3 proof expected aligned queue rejection with '
                f'ALIGNED_QUERY_QUEUE_LIMIT_REACHED, got status={queue_status} code={queue_code}'
            )

        export_status, export_body = _post_json(
            client=client,
            path='/v1/raw/export',
            headers=headers,
            payload={
                'mode': 'aligned_1s',
                'format': 'parquet',
                'dataset': 'etf_daily_metrics',
                'fields': [
                    'source_id',
                    'metric_name',
                    'valid_from_utc',
                    'valid_to_utc_exclusive',
                ],
                'time_range': ['2026-03-05T12:00:00Z', '2026-03-07T12:00:00Z'],
                'include_datetime': True,
                'strict': False,
            },
        )
        export_detail = _expect_dict(export_body.get('detail'), 'S5-G4 export failure detail')
        export_code = export_detail.get('code')
        if export_status != 409 or export_code != 'EXPORT_RIGHTS_INGEST_ONLY':
            raise RuntimeError(
                'S5-G4 proof expected aligned export rights rejection with '
                f'EXPORT_RIGHTS_INGEST_ONLY, got status={export_status} code={export_code}'
            )

        if not export_audit_log_path.exists():
            raise RuntimeError('S5-G4 proof expected export audit log file to be created')
        audit_events = [
            json.loads(line)
            for line in export_audit_log_path.read_text(encoding='utf-8').splitlines()
            if line.strip() != ''
        ]
        if len(audit_events) == 0:
            raise RuntimeError('S5-G4 proof expected at least one audit event')
        last_event = _expect_dict(audit_events[-1], 'S5-G4 last audit event')
        if last_event.get('event_type') != 'export_submit_rejected':
            raise RuntimeError(
                'S5-G4 proof expected last audit event_type=export_submit_rejected'
            )
        audit_payload = _expect_dict(last_event.get('payload'), 'S5-G4 audit payload')
        if audit_payload.get('mode') != 'aligned_1s':
            raise RuntimeError('S5-G4 proof expected audit payload mode=aligned_1s')

        return {
            'generated_at_utc': datetime.now(UTC).isoformat(),
            'proof_scope': 'Slice 5 G1-G4 aligned query/export guardrails',
            'g1_strict_warning_failure': {
                'status_code': strict_status,
                'error_code': strict_code,
            },
            'g2_freshness_warning': {
                'status_code': freshness_status,
                'warning_codes': freshness_warning_codes,
                'freshness': freshness_payload,
            },
            'g3_aligned_queue_limit': {
                'status_code': queue_status,
                'error_code': queue_code,
            },
            'g4_aligned_export_rights_audit': {
                'status_code': export_status,
                'error_code': export_code,
                'audit_event_count': len(audit_events),
                'last_event_type': last_event.get('event_type'),
                'last_event_mode': audit_payload.get('mode'),
            },
        }


def main() -> None:
    _require_env('CLICKHOUSE_HOST')
    _require_env('CLICKHOUSE_HTTP_PORT')
    _require_env('CLICKHOUSE_USER')
    _require_env('CLICKHOUSE_PASSWORD')
    _require_env('CLICKHOUSE_DATABASE')

    result = run_s5_g1_g4_proof()
    output_path = Path('spec/slices/slice-5-raw-query-aligned-1s/guardrails-proof-s5-g1-g4.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
