from __future__ import annotations

import importlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, cast

from fastapi.testclient import TestClient

from origo.scraper.errors import ScraperError
from origo.scraper.rights import resolve_scraper_rights

from .rights import RightsGateError, resolve_export_rights, resolve_query_rights


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def _build_matrix(
    *,
    legal_signoff_artifact: str,
    binance_rights_state: str,
    include_etf_dataset: bool,
    include_ishares_source_id: bool,
) -> dict[str, Any]:
    ishares_datasets: list[str] = ['etf_daily_metrics'] if include_etf_dataset else []
    ishares_source_ids: list[str] = (
        ['etf_ishares_ibit_daily'] if include_ishares_source_id else []
    )
    return {
        'version': 's4-g2-proof',
        'sources': {
            'binance': {
                'rights_state': binance_rights_state,
                'datasets': ['spot_trades', 'spot_agg_trades', 'futures_trades'],
                'legal_signoff_artifact': legal_signoff_artifact,
            },
            'ishares': {
                'rights_state': 'Ingest Only',
                'datasets': ishares_datasets,
                'source_ids': ishares_source_ids,
            },
        },
    }


def run_s4_g2_rights_enforcement_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s4-g2-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        matrix_path = temp_path / 'rights-matrix.json'
        legal_artifact_path = temp_path / 'legal-signoff.md'
        export_audit_log_path = temp_path / 'export-audit.log'
        legal_artifact_path.write_text('# legal signoff\n', encoding='utf-8')

        original_matrix = os.environ.get('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
        original_api_env: dict[str, str | None] = {
            'ORIGO_INTERNAL_API_KEY': os.environ.get('ORIGO_INTERNAL_API_KEY'),
            'ORIGO_QUERY_MAX_CONCURRENCY': os.environ.get('ORIGO_QUERY_MAX_CONCURRENCY'),
            'ORIGO_QUERY_MAX_QUEUE': os.environ.get('ORIGO_QUERY_MAX_QUEUE'),
            'ORIGO_EXPORT_MAX_CONCURRENCY': os.environ.get('ORIGO_EXPORT_MAX_CONCURRENCY'),
            'ORIGO_EXPORT_MAX_QUEUE': os.environ.get('ORIGO_EXPORT_MAX_QUEUE'),
            'ORIGO_EXPORT_AUDIT_LOG_PATH': os.environ.get('ORIGO_EXPORT_AUDIT_LOG_PATH'),
            'ORIGO_AUDIT_LOG_RETENTION_DAYS': os.environ.get(
                'ORIGO_AUDIT_LOG_RETENTION_DAYS'
            ),
        }
        query_ingest_only_code: str | None = None
        query_hosted_allowed_ok = False
        export_hosted_allowed_ok = False
        scraper_source_id_check_ok = False
        scraper_unclassified_source_id_code: str | None = None
        api_query_ingest_only_status: int | None = None
        api_query_ingest_only_code: str | None = None
        query_byok_required_code: str | None = None
        export_byok_required_code: str | None = None
        query_missing_state_code: str | None = None
        scraper_missing_source_id_contract_code: str | None = None

        try:
            os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = str(matrix_path)

            base_matrix = _build_matrix(
                legal_signoff_artifact=str(legal_artifact_path),
                binance_rights_state='Hosted Allowed',
                include_etf_dataset=True,
                include_ishares_source_id=True,
            )
            _write_json(matrix_path, base_matrix)

            try:
                resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
            except RightsGateError as exc:
                query_ingest_only_code = exc.code

            query_hosted_allowed_ok = True
            try:
                resolve_query_rights(dataset='spot_trades', auth_token=None)
            except RightsGateError:
                query_hosted_allowed_ok = False

            export_hosted_allowed_ok = True
            try:
                resolve_export_rights(dataset='spot_trades', auth_token=None)
            except RightsGateError:
                export_hosted_allowed_ok = False

            scraper_source_id_check_ok = True
            try:
                resolve_scraper_rights(
                    source_key='ishares',
                    source_id='etf_ishares_ibit_daily',
                )
            except ScraperError:
                scraper_source_id_check_ok = False

            try:
                resolve_scraper_rights(
                    source_key='ishares',
                    source_id='etf_ishares_invalid_daily',
                )
            except ScraperError as exc:
                scraper_unclassified_source_id_code = exc.code

            os.environ['ORIGO_INTERNAL_API_KEY'] = 'proof-api-key'
            os.environ['ORIGO_QUERY_MAX_CONCURRENCY'] = '2'
            os.environ['ORIGO_QUERY_MAX_QUEUE'] = '8'
            os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'] = '2'
            os.environ['ORIGO_EXPORT_MAX_QUEUE'] = '8'
            os.environ['ORIGO_EXPORT_AUDIT_LOG_PATH'] = str(export_audit_log_path)
            os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '365'

            main_module = importlib.import_module('api.origo_api.main')
            reloaded_main_module = importlib.reload(main_module)

            with TestClient(reloaded_main_module.app) as client:
                client_any = cast(Any, client)
                raw_response: Any = client_any.post(
                    '/v1/raw/query',
                    headers={'X-API-Key': 'proof-api-key'},
                    json={
                        'dataset': 'etf_daily_metrics',
                        'fields': ['source_id', 'metric_name'],
                        'time_range': ['2026-03-04T00:00:00Z', '2026-03-05T00:00:00Z'],
                        'strict': False,
                    },
                )
            api_query_ingest_only_status = int(raw_response.status_code)
            response_payload_any = raw_response.json()
            detail_payload: Any = None
            if isinstance(response_payload_any, dict):
                response_payload = cast(dict[str, Any], response_payload_any)
                detail_payload = response_payload.get('detail')
            if isinstance(detail_payload, dict):
                detail_payload_dict = cast(dict[str, Any], detail_payload)
                code_payload = detail_payload_dict.get('code')
                if isinstance(code_payload, str):
                    api_query_ingest_only_code = code_payload

            byok_matrix = _build_matrix(
                legal_signoff_artifact=str(legal_artifact_path),
                binance_rights_state='BYOK Required',
                include_etf_dataset=True,
                include_ishares_source_id=True,
            )
            _write_json(matrix_path, byok_matrix)

            try:
                resolve_query_rights(dataset='spot_trades', auth_token=None)
            except RightsGateError as exc:
                query_byok_required_code = exc.code

            try:
                resolve_export_rights(dataset='spot_trades', auth_token=None)
            except RightsGateError as exc:
                export_byok_required_code = exc.code

            missing_state_matrix = _build_matrix(
                legal_signoff_artifact=str(legal_artifact_path),
                binance_rights_state='Hosted Allowed',
                include_etf_dataset=False,
                include_ishares_source_id=True,
            )
            _write_json(matrix_path, missing_state_matrix)

            try:
                resolve_query_rights(dataset='etf_daily_metrics', auth_token=None)
            except RightsGateError as exc:
                query_missing_state_code = exc.code

            source_id_contract_matrix = _build_matrix(
                legal_signoff_artifact=str(legal_artifact_path),
                binance_rights_state='Hosted Allowed',
                include_etf_dataset=True,
                include_ishares_source_id=False,
            )
            _write_json(matrix_path, source_id_contract_matrix)

            try:
                resolve_scraper_rights(
                    source_key='ishares',
                    source_id='etf_ishares_ibit_daily',
                )
            except ScraperError as exc:
                scraper_missing_source_id_contract_code = exc.code
        finally:
            if original_matrix is None:
                os.environ.pop('ORIGO_SOURCE_RIGHTS_MATRIX_PATH', None)
            else:
                os.environ['ORIGO_SOURCE_RIGHTS_MATRIX_PATH'] = original_matrix
            for env_key, env_value in original_api_env.items():
                if env_value is None:
                    os.environ.pop(env_key, None)
                else:
                    os.environ[env_key] = env_value

    if query_ingest_only_code != 'QUERY_RIGHTS_INGEST_ONLY':
        raise RuntimeError(
            'S4-G2 proof expected QUERY_RIGHTS_INGEST_ONLY for ETF query rights'
        )
    if api_query_ingest_only_status != 409:
        raise RuntimeError(
            'S4-G2 proof expected HTTP status 409 for ETF query ingest-only API path'
        )
    if api_query_ingest_only_code != 'QUERY_RIGHTS_INGEST_ONLY':
        raise RuntimeError(
            'S4-G2 proof expected QUERY_RIGHTS_INGEST_ONLY from API response detail'
        )
    if query_byok_required_code != 'QUERY_RIGHTS_BYOK_REQUIRED':
        raise RuntimeError(
            'S4-G2 proof expected QUERY_RIGHTS_BYOK_REQUIRED for query BYOK scenario'
        )
    if query_missing_state_code != 'QUERY_RIGHTS_MISSING_STATE':
        raise RuntimeError(
            'S4-G2 proof expected QUERY_RIGHTS_MISSING_STATE for query missing-state scenario'
        )
    if export_byok_required_code != 'EXPORT_RIGHTS_BYOK_REQUIRED':
        raise RuntimeError(
            'S4-G2 proof expected EXPORT_RIGHTS_BYOK_REQUIRED for export BYOK scenario'
        )
    if scraper_unclassified_source_id_code != 'SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED':
        raise RuntimeError(
            'S4-G2 proof expected SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED for unclassified source_id'
        )
    if scraper_missing_source_id_contract_code != 'SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED':
        raise RuntimeError(
            'S4-G2 proof expected SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED when source_ids contract is empty'
        )
    if not query_hosted_allowed_ok:
        raise RuntimeError(
            'S4-G2 proof expected Hosted Allowed query rights to pass with legal signoff'
        )
    if not export_hosted_allowed_ok:
        raise RuntimeError(
            'S4-G2 proof expected Hosted Allowed export rights to pass with legal signoff'
        )
    if not scraper_source_id_check_ok:
        raise RuntimeError('S4-G2 proof expected scraper source_id classification to pass')

    return {
        'proof_scope': 'Slice 4 S4-G2 rights matrix enforcement in ingestion and API paths',
        'query_ingest_only_error_code': query_ingest_only_code,
        'api_query_ingest_only_http_status': api_query_ingest_only_status,
        'api_query_ingest_only_error_code': api_query_ingest_only_code,
        'query_byok_required_error_code': query_byok_required_code,
        'query_missing_state_error_code': query_missing_state_code,
        'query_hosted_allowed_passed': query_hosted_allowed_ok,
        'export_byok_required_error_code': export_byok_required_code,
        'export_hosted_allowed_passed': export_hosted_allowed_ok,
        'scraper_source_id_check_passed': scraper_source_id_check_ok,
        'scraper_unclassified_source_id_error_code': scraper_unclassified_source_id_code,
        'scraper_missing_source_id_contract_error_code': (
            scraper_missing_source_id_contract_code
        ),
    }


def main() -> None:
    payload = run_s4_g2_rights_enforcement_proof()
    output_path = Path('spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g2.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
