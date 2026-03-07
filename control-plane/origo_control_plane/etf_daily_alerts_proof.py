from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

_MODULE_NAME = 'origo_control_plane.definitions'
_SENSOR_NAME = 'origo_etf_ingest_anomaly_alert_sensor'
_WEBHOOK_ENV = 'ORIGO_ETF_DISCORD_WEBHOOK_URL'


def _set_env(updates: dict[str, str]) -> dict[str, str | None]:
    original: dict[str, str | None] = {}
    for key, value in updates.items():
        original[key] = os.environ.get(key)
        os.environ[key] = value
    return original


def _restore_env(original: dict[str, str | None]) -> None:
    for key, value in original.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _clear_module_cache(module_name: str) -> None:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(f'{module_name}.'):
            del sys.modules[name]


def _load_definitions_module() -> ModuleType:
    if _MODULE_NAME in sys.modules:
        return importlib.reload(sys.modules[_MODULE_NAME])
    return importlib.import_module(_MODULE_NAME)


def run_s4_g7_alerts_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s4-g7-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        anomaly_log_path = temp_path / 'etf-anomalies.jsonl'
        base_env: dict[str, str] = {
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_PORT': '9000',
            'CLICKHOUSE_HTTP_PORT': '8123',
            'CLICKHOUSE_USER': 'default',
            'CLICKHOUSE_PASSWORD': 'origo',
            'CLICKHOUSE_DATABASE': 'origo',
            'ORIGO_ETF_DAILY_SCHEDULE_CRON': '0 2 * * *',
            'ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE': 'UTC',
            'ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS': '2',
            'ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS': '60',
            'ORIGO_ETF_ANOMALY_LOG_PATH': str(anomaly_log_path),
            'ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS': '45',
            'ORIGO_ETF_DISCORD_TIMEOUT_SECONDS': '10',
        }

        env_original = _set_env(base_env)
        missing_webhook_error: str | None = None
        sensor_min_interval_seconds: int | None = None
        emitted_anomaly_code: str | None = None
        emitted_run_id: str | None = None
        log_line_count: int | None = None
        webhook_url: str | None = None
        webhook_status_code: int | None = None

        try:
            os.environ.pop(_WEBHOOK_ENV, None)
            _clear_module_cache(_MODULE_NAME)
            try:
                _load_definitions_module()
            except RuntimeError as exc:
                missing_webhook_error = str(exc)

            os.environ[_WEBHOOK_ENV] = 'https://discord.example/webhook'
            _clear_module_cache(_MODULE_NAME)
            module = _load_definitions_module()
            defs = getattr(module, 'defs')
            sensor_def = defs.get_sensor_def(_SENSOR_NAME)
            sensor_min_interval_seconds = int(sensor_def.minimum_interval_seconds)

            post_calls: list[dict[str, Any]] = []
            original_post = module.requests.post

            class _FakeResponse:
                def __init__(self, status_code: int, text: str) -> None:
                    self.status_code = status_code
                    self.text = text

            def _fake_post(url: str, json: Any, timeout: int) -> Any:
                post_calls.append(
                    {'url': url, 'json': json, 'timeout': timeout}
                )
                return _FakeResponse(status_code=204, text='')

            module.requests.post = _fake_post
            try:
                emitter = getattr(module, '_emit_etf_ingest_anomaly_alert')
                event_payload = emitter(
                    anomaly_code='ETF_DAILY_INGEST_RUN_FAILURE',
                    run_id='run-s4-g7-proof-1',
                    details={
                        'job_name': 'origo_etf_daily_ingest_job',
                        'retry_attempt': '1',
                    },
                )
            finally:
                module.requests.post = original_post

            emitted_anomaly_code_raw = event_payload.get('anomaly_code')
            if isinstance(emitted_anomaly_code_raw, str):
                emitted_anomaly_code = emitted_anomaly_code_raw
            emitted_run_id_raw = event_payload.get('run_id')
            if isinstance(emitted_run_id_raw, str):
                emitted_run_id = emitted_run_id_raw

            if len(post_calls) != 1:
                raise RuntimeError(
                    f'S4-G7 proof expected one webhook call, got {len(post_calls)}'
                )
            webhook_call = post_calls[0]
            webhook_url_raw = webhook_call.get('url')
            if isinstance(webhook_url_raw, str):
                webhook_url = webhook_url_raw
            webhook_status_code = 204

            if not anomaly_log_path.exists():
                raise RuntimeError(
                    'S4-G7 proof expected anomaly log file to be created'
                )
            lines = anomaly_log_path.read_text(encoding='utf-8').strip().splitlines()
            log_line_count = len(lines)
            if log_line_count != 1:
                raise RuntimeError(
                    f'S4-G7 proof expected one anomaly log line, got {log_line_count}'
                )
            parsed_line = json.loads(lines[0])
            if parsed_line.get('anomaly_code') != 'ETF_DAILY_INGEST_RUN_FAILURE':
                raise RuntimeError(
                    'S4-G7 proof expected anomaly_code ETF_DAILY_INGEST_RUN_FAILURE '
                    f'in log payload, got {parsed_line.get("anomaly_code")}'
                )
        finally:
            _restore_env(env_original)

    if missing_webhook_error != f'{_WEBHOOK_ENV} must be set and non-empty':
        raise RuntimeError(
            'S4-G7 proof expected strict missing-env failure for '
            f'{_WEBHOOK_ENV}, got: {missing_webhook_error}'
        )
    if sensor_min_interval_seconds != 45:
        raise RuntimeError(
            'S4-G7 proof expected alert sensor interval 45 seconds, '
            f'got {sensor_min_interval_seconds}'
        )
    if emitted_anomaly_code != 'ETF_DAILY_INGEST_RUN_FAILURE':
        raise RuntimeError(
            'S4-G7 proof expected emitted anomaly code ETF_DAILY_INGEST_RUN_FAILURE, '
            f'got {emitted_anomaly_code}'
        )
    if emitted_run_id != 'run-s4-g7-proof-1':
        raise RuntimeError(
            'S4-G7 proof expected emitted run_id run-s4-g7-proof-1, '
            f'got {emitted_run_id}'
        )
    if webhook_url != 'https://discord.example/webhook':
        raise RuntimeError(
            'S4-G7 proof expected webhook URL https://discord.example/webhook, '
            f'got {webhook_url}'
        )

    return {
        'proof_scope': 'Slice 4 S4-G7 ETF anomaly logs and Discord alerts',
        'missing_webhook_env_error': missing_webhook_error,
        'sensor_name': _SENSOR_NAME,
        'sensor_min_interval_seconds': sensor_min_interval_seconds,
        'emitted_anomaly_code': emitted_anomaly_code,
        'emitted_run_id': emitted_run_id,
        'log_line_count': log_line_count,
        'webhook_url': webhook_url,
        'webhook_status_code': webhook_status_code,
    }


def main() -> None:
    payload = run_s4_g7_alerts_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = repo_root / 'spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g7.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
