from __future__ import annotations

import importlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, cast

from .schemas import RawQueryWarning


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in cast(dict[Any, Any], value).items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


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


def run_s6_g4_fred_alerts_audit_proof() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix='origo-s6-g4-proof-') as temp_dir:
        temp_path = Path(temp_dir)
        audit_log_path = temp_path / 'fred-alert-events.jsonl'
        base_env = {
            'ORIGO_FRED_ALERT_AUDIT_LOG_PATH': str(audit_log_path),
            'ORIGO_FRED_DISCORD_TIMEOUT_SECONDS': '10',
        }

        original_env = _set_env(base_env)
        missing_webhook_error: str | None = None
        warning_codes: list[str] | None = None
        webhook_url: str | None = None
        webhook_status_code: int | None = None
        log_line_count: int | None = None
        last_event_type: str | None = None
        last_event_warning_codes: list[str] | None = None

        try:
            module = cast(Any, importlib.import_module('api.origo_api.fred_alert_audit'))
            setattr(module, '_fred_alert_audit_singleton', None)

            os.environ.pop('ORIGO_FRED_DISCORD_WEBHOOK_URL', None)
            try:
                module.emit_fred_warning_alerts_and_audit(
                    warnings=[
                        RawQueryWarning(
                            code='FRED_SOURCE_PUBLISH_STALE',
                            message='stale publish timestamp',
                        )
                    ],
                    dataset='fred_series_metrics',
                    mode='native',
                    strict=False,
                )
            except RuntimeError as exc:
                missing_webhook_error = str(exc)

            os.environ['ORIGO_FRED_DISCORD_WEBHOOK_URL'] = 'https://discord.example/webhook'
            setattr(module, '_fred_alert_audit_singleton', None)

            post_calls: list[dict[str, Any]] = []
            original_post = module._post_fred_discord_alert

            def _fake_post_fred_discord_alert(
                *, webhook_url: str, timeout_seconds: int, content: str
            ) -> int:
                post_calls.append(
                    {
                        'webhook_url': webhook_url,
                        'timeout_seconds': timeout_seconds,
                        'content': content,
                    }
                )
                return 204

            module._post_fred_discord_alert = _fake_post_fred_discord_alert
            try:
                emit_result = module.emit_fred_warning_alerts_and_audit(
                    warnings=[
                        RawQueryWarning(
                            code='FRED_SOURCE_PUBLISH_STALE',
                            message='stale publish timestamp',
                        ),
                        RawQueryWarning(
                            code='WINDOW_RANDOM_SAMPLE',
                            message='random sample window',
                        ),
                    ],
                    dataset='fred_series_metrics',
                    mode='native',
                    strict=False,
                )
            finally:
                module._post_fred_discord_alert = original_post

            if emit_result is None:
                raise RuntimeError(
                    'S6-G4 proof expected emit_fred_warning_alerts_and_audit to return payload'
                )

            warning_codes_raw = emit_result.get('warning_codes')
            if not isinstance(warning_codes_raw, list):
                raise RuntimeError('S6-G4 proof expected warning_codes list in emit payload')
            normalized_warning_codes: list[str] = []
            for code in cast(list[Any], warning_codes_raw):
                if not isinstance(code, str):
                    raise RuntimeError(
                        'S6-G4 proof expected warning_codes items to be strings'
                    )
                normalized_warning_codes.append(code)
            warning_codes = sorted(normalized_warning_codes)

            webhook_status_code_raw = emit_result.get('webhook_status_code')
            if not isinstance(webhook_status_code_raw, int):
                raise RuntimeError(
                    'S6-G4 proof expected webhook_status_code int in emit payload'
                )
            webhook_status_code = webhook_status_code_raw

            if len(post_calls) != 1:
                raise RuntimeError(
                    f'S6-G4 proof expected one webhook call, got {len(post_calls)}'
                )
            webhook_url_raw = post_calls[0].get('webhook_url')
            if not isinstance(webhook_url_raw, str):
                raise RuntimeError('S6-G4 proof expected webhook_url string in post call')
            webhook_url = webhook_url_raw

            if not audit_log_path.exists():
                raise RuntimeError('S6-G4 proof expected audit log file to be created')
            lines = [line for line in audit_log_path.read_text(encoding='utf-8').splitlines() if line]
            log_line_count = len(lines)
            if log_line_count < 2:
                raise RuntimeError(
                    'S6-G4 proof expected at least two audit log lines '
                    '(missing-webhook attempt + successful alert)'
                )

            last_event = _expect_dict(
                json.loads(lines[-1]),
                'S6-G4 last audit event',
            )
            last_event_type_raw = last_event.get('event_type')
            if not isinstance(last_event_type_raw, str):
                raise RuntimeError('S6-G4 proof expected event_type in last audit event')
            last_event_type = last_event_type_raw

            payload = _expect_dict(
                last_event.get('payload'),
                'S6-G4 last audit event payload',
            )
            payload_warning_codes_raw = payload.get('warning_codes')
            if not isinstance(payload_warning_codes_raw, list):
                raise RuntimeError(
                    'S6-G4 proof expected warning_codes list in last audit event payload'
                )
            normalized_last_codes: list[str] = []
            for code in cast(list[Any], payload_warning_codes_raw):
                if not isinstance(code, str):
                    raise RuntimeError(
                        'S6-G4 proof expected payload warning_codes items to be strings'
                    )
                normalized_last_codes.append(code)
            last_event_warning_codes = sorted(normalized_last_codes)
        finally:
            _restore_env(original_env)

    if missing_webhook_error != 'ORIGO_FRED_DISCORD_WEBHOOK_URL must be set and non-empty':
        raise RuntimeError(
            'S6-G4 proof expected fail-loud missing env error for '
            'ORIGO_FRED_DISCORD_WEBHOOK_URL'
        )
    if warning_codes != ['FRED_SOURCE_PUBLISH_STALE']:
        raise RuntimeError(
            'S6-G4 proof expected warning_codes=[FRED_SOURCE_PUBLISH_STALE], '
            f'got {warning_codes}'
        )
    if webhook_url != 'https://discord.example/webhook':
        raise RuntimeError(
            'S6-G4 proof expected webhook URL https://discord.example/webhook, '
            f'got {webhook_url}'
        )
    if webhook_status_code != 204:
        raise RuntimeError(
            f'S6-G4 proof expected webhook status code 204, got {webhook_status_code}'
        )
    if last_event_type != 'fred_query_warning':
        raise RuntimeError(
            f'S6-G4 proof expected last event_type fred_query_warning, got {last_event_type}'
        )
    if last_event_warning_codes != ['FRED_SOURCE_PUBLISH_STALE']:
        raise RuntimeError(
            'S6-G4 proof expected last event warning_codes=[FRED_SOURCE_PUBLISH_STALE], '
            f'got {last_event_warning_codes}'
        )

    return {
        'proof_scope': 'Slice 6 S6-G4 FRED warning alerts and audit-event coverage',
        'missing_webhook_env_error': missing_webhook_error,
        'warning_codes': warning_codes,
        'webhook_url': webhook_url,
        'webhook_status_code': webhook_status_code,
        'log_line_count': log_line_count,
        'last_event_type': last_event_type,
        'last_event_warning_codes': last_event_warning_codes,
    }


def main() -> None:
    payload = run_s6_g4_fred_alerts_audit_proof()
    output_path = Path(
        'spec/slices/slice-6-fred-integration/guardrails-proof-s6-g4-fred-alerts-audit.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
