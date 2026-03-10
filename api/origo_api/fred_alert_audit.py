from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from origo.audit import (
    ImmutableAuditLog,
    load_audit_log_retention_days,
    resolve_required_path_env,
)

from .schemas import RawQueryDataset, RawQueryMode, RawQueryWarning


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _require_positive_int_env(name: str) -> int:
    raw = _require_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f'{name} must be an integer') from exc
    if value <= 0:
        raise RuntimeError(f'{name} must be > 0')
    return value


def _post_fred_discord_alert(
    *, webhook_url: str, timeout_seconds: int, content: str
) -> int:
    payload = json.dumps({'content': content}, ensure_ascii=True).encode('utf-8')
    request = Request(
        webhook_url,
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            response_body = response.read().decode('utf-8', errors='replace')
    except HTTPError as exc:
        response_body = exc.read().decode('utf-8', errors='replace')
        raise RuntimeError(
            'FRED Discord webhook request failed '
            f'(status_code={exc.code}, body={response_body})'
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            'FRED Discord webhook request failed '
            f'(reason={exc.reason})'
        ) from exc

    if status_code < 200 or status_code >= 300:
        raise RuntimeError(
            'FRED Discord webhook request failed '
            f'(status_code={status_code}, body={response_body})'
        )
    return status_code


class FredAlertAuditLog:
    def __init__(self, *, path: Path, retention_days: int) -> None:
        self._sink = ImmutableAuditLog(
            path=path,
            sink_name='fred_alert',
            retention_days=retention_days,
        )

    def append_event(
        self,
        *,
        event_type: str,
        payload: dict[str, Any],
    ) -> str:
        return self._sink.append_event(
            event_type=event_type,
            payload=payload,
        )


_fred_alert_audit_singleton: FredAlertAuditLog | None = None


def get_fred_alert_audit_log() -> FredAlertAuditLog:
    global _fred_alert_audit_singleton
    if _fred_alert_audit_singleton is None:
        _fred_alert_audit_singleton = FredAlertAuditLog(
            path=resolve_required_path_env('ORIGO_FRED_ALERT_AUDIT_LOG_PATH'),
            retention_days=load_audit_log_retention_days(),
        )
    return _fred_alert_audit_singleton


def emit_fred_warning_alerts_and_audit(
    *,
    warnings: list[RawQueryWarning],
    dataset: RawQueryDataset,
    mode: RawQueryMode,
    strict: bool,
) -> dict[str, Any] | None:
    fred_warnings = [warning for warning in warnings if warning.code.startswith('FRED_')]
    if len(fred_warnings) == 0:
        return None

    warning_codes = sorted({warning.code for warning in fred_warnings})
    warning_payload = [warning.model_dump() for warning in fred_warnings]
    event_hash = get_fred_alert_audit_log().append_event(
        event_type='fred_query_warning',
        payload={
            'dataset': dataset,
            'mode': mode,
            'strict': strict,
            'warning_codes': warning_codes,
            'warning_count': len(fred_warnings),
            'warnings': warning_payload,
        },
    )

    webhook_url = _require_env('ORIGO_FRED_DISCORD_WEBHOOK_URL')
    timeout_seconds = _require_positive_int_env('ORIGO_FRED_DISCORD_TIMEOUT_SECONDS')
    status_code = _post_fred_discord_alert(
        webhook_url=webhook_url,
        timeout_seconds=timeout_seconds,
        content=(
            '[FRED_QUERY_WARNING] '
            f'dataset={dataset} mode={mode} strict={strict} '
            f'warning_codes={warning_codes} '
            f'event_hash={event_hash}'
        ),
    )
    return {
        'event_hash': event_hash,
        'warning_codes': warning_codes,
        'webhook_status_code': status_code,
    }
