from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .schemas import RawQueryDataset, RawQueryMode, RawQueryWarning

_JSON_DICT_SEPARATORS = (',', ':')


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


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_int(value: Any, label: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f'{label} must be an integer')
    return value


def _expect_nullable_str(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be a string or null')
    if value == '':
        raise RuntimeError(f'{label} must be non-empty when set')
    return value


def _compute_event_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=_JSON_DICT_SEPARATORS,
        ensure_ascii=True,
    )
    digest = hashlib.sha256()
    digest.update(canonical.encode('utf-8'))
    return digest.hexdigest()


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


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


@dataclass(frozen=True)
class _ValidatedEvent:
    sequence: int
    event_hash: str


class FredAlertAuditLog:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = Lock()

    def append_event(
        self,
        *,
        event_type: str,
        payload: dict[str, Any],
    ) -> str:
        if event_type.strip() == '':
            raise RuntimeError('event_type must be non-empty')

        self._path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            last_event = self._validate_existing_chain()
            sequence = 1 if last_event is None else last_event.sequence + 1
            previous_event_hash = None if last_event is None else last_event.event_hash
            event_payload: dict[str, Any] = {
                'sequence': sequence,
                'occurred_at': _utc_now_iso(),
                'event_type': event_type,
                'previous_event_hash': previous_event_hash,
                'payload': payload,
            }
            event_hash = _compute_event_hash(event_payload)
            event_payload['event_hash'] = event_hash
            with self._path.open('a', encoding='utf-8') as handle:
                handle.write(
                    json.dumps(
                        event_payload,
                        sort_keys=True,
                        separators=_JSON_DICT_SEPARATORS,
                        ensure_ascii=True,
                    )
                )
                handle.write('\n')
            return event_hash

    def _validate_existing_chain(self) -> _ValidatedEvent | None:
        if not self._path.exists():
            return None

        expected_previous_hash: str | None = None
        expected_sequence = 1
        last_event: _ValidatedEvent | None = None

        with self._path.open('r', encoding='utf-8') as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if line == '':
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        'Invalid JSON in FRED alert audit log at line '
                        f'{line_number}: {exc.msg}'
                    ) from exc
                event = _expect_dict(parsed, f'FRED alert audit event line {line_number}')
                event_hash = _expect_nullable_str(
                    event.get('event_hash'),
                    f'FRED alert audit event line {line_number}.event_hash',
                )
                if event_hash is None:
                    raise RuntimeError(
                        f'FRED alert audit event line {line_number} missing event_hash'
                    )
                sequence = _expect_int(
                    event.get('sequence'),
                    f'FRED alert audit event line {line_number}.sequence',
                )
                previous_event_hash = _expect_nullable_str(
                    event.get('previous_event_hash'),
                    f'FRED alert audit event line {line_number}.previous_event_hash',
                )
                if sequence != expected_sequence:
                    raise RuntimeError(
                        'FRED alert audit event sequence mismatch at line '
                        f'{line_number}: expected={expected_sequence} got={sequence}'
                    )
                if previous_event_hash != expected_previous_hash:
                    raise RuntimeError(
                        'FRED alert audit chain mismatch at line '
                        f'{line_number}: expected_previous_hash={expected_previous_hash} '
                        f'got={previous_event_hash}'
                    )
                unsigned_event = dict(event)
                del unsigned_event['event_hash']
                recomputed_hash = _compute_event_hash(unsigned_event)
                if recomputed_hash != event_hash:
                    raise RuntimeError(
                        'FRED alert audit hash mismatch at line '
                        f'{line_number}: expected={event_hash} got={recomputed_hash}'
                    )
                expected_sequence += 1
                expected_previous_hash = event_hash
                last_event = _ValidatedEvent(sequence=sequence, event_hash=event_hash)

        return last_event


_fred_alert_audit_singleton: FredAlertAuditLog | None = None


def get_fred_alert_audit_log() -> FredAlertAuditLog:
    global _fred_alert_audit_singleton
    if _fred_alert_audit_singleton is None:
        raw_path = _require_env('ORIGO_FRED_ALERT_AUDIT_LOG_PATH')
        _fred_alert_audit_singleton = FredAlertAuditLog(path=_resolve_path(raw_path))
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
