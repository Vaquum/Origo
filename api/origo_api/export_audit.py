from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, cast

_JSON_DICT_SEPARATORS = (',', ':')


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
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


@dataclass(frozen=True)
class _ValidatedEvent:
    sequence: int
    event_hash: str


class ExportAuditLog:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = Lock()

    def append_event(
        self,
        *,
        event_type: str,
        export_id: str | None,
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
                'export_id': export_id,
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
                        f'Invalid JSON in export audit log at line {line_number}: {exc.msg}'
                    ) from exc
                event = _expect_dict(parsed, f'Export audit event line {line_number}')
                event_hash = _expect_nullable_str(
                    event.get('event_hash'),
                    f'Export audit event line {line_number}.event_hash',
                )
                if event_hash is None:
                    raise RuntimeError(
                        f'Export audit event line {line_number} missing event_hash'
                    )
                sequence = _expect_int(
                    event.get('sequence'),
                    f'Export audit event line {line_number}.sequence',
                )
                previous_event_hash = _expect_nullable_str(
                    event.get('previous_event_hash'),
                    f'Export audit event line {line_number}.previous_event_hash',
                )

                if sequence != expected_sequence:
                    raise RuntimeError(
                        'Export audit event sequence mismatch at line '
                        f'{line_number}: expected={expected_sequence} got={sequence}'
                    )
                if previous_event_hash != expected_previous_hash:
                    raise RuntimeError(
                        'Export audit chain mismatch at line '
                        f'{line_number}: expected_previous_hash={expected_previous_hash} '
                        f'got={previous_event_hash}'
                    )

                unsigned_event = dict(event)
                del unsigned_event['event_hash']
                recomputed_hash = _compute_event_hash(unsigned_event)
                if recomputed_hash != event_hash:
                    raise RuntimeError(
                        'Export audit hash mismatch at line '
                        f'{line_number}: expected={event_hash} got={recomputed_hash}'
                    )

                expected_sequence += 1
                expected_previous_hash = event_hash
                last_event = _ValidatedEvent(sequence=sequence, event_hash=event_hash)

        return last_event


_audit_log_singleton: ExportAuditLog | None = None


def get_export_audit_log() -> ExportAuditLog:
    global _audit_log_singleton
    if _audit_log_singleton is None:
        raw_path = _require_env('ORIGO_EXPORT_AUDIT_LOG_PATH')
        path = Path(raw_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        _audit_log_singleton = ExportAuditLog(path=path)
    return _audit_log_singleton
