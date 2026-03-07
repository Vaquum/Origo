from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, cast

_JSON_SEPARATORS = (',', ':')


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
    for key, item_value in raw_map.items():
        if not isinstance(key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[key] = item_value
    return normalized


def _expect_int(value: Any, label: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f'{label} must be an integer')
    return value


def _expect_nullable_str(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be string or null')
    if value == '':
        raise RuntimeError(f'{label} must be non-empty when set')
    return value


def _compute_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=_JSON_SEPARATORS,
        ensure_ascii=True,
    ).encode('utf-8')
    digest = hashlib.sha256()
    digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True)
class _ChainTip:
    sequence: int
    event_hash: str


class ScraperAuditLog:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = Lock()

    def append_event(
        self,
        *,
        event_type: str,
        run_id: str,
        source_id: str | None,
        payload: dict[str, Any],
    ) -> str:
        if event_type.strip() == '':
            raise RuntimeError('event_type must be non-empty')
        if run_id.strip() == '':
            raise RuntimeError('run_id must be non-empty')

        self._path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            tip = self._validate_chain()
            sequence = 1 if tip is None else tip.sequence + 1
            previous_hash = None if tip is None else tip.event_hash
            event_payload: dict[str, Any] = {
                'sequence': sequence,
                'occurred_at': _utc_now_iso(),
                'event_type': event_type,
                'run_id': run_id,
                'source_id': source_id,
                'previous_event_hash': previous_hash,
                'payload': payload,
            }
            event_hash = _compute_hash(event_payload)
            event_payload['event_hash'] = event_hash

            with self._path.open('a', encoding='utf-8') as file_obj:
                file_obj.write(
                    json.dumps(
                        event_payload,
                        sort_keys=True,
                        separators=_JSON_SEPARATORS,
                        ensure_ascii=True,
                    )
                )
                file_obj.write('\n')
            return event_hash

    def _validate_chain(self) -> _ChainTip | None:
        if not self._path.exists():
            return None

        expected_sequence = 1
        expected_previous_hash: str | None = None
        last_event: _ChainTip | None = None

        with self._path.open('r', encoding='utf-8') as file_obj:
            for line_number, raw_line in enumerate(file_obj, start=1):
                line = raw_line.strip()
                if line == '':
                    continue

                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f'Invalid JSON in scraper audit log line {line_number}: {exc.msg}'
                    ) from exc

                event = _expect_dict(parsed, f'Scraper audit event line {line_number}')
                sequence = _expect_int(
                    event.get('sequence'),
                    f'Scraper audit event line {line_number}.sequence',
                )
                previous_hash = _expect_nullable_str(
                    event.get('previous_event_hash'),
                    f'Scraper audit event line {line_number}.previous_event_hash',
                )
                event_hash = _expect_nullable_str(
                    event.get('event_hash'),
                    f'Scraper audit event line {line_number}.event_hash',
                )
                if event_hash is None:
                    raise RuntimeError(
                        f'Scraper audit event line {line_number} missing event_hash'
                    )

                if sequence != expected_sequence:
                    raise RuntimeError(
                        'Scraper audit sequence mismatch at line '
                        f'{line_number}: expected={expected_sequence} got={sequence}'
                    )
                if previous_hash != expected_previous_hash:
                    raise RuntimeError(
                        'Scraper audit chain mismatch at line '
                        f'{line_number}: expected_previous_hash={expected_previous_hash} '
                        f'got={previous_hash}'
                    )

                unsigned_event = dict(event)
                del unsigned_event['event_hash']
                recomputed = _compute_hash(unsigned_event)
                if recomputed != event_hash:
                    raise RuntimeError(
                        'Scraper audit hash mismatch at line '
                        f'{line_number}: expected={event_hash} got={recomputed}'
                    )

                expected_sequence += 1
                expected_previous_hash = event_hash
                last_event = _ChainTip(sequence=sequence, event_hash=event_hash)

        return last_event


_audit_log_singleton: ScraperAuditLog | None = None


def get_scraper_audit_log() -> ScraperAuditLog:
    global _audit_log_singleton
    if _audit_log_singleton is None:
        raw_path = _require_env('ORIGO_SCRAPER_AUDIT_LOG_PATH')
        path = Path(raw_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        _audit_log_singleton = ScraperAuditLog(path=path)
    return _audit_log_singleton
