from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Final, cast

_JSON_SEPARATORS: Final[tuple[str, str]] = (',', ':')
_MIN_RETENTION_DAYS: Final[int] = 365
_STATE_SCHEMA_VERSION: Final[int] = 1
_RESERVED_EVENT_KEYS: Final[frozenset[str]] = frozenset(
    {
        'event_hash',
        'event_type',
        'occurred_at',
        'payload',
        'previous_event_hash',
        'sequence',
    }
)


def require_non_empty_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def resolve_required_path_env(name: str) -> Path:
    raw_path = require_non_empty_env(name)
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def load_audit_log_retention_days() -> int:
    raw_days = require_non_empty_env('ORIGO_AUDIT_LOG_RETENTION_DAYS')
    try:
        retention_days = int(raw_days)
    except ValueError as exc:
        raise RuntimeError(
            'ORIGO_AUDIT_LOG_RETENTION_DAYS must be an integer'
        ) from exc
    if retention_days < _MIN_RETENTION_DAYS:
        raise RuntimeError(
            'ORIGO_AUDIT_LOG_RETENTION_DAYS must be >= '
            f'{_MIN_RETENTION_DAYS} (1-year minimum retention)'
        )
    return retention_days


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _compute_event_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=_JSON_SEPARATORS,
        ensure_ascii=True,
    )
    digest = hashlib.sha256()
    digest.update(canonical.encode('utf-8'))
    return digest.hexdigest()


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in cast(dict[Any, Any], value).items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_int(value: Any, label: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f'{label} must be an integer')
    return value


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be a string')
    if value.strip() == '':
        raise RuntimeError(f'{label} must be non-empty')
    return value


def _expect_nullable_non_empty_str(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be a string or null')
    if value == '':
        raise RuntimeError(f'{label} must be non-empty when set')
    return value


def _parse_utc_timestamp(value: Any, label: str) -> datetime:
    raw_value = _expect_non_empty_str(value, label)
    normalized = raw_value
    if raw_value.endswith('Z'):
        normalized = f'{raw_value[:-1]}+00:00'
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be ISO timestamp') from exc
    if parsed.tzinfo is None:
        raise RuntimeError(f'{label} must include timezone')
    return parsed.astimezone(UTC)


def _normalize_attributes(attributes: dict[str, Any] | None) -> dict[str, Any]:
    if attributes is None:
        return {}
    normalized = _expect_dict(attributes, 'Immutable audit attributes')
    for key, value in normalized.items():
        if key.strip() == '':
            raise RuntimeError('Immutable audit attributes cannot have empty keys')
        if key in _RESERVED_EVENT_KEYS:
            raise RuntimeError(f'Immutable audit attribute key is reserved: {key}')
        if isinstance(value, str) and value == '':
            raise RuntimeError(
                f'Immutable audit attribute {key} must be non-empty when set'
            )
    return normalized


@dataclass(frozen=True)
class _ChainState:
    first_event_occurred_at: datetime | None
    last_sequence: int
    last_event_hash: str | None
    captured_sequence_hash: str | None


@dataclass(frozen=True)
class _AuditState:
    sink_name: str
    log_path: str
    retention_days: int
    first_event_occurred_at: datetime
    max_sequence: int
    max_event_hash: str


@dataclass(frozen=True)
class ImmutableAuditAppendInput:
    event_type: str
    payload: dict[str, Any]
    attributes: dict[str, Any] | None = None


class ImmutableAuditLog:
    def __init__(
        self,
        *,
        path: Path,
        sink_name: str,
        retention_days: int,
    ) -> None:
        if sink_name.strip() == '':
            raise RuntimeError('sink_name must be non-empty')
        if retention_days < _MIN_RETENTION_DAYS:
            raise RuntimeError(
                'retention_days must be >= '
                f'{_MIN_RETENTION_DAYS} (1-year minimum retention)'
            )
        self._path = path
        self._sink_name = sink_name
        self._retention_days = retention_days
        self._state_path = Path(f'{path}.state.json')
        self._lock = Lock()
        self._chain_state_cache: _ChainState | None = None

    @property
    def state_path(self) -> Path:
        return self._state_path

    def append_event(
        self,
        *,
        event_type: str,
        payload: dict[str, Any],
        attributes: dict[str, Any] | None = None,
    ) -> str:
        return self.append_events(
            [
                ImmutableAuditAppendInput(
                    event_type=event_type,
                    payload=payload,
                    attributes=attributes,
                )
            ]
        )[0]

    def append_events(
        self,
        events: list[ImmutableAuditAppendInput],
    ) -> list[str]:
        if events == []:
            raise RuntimeError('events must contain at least one entry')

        normalized_events: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for event in events:
            event_type = event.event_type.strip()
            if event_type == '':
                raise RuntimeError('event_type must be non-empty')
            normalized_payload = _expect_dict(event.payload, 'Immutable audit payload')
            normalized_attributes = _normalize_attributes(event.attributes)
            normalized_events.append(
                (event_type, normalized_payload, normalized_attributes)
            )

        self._path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            if self._chain_state_cache is None:
                state = self._load_state()
                capture_sequence = None if state is None else state.max_sequence
                chain_state = self._validate_existing_chain(
                    capture_sequence=capture_sequence
                )
                self._validate_state_against_chain(
                    state=state,
                    chain_state=chain_state,
                )
            else:
                chain_state = self._chain_state_cache

            sequence = chain_state.last_sequence
            previous_hash = chain_state.last_event_hash
            first_event_at = chain_state.first_event_occurred_at
            payloads_to_append: list[dict[str, Any]] = []
            event_hashes: list[str] = []
            for event_type, normalized_payload, normalized_attributes in normalized_events:
                sequence += 1
                event_payload: dict[str, Any] = {
                    'sequence': sequence,
                    'occurred_at': _utc_now_iso(),
                    'event_type': event_type,
                    **normalized_attributes,
                    'previous_event_hash': previous_hash,
                    'payload': normalized_payload,
                }
                event_hash = _compute_event_hash(event_payload)
                event_payload['event_hash'] = event_hash
                payloads_to_append.append(event_payload)
                event_hashes.append(event_hash)
                previous_hash = event_hash

                event_occurred_at = _parse_utc_timestamp(
                    event_payload['occurred_at'],
                    'Immutable audit append occurred_at',
                )
                if first_event_at is None:
                    first_event_at = event_occurred_at

            self._append_event_payloads(payloads_to_append)
            if first_event_at is None:
                raise RuntimeError('first_event_occurred_at missing after append')
            if previous_hash is None:
                raise RuntimeError('max_event_hash missing after append')

            self._write_state(
                first_event_occurred_at=first_event_at,
                max_sequence=sequence,
                max_event_hash=previous_hash,
            )
            self._chain_state_cache = _ChainState(
                first_event_occurred_at=first_event_at,
                last_sequence=sequence,
                last_event_hash=previous_hash,
                captured_sequence_hash=previous_hash,
            )
            return event_hashes

    def _append_event_payload(self, payload: dict[str, Any]) -> None:
        self._append_event_payloads([payload])

    def _append_event_payloads(self, payloads: list[dict[str, Any]]) -> None:
        if payloads == []:
            raise RuntimeError('payloads must contain at least one entry')
        with self._path.open('a', encoding='utf-8') as handle:
            for payload in payloads:
                handle.write(
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=_JSON_SEPARATORS,
                        ensure_ascii=True,
                    )
                )
                handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())

    def _validate_existing_chain(self, *, capture_sequence: int | None) -> _ChainState:
        if not self._path.exists():
            return _ChainState(
                first_event_occurred_at=None,
                last_sequence=0,
                last_event_hash=None,
                captured_sequence_hash=None,
            )

        expected_sequence = 1
        expected_previous_hash: str | None = None
        first_event_at: datetime | None = None
        last_sequence = 0
        last_event_hash: str | None = None
        captured_sequence_hash: str | None = None

        with self._path.open('r', encoding='utf-8') as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if line == '':
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f'{self._sink_name} audit log line {line_number} has invalid JSON: '
                        f'{exc.msg}'
                    ) from exc

                event = _expect_dict(
                    parsed,
                    f'{self._sink_name} audit event line {line_number}',
                )
                event_hash = _expect_non_empty_str(
                    event.get('event_hash'),
                    f'{self._sink_name} audit event line {line_number}.event_hash',
                )
                sequence = _expect_int(
                    event.get('sequence'),
                    f'{self._sink_name} audit event line {line_number}.sequence',
                )
                previous_event_hash = _expect_nullable_non_empty_str(
                    event.get('previous_event_hash'),
                    f'{self._sink_name} audit event line '
                    f'{line_number}.previous_event_hash',
                )
                occurred_at = _parse_utc_timestamp(
                    event.get('occurred_at'),
                    f'{self._sink_name} audit event line {line_number}.occurred_at',
                )

                if sequence != expected_sequence:
                    raise RuntimeError(
                        f'{self._sink_name} audit sequence mismatch at line '
                        f'{line_number}: expected={expected_sequence} got={sequence}'
                    )
                if previous_event_hash != expected_previous_hash:
                    raise RuntimeError(
                        f'{self._sink_name} audit chain mismatch at line '
                        f'{line_number}: expected_previous_hash={expected_previous_hash} '
                        f'got={previous_event_hash}'
                    )

                unsigned_event = dict(event)
                del unsigned_event['event_hash']
                recomputed_hash = _compute_event_hash(unsigned_event)
                if recomputed_hash != event_hash:
                    raise RuntimeError(
                        f'{self._sink_name} audit hash mismatch at line '
                        f'{line_number}: expected={event_hash} got={recomputed_hash}'
                    )

                if first_event_at is None:
                    first_event_at = occurred_at
                last_sequence = sequence
                last_event_hash = event_hash
                expected_sequence += 1
                expected_previous_hash = event_hash

                if capture_sequence is not None and sequence == capture_sequence:
                    captured_sequence_hash = event_hash

        return _ChainState(
            first_event_occurred_at=first_event_at,
            last_sequence=last_sequence,
            last_event_hash=last_event_hash,
            captured_sequence_hash=captured_sequence_hash,
        )

    def _load_state(self) -> _AuditState | None:
        if not self._state_path.exists():
            return None

        raw_payload = self._state_path.read_text(encoding='utf-8')
        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f'{self._sink_name} audit state has invalid JSON: {exc.msg}'
            ) from exc
        payload = _expect_dict(parsed, f'{self._sink_name} audit state payload')

        schema_version = _expect_int(
            payload.get('schema_version'),
            f'{self._sink_name} audit state.schema_version',
        )
        if schema_version != _STATE_SCHEMA_VERSION:
            raise RuntimeError(
                f'{self._sink_name} audit state schema_version mismatch: '
                f'expected={_STATE_SCHEMA_VERSION} got={schema_version}'
            )

        sink_name = _expect_non_empty_str(
            payload.get('sink_name'),
            f'{self._sink_name} audit state.sink_name',
        )
        if sink_name != self._sink_name:
            raise RuntimeError(
                f'{self._sink_name} audit state sink_name mismatch: '
                f'expected={self._sink_name} got={sink_name}'
            )

        state_log_path = _expect_non_empty_str(
            payload.get('log_path'),
            f'{self._sink_name} audit state.log_path',
        )
        expected_log_path = str(self._path.resolve())
        if state_log_path != expected_log_path:
            raise RuntimeError(
                f'{self._sink_name} audit state log_path mismatch: '
                f'expected={expected_log_path} got={state_log_path}'
            )

        retention_days = _expect_int(
            payload.get('retention_days'),
            f'{self._sink_name} audit state.retention_days',
        )
        if retention_days < _MIN_RETENTION_DAYS:
            raise RuntimeError(
                f'{self._sink_name} audit state retention_days must be >= '
                f'{_MIN_RETENTION_DAYS}'
            )

        first_event_occurred_at = _parse_utc_timestamp(
            payload.get('first_event_occurred_at'),
            f'{self._sink_name} audit state.first_event_occurred_at',
        )
        max_sequence = _expect_int(
            payload.get('max_sequence'),
            f'{self._sink_name} audit state.max_sequence',
        )
        if max_sequence <= 0:
            raise RuntimeError(
                f'{self._sink_name} audit state.max_sequence must be > 0'
            )
        max_event_hash = _expect_non_empty_str(
            payload.get('max_event_hash'),
            f'{self._sink_name} audit state.max_event_hash',
        )

        return _AuditState(
            sink_name=sink_name,
            log_path=state_log_path,
            retention_days=retention_days,
            first_event_occurred_at=first_event_occurred_at,
            max_sequence=max_sequence,
            max_event_hash=max_event_hash,
        )

    def _validate_state_against_chain(
        self,
        *,
        state: _AuditState | None,
        chain_state: _ChainState,
    ) -> None:
        if state is None:
            return

        if state.retention_days != self._retention_days:
            raise RuntimeError(
                f'{self._sink_name} audit retention mismatch: '
                f'expected={self._retention_days} got={state.retention_days}'
            )
        if chain_state.last_sequence == 0:
            raise RuntimeError(
                f'{self._sink_name} audit log is empty but state exists'
            )
        if chain_state.captured_sequence_hash != state.max_event_hash:
            raise RuntimeError(
                f'{self._sink_name} audit mutation detected at sequence '
                f'{state.max_sequence}: expected_hash={state.max_event_hash} '
                f'got={chain_state.captured_sequence_hash}'
            )
        if chain_state.first_event_occurred_at is None:
            raise RuntimeError(
                f'{self._sink_name} audit chain missing first_event_occurred_at'
            )
        if chain_state.first_event_occurred_at > state.first_event_occurred_at:
            raise RuntimeError(
                f'{self._sink_name} audit retention violation: '
                'head truncation detected'
            )

    def _write_state(
        self,
        *,
        first_event_occurred_at: datetime,
        max_sequence: int,
        max_event_hash: str,
    ) -> None:
        payload = {
            'schema_version': _STATE_SCHEMA_VERSION,
            'sink_name': self._sink_name,
            'log_path': str(self._path.resolve()),
            'retention_days': self._retention_days,
            'first_event_occurred_at': first_event_occurred_at.isoformat(
                timespec='microseconds'
            ).replace('+00:00', 'Z'),
            'max_sequence': max_sequence,
            'max_event_hash': max_event_hash,
            'updated_at': _utc_now_iso(),
        }
        temp_path = Path(f'{self._state_path}.tmp')
        with temp_path.open('w', encoding='utf-8') as handle:
            handle.write(
                json.dumps(
                    payload,
                    sort_keys=True,
                    indent=2,
                    ensure_ascii=True,
                )
            )
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self._state_path)
