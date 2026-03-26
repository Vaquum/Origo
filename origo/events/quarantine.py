from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from origo.pathing import resolve_repo_relative_path

from .errors import StreamQuarantineError

if TYPE_CHECKING:
    from .ingest_state import CanonicalStreamKey

_QUARANTINE_PATH_ENV = 'ORIGO_STREAM_QUARANTINE_STATE_PATH'


def load_stream_quarantine_state_path() -> Path:
    raw_value = os.environ.get(_QUARANTINE_PATH_ENV)
    if raw_value is None or raw_value.strip() == '':
        raise StreamQuarantineError(
            code='QUARANTINE_PATH_ENV_MISSING',
            message=f'{_QUARANTINE_PATH_ENV} must be set and non-empty',
        )
    return resolve_repo_relative_path(raw_value)


@dataclass(frozen=True)
class QuarantinedStreamState:
    stream_key: CanonicalStreamKey
    quarantined_at_utc: datetime
    run_id: str
    reason: str
    gap_details: dict[str, Any]


class StreamQuarantineRegistryProtocol(Protocol):
    def get(self, *, stream_key: CanonicalStreamKey) -> QuarantinedStreamState | None: ...

    def assert_not_quarantined(self, *, stream_key: CanonicalStreamKey) -> None: ...

    def quarantine(
        self,
        *,
        stream_key: CanonicalStreamKey,
        run_id: str,
        reason: str,
        gap_details: dict[str, Any],
        quarantined_at_utc: datetime,
    ) -> QuarantinedStreamState: ...


def _stream_key_id(stream_key: CanonicalStreamKey) -> str:
    return (
        f'{stream_key.source_id.strip()}|'
        f'{stream_key.stream_id.strip()}|'
        f'{stream_key.partition_id.strip()}'
    )


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise StreamQuarantineError(
            code='QUARANTINE_INVALID_INPUT',
            message=f'{field_name} must be non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise StreamQuarantineError(
            code='QUARANTINE_INVALID_INPUT',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


class NoopStreamQuarantineRegistry:
    def get(self, *, stream_key: CanonicalStreamKey) -> QuarantinedStreamState | None:
        return None

    def assert_not_quarantined(self, *, stream_key: CanonicalStreamKey) -> None:
        return None

    def quarantine(
        self,
        *,
        stream_key: CanonicalStreamKey,
        run_id: str,
        reason: str,
        gap_details: dict[str, Any],
        quarantined_at_utc: datetime,
    ) -> QuarantinedStreamState:
        return QuarantinedStreamState(
            stream_key=stream_key,
            quarantined_at_utc=_ensure_utc(
                quarantined_at_utc,
                field_name='quarantined_at_utc',
            ),
            run_id=_require_non_empty(run_id, field_name='run_id'),
            reason=_require_non_empty(reason, field_name='reason'),
            gap_details=gap_details,
        )


class StreamQuarantineRegistry:
    def __init__(self, *, path: Path) -> None:
        self._path = path

    def get(self, *, stream_key: CanonicalStreamKey) -> QuarantinedStreamState | None:
        payload = self._load_state_payload()
        state_raw = payload.get(_stream_key_id(stream_key))
        if state_raw is None:
            return None
        if not isinstance(state_raw, dict):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='Quarantine state entry must be an object',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        return self._parse_state(
            stream_key=stream_key,
            state_raw=cast(dict[str, Any], state_raw),
        )

    def assert_not_quarantined(self, *, stream_key: CanonicalStreamKey) -> None:
        state = self.get(stream_key=stream_key)
        if state is None:
            return
        raise StreamQuarantineError(
            code='STREAM_QUARANTINED',
            message=(
                'Stream is quarantined and cannot accept writes: '
                f'{stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
            ),
            context={
                'quarantined_at_utc': state.quarantined_at_utc.isoformat(),
                'run_id': state.run_id,
                'reason': state.reason,
                'gap_details': state.gap_details,
            },
        )

    def quarantine(
        self,
        *,
        stream_key: CanonicalStreamKey,
        run_id: str,
        reason: str,
        gap_details: dict[str, Any],
        quarantined_at_utc: datetime,
    ) -> QuarantinedStreamState:
        normalized_run_id = _require_non_empty(run_id, field_name='run_id')
        normalized_reason = _require_non_empty(reason, field_name='reason')
        normalized_at = _ensure_utc(
            quarantined_at_utc,
            field_name='quarantined_at_utc',
        )

        payload = self._load_state_payload()
        stream_id = _stream_key_id(stream_key)
        if stream_id in payload:
            existing_raw = payload[stream_id]
            if not isinstance(existing_raw, dict):
                raise StreamQuarantineError(
                    code='QUARANTINE_STATE_CORRUPT',
                    message='Quarantine state entry must be an object',
                    context={'stream_key': stream_id},
                )
            return self._parse_state(
                stream_key=stream_key,
                state_raw=cast(dict[str, Any], existing_raw),
            )

        payload[stream_id] = {
            'quarantined_at_utc': normalized_at.isoformat(),
            'run_id': normalized_run_id,
            'reason': normalized_reason,
            'gap_details': gap_details,
        }
        self._write_state_payload(payload)
        return QuarantinedStreamState(
            stream_key=stream_key,
            quarantined_at_utc=normalized_at,
            run_id=normalized_run_id,
            reason=normalized_reason,
            gap_details=gap_details,
        )

    def _load_state_payload(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message=f'Invalid JSON in quarantine state file: {exc.msg}',
                context={'path': str(self._path)},
            ) from exc
        if not isinstance(raw, dict):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='Quarantine state file root must be an object',
                context={'path': str(self._path)},
            )
        return cast(dict[str, Any], raw)

    def _write_state_payload(self, payload: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f'{self._path.suffix}.tmp')
        tmp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + '\n',
            encoding='utf-8',
        )
        tmp_path.replace(self._path)

    def _parse_state(
        self,
        *,
        stream_key: CanonicalStreamKey,
        state_raw: dict[str, Any],
    ) -> QuarantinedStreamState:
        quarantined_at_raw = state_raw.get('quarantined_at_utc')
        run_id_raw = state_raw.get('run_id')
        reason_raw = state_raw.get('reason')
        gap_details_raw = state_raw.get('gap_details')
        if not isinstance(quarantined_at_raw, str):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='quarantined_at_utc must be a string',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        if not isinstance(run_id_raw, str):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='run_id must be a string',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        if not isinstance(reason_raw, str):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='reason must be a string',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        if not isinstance(gap_details_raw, dict):
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='gap_details must be an object',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        normalized_timestamp = quarantined_at_raw
        if quarantined_at_raw.endswith('Z'):
            normalized_timestamp = f'{quarantined_at_raw[:-1]}+00:00'
        try:
            parsed_at = datetime.fromisoformat(normalized_timestamp)
        except ValueError as exc:
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='quarantined_at_utc must be ISO timestamp',
                context={'stream_key': _stream_key_id(stream_key)},
            ) from exc
        if parsed_at.tzinfo is None:
            raise StreamQuarantineError(
                code='QUARANTINE_STATE_CORRUPT',
                message='quarantined_at_utc must include timezone',
                context={'stream_key': _stream_key_id(stream_key)},
            )
        return QuarantinedStreamState(
            stream_key=stream_key,
            quarantined_at_utc=parsed_at.astimezone(UTC),
            run_id=run_id_raw,
            reason=reason_raw,
            gap_details=cast(dict[str, Any], gap_details_raw),
        )
