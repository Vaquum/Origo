from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from origo.audit import (
    ImmutableAuditAppendInput,
    ImmutableAuditLog,
    load_audit_log_retention_days,
    resolve_required_path_env,
)

from .ingest_state import CanonicalStreamKey

_RUNTIME_AUDIT_PATH_ENV = 'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'


@dataclass(frozen=True)
class CanonicalRuntimeIngestEvent:
    event_type: str
    source_offset_or_equivalent: str
    event_id: str
    payload_sha256_raw: str
    status: str
    run_id: str | None


@dataclass(frozen=True)
class _CanonicalRuntimeAuditScope:
    source_id: str
    stream_id: str
    partition_id: str


def _validate_scope_component(*, value: str, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be non-empty')
    invalid_separators = {os.sep}
    if os.altsep is not None:
        invalid_separators.add(os.altsep)
    for separator in invalid_separators:
        if separator != '' and separator in normalized:
            raise RuntimeError(
                f'{label} must not contain path separator {separator!r}: {value!r}'
            )
    if normalized in {'.', '..'}:
        raise RuntimeError(f'{label} must not be {normalized!r}')
    return normalized


class CanonicalRuntimeAuditLog:
    def __init__(self, *, path: Path, retention_days: int) -> None:
        self._path = path
        self._retention_days = retention_days
        self._scoped_root = path.parent / f'{path.name}.d'
        self._sink_cache: dict[_CanonicalRuntimeAuditScope, ImmutableAuditLog] = {}
        self._sink_cache_lock = Lock()

    @property
    def scoped_root(self) -> Path:
        return self._scoped_root

    def resolve_stream_log_path(self, *, stream_key: CanonicalStreamKey) -> Path:
        return self._resolve_scope_path(
            scope=_CanonicalRuntimeAuditScope(
                source_id=stream_key.source_id,
                stream_id=stream_key.stream_id,
                partition_id=stream_key.partition_id,
            )
        )

    def _resolve_scope_path(self, *, scope: _CanonicalRuntimeAuditScope) -> Path:
        source_id = _validate_scope_component(
            value=scope.source_id,
            label='runtime audit source_id',
        )
        stream_id = _validate_scope_component(
            value=scope.stream_id,
            label='runtime audit stream_id',
        )
        partition_id = _validate_scope_component(
            value=scope.partition_id,
            label='runtime audit partition_id',
        )
        return self._scoped_root / source_id / stream_id / partition_id / 'events.jsonl'

    def _get_sink_or_raise(
        self,
        *,
        scope: _CanonicalRuntimeAuditScope,
    ) -> ImmutableAuditLog:
        with self._sink_cache_lock:
            existing = self._sink_cache.get(scope)
            if existing is not None:
                return existing
            sink = ImmutableAuditLog(
                path=self._resolve_scope_path(scope=scope),
                sink_name='canonical_runtime',
                retention_days=self._retention_days,
            )
            self._sink_cache[scope] = sink
            return sink

    def _append_scope_events(
        self,
        *,
        scope: _CanonicalRuntimeAuditScope,
        audit_events: list[ImmutableAuditAppendInput],
    ) -> list[str]:
        return self._get_sink_or_raise(scope=scope).append_events(events=audit_events)

    def append_ingest_event(
        self,
        *,
        event_type: str,
        stream_key: CanonicalStreamKey,
        source_offset_or_equivalent: str,
        event_id: str,
        payload_sha256_raw: str,
        status: str,
        run_id: str | None,
    ) -> str:
        return self.append_ingest_events(
            stream_key=stream_key,
            events=[
                CanonicalRuntimeIngestEvent(
                    event_type=event_type,
                    source_offset_or_equivalent=source_offset_or_equivalent,
                    event_id=event_id,
                    payload_sha256_raw=payload_sha256_raw,
                    status=status,
                    run_id=run_id,
                )
            ],
        )[0]

    def append_ingest_events(
        self,
        *,
        events: list[dict[str, object]] | list[CanonicalRuntimeIngestEvent],
        stream_key: CanonicalStreamKey | None = None,
    ) -> list[str]:
        if events == []:
            return []
        first_event = events[0]
        if isinstance(first_event, CanonicalRuntimeIngestEvent):
            if stream_key is None:
                raise RuntimeError(
                    'stream_key is required when events are CanonicalRuntimeIngestEvent'
                )
            typed_events = [
                event for event in events if isinstance(event, CanonicalRuntimeIngestEvent)
            ]
            if len(typed_events) != len(events):
                raise RuntimeError(
                    'all events must be CanonicalRuntimeIngestEvent when stream_key is set'
                )
            scope = _CanonicalRuntimeAuditScope(
                source_id=stream_key.source_id,
                stream_id=stream_key.stream_id,
                partition_id=stream_key.partition_id,
            )
            audit_events: list[ImmutableAuditAppendInput] = []
            for event in typed_events:
                audit_events.append(
                    ImmutableAuditAppendInput(
                        event_type=event.event_type,
                        attributes={
                            'source_id': stream_key.source_id,
                            'stream_id': stream_key.stream_id,
                            'partition_id': stream_key.partition_id,
                            'run_id': event.run_id,
                        },
                        payload={
                            'source_offset_or_equivalent': event.source_offset_or_equivalent,
                            'event_id': event.event_id,
                            'payload_sha256_raw': event.payload_sha256_raw,
                            'status': event.status,
                        },
                    )
                )
            return self._append_scope_events(scope=scope, audit_events=audit_events)

        dict_events = [event for event in events if isinstance(event, dict)]
        if len(dict_events) != len(events):
            raise RuntimeError('all events must be dict[str, object] in legacy mode')
        grouped_events: dict[
            _CanonicalRuntimeAuditScope,
            list[tuple[int, ImmutableAuditAppendInput]],
        ] = {}
        hashes: list[str | None] = [None] * len(dict_events)
        for event_index, event in enumerate(dict_events):
            event_number = event_index + 1
            event_type = event.get('event_type')
            source_id = event.get('source_id')
            stream_id = event.get('stream_id')
            partition_id = event.get('partition_id')
            source_offset_or_equivalent = event.get('source_offset_or_equivalent')
            event_id = event.get('event_id')
            payload_sha256_raw = event.get('payload_sha256_raw')
            status = event.get('status')
            run_id = event.get('run_id')
            if not isinstance(event_type, str):
                raise RuntimeError(f'event #{event_number} event_type must be string')
            if not isinstance(source_id, str):
                raise RuntimeError(f'event #{event_number} source_id must be string')
            if not isinstance(stream_id, str):
                raise RuntimeError(f'event #{event_number} stream_id must be string')
            if not isinstance(partition_id, str):
                raise RuntimeError(f'event #{event_number} partition_id must be string')
            if not isinstance(source_offset_or_equivalent, str):
                raise RuntimeError(
                    f'event #{event_number} source_offset_or_equivalent must be string'
                )
            if not isinstance(event_id, str):
                raise RuntimeError(f'event #{event_number} event_id must be string')
            if not isinstance(payload_sha256_raw, str):
                raise RuntimeError(
                    f'event #{event_number} payload_sha256_raw must be string'
                )
            if not isinstance(status, str):
                raise RuntimeError(f'event #{event_number} status must be string')
            if run_id is not None and not isinstance(run_id, str):
                raise RuntimeError(
                    f'event #{event_number} run_id must be string when set'
                )

            scope = _CanonicalRuntimeAuditScope(
                source_id=source_id,
                stream_id=stream_id,
                partition_id=partition_id,
            )
            audit_event = ImmutableAuditAppendInput(
                event_type=event_type,
                attributes={
                    'source_id': source_id,
                    'stream_id': stream_id,
                    'partition_id': partition_id,
                    'run_id': run_id,
                },
                payload={
                    'source_offset_or_equivalent': source_offset_or_equivalent,
                    'event_id': event_id,
                    'payload_sha256_raw': payload_sha256_raw,
                    'status': status,
                },
            )
            existing_group = grouped_events.get(scope)
            if existing_group is None:
                grouped_events[scope] = [(event_index, audit_event)]
            else:
                existing_group.append((event_index, audit_event))

        for scope, grouped in grouped_events.items():
            scope_hashes = self._append_scope_events(
                scope=scope,
                audit_events=[event for _, event in grouped],
            )
            if len(scope_hashes) != len(grouped):
                raise RuntimeError(
                    'runtime audit append returned mismatched hash count '
                    f'for scope={scope}'
                )
            for (event_index, _), event_hash in zip(grouped, scope_hashes, strict=True):
                hashes[event_index] = event_hash

        if any(event_hash is None for event_hash in hashes):
            raise RuntimeError('runtime audit hash mapping incomplete')
        return [str(event_hash) for event_hash in hashes]

    def append_ingest_batch_event(
        self,
        *,
        stream_key: CanonicalStreamKey,
        event_type: str,
        run_id: str | None,
        batch_event_count: int,
        inserted_count: int,
        duplicate_count: int,
        first_source_offset_or_equivalent: str,
        last_source_offset_or_equivalent: str,
        first_event_id: str,
        last_event_id: str,
    ) -> str:
        if batch_event_count <= 0:
            raise RuntimeError('batch_event_count must be positive')
        if inserted_count < 0:
            raise RuntimeError('inserted_count must be non-negative')
        if duplicate_count < 0:
            raise RuntimeError('duplicate_count must be non-negative')
        if inserted_count + duplicate_count != batch_event_count:
            raise RuntimeError(
                'inserted_count + duplicate_count must equal batch_event_count'
            )
        if first_source_offset_or_equivalent.strip() == '':
            raise RuntimeError('first_source_offset_or_equivalent must be non-empty')
        if last_source_offset_or_equivalent.strip() == '':
            raise RuntimeError('last_source_offset_or_equivalent must be non-empty')
        if first_event_id.strip() == '':
            raise RuntimeError('first_event_id must be non-empty')
        if last_event_id.strip() == '':
            raise RuntimeError('last_event_id must be non-empty')

        scope = _CanonicalRuntimeAuditScope(
            source_id=stream_key.source_id,
            stream_id=stream_key.stream_id,
            partition_id=stream_key.partition_id,
        )
        return self._get_sink_or_raise(scope=scope).append_event(
            event_type=event_type,
            attributes={
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
                'run_id': run_id,
            },
            payload={
                'batch_event_count': batch_event_count,
                'inserted_count': inserted_count,
                'duplicate_count': duplicate_count,
                'first_source_offset_or_equivalent': first_source_offset_or_equivalent,
                'last_source_offset_or_equivalent': last_source_offset_or_equivalent,
                'first_event_id': first_event_id,
                'last_event_id': last_event_id,
            },
        )

    def append_projector_checkpoint_event(
        self,
        *,
        event_type: str,
        projector_id: str,
        stream_key: CanonicalStreamKey,
        run_id: str,
        checkpoint_revision: int,
        last_event_id: str,
        last_source_offset_or_equivalent: str,
        status: str,
        state: dict[str, Any],
    ) -> str:
        scope = _CanonicalRuntimeAuditScope(
            source_id=stream_key.source_id,
            stream_id=stream_key.stream_id,
            partition_id=stream_key.partition_id,
        )
        return self._get_sink_or_raise(scope=scope).append_event(
            event_type=event_type,
            attributes={
                'projector_id': projector_id,
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
                'run_id': run_id,
            },
            payload={
                'checkpoint_revision': checkpoint_revision,
                'last_event_id': last_event_id,
                'last_source_offset_or_equivalent': last_source_offset_or_equivalent,
                'status': status,
                'state': state,
            },
        )


_runtime_audit_singleton: CanonicalRuntimeAuditLog | None = None


def get_canonical_runtime_audit_log() -> CanonicalRuntimeAuditLog:
    global _runtime_audit_singleton
    if _runtime_audit_singleton is None:
        _runtime_audit_singleton = CanonicalRuntimeAuditLog(
            path=resolve_required_path_env(_RUNTIME_AUDIT_PATH_ENV),
            retention_days=load_audit_log_retention_days(),
        )
    return _runtime_audit_singleton
