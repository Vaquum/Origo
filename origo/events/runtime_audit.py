from __future__ import annotations

from pathlib import Path
from typing import Any

from origo.audit import (
    ImmutableAuditAppendInput,
    ImmutableAuditLog,
    load_audit_log_retention_days,
    resolve_required_path_env,
)

from .ingest_state import CanonicalStreamKey

_RUNTIME_AUDIT_PATH_ENV = 'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'


class CanonicalRuntimeAuditLog:
    def __init__(self, *, path: Path, retention_days: int) -> None:
        self._sink = ImmutableAuditLog(
            path=path,
            sink_name='canonical_runtime',
            retention_days=retention_days,
        )

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
        return self._sink.append_event(
            event_type=event_type,
            attributes={
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
                'run_id': run_id,
            },
            payload={
                'source_offset_or_equivalent': source_offset_or_equivalent,
                'event_id': event_id,
                'payload_sha256_raw': payload_sha256_raw,
                'status': status,
            },
        )

    def append_ingest_events(self, *, events: list[dict[str, object]]) -> list[str]:
        if events == []:
            return []

        audit_events: list[ImmutableAuditAppendInput] = []
        for index, event in enumerate(events, start=1):
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
                raise RuntimeError(f'events[{index}].event_type must be string')
            if not isinstance(source_id, str):
                raise RuntimeError(f'events[{index}].source_id must be string')
            if not isinstance(stream_id, str):
                raise RuntimeError(f'events[{index}].stream_id must be string')
            if not isinstance(partition_id, str):
                raise RuntimeError(f'events[{index}].partition_id must be string')
            if not isinstance(source_offset_or_equivalent, str):
                raise RuntimeError(
                    f'events[{index}].source_offset_or_equivalent must be string'
                )
            if not isinstance(event_id, str):
                raise RuntimeError(f'events[{index}].event_id must be string')
            if not isinstance(payload_sha256_raw, str):
                raise RuntimeError(
                    f'events[{index}].payload_sha256_raw must be string'
                )
            if not isinstance(status, str):
                raise RuntimeError(f'events[{index}].status must be string')
            if run_id is not None and not isinstance(run_id, str):
                raise RuntimeError(f'events[{index}].run_id must be string when set')

            audit_events.append(
                ImmutableAuditAppendInput(
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
            )

        return self._sink.append_events(events=audit_events)

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
        return self._sink.append_event(
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
