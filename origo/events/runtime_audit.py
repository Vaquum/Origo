from __future__ import annotations

from pathlib import Path
from typing import Any

from origo.audit import (
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
