from __future__ import annotations

from pathlib import Path
from typing import Any

from origo.audit import (
    ImmutableAuditLog,
    load_audit_log_retention_days,
    resolve_required_path_env,
)


class ExportAuditLog:
    def __init__(self, *, path: Path, retention_days: int) -> None:
        self._sink = ImmutableAuditLog(
            path=path,
            sink_name='export',
            retention_days=retention_days,
        )

    def append_event(
        self,
        *,
        event_type: str,
        export_id: str | None,
        payload: dict[str, Any],
    ) -> str:
        if export_id is not None and export_id.strip() == '':
            raise RuntimeError('export_id must be non-empty when set')
        return self._sink.append_event(
            event_type=event_type,
            payload=payload,
            attributes={'export_id': export_id},
        )


_audit_log_singleton: ExportAuditLog | None = None


def get_export_audit_log() -> ExportAuditLog:
    global _audit_log_singleton
    if _audit_log_singleton is None:
        _audit_log_singleton = ExportAuditLog(
            path=resolve_required_path_env('ORIGO_EXPORT_AUDIT_LOG_PATH'),
            retention_days=load_audit_log_retention_days(),
        )
    return _audit_log_singleton
