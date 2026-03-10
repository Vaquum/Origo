from __future__ import annotations

from pathlib import Path
from typing import Any

from origo.audit import (
    ImmutableAuditLog,
    load_audit_log_retention_days,
    resolve_required_path_env,
)


class ScraperAuditLog:
    def __init__(self, *, path: Path, retention_days: int) -> None:
        self._sink = ImmutableAuditLog(
            path=path,
            sink_name='scraper',
            retention_days=retention_days,
        )

    def append_event(
        self,
        *,
        event_type: str,
        run_id: str,
        source_id: str | None,
        payload: dict[str, Any],
    ) -> str:
        if run_id.strip() == '':
            raise RuntimeError('run_id must be non-empty')
        if source_id is not None and source_id.strip() == '':
            raise RuntimeError('source_id must be non-empty when set')
        return self._sink.append_event(
            event_type=event_type,
            payload=payload,
            attributes={'run_id': run_id, 'source_id': source_id},
        )


_audit_log_singleton: ScraperAuditLog | None = None


def get_scraper_audit_log() -> ScraperAuditLog:
    global _audit_log_singleton
    if _audit_log_singleton is None:
        _audit_log_singleton = ScraperAuditLog(
            path=resolve_required_path_env('ORIGO_SCRAPER_AUDIT_LOG_PATH'),
            retention_days=load_audit_log_retention_days(),
        )
    return _audit_log_singleton
