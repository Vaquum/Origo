"""Versioned, bounded maintenance journal; never a second execution-history store."""

import hashlib
import os
from pathlib import Path
from typing import Literal

from dagster import Config
from pydantic import BaseModel, Field


class OperationalMetadataMaintenanceConfig(Config):
    dry_run: bool = True
    projection_success_minutes: int = Field(default=1, ge=1)
    projection_failure_hours: int = Field(default=24, ge=1)
    source_archive_after_hours: int = Field(default=1, ge=1)
    diagnostic_retention_days: int = Field(default=14, ge=14, le=14)
    max_runs_per_batch: int = Field(default=500, ge=1, le=500)
    max_runtime_seconds: int = Field(default=60, ge=10, le=3600)
    lock_wait_seconds: float = Field(default=1.0, ge=0, le=5)
    metadata_budget_bytes: int = Field(gt=0)
    backup_receipt: str = ''
    approved_manifest_sha256: str = ''
    diagnostic_max_partition_bytes: int = Field(default=4 * 1024**3, gt=0)
    diagnostic_min_free_bytes: int = Field(default=20 * 1024**3, gt=0)
    diagnostic_max_lag_seconds: int = Field(default=86400, ge=3600)


class Candidate(BaseModel):
    run_id: str
    storage_id: int
    status: str
    ended_at: float
    allocated_bytes: int
    artifacts: dict[str, int] = Field(default_factory=dict)
    reason: str = Field(default='', frozen=True)
    revalidation_reason: str = ''
    action: Literal['retire', 'archive'] = 'retire'
    phase: Literal['planned', 'deleting', 'logs', 'reclaimed'] = 'planned'

    @property
    def exclusion(self) -> str:
        return self.revalidation_reason or self.reason


class Report(BaseModel):
    observed_at: float
    dry_run: bool = True
    eligible_ingress_runs: int = 0
    ingress_runs_per_second: float = 0
    cleanup_runs_per_second: float = 0
    active_cleanup_runs_per_second: float = 0
    scanned: int = 0
    candidates: int = 0
    protected: int = 0
    deleted: int = 0
    archived: int = 0
    reclaimed_bytes: int = 0
    allocated_bytes: int = 0
    backlog_runs: int = 0
    duration_seconds: float = 0
    exclusions: dict[str, int] = Field(default_factory=dict)


class Journal(BaseModel):
    schema_version: Literal[1] = 1
    instance_id: str
    policy_sha256: str
    scan_cursor: int = 0
    inventory_upper_id: int = 0
    inventory_started_at: float = 0
    inventory_eligible_bytes: int = 0
    inventory_scanned: int = 0
    inventory_complete: bool = False
    retained_floor_bytes: int = 0
    last_success_at: float = 0
    state_cursor: int = 0
    manifest: list[Candidate] = Field(default_factory=lambda: list[Candidate](), max_length=500)
    manifest_sha256: str = ''
    manifest_created_at: float = 0
    backup_verified_until: float = 0
    backup_sha256: str = ''
    first_apply_completed: bool = False
    reports: list[Report] = Field(default_factory=lambda: list[Report](), max_length=32)


def policy_sha256(config: OperationalMetadataMaintenanceConfig) -> str:
    value = f'3:{config.projection_success_minutes}:{config.projection_failure_hours}:{config.source_archive_after_hours}:{config.diagnostic_retention_days}'
    return hashlib.sha256(value.encode()).hexdigest()


def manifest_sha256(journal: Journal) -> str:
    value = '\n'.join(
        [journal.instance_id, journal.policy_sha256, str(journal.manifest_created_at)]
        + [
            f'{row.action}:{row.run_id}:{row.storage_id}:{row.status}:{row.ended_at}:{row.allocated_bytes}:{row.reason}:{sorted(row.artifacts.items())}'
            for row in journal.manifest
        ]
    )
    return hashlib.sha256(value.encode()).hexdigest()


def save_journal(path: Path, journal: Journal) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as handle:
        handle.write(journal.model_dump_json(indent=2))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
