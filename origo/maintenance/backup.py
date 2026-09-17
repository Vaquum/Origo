"""Verify an independently restored, consistent instance before first cleanup."""

import hashlib
import time
from pathlib import Path
from typing import Literal

from dagster import DagsterInstance, RunsFilter
from pydantic import BaseModel

from .archive import archive_path, read_image
from .protocol import Journal, OperationalMetadataMaintenanceConfig, manifest_sha256
from .run_storage import OrigoSqliteRunStorage
from .sqlite import Layout, artifacts, connection


class BackupReceipt(BaseModel):
    schema_version: Literal[1] = 1
    instance_id: str
    manifest_sha256: str
    snapshot_id: str
    restored_home: str
    verified_at: float
    expires_at: float
    verified_runs: int


def _separate_filesystems(restored_home: Path, production: Layout) -> bool:
    return restored_home.stat().st_dev != production.runs.stat().st_dev


def verify_restored_backup(
    restored_home: Path,
    production: Layout,
    journal: Journal,
    snapshot_id: str,
    deadline: float,
) -> BackupReceipt:
    if not snapshot_id.strip():
        raise ValueError('A consistent filesystem/storage snapshot identity is required.')
    restored_home = restored_home.resolve(strict=True)
    if not _separate_filesystems(restored_home, production):
        raise ValueError('The restored backup must not consume the production filesystem reserve.')
    with DagsterInstance.from_config(str(restored_home)) as restored:
        layout = Layout.from_instance(restored)
        restored_paths = [layout.runs, layout.events, layout.schedules, layout.compute]
        if layout.artifact_root is not None:
            restored_paths.append(layout.artifact_root)
        for path in restored_paths:
            if not path.is_relative_to(restored_home):
                raise ValueError(
                    'Restored Dagster configuration points outside the restored snapshot.'
                )
        if not isinstance(restored.run_storage, OrigoSqliteRunStorage):
            raise TypeError('Restored instance must use the configured Origo run storage.')
        if restored.run_storage.get_run_storage_id() != journal.instance_id:
            raise ValueError('Restored backup belongs to a different Dagster instance.')
        shared_paths = [layout.runs, layout.events, layout.schedules]
        packed = archive_path(layout.events.parent)
        if packed.exists():
            shared_paths.append(packed)
        outputs = Path(restored.storage_directory()) / '.origo-outputs.sqlite'
        if outputs.exists():
            shared_paths.append(outputs)
        for path in shared_paths:
            with connection(path, deadline, 1) as database:
                if [tuple(row) for row in database.execute('PRAGMA quick_check')] != [('ok',)]:
                    raise RuntimeError(f'Restored metadata failed quick_check: {path.name}')
        verified = 0
        for candidate in journal.manifest:
            if candidate.reason:
                continue
            records = restored.get_run_records(RunsFilter(run_ids=[candidate.run_id]), limit=1)
            if not records or records[0].dagster_run.status.value != candidate.status:
                raise RuntimeError(f'Restored run does not match the manifest: {candidate.run_id}')
            if (
                artifacts(production, candidate.run_id)[0].exists()
                or read_image(production.events.parent, candidate.run_id, deadline) is not None
            ):
                shard = artifacts(layout, candidate.run_id)[0]
                if shard.exists():
                    with connection(shard, deadline, 1) as database:
                        if [tuple(row) for row in database.execute('PRAGMA quick_check')] != [
                            ('ok',)
                        ]:
                            raise RuntimeError(
                                f'Restored run shard failed quick_check: {candidate.run_id}'
                            )
                live_events = restored.get_records_for_run(candidate.run_id, limit=1).records
                if not live_events:
                    raise RuntimeError(
                        f'Restored shard has no execution evidence: {candidate.run_id}'
                    )
            verified += 1
    now = time.time()
    return BackupReceipt(
        instance_id=journal.instance_id,
        manifest_sha256=journal.manifest_sha256,
        snapshot_id=snapshot_id,
        restored_home=str(restored_home),
        verified_at=now,
        expires_at=now + 30 * 86400,
        verified_runs=verified,
    )


def require_backup(
    journal: Journal, config: OperationalMetadataMaintenanceConfig, now: float
) -> None:
    if journal.first_apply_completed or (
        journal.backup_sha256 and any(row.phase != 'planned' for row in journal.manifest)
    ):
        return
    if not config.backup_receipt or not config.approved_manifest_sha256:
        raise RuntimeError(
            'First apply requires the exact approved dry-run manifest and a verified restore receipt.'
        )
    if not journal.inventory_complete or journal.retained_floor_bytes <= 0:
        raise RuntimeError(
            'Complete the bounded inventory before the first apply.'
        )
    # An instance whose health check fails must still be allowed to reclaim history.
    # The pre-compaction floor includes reusable shared SQLite pages.
    if manifest_sha256(journal) != journal.manifest_sha256:
        raise RuntimeError('The dry-run manifest changed after it was approved.')
    payload = Path(config.backup_receipt).read_bytes()
    receipt = BackupReceipt.model_validate_json(payload)
    if not (
        receipt.instance_id == journal.instance_id
        and receipt.manifest_sha256 == journal.manifest_sha256 == config.approved_manifest_sha256
        and receipt.verified_at <= now < receipt.expires_at <= receipt.verified_at + 30 * 86400
        and receipt.verified_runs == sum(not candidate.reason for candidate in journal.manifest)
        and receipt.verified_runs > 0
    ):
        raise RuntimeError(
            'The restore receipt is expired or does not cover this instance and manifest.'
        )
    journal.backup_verified_until = receipt.expires_at
    journal.backup_sha256 = hashlib.sha256(payload).hexdigest()
