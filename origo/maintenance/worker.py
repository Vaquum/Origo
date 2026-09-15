"""Interruptible worker: Dagit remains the log writer outside maintenance locks."""

import argparse
import json
import os
import stat
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

from dagster import DagsterInstance
from dagster._core.instance.ref import InstanceRef
from dagster._serdes import deserialize_value
from pydantic import BaseModel, Field

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from . import roles
from .archive import archive_path, failed_compaction_count, failed_compactions
from .backup import require_backup, verify_restored_backup
from .clickhouse import diagnostic_allocated_bytes, maintain_diagnostics
from .compaction import incremental_compaction
from .event_storage import OrigoSqliteEventLogStorage
from .protocol import (
    Journal,
    OperationalMetadataMaintenanceConfig,
    Report,
    manifest_sha256,
    policy_sha256,
    save_journal,
)
from .retention import live_sensor_states, reclaim, scan_batch
from .run_storage import OrigoSqliteRunStorage
from .sqlite import (
    Layout,
    MaintenanceDeadlineReached,
    connection,
    maintenance_lock,
    query_latencies,
    refresh_statistics,
    shared_bytes,
)


class Outcome(BaseModel):
    report: Report
    journal_path: str
    manifest_sha256: str
    shared_sqlite: dict[str, dict[str, int]] = Field(default_factory=dict)
    query_p95_seconds: dict[str, float] = Field(default_factory=dict)
    dagster_allocated_bytes: int = 0
    clickhouse_business_bytes: int = 0
    dagster_to_business_fraction: float | None = None
    diagnostic_active_bytes: int = 0
    diagnostic_inactive_bytes: int = 0
    diagnostic_expired_part_bytes: int = 0
    retained_floor_bytes: int = 0
    inventory_complete: bool = False
    last_success_at: float = 0
    diagnostic_action: str = ''
    diagnostic_allocated_bytes: int = 0
    diagnostic_net_reclaimed_bytes: int = 0
    filesystem_free_bytes: int = 0
    source_archive_error_count: int = 0
    source_archive_errors: dict[str, str] = Field(default_factory=dict)
    violations: list[str] = Field(default_factory=lambda: list[str]())


def directory_bytes(layout: Layout, deadline: float) -> int:
    paths = {layout.runs.parent, layout.events.parent, layout.schedules.parent, layout.compute}
    if layout.artifact_root is not None:
        paths.add(layout.artifact_root)
    roots = sorted(
        path
        for path in paths
        if not any(path != other and path.is_relative_to(other) for other in paths)
    )
    if layout.artifact_root is not None and not layout.artifact_root.exists():
        paths.discard(layout.artifact_root)
        roots = [path for path in roots if path != layout.artifact_root]
    for path in paths:
        if not stat.S_ISDIR(path.lstat().st_mode):
            raise NotADirectoryError(f'Configured storage root is not a directory: {path}')
    seen: set[tuple[int, int]] = set()
    disappeared = 0

    def measure(path: Path) -> int:
        nonlocal disappeared
        if time.monotonic() >= deadline:
            raise TimeoutError('Allocated-byte measurement exceeded maintenance deadline.')
        result = 0
        try:
            metadata = path.lstat()
            if metadata.st_nlink > 1:
                identity = (metadata.st_dev, metadata.st_ino)
                if identity in seen:
                    return 0
                seen.add(identity)
            result = metadata.st_blocks * 512
            if stat.S_ISDIR(metadata.st_mode):
                with os.scandir(path) as entries:
                    for entry in entries:
                        result += measure(Path(entry.path))
        except FileNotFoundError:
            if path in paths:
                raise
            disappeared += 1
        return result

    total = sum(measure(root) for root in roots)
    if disappeared:
        print(
            f'Allocated-byte measurement: {disappeared} paths disappeared during the walk.',
            flush=True,
        )
    return total


def maintain(instance: DagsterInstance, config: OperationalMetadataMaintenanceConfig) -> Outcome:
    started = time.monotonic()
    deadline = started + config.max_runtime_seconds - 3
    reporting_reserve = min(
        300.0, max(15.0, config.max_runtime_seconds / 10), (deadline - started) / 2
    )
    work_deadline = deadline - reporting_reserve
    layout = Layout.from_instance(instance)
    if not isinstance(instance.run_storage, OrigoSqliteRunStorage) or not isinstance(
        instance.event_log_storage, OrigoSqliteEventLogStorage
    ):
        raise TypeError('Configure both Origo SQLite adapters before operational maintenance.')
    directory = layout.runs.parent / 'operational-maintenance'
    journal_path = directory / 'journal.json'
    with (
        maintenance_lock(directory / 'maintenance.lock', config.lock_wait_seconds),
        live_sensor_states(instance, layout, config, work_deadline) as sensor_states,
    ):
        instance_id = instance.run_storage.get_run_storage_id()
        policy = policy_sha256(config)
        journal = (
            Journal.model_validate_json(journal_path.read_bytes())
            if journal_path.exists()
            else Journal(instance_id=instance_id, policy_sha256=policy)
        )
        if journal.instance_id != instance_id:
            raise RuntimeError('Maintenance journal belongs to a different instance.')
        if journal.policy_sha256 != policy:
            if any(row.phase in ('deleting', 'logs') for row in journal.manifest):
                raise RuntimeError(
                    'Resume the interrupted batch with its original policy before changing retention.'
                )
            journal = Journal(instance_id=instance_id, policy_sha256=policy)
        report = Report(observed_at=time.time(), dry_run=config.dry_run)
        print('Refreshing bounded SQLite statistics.', flush=True)
        timings = refresh_statistics(
            (layout.runs, layout.events, layout.schedules), deadline, config.lock_wait_seconds
        )
        print('Statistics: ' + json.dumps(timings), flush=True)
        latencies = query_latencies(instance, deadline)
        violations = [
            f'job_query_p95:{job}:{seconds:.3f}s'
            for job, seconds in latencies.items()
            if seconds > 0.25
        ]
        if violations:
            raise RuntimeError('Query latency regression: ' + ', '.join(violations))
        before_bytes = directory_bytes(layout, deadline)
        print(f'Dagster allocated bytes before maintenance: {before_bytes}', flush=True)
        counts: Counter[str] = Counter()
        resumed_candidates = 0
        retirement_candidates = 0
        resumed_completions = 0
        inventory_held = (
            config.dry_run
            and journal.inventory_complete
            and not journal.first_apply_completed
            and any(not row.reason for row in journal.manifest)
        )
        if inventory_held:
            print(
                f'Completed inventory retained for first apply: manifest={journal.manifest_sha256}',
                flush=True,
            )
        save_journal(journal_path, journal)
        try:
            while not inventory_held and time.monotonic() < work_deadline:
                if (
                    config.dry_run
                    or not journal.manifest
                    or all(row.exclusion or row.phase == 'reclaimed' for row in journal.manifest)
                ):
                    batch = scan_batch(
                        instance, layout, journal, config, report.observed_at, work_deadline
                    )
                    if (
                        not config.dry_run
                        or not journal.manifest
                        or not any(not row.exclusion for row in journal.manifest)
                    ):
                        journal.manifest = batch
                        journal.manifest_created_at = time.time()
                        journal.manifest_sha256 = manifest_sha256(journal)
                    save_journal(journal_path, journal)
                else:
                    batch = journal.manifest
                report.scanned += len(batch)
                report.candidates += sum(not row.exclusion for row in batch)
                retirement_candidates += sum(
                    not row.exclusion and row.action == 'retire' for row in batch
                )
                report.protected += sum(bool(row.exclusion) for row in batch)
                counts.update(row.exclusion for row in batch if row.exclusion)
                print(
                    f'Manifest {journal.manifest_sha256}: scanned={len(journal.manifest)} eligible={sum(not row.exclusion for row in journal.manifest)} cursor={journal.scan_cursor}',
                    flush=True,
                )
                if not config.dry_run:
                    require_backup(journal, config, time.time())
                    for candidate in journal.manifest:
                        if candidate.exclusion or candidate.phase == 'reclaimed':
                            continue
                        already_retired = (
                            candidate.phase != 'planned'
                            and instance.get_run_by_id(candidate.run_id) is None
                        )
                        resumed_candidates += int(already_retired)
                        report.reclaimed_bytes += reclaim(
                            instance,
                            layout,
                            candidate,
                            journal,
                            journal_path,
                            config,
                            work_deadline,
                            sensor_states=sensor_states,
                        )
                        if not candidate.exclusion:
                            if candidate.action == 'archive':
                                report.archived += 1
                            else:
                                report.deleted += 1
                                resumed_completions += int(already_retired)
                        else:
                            report.protected += 1
                            report.candidates -= 1
                            retirement_candidates -= int(candidate.action == 'retire')
                            counts[candidate.exclusion] += 1
                    journal.first_apply_completed = (
                        journal.first_apply_completed or report.deleted + report.archived > 0
                    )
                    journal.state_cursor, scanned, compacted = (
                        instance.event_log_storage.compact_retired_state(
                            layout.runs,
                            journal.state_cursor,
                            config.max_runs_per_batch,
                            work_deadline,
                            config.lock_wait_seconds,
                        )
                    )
                    print(
                        f'Retained-state compaction: scanned={scanned} removed={compacted}',
                        flush=True,
                    )
                    save_journal(journal_path, journal)
                if journal.scan_cursor == 0:
                    break
        except MaintenanceDeadlineReached as error:
            journal = Journal.model_validate_json(journal_path.read_bytes())
            journal.first_apply_completed = (
                journal.first_apply_completed or report.deleted + report.archived > 0
            )
            print(f'Maintenance work window exhausted; reporting checkpoint: {error}', flush=True)
        report.exclusions = dict(counts)
        archive_errors = failed_compactions(layout.events.parent, deadline)
        archive_error_count = failed_compaction_count(layout.events.parent, deadline)
        if archive_error_count:
            violations.append(f'source_archive_failures:{archive_error_count}')
        with connection(layout.runs, deadline, config.lock_wait_seconds) as database:
            projection_names = sorted(roles.PROJECTION_JOBS - roles.SOURCE_JOBS)
            job_placeholders = ','.join('?' for _ in projection_names)
            report.backlog_runs = int(
                database.execute(
                    f'SELECT count(*) FROM runs WHERE pipeline_name IN ({job_placeholders}) '
                    "AND NOT EXISTS (SELECT 1 FROM run_tags t WHERE t.run_id=runs.run_id AND t.key='origo_source_key' AND length(t.value)>0) "
                    "AND ((status='SUCCESS' AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?) OR (status IN ('FAILURE','CANCELED') AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?))",
                    (
                        *projection_names,
                        time.time() - config.projection_success_minutes * 60,
                        time.time() - config.projection_failure_hours * 3600,
                    ),
                ).fetchone()[0]
            )
        shared_paths = [layout.runs, layout.events, layout.schedules]
        packed = archive_path(layout.events.parent)
        if packed.exists():
            shared_paths.append(packed)
        outputs = Path(instance.storage_directory()) / '.origo-outputs.sqlite'
        if outputs.exists():
            shared_paths.append(outputs)
        if not config.dry_run:
            for path in shared_paths:
                with connection(path, deadline, config.lock_wait_seconds) as database:
                    initialized = database.execute('PRAGMA auto_vacuum').fetchone()[0] == 2
                if not initialized:
                    violations.append(f'sqlite_compaction_not_initialized:{path.name}')
                elif time.monotonic() < work_deadline:
                    try:
                        report.reclaimed_bytes += incremental_compaction(
                            path, work_deadline, config.lock_wait_seconds
                        )
                    except MaintenanceDeadlineReached as error:
                        print(
                            f'SQLite page reclamation paused; reporting checkpoint: {error}',
                            flush=True,
                        )
        shared = {
            path.name: shared_bytes(path, deadline, config.lock_wait_seconds)
            for path in shared_paths
        }
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            business_rows = client.execute(
                'SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database=%(database)s',
                {'database': settings.database},
                settings={'max_execution_time': 5, 'max_threads': 1},
            )
            business_bytes = int(str(business_rows[0][0]))
            clickhouse_root = Path(
                os.environ.get('ORIGO_CLICKHOUSE_DATA_ROOT', '/opt/origo/clickhouse-data')
            )
            diagnostic_before = diagnostic_allocated_bytes(client, clickhouse_root, deadline)
            diagnostics = maintain_diagnostics(client, config, deadline)
            diagnostic_after = diagnostic_allocated_bytes(client, clickhouse_root, deadline)
        finally:
            client.disconnect()
        print('ClickHouse diagnostics: ' + json.dumps(asdict(diagnostics)), flush=True)
        # These are separate measures: exact retired artifact blocks and ClickHouse
        # server-reported active/inactive part bytes. TTL work is never called reclaimed.
        after_bytes = directory_bytes(layout, deadline)
        report.allocated_bytes = after_bytes + diagnostic_after
        if (
            config.dry_run
            and journal.inventory_complete
            and (not inventory_held or journal.retained_floor_bytes == 0)
        ):
            journal.retained_floor_bytes = max(0, after_bytes - journal.inventory_eligible_bytes)
            print(
                f'Inventory complete: runs={journal.inventory_scanned} conservative_retained_floor_bytes={journal.retained_floor_bytes}',
                flush=True,
            )
        violations.extend(f'diagnostic_ttl_drift:{table}' for table in diagnostics.drift)
        violations.extend(diagnostics.errors)
        if after_bytes * 10 >= business_bytes:
            violations.append(f'metadata_business_fraction:{after_bytes}/{business_bytes}>=0.1')
        if after_bytes > config.metadata_budget_bytes:
            violations.append(f'metadata_budget:{after_bytes}>{config.metadata_budget_bytes}')
        report.duration_seconds = time.monotonic() - started
        report.active_cleanup_runs_per_second = report.deleted / max(report.duration_seconds, 0.001)
        previous = next(
            (row for row in reversed(journal.reports) if row.dry_run == report.dry_run), None
        )
        if previous:
            interval = max(report.observed_at - previous.observed_at, 0.001)
            report.eligible_ingress_runs = max(
                0,
                report.backlog_runs - previous.backlog_runs + report.deleted - resumed_completions,
            )
            report.ingress_runs_per_second = report.eligible_ingress_runs / interval
            report.cleanup_runs_per_second = report.deleted / interval
            if (
                not config.dry_run
                and retirement_candidates > resumed_candidates
                and report.backlog_runs >= previous.backlog_runs > 0
                # A complete scan has handled its frozen cohort. Raw backlog
                # also counts policy holds and runs that became old afterward.
                and (
                    journal.scan_cursor != 0
                    or any(
                        row.action == 'retire' and not row.exclusion and row.phase != 'reclaimed'
                        for row in journal.manifest
                    )
                )
            ):
                violations.append('retention_backlog_not_decreasing')
        if not violations:
            journal.last_success_at = report.observed_at
        journal.reports = [
            *[row for row in journal.reports if row.observed_at >= time.time() - 30 * 86400][-31:],
            report,
        ]
        save_journal(journal_path, journal)
        return Outcome(
            report=report,
            journal_path=str(journal_path),
            manifest_sha256=journal.manifest_sha256,
            shared_sqlite=shared,
            query_p95_seconds=latencies,
            retained_floor_bytes=journal.retained_floor_bytes,
            inventory_complete=journal.inventory_complete,
            last_success_at=journal.last_success_at,
            dagster_allocated_bytes=after_bytes,
            clickhouse_business_bytes=business_bytes,
            dagster_to_business_fraction=after_bytes / business_bytes if business_bytes else None,
            diagnostic_active_bytes=diagnostics.active_bytes,
            diagnostic_inactive_bytes=diagnostics.inactive_bytes,
            diagnostic_expired_part_bytes=diagnostics.entirely_expired_bytes,
            diagnostic_action=diagnostics.scheduled_action,
            diagnostic_allocated_bytes=diagnostic_after,
            diagnostic_net_reclaimed_bytes=max(0, diagnostic_before - diagnostic_after),
            filesystem_free_bytes=os.statvfs(layout.runs).f_bavail
            * os.statvfs(layout.runs).f_frsize,
            source_archive_error_count=archive_error_count,
            source_archive_errors=archive_errors,
            violations=violations,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--instance-ref')
    parser.add_argument('--verify-restored-home')
    parser.add_argument('--snapshot-id', default='')
    args = parser.parse_args()
    config = OperationalMetadataMaintenanceConfig.model_validate_json(args.config)
    reference = (
        deserialize_value(Path(args.instance_ref).read_text(), InstanceRef)
        if args.instance_ref
        else None
    )
    with DagsterInstance.from_ref(reference) if reference else DagsterInstance.get() as instance:
        if args.verify_restored_home:
            layout = Layout.from_instance(instance)
            path = layout.runs.parent / 'operational-maintenance' / 'journal.json'
            journal = Journal.model_validate_json(path.read_bytes())
            receipt = verify_restored_backup(
                Path(args.verify_restored_home),
                layout,
                journal,
                str(args.snapshot_id),
                time.monotonic() + config.max_runtime_seconds,
            )
            print(receipt.model_dump_json(indent=2), flush=True)
        else:
            print('RESULT\t' + maintain(instance, config).model_dump_json(), flush=True)


if __name__ == '__main__':
    main()
