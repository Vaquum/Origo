"""Interruptible worker: Dagit remains the log writer outside maintenance locks."""

import argparse
import json
import os
import subprocess
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

from dagster import DagsterInstance
from dagster._core.instance.ref import InstanceRef
from dagster._serdes import deserialize_value
from pydantic import BaseModel, Field

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .backup import require_backup, verify_restored_backup
from .clickhouse import diagnostic_allocated_bytes, maintain_diagnostics
from .event_storage import OrigoSqliteEventLogStorage
from .protocol import (
    Journal,
    OperationalMetadataMaintenanceConfig,
    Report,
    manifest_sha256,
    policy_sha256,
    save_journal,
)
from .retention import reclaim, scan_batch
from .run_storage import OrigoSqliteRunStorage
from .sqlite import (
    Layout,
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
    violations: list[str] = Field(default_factory=lambda: list[str]())


def directory_bytes(layout: Layout, deadline: float) -> int:
    paths = {layout.runs.parent, layout.events.parent, layout.schedules.parent, layout.compute}
    roots = sorted(
        path
        for path in paths
        if not any(path != other and path.is_relative_to(other) for other in paths)
    )
    result = subprocess.run(
        ['du', '-sk', *(str(root) for root in roots)],
        capture_output=True,
        text=True,
        check=True,
        timeout=max(0.01, deadline - time.monotonic()),
    )
    return sum(int(line.split()[0]) * 1024 for line in result.stdout.splitlines())


def maintain(instance: DagsterInstance, config: OperationalMetadataMaintenanceConfig) -> Outcome:
    started = time.monotonic()
    deadline = started + config.max_runtime_seconds - 3
    layout = Layout.from_instance(instance)
    if not isinstance(instance.run_storage, OrigoSqliteRunStorage) or not isinstance(
        instance.event_log_storage, OrigoSqliteEventLogStorage
    ):
        raise TypeError('Configure both Origo SQLite adapters before operational maintenance.')
    directory = layout.runs.parent / 'operational-maintenance'
    journal_path = directory / 'journal.json'
    with maintenance_lock(directory / 'maintenance.lock', config.lock_wait_seconds):
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
        first = True
        while first or time.monotonic() < deadline - 15:
            first = False
            if (
                config.dry_run
                or not journal.manifest
                or all(row.reason or row.phase == 'reclaimed' for row in journal.manifest)
            ):
                batch = scan_batch(instance, layout, journal, config, report.observed_at, deadline)
                if (
                    not config.dry_run
                    or not journal.manifest
                    or not any(not row.reason for row in journal.manifest)
                ):
                    journal.manifest = batch
                    journal.manifest_created_at = time.time()
                    journal.manifest_sha256 = manifest_sha256(journal)
                save_journal(journal_path, journal)
            else:
                batch = journal.manifest
            report.scanned += len(batch)
            report.candidates += sum(not row.reason for row in batch)
            report.protected += sum(bool(row.reason) for row in batch)
            counts.update(row.reason for row in batch if row.reason)
            print(
                f'Manifest {journal.manifest_sha256}: scanned={len(journal.manifest)} eligible={sum(not row.reason for row in journal.manifest)} cursor={journal.scan_cursor}',
                flush=True,
            )
            if not config.dry_run:
                require_backup(journal, config, time.time())
                for candidate in journal.manifest:
                    if candidate.reason or candidate.phase == 'reclaimed':
                        continue
                    report.reclaimed_bytes += reclaim(
                        instance, layout, candidate, journal, journal_path, config, deadline
                    )
                    if not candidate.reason:
                        report.deleted += 1
                    else:
                        report.protected += 1
                        report.candidates -= 1
                        counts[candidate.reason] += 1
                journal.first_apply_completed = journal.first_apply_completed or report.deleted > 0
                journal.state_cursor, scanned, compacted = (
                    instance.event_log_storage.compact_retired_state(
                        layout.runs,
                        journal.state_cursor,
                        config.max_runs_per_batch,
                        deadline,
                        config.lock_wait_seconds,
                    )
                )
                print(
                    f'Retained-state compaction: scanned={scanned} removed={compacted}', flush=True
                )
                save_journal(journal_path, journal)
            if journal.scan_cursor == 0:
                break
        report.exclusions = dict(counts)
        with connection(layout.runs, deadline, config.lock_wait_seconds) as database:
            report.backlog_runs = int(
                database.execute(
                    "SELECT count(*) FROM runs WHERE (status='SUCCESS' AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?) OR (status IN ('FAILURE','CANCELED') AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?)",
                    (
                        time.time() - config.success_retention_days * 86400,
                        time.time() - config.failure_retention_days * 86400,
                    ),
                ).fetchone()[0]
            )
        shared = {
            path.name: shared_bytes(path, deadline, config.lock_wait_seconds)
            for path in (layout.runs, layout.events, layout.schedules)
        }
        client = make_clickhouse_client(get_clickhouse_settings())
        try:
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
        if config.dry_run and journal.inventory_complete:
            journal.retained_floor_bytes = max(
                0, after_bytes - journal.inventory_eligible_bytes
            ) + max(0, diagnostic_after - diagnostics.entirely_expired_bytes)
            print(
                f'Inventory complete: runs={journal.inventory_scanned} conservative_retained_floor_bytes={journal.retained_floor_bytes}',
                flush=True,
            )
        violations.extend(f'diagnostic_ttl_drift:{table}' for table in diagnostics.drift)
        violations.extend(diagnostics.errors)
        if report.allocated_bytes > config.metadata_budget_bytes:
            violations.append(
                f'metadata_budget:{report.allocated_bytes}>{config.metadata_budget_bytes}'
            )
        report.duration_seconds = time.monotonic() - started
        report.active_cleanup_runs_per_second = report.deleted / max(report.duration_seconds, 0.001)
        previous = next(
            (row for row in reversed(journal.reports) if row.dry_run == report.dry_run), None
        )
        if previous:
            interval = max(report.observed_at - previous.observed_at, 0.001)
            report.eligible_ingress_runs = max(
                0, report.backlog_runs - previous.backlog_runs + report.deleted
            )
            report.ingress_runs_per_second = report.eligible_ingress_runs / interval
            report.cleanup_runs_per_second = report.deleted / interval
            if (
                not config.dry_run
                and report.candidates > 0
                and report.backlog_runs >= previous.backlog_runs > 0
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
            diagnostic_active_bytes=diagnostics.active_bytes,
            diagnostic_inactive_bytes=diagnostics.inactive_bytes,
            diagnostic_expired_part_bytes=diagnostics.entirely_expired_bytes,
            diagnostic_action=diagnostics.scheduled_action,
            diagnostic_allocated_bytes=diagnostic_after,
            diagnostic_net_reclaimed_bytes=max(0, diagnostic_before - diagnostic_after),
            filesystem_free_bytes=os.statvfs(layout.runs).f_bavail
            * os.statvfs(layout.runs).f_frsize,
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
