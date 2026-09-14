"""Select, revalidate and retire execution history without changing current facts."""

import os
import sqlite3
import sys
import time
import traceback
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import cast

from dagster import (
    AssetSensorDefinition,
    DagsterInstance,
    DagsterRunStatus,
    Definitions,
    JobDefinition,
    RunRecord,
    RunsFilter,
    RunStatusSensorDefinition,
    SensorDefinition,
)
from dagster._core.definitions.run_status_sensor_definition import RunStatusSensorCursor
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._core.events import PIPELINE_RUN_STATUS_TO_EVENT_TYPE
from dagster._core.execution.backfill import BULK_ACTION_TERMINAL_STATUSES
from dagster._core.scheduler.instigation import InstigatorState, SensorInstigatorData
from dagster._core.storage.local_compute_log_manager import LocalComputeLogManager
from sqlalchemy.exc import SQLAlchemyError

from .archive import (
    MAX_DATABASE_BYTES,
    archive_path,
    clear_compaction_error,
    failed_compactions,
    record_compaction_error,
)
from .event_storage import OrigoSqliteEventLogStorage
from .protocol import Candidate, Journal, OperationalMetadataMaintenanceConfig, save_journal
from .roles import run_role
from .run_locks import run_lock
from .source_receipts import preserve_source_receipt, source_reference_reason
from .sqlite import Layout, MaintenanceDeadlineReached, allocated, artifacts, connection

# Explicit row-ID order avoids SQLite's status-index OR plan, which sorts the
# remaining backlog before LIMIT on every batch. NOT INDEXED still uses rowid.
_INVENTORY_QUERY = "SELECT id,run_id FROM runs NOT INDEXED WHERE id>? AND id<=? AND ((status='SUCCESS' AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?) OR (status IN ('FAILURE','CANCELED') AND coalesce(end_time,CAST(strftime('%s',update_timestamp) AS REAL))<?)) ORDER BY id LIMIT ?"


@dataclass(frozen=True)
class ScanReferences:
    retry_run_ids: frozenset[str]
    sensor_states: Sequence[InstigatorState]
    event_database: sqlite3.Connection


def _scan_references(
    instance: DagsterInstance,
    layout: Layout,
    run_ids: Sequence[str],
    config: OperationalMetadataMaintenanceConfig,
    deadline: float,
    event_database: sqlite3.Connection,
) -> ScanReferences:
    placeholders = ','.join('?' for _ in run_ids)
    with connection(layout.runs, deadline, config.lock_wait_seconds) as database:
        rows = database.execute(
            'SELECT DISTINCT t.value FROM run_tags t JOIN runs r ON r.run_id=t.run_id '
            "WHERE t.key IN ('dagster/parent_run_id','dagster/root_run_id') "
            f'AND t.value IN ({placeholders})',
            tuple(run_ids),
        ).fetchall()
    return ScanReferences(
        frozenset(str(row['value']) for row in rows),
        tuple(instance.all_instigator_state()),
        event_database,
    )


@lru_cache(maxsize=1)
def _sensor_definitions() -> dict[str, SensorDefinition]:
    defs = cast(Definitions, getattr(import_module('origo.definitions'), 'defs'))
    return {sensor.name: sensor for sensor in (defs.sensors or [])}


def artifact_inventory(layout: Layout, run_id: str, deadline: float) -> dict[str, int]:
    if time.monotonic() >= deadline:
        raise MaintenanceDeadlineReached('Artifact inventory exceeded maintenance deadline.')
    inventory = {str(path): allocated(path) for path in artifacts(layout, run_id)}
    directory = layout.compute / run_id
    inventory[str(directory)] = allocated(directory)
    for root, directories, files in os.walk(directory, followlinks=False):
        if time.monotonic() >= deadline:
            raise MaintenanceDeadlineReached('Compute-log inventory exceeded maintenance deadline.')
        for name in directories + files:
            path = Path(root) / name
            inventory[str(path)] = allocated(path)
    return inventory


def artifact_bytes(layout: Layout, run_id: str, deadline: float) -> int:
    return sum(artifact_inventory(layout, run_id, deadline).values())


def protection(
    instance: DagsterInstance,
    layout: Layout,
    record: RunRecord,
    config: OperationalMetadataMaintenanceConfig,
    now: float,
    deadline: float,
    *,
    scan_references: ScanReferences | None = None,
) -> str:
    run = record.dagster_run
    if run.status not in (
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.FAILURE,
        DagsterRunStatus.CANCELED,
    ):
        return 'nonterminal'
    role = run_role(run)
    if role != 'projection':
        return 'source_provenance' if role == 'source' else 'unclassified_provenance'
    retention_seconds = (
        config.projection_success_minutes * 60
        if run.status == DagsterRunStatus.SUCCESS
        else config.projection_failure_hours * 3600
    )
    if (record.end_time or record.update_timestamp.timestamp()) >= now - retention_seconds:
        return 'retention_window'
    activity_grace = min(3600, retention_seconds)
    if any(
        path.exists() and path.stat().st_mtime > now - activity_grace
        for path in artifacts(layout, run.run_id)
    ):
        return 'recent_artifact_activity'
    if run.tags.get('origo_metadata_preserve') == 'true':
        return 'operator_preserved'
    backfill_id = run.tags.get('dagster/backfill')
    if backfill_id:
        backfill = instance.get_backfill(backfill_id)
        if backfill is None or backfill.status not in BULK_ACTION_TERMINAL_STATUSES:
            return 'active_or_unknown_backfill'
    if scan_references is None:
        for tag in ('dagster/parent_run_id', 'dagster/root_run_id'):
            if instance.get_runs(RunsFilter(tags={tag: run.run_id}), limit=1):
                return 'retry_lineage_reference'
    elif run.run_id in scan_references.retry_run_ids:
        return 'retry_lineage_reference'
    if run.tags.get('dagster/will_retry') == 'true':
        return 'retry_pending'
    if run.status != DagsterRunStatus.SUCCESS and run.tags.get(
        'dagster/asset_partition_range_start'
    ):
        return 'failed_partition_range'
    newer_verdict = True
    if run.status != DagsterRunStatus.SUCCESS:
        partition_tags = {
            key: value
            for key, value in run.tags.items()
            if key
            in (
                'dagster/partition',
                'dagster/asset_partition_range_start',
                'origo_source_partition',
            )
        }
        joins = ''.join(
            f' JOIN run_tags t{index} ON t{index}.run_id=r.run_id AND t{index}.key=? AND t{index}.value=?'
            for index in range(len(partition_tags))
        )
        parameters = [value for pair in partition_tags.items() for value in pair]
        with connection(layout.runs, deadline, config.lock_wait_seconds) as database:
            # Broad partition tags can match hundreds of thousands of old runs.
            # Try at most 1,000 recent IDs before the complete historical lookup.
            upper = int(
                database.execute(
                    'SELECT coalesce(max(id),?) FROM runs', (record.storage_id,)
                ).fetchone()[0]
            )
            recent_terms = ''.join(
                ' AND EXISTS(SELECT 1 FROM run_tags t '
                'WHERE t.run_id=r.run_id AND t.key=? AND t.value=?)'
                for _ in partition_tags
            )
            newer = database.execute(
                'SELECT 1 FROM runs r NOT INDEXED WHERE r.id>? AND r.pipeline_name=?'
                + recent_terms
                + ' LIMIT 1',
                (max(record.storage_id, upper - 1000), run.job_name, *parameters),
            ).fetchone()
            if newer is None:
                newer = database.execute(
                    'SELECT 1 FROM runs r' + joins + ' WHERE r.pipeline_name=? AND r.id>? LIMIT 1',
                    (*parameters, run.job_name, record.storage_id),
                ).fetchone()
        newer_verdict = newer is not None
    source_reason = source_reference_reason(run)
    if source_reason:
        return source_reason
    event_connection = (
        connection(layout.events, deadline, config.lock_wait_seconds)
        if scan_references is None
        else nullcontext(scan_references.event_database)
    )
    with event_connection as database:
        events = database.execute(
            'SELECT id,asset_key,dagster_event_type,partition,timestamp,event FROM event_logs WHERE run_id=? ORDER BY id LIMIT 1001',
            (run.run_id,),
        ).fetchall()
        if len(events) > 1000:
            return 'oversized_reference_set'
        resolved_plans: set[int] = set()
        if run.status != DagsterRunStatus.SUCCESS:
            plans = [row for row in events if row['dagster_event_type'] == 'ASSET_MATERIALIZATION_PLANNED']
            ended = datetime.fromtimestamp(
                record.end_time or record.update_timestamp.timestamp(), UTC
            ).replace(tzinfo=None)
            for plan in plans:
                # Successful projection runs may already have expired. Their
                # retained current facts still resolve earlier failed plans.
                materialized = database.execute(
                    "SELECT 1 FROM event_logs WHERE asset_key=? AND dagster_event_type='ASSET_MATERIALIZATION' "
                    'AND partition IS ? AND id>? AND timestamp>? AND run_id!=? LIMIT 1',
                    (plan['asset_key'], plan['partition'], plan['id'], ended, run.run_id),
                ).fetchone()
                if materialized is not None:
                    resolved_plans.add(int(plan['id']))
            if not newer_verdict and (not plans or len(resolved_plans) != len(plans)):
                return 'latest_failure_or_verdict'
        for row in events:
            if (
                run.status != DagsterRunStatus.SUCCESS
                and row['dagster_event_type'] == 'ASSET_MATERIALIZATION_PLANNED'
            ):
                latest_plan = database.execute(
                    'SELECT run_id FROM event_logs WHERE asset_key=? AND dagster_event_type=? AND partition IS ? ORDER BY id DESC LIMIT 1',
                    (row['asset_key'], row['dagster_event_type'], row['partition']),
                ).fetchone()
                if (
                    latest_plan is not None
                    and latest_plan['run_id'] == run.run_id
                    and int(row['id']) not in resolved_plans
                ):
                    return 'current_failed_asset_partition'
            if str(row['dagster_event_type']).startswith('ASSET_CHECK'):
                from dagster._core.events.log import EventLogEntry
                from dagster._serdes import deserialize_value

                event = deserialize_value(str(row['event']), EventLogEntry).dagster_event
                if event is None:
                    raise ValueError('Asset check event has no Dagster payload.')
                payload = (
                    event.asset_check_evaluation_data
                    if row['dagster_event_type'] == 'ASSET_CHECK_EVALUATION'
                    else event.asset_check_planned_data
                )
                asset_key = payload.asset_key.to_string()
                check_name = payload.check_name
                executions = database.execute(
                    'SELECT partition FROM asset_check_executions WHERE asset_key=? AND check_name=? AND run_id=? LIMIT 1001',
                    (asset_key, check_name, run.run_id),
                ).fetchall()
                if len(executions) > 1000:
                    return 'oversized_check_reference_set'
                for execution in executions:
                    for completed in (False, True):
                        condition = (
                            " AND execution_status IN ('SUCCEEDED','FAILED')" if completed else ''
                        )
                        latest = database.execute(
                            'SELECT run_id FROM asset_check_executions WHERE asset_key=? AND check_name=? AND partition IS ?'
                            + condition
                            + ' ORDER BY id DESC LIMIT 1',
                            (asset_key, check_name, execution['partition']),
                        ).fetchone()
                        if latest is not None and latest['run_id'] == run.run_id:
                            return 'current_asset_check'
    states = (
        instance.all_instigator_state()
        if scan_references is None
        else scan_references.sensor_states
    )
    for state in states:
        data = state.instigator_data
        if not isinstance(data, SensorInstigatorData):
            continue
        if data.last_run_key and run.tags.get('dagster/run_key') == data.last_run_key:
            return 'sensor_last_run_key'
        sensor = _sensor_definitions().get(state.name)
        relevant = []
        if isinstance(sensor, AssetSensorDefinition):
            asset_key = sensor.asset_key.to_string()
            relevant = [
                row
                for row in events
                if row['asset_key'] == asset_key
                and row['dagster_event_type'] == 'ASSET_MATERIALIZATION'
            ]
        elif isinstance(sensor, RunStatusSensorDefinition):
            status = cast(DagsterRunStatus, getattr(sensor, '_run_status'))
            monitored = cast(Sequence[object], getattr(sensor, '_monitored_jobs'))
            names = [
                job.name
                for job in monitored
                if isinstance(job, (JobDefinition, UnresolvedAssetJobDefinition))
            ]
            repository = run.tags_for_storage().get('.dagster/repository')
            if (
                names
                and len(names) == len(monitored)
                and repository is not None
                and repository != state.origin.repository_origin.get_label()
            ):
                continue  # Local monitored jobs belong to this sensor's repository.
            if len(names) != len(monitored) or not names or run.job_name in names:
                relevant = [
                    row
                    for row in events
                    if row['dagster_event_type'] == PIPELINE_RUN_STATUS_TO_EVENT_TYPE[status].value
                ]
        if not relevant:
            continue
        max_event_id = max(int(row['id']) for row in relevant)
        if data.cursor and RunStatusSensorCursor.is_valid(data.cursor):
            cursor = RunStatusSensorCursor.from_json(data.cursor)
            if cursor.update_timestamp:
                if (
                    record.update_timestamp.timestamp()
                    >= datetime.fromisoformat(cursor.update_timestamp).timestamp()
                ):
                    return 'unconsumed_run_status_cursor'
            elif max_event_id > cursor.record_id:
                return 'unconsumed_run_status_cursor'
        elif data.cursor and data.cursor.isdecimal() and max_event_id > int(data.cursor):
            return 'unconsumed_asset_cursor'
    return ''


def scan_batch(
    instance: DagsterInstance,
    layout: Layout,
    journal: Journal,
    config: OperationalMetadataMaintenanceConfig,
    now: float,
    deadline: float,
) -> list[Candidate]:
    # Integer keyset cursor remains usable after the corresponding run is deleted.
    with connection(layout.runs, deadline, config.lock_wait_seconds) as database:
        if journal.inventory_upper_id == 0:
            journal.inventory_upper_id = int(
                database.execute('SELECT coalesce(max(id),0) FROM runs').fetchone()[0]
            )
            journal.inventory_started_at = now
            journal.inventory_eligible_bytes = 0
            journal.inventory_scanned = 0
            journal.inventory_complete = False
        ids = database.execute(
            _INVENTORY_QUERY,
            (
                journal.scan_cursor,
                journal.inventory_upper_id,
                journal.inventory_started_at
                - min(config.projection_success_minutes * 60, config.source_archive_after_hours * 3600),
                journal.inventory_started_at
                - min(config.projection_failure_hours, config.source_archive_after_hours) * 3600,
                config.max_runs_per_batch,
            ),
        ).fetchall()
    # Only inventory shares these reads. Reclamation revalidates every candidate
    # against fresh run/dependency state under its existing writer lock.
    run_ids = [str(row['run_id']) for row in ids]
    records = (
        {
            record.dagster_run.run_id: record
            for record in instance.get_run_records(RunsFilter(run_ids=run_ids), limit=len(run_ids))
        }
        if run_ids
        else {}
    )
    packed: set[str] = set()
    archives = archive_path(layout.events.parent)
    if run_ids and archives.exists():
        with connection(archives, deadline, config.lock_wait_seconds) as database:
            placeholders = ','.join('?' for _ in run_ids)
            packed = {
                str(row[0])
                for row in database.execute(
                    f'SELECT run_id FROM source_runs WHERE run_id IN ({placeholders})', run_ids
                )
            }
    failures = failed_compactions(layout.events.parent, deadline, run_ids)
    with connection(layout.events, deadline, config.lock_wait_seconds) as event_database:
        references = (
            _scan_references(instance, layout, run_ids, config, deadline, event_database)
            if run_ids
            else None
        )
        candidates: list[Candidate] = []
        for row in ids:
            if time.monotonic() >= deadline:
                raise MaintenanceDeadlineReached('Candidate scan exceeded maintenance deadline.')
            record = records.get(str(row['run_id']))
            if record is not None:
                reason = protection(
                    instance,
                    layout,
                    record,
                    config,
                    journal.inventory_started_at,
                    deadline,
                    scan_references=references,
                )
                inventory = artifact_inventory(layout, record.dagster_run.run_id, deadline)
                archive_candidate = reason in ('source_provenance', 'unclassified_provenance')
                if archive_candidate:
                    if (
                        record.dagster_run.run_id in packed
                        and record.dagster_run.run_id not in failures
                        and not layout.shard(record.dagster_run.run_id).exists()
                    ):
                        reason = 'source_already_compacted'
                    elif (
                        not layout.shard(record.dagster_run.run_id).exists()
                        and record.dagster_run.run_id not in packed
                    ):
                        reason = 'source_no_event_shard'
                    elif (
                        layout.shard(record.dagster_run.run_id).exists()
                        and layout.shard(record.dagster_run.run_id).stat().st_size
                        > MAX_DATABASE_BYTES
                    ):
                        reason = 'source_archive_size_limit'
                    elif any(
                        path.exists()
                        and path.stat().st_mtime > now - config.source_archive_after_hours * 3600
                        for path in artifacts(layout, record.dagster_run.run_id)
                    ):
                        reason = 'recent_artifact_activity'
                    elif record.dagster_run.tags.get('origo_metadata_preserve') == 'true':
                        reason = 'operator_preserved'
                    else:
                        reason = ''
                candidates.append(
                    Candidate(
                        run_id=record.dagster_run.run_id,
                        storage_id=record.storage_id,
                        status=record.dagster_run.status.value,
                        ended_at=record.end_time or record.update_timestamp.timestamp(),
                        allocated_bytes=sum(inventory.values()),
                        artifacts=inventory,
                        reason=reason,
                        action='archive' if archive_candidate else 'retire',
                    )
                )
            journal.scan_cursor = int(row['id'])
    journal.inventory_scanned += len(ids)
    journal.inventory_eligible_bytes += sum(
        row.allocated_bytes for row in candidates if not row.reason
    )
    if len(ids) < config.max_runs_per_batch or journal.scan_cursor >= journal.inventory_upper_id:
        journal.scan_cursor = 0
        journal.inventory_upper_id = 0
        journal.inventory_complete = True
    return candidates


def reclaim(
    instance: DagsterInstance,
    layout: Layout,
    candidate: Candidate,
    journal: Journal,
    journal_path: Path,
    config: OperationalMetadataMaintenanceConfig,
    deadline: float,
) -> int:
    storage = instance.event_log_storage
    if not isinstance(storage, OrigoSqliteEventLogStorage):
        raise TypeError('Retention requires current-state-preserving event storage.')
    if candidate.action == 'archive':
        records = instance.get_run_records(RunsFilter(run_ids=[candidate.run_id]), limit=1)
        if not records or run_role(records[0].dagster_run) == 'projection':
            raise RuntimeError('The retained source identity changed before compaction.')
        candidate.revalidation_reason = ''
        candidate.phase = 'deleting'
        save_journal(journal_path, journal)
        try:
            released = storage.archive_run(candidate.run_id, deadline)
        except MaintenanceDeadlineReached:
            raise
        except (OSError, sqlite3.DatabaseError, SQLAlchemyError, RuntimeError, ValueError) as error:
            record_compaction_error(layout.events.parent, candidate.run_id, error, deadline)
            candidate.revalidation_reason = 'source_archive_error'
            candidate.phase = 'planned'
            save_journal(journal_path, journal)
            print(f'Source compaction failed for {candidate.run_id}; history retained.', flush=True)
            traceback.print_exc(file=sys.stdout)
            return 0
        clear_compaction_error(layout.events.parent, candidate.run_id, deadline)
        candidate.phase = 'reclaimed'
        save_journal(journal_path, journal)
        return released
    with run_lock(storage.writer_lock_path(), candidate.storage_id, config.lock_wait_seconds):
        run_id = candidate.run_id
        records = instance.get_run_records(RunsFilter(run_ids=[run_id]), limit=1)
        if records:
            reason = protection(instance, layout, records[0], config, time.time(), deadline)
            if reason:
                candidate.revalidation_reason = 'live_revalidation:' + reason
                save_journal(journal_path, journal)
                return 0
            preserve_source_receipt(records[0].dagster_run)
            candidate.phase = 'deleting'
            save_journal(journal_path, journal)
            instance.run_storage.delete_run(run_id)
        elif candidate.phase == 'planned':
            candidate.revalidation_reason = 'run_disappeared_before_owned_deletion'
            save_journal(journal_path, journal)
            return 0
    # Once the run row is absent, every late writer rejects its UUID. Release
    # the range before event/log cleanup so SQLite can safely reuse a deleted
    # maximum row ID for a new run without inheriting the old run's lock wait.
    run_id = candidate.run_id
    if candidate.phase == 'deleting':
        storage.delete_events(run_id)
    candidate.phase = 'logs'
    save_journal(journal_path, journal)
    if instance.get_run_by_id(run_id) is not None:
        raise RuntimeError(f'Retired run reappeared: {run_id}')
    logs = instance.compute_log_manager
    if not isinstance(logs, LocalComputeLogManager):
        raise TypeError('Unsupported compute-log manager.')
    before = artifact_bytes(layout, run_id, deadline)
    logs.delete_logs(prefix=[run_id])
    for path in artifacts(layout, run_id):
        if time.monotonic() >= deadline:
            raise MaintenanceDeadlineReached('Artifact reclamation exceeded maintenance deadline.')
        if path.exists():
            metadata = path.lstat()
            if (
                path.is_symlink()
                or metadata.st_nlink != 1
                or path.resolve().parent != layout.events.parent
            ):
                raise ValueError(f'Unsafe retired artifact: {path}')
            path.unlink()
    reclaimed = max(0, before - artifact_bytes(layout, run_id, deadline))
    candidate.phase = 'reclaimed'
    save_journal(journal_path, journal)
    return reclaimed
