from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import dagster
from dagster import (
    AssetKey,
    DagsterEventType,
    DagsterRunStatus,
    DailyPartitionsDefinition,
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    RunsFilter,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    get_dagster_logger,
)
from dagster._core.storage.dagster_run import RunRecord

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .contracts import RevisionedSourceSpec, RolloutStage
from .failures import REMOVED_PARITY_CODES
from .lifecycle import SourceRuntime
from .storage import SourceStore


class _SensorFactory(Protocol):
    def __call__(
        self,
        *,
        name: str,
        jobs: list[JobDefinition],
        minimum_interval_seconds: int,
        default_status: DefaultSensorStatus,
    ) -> Callable[
        [Callable[[SensorEvaluationContext], list[RunRequest] | SkipReason]], SensorDefinition
    ]: ...


_sensor = cast(_SensorFactory, getattr(dagster, 'sensor'))
_ACTIVE = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


def observe_source(runtime: SourceRuntime) -> dict[str, object]:
    """Read source authority and bridge durable failure/recovery events into Dagit."""
    logger = get_dagster_logger('origo.sources')
    runtime.require_shared_mount()
    records = runtime.store.records(canonical_only=True)
    events = runtime.store.execute(
        f"""SELECT event_id, event_time, event_type, severity, partition_key, operation,
        revision, build_id, component, consumer, dagster_run_id, error_code, message
        FROM {runtime.store.table('source_failure_log')}
        WHERE source_key=%(source)s AND event_id NOT IN (
            SELECT event_id FROM {runtime.store.table('source_run_log')} WHERE status='LOGGED'
        ) ORDER BY event_time, event_id LIMIT 1000""",
        {'source': runtime.spec.key},
    )
    for row in events:
        logger.log(
            40 if row[2] == 'FAILED' else 20,
            'source=%s partition=%s operation=%s revision=%s build=%s component=%s '
            'consumer=%s origin_run=%s event=%s event_id=%s occurred_at=%s code=%s message=%s',
            runtime.spec.key,
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[2],
            row[0],
            row[1],
            row[11],
            row[12],
        )
    if events:
        runtime.store.execute(
            f'INSERT INTO {runtime.store.table("source_run_log")} VALUES',
            [
                (
                    row[0],
                    runtime.spec.key,
                    f'{runtime.spec.key}:dagit:{row[0]}',
                    0,
                    'LOGGED',
                    runtime.run_id,
                    datetime.now(UTC),
                )
                for row in events
            ],
        )
    failures = runtime.store.execute(
        f"""SELECT failure_key FROM {runtime.store.table('source_failure_log')}
        WHERE source_key=%(source)s AND operation!='reconcile'
        GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
        {'source': runtime.spec.key},
    )
    observed = datetime.now(UTC).isoformat()
    logger.info(
        'source=%s phase=authority_observed active_days=%s unresolved_failures=%s observed_at=%s',
        runtime.spec.key,
        len(records),
        len(failures),
        observed,
    )
    return {
        'observed_at': observed,
        'active_days': len(records),
        'bridged_events': len(events),
        'healthy': not failures,
        'unresolved_failures': len(failures),
    }


HEALTH_RECONCILIATION_BATCH_SIZE = 4
HEALTH_RECONCILIATION_RETRY_DELAYS = (60, 300, 1800, 3600)


def _reconciliation_selection(
    repairs: list[str], upgrades: list[str], offset: int, after: str
) -> tuple[list[str], str]:
    """Up to four of the partitions whose version or status differs, rotating the start
    with the tick so a failing batch cannot starve the rest. Component upgrades only fill
    the slots repairs leave, so a history upgrade never delays a repair. They continue in
    key order after the last upgrade taken, so every pending upgrade is reached whatever
    slots are free. Nothing else is selected: a canonical day whose Dagster record matches
    the store is not re-materialized, because every build and repair re-checks its retained
    content and an operator can launch the canonical job for any day. Returns the batch and
    the last upgrade taken."""
    start = offset * HEALTH_RECONCILIATION_BATCH_SIZE % max(1, len(repairs))
    selected = (repairs[start:] + repairs[:start])[:HEALTH_RECONCILIATION_BATCH_SIZE]
    ordered = [key for key in upgrades if key > after] + [key for key in upgrades if key <= after]
    chosen = ordered[: HEALTH_RECONCILIATION_BATCH_SIZE - len(selected)]
    return selected + chosen, chosen[-1] if chosen else after


def _partition_runs(
    context: SensorEvaluationContext,
    spec: RevisionedSourceSpec,
    canonical_job: JobDefinition,
    asset_name: str,
    key: str,
) -> list[RunRecord]:
    jobs = {canonical_job.name, f'backfill_{spec.key}_source_job'}
    asset_key = AssetKey(asset_name)
    found: dict[int, RunRecord] = {}
    for tag in ('dagster/partition', 'dagster/asset_partition_range_start'):
        for statuses in (_ACTIVE, None):
            cursor = None
            while True:
                page = context.instance.get_run_records(
                    RunsFilter(tags={tag: key}, statuses=statuses),
                    limit=25,
                    cursor=cursor,
                )
                match = next(
                    (
                        record
                        for record in page
                        if record.dagster_run.job_name in jobs
                        or asset_key in (record.dagster_run.asset_selection or set())
                    ),
                    None,
                )
                if match is not None:
                    found[match.storage_id] = match
                if match is not None or len(page) < 25:
                    break
                cursor = page[-1].dagster_run.run_id
    return sorted(
        found.values(),
        key=lambda record: (
            record.end_time or record.update_timestamp.timestamp(),
            record.storage_id,
        ),
        reverse=True,
    )


HEALTH_MIN_INTERVAL_SECONDS = 300
HEALTH_IDLE_INTERVAL_SECONDS = 3600


def _health_due(
    context: SensorEvaluationContext, runtime: SourceRuntime, job: JobDefinition, now: float
) -> bool:
    if context.instance.get_runs(RunsFilter(job_name=job.name, statuses=_ACTIVE), limit=1):
        return False
    last = context.instance.get_run_records(RunsFilter(job_name=job.name), limit=1)
    age = now - (last[0].end_time or last[0].update_timestamp.timestamp()) if last else HEALTH_IDLE_INTERVAL_SECONDS
    if age < HEALTH_MIN_INTERVAL_SECONDS:
        return False
    if age >= HEALTH_IDLE_INTERVAL_SECONDS:
        return True
    return bool(
        runtime.store.execute(
            f"""SELECT event_id FROM {runtime.store.table('source_failure_log')}
        WHERE source_key=%(source)s AND event_id NOT IN (
            SELECT event_id FROM {runtime.store.table('source_run_log')} WHERE status='LOGGED'
        ) LIMIT 1""",
            {'source': runtime.spec.key},
        )
    )


def build_reconciliation_sensor(
    spec: RevisionedSourceSpec,
    canonical_job: JobDefinition,
    health_job: JobDefinition,
    asset_name: str,
) -> SensorDefinition:
    """Request native partition reconciliation and source health runs from database state."""

    @_sensor(
        name=f'{spec.key}_reconciliation_sensor',
        jobs=[canonical_job, health_job],
        minimum_interval_seconds=60,
        default_status=DefaultSensorStatus.STOPPED
        if spec.rollout_stage == RolloutStage.DORMANT
        else DefaultSensorStatus.RUNNING,
    )
    def reconcile(context: SensorEvaluationContext) -> list[RunRequest] | SkipReason:
        if spec.rollout_stage == RolloutStage.DORMANT:
            return SkipReason(f'{spec.key} is DORMANT.')
        tick = int(datetime.now(UTC).timestamp()) // 60
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                f'reconciliation:{tick}',
            )
            runtime.require_shared_mount()
            records = runtime.store.records(canonical_only=True)
            enabled = runtime.store.enabled_groups()
            missing = {
                record.partition.key: tuple(
                    item.key for item in runtime.store.missing_components(record, enabled=enabled)
                )
                for record in records
            } if enabled else {}
            offset, after = 0, ''
            if context.cursor:
                loaded: object = json.loads(context.cursor)
                if not isinstance(loaded, dict):
                    raise ValueError('Reconciliation cursor must be an object.')
                # Old cursors may contain `known`; identity now comes from Dagster itself.
                offset = int(str(cast(dict[str, object], loaded).get('offset', 0)))
                after = str(cast(dict[str, object], loaded).get('upgraded_after', ''))
            current = {
                record.partition.key: f'{record.revision}:{record.build_id}:{record.generation}'
                for record in records
            }
            versions = {
                record.partition.key: hashlib.sha256(
                    str((record.revision, str(record.build_id), record.generation)).encode()
                ).hexdigest()
                for record in records
            }
            materialized = context.instance.get_materialized_partitions(AssetKey(asset_name))
            tags_by_partition = context.instance.event_log_storage.get_latest_tags_by_partition(
                AssetKey(asset_name),
                DagsterEventType.ASSET_MATERIALIZATION,
                ['dagster/data_version'],
            )
            keys = sorted(set(current) | materialized)
            changed = [
                key
                for key in keys
                if key not in current
                or tags_by_partition.get(key, {}).get('dagster/data_version') != versions[key]
            ]
            statuses = (
                context.instance.get_status_by_partition(
                    AssetKey(asset_name),
                    keys,
                    DailyPartitionsDefinition(
                        start_date=spec.partitions.first_day.isoformat(), timezone='UTC'
                    ),
                )
                or {}
            )
            failed_keys = [
                key
                for key, status in statuses.items()
                if status is not None and status.value == 'FAILED'
            ]
            repairs = list(dict.fromkeys(changed + failed_keys))
            upgrades = [key for key in keys if missing.get(key) and key not in repairs]
            selected, last_upgrade = _reconciliation_selection(repairs, upgrades, offset, after)
            now = datetime.now(UTC).timestamp()
            requests = (
                [RunRequest(job_name=health_job.name, run_key=f'{spec.key}:health:{tick}')]
                if _health_due(context, runtime, health_job, now)
                else []
            )
            health_count = len(requests)
            context.update_cursor(json.dumps({'offset': offset + 1, 'upgraded_after': last_upgrade}))
            inflight = context.instance.get_runs(
                filters=RunsFilter(
                    tags={'origo_source_key': spec.key, 'origo_source_reconciliation': 'true'},
                    statuses=_ACTIVE,
                ),
                limit=1,
            )
            from .prepare import backfill_active

            if inflight or backfill_active(context.instance, spec):
                context.log.info(
                    'source=%s reconciliation batch is still queued or running', spec.key
                )
                return requests
            for key in selected:
                runs = _partition_runs(context, spec, canonical_job, asset_name, key)
                if any(record.dagster_run.status in _ACTIVE for record in runs):
                    continue
                authority = current.get(key, 'MISSING')
                if missing.get(key):
                    authority += ':components=' + ','.join(missing[key])
                latest_record = runs[0] if runs else None
                latest = latest_record.dagster_run if latest_record else None
                attempt = 0
                if (
                    latest is not None
                    and latest_record is not None
                    and latest.status in (DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED)
                    and latest.tags.get('origo_source_reconciliation') == 'true'
                    and latest.tags.get('origo_source_authority') == authority
                ):
                    if (
                        latest.status == DagsterRunStatus.FAILURE
                        and latest.tags.get('origo_source_verdict')
                        and latest.tags.get('origo_source_verdict_run') == latest.run_id
                        and latest.tags['origo_source_verdict'] not in REMOVED_PARITY_CODES
                    ):
                        context.log.info(
                            'source=%s partition=%s phase=reconciliation_held failed_run=%s '
                            'code=%s authority=%s action=operator_retry_or_state_change',
                            spec.key,
                            key,
                            latest.run_id,
                            latest.tags['origo_source_verdict'],
                            authority,
                        )
                        continue
                    attempt = min(int(latest.tags.get('origo_source_retry_attempt', '1')), len(HEALTH_RECONCILIATION_RETRY_DELAYS))
                    delay = HEALTH_RECONCILIATION_RETRY_DELAYS[attempt - 1]
                    ended = latest_record.end_time or latest_record.update_timestamp.timestamp()
                    if now - ended < delay:
                        continue
                requests.append(
                    RunRequest(
                        job_name=canonical_job.name,
                        partition_key=key,
                        run_key=f'{spec.key}:reconcile:{key}:{tick}',
                        run_config={'ops': {asset_name: {'config': {'reconcile_only': True}}}},
                        tags={
                            'origo_source_key': spec.key,
                            'origo_source_operation': 'canonical',
                            'origo_source_partition': key,
                            'origo_source_reconciliation': 'true',
                            'origo_source_authority': authority,
                            'origo_source_retry_attempt': str(min(attempt + 1, len(HEALTH_RECONCILIATION_RETRY_DELAYS))),
                            'dagster/priority': '10',
                        },
                    )
                )
            context.log.info(
                'source=%s active_days=%s reconciliation_requests=%s',
                spec.key,
                len(records),
                len(requests) - health_count,
            )
            return requests
        finally:
            client.disconnect()

    return reconcile
