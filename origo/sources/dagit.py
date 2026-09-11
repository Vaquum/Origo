from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import dagster
from dagster import (
    AssetKey,
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

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .contracts import RevisionedSourceSpec, RolloutStage, SourceError
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
    if failures:
        raise SourceError(
            'SOURCE_RECONCILIATION_UNHEALTHY',
            f'Source has {len(failures)} unresolved failure(s); see preceding Dagit logs.',
        )
    return {'observed_at': observed, 'active_days': len(records), 'bridged_events': len(events)}


def _reconciliation_selection(keys: list[str], urgent: list[str], offset: int) -> list[str]:
    start = offset * 4 % max(1, len(urgent))
    prioritized = (urgent[start:] + urgent[:start])[:4]
    rotating = keys[offset % len(keys) : offset % len(keys) + 1] if keys else []
    return list(dict.fromkeys(prioritized + rotating))


def build_reconciliation_sensor(
    spec: RevisionedSourceSpec,
    canonical_job: JobDefinition,
    health_job: JobDefinition,
    asset_name: str,
) -> SensorDefinition:
    """Request native partition verification and source health runs from database state."""

    @_sensor(
        name=f'{spec.key}_reconciliation_sensor',
        jobs=[canonical_job, health_job],
        minimum_interval_seconds=60,
        default_status=DefaultSensorStatus.STOPPED,
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
            known: dict[str, str] = {}
            offset = 0
            if context.cursor:
                loaded: object = json.loads(context.cursor)
                if not isinstance(loaded, dict):
                    raise ValueError('Reconciliation cursor must be an object.')
                state = cast(dict[str, object], loaded)
                saved = state.get('known')
                if not isinstance(saved, dict):
                    raise ValueError('Reconciliation cursor must contain generation identities.')
                for name, value in cast(dict[str, object], saved).items():
                    if not isinstance(value, str):
                        raise ValueError('Reconciliation generation identity must be a string.')
                    known[name] = value
                offset = int(str(state.get('offset', 0)))
            current = {
                record.partition.key: f'{record.revision}:{record.build_id}:{record.generation}'
                for record in records
            }
            materialized = context.instance.get_materialized_partitions(AssetKey(asset_name))
            keys = sorted(set(current) | materialized)
            changed = [
                key
                for key in keys
                if known.get(key) != current.get(key, 'MISSING') or key not in materialized
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
            selected = _reconciliation_selection(
                keys, list(dict.fromkeys(changed + failed_keys)), offset
            )
            requests = [RunRequest(job_name=health_job.name, run_key=f'{spec.key}:health:{tick}')]
            inflight = context.instance.get_runs(
                filters=RunsFilter(
                    tags={'origo_source_key': spec.key, 'origo_source_reconciliation': 'true'},
                    statuses=_ACTIVE,
                ),
                limit=1,
            )
            if inflight:
                context.log.info(
                    'source=%s reconciliation batch is still queued or running', spec.key
                )
                return requests
            for key in selected:
                active = context.instance.get_runs(
                    filters=RunsFilter(
                        job_name=canonical_job.name,
                        tags={'dagster/partition': key},
                        statuses=_ACTIVE,
                    ),
                    limit=1,
                )
                if active:
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
                            'dagster/priority': '10',
                        },
                    )
                )
                known[key] = current.get(key, 'MISSING')
            context.update_cursor(
                json.dumps({'known': known, 'offset': offset + 1}, sort_keys=True)
            )
            context.log.info(
                'source=%s active_days=%s reconciliation_requests=%s',
                spec.key,
                len(records),
                len(requests) - 1,
            )
            return requests
        finally:
            client.disconnect()

    return reconcile
