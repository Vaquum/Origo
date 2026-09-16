"""Preserve the source runtime's existing deduplication receipts before retirement."""

import hashlib
import os
from pathlib import Path

from dagster import DagsterRun

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.locking import source_lock
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore


def source_reference_reason(run: DagsterRun) -> str:
    source = run.tags.get('origo_source_key') or run.tags.get('origo_projection_source_key')
    if not source:
        return ''
    spec = next((spec for spec in SOURCE_REGISTRY if spec.key == source), None)
    if spec is None:
        return 'unknown_source_dependencies'
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        store = SourceStore(client, settings.database, spec)
        failures = store.execute(
            f'SELECT failure_key FROM {store.table("source_failure_log")} WHERE source_key=%(source)s '
            "GROUP BY failure_key HAVING argMax(event_type,event_time)='FAILED' "
            'AND argMax(dagster_run_id,event_time)=%(run_id)s LIMIT 1',
            {'source': source, 'run_id': run.run_id},
        )
        return 'unresolved_source_failure' if failures else ''
    finally:
        client.disconnect()


def preserve_source_receipt(run: DagsterRun) -> None:
    identity = run.tags.get('origo_source_event')
    if identity:
        source = run.tags.get('origo_source_key') or run.tags['origo_projection_source_key']
        spec = next(spec for spec in SOURCE_REGISTRY if spec.key == source)
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            store = SourceStore(client, settings.database, spec)
            lock_root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
            with source_lock(
                lock_root, source, 'request_' + hashlib.sha256(identity.encode()).hexdigest()
            ):
                store.record_run_receipt(
                    identity, int(run.tags['origo_source_attempt']), run.status.value, run.run_id
                )
                receipt = store.run_receipt(identity)
                if receipt is None or receipt[0] < int(run.tags['origo_source_attempt']):
                    raise RuntimeError(f'Source receipt verification failed for {run.run_id}.')
        finally:
            client.disconnect()
