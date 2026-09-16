import os
from collections.abc import Callable
from pathlib import Path

from dagster import get_dagster_logger

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .capacity import CapacityMonitor
from .cleanup import preserve_primary_failure
from .contracts import RevisionedSourceSpec, SourceError
from .lifecycle import SourceRuntime
from .prepare import publication_current
from .storage import SourceStore


def publish_backfill(
    spec: RevisionedSourceSpec,
    verified: list[dict[str, object]],
    root: Path,
    *,
    run_id: str,
    materialized: Callable[[str, dict[str, object]], None],
) -> dict[str, dict[str, object]]:
    from .dagit import observe_source

    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    runtime = SourceRuntime(
        spec,
        SourceStore(client, settings.database, spec),
        Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
        run_id,
    )
    with preserve_primary_failure('disconnect', client.disconnect):

        def check() -> None:
            current = {
                record.partition.key: (record.revision, str(record.build_id), record.generation)
                for record in runtime.store.records(canonical_only=True)
            }
            if not verified or any(
                current.get(str(item['partition_key']))
                != (item['revision'], item['build_id'], item['generation'])
                for item in verified
            ):
                raise SourceError(
                    'GENERATION_CHANGED',
                    'Selected source generations changed before publication completed.',
                )

        check()
        if not runtime.store.canonical_verified():
            raise SourceError(
                'PARITY_EVIDENCE_MISSING',
                'Every active canonical generation must pass verification before publication.',
            )
        runtime.failures.recover(operation='backfill')
        get_dagster_logger('origo.sources').info(
            'source=%s phase=publication_started consumers=%s',
            spec.key,
            [consumer.key for consumer in spec.consumers],
        )
        capacity = CapacityMonitor(runtime, probe=False)
        capacity.check()
        capacity.start_sampling()
        published = False
        with preserve_primary_failure(
            'publication capacity', lambda: capacity.finish(successful=published)
        ):
            results: dict[str, dict[str, object]] = {}
            for consumer in spec.consumers:
                snapshot = runtime.store.snapshot(canonical_only=consumer.canonical_only)
                if publication_current(spec, consumer.key, snapshot.token, root=root):
                    runtime.failures.recover(operation='consumer', consumer=consumer.key)
                else:
                    snapshot = runtime.publish(consumer.key, str(root / spec.key / consumer.key))
                result: dict[str, object] = {
                    'state_token': snapshot.token,
                    'destination': str(root / spec.key / consumer.key),
                }
                results[consumer.key] = result
                materialized(consumer.key, result)
            published = True

        check()
        if any(
            results[consumer.key]['state_token']
            != runtime.store.snapshot(canonical_only=consumer.canonical_only).token
            for consumer in spec.consumers
        ):
            raise SourceError(
                'GENERATION_CHANGED', 'Source changed between declared file publications.'
            )
        state = observe_source(runtime)
        if state['healthy'] is not True:
            raise SourceError(
                'SOURCE_HEALTH_BLOCKED',
                'Publication completed with unresolved source failures; inspect this run logs.',
            )
        get_dagster_logger('origo.sources').info('source=%s phase=publication_completed', spec.key)
        return results
