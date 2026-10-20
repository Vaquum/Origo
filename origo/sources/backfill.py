from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol, cast

import dagster
from dagster import (
    AssetExecutionContext,
    ExecutorDefinition,
    Failure,
    JobDefinition,
    RetryRequested,
)

from .contracts import (
    CanonicalAuditPolicy, CanonicalBackfillBatch, RevisionedSourceSpec, SourceError,
    failure_code, failure_message, retryable_source_error,
)


class _JobFactory(Protocol):
    def __call__(
        self, *, name: str, description: str, executor_def: ExecutorDefinition, tags: dict[str, str]
    ) -> Callable[[Callable[[], None]], JobDefinition]: ...


source_job = cast(_JobFactory, getattr(dagster, 'job'))


def execute_partition_backfill(
    spec: RevisionedSourceSpec,
    context: AssetExecutionContext,
    run_day: Callable[[str], dict[str, object]],
    *,
    partition_key: str | None = None,
) -> dict[str, object]:
    from .prepare import prepare_source

    spec.require_enabled('backfill')
    days = tuple(context.partition_keys)
    if partition_key is not None:
        if (spec.consumers or not isinstance(spec.canonical, CanonicalBackfillBatch)
                or partition_key not in days or len(days) > spec.canonical.backfill_batch_size):
            raise ValueError('Partition is outside the native source batch.')
        day = partition_key
    elif len(days) != 1:
        raise ValueError('Source backfills require one canonical partition per native Dagster run.')
    else:
        day = days[0]
    if partition_key is None or day == days[0]:
        prepare_source(spec, context.instance)
    context.instance.add_run_tags(
        context.run.run_id,
        {
            'origo_source_key': spec.key,
            'origo_source_operation': 'backfill',
            'origo_source_partition': day,
            'origo_source_phase': 'canonical',
            'dagster/max_runtime': '0',
        },
    )
    context.log.info('source=%s partition=%s phase=backfill_started', spec.key, day)
    try:
        result = run_day(day)
    except Exception as error:
        context.log.exception('source=%s partition=%s phase=backfill_failed', spec.key, day)
        historical_book_404 = (
            isinstance(error, SourceError) and error.code == 'PROVIDER_HTTP_404'
            and spec.partitions.interval == 'hour'
            and isinstance(spec.canonical, CanonicalAuditPolicy)
            and spec.canonical.partition(day).start < spec.canonical.candidate(datetime.now(UTC)).start
        )
        if retryable_source_error(error) and not historical_book_404:
            retry_delay = spec.orchestration.retry_delay
            if (spec.partitions.interval == 'hour'
                    and isinstance(spec.canonical, CanonicalBackfillBatch)
                    and isinstance(error, SourceError)
                    and (error.code == 'PROVIDER_TRANSPORT_FAILED'
                         or error.code.startswith('PROVIDER_HTTP_5'))):
                retry_delay = 1
            raise RetryRequested(
                max_retries=spec.orchestration.retry_count,
                seconds_to_wait=retry_delay,
            ) from error
        raise Failure(
            description=f'{failure_code(error)}: {failure_message(error)}', allow_retries=False
        ) from error
    return result


def publication_ready(
    spec: RevisionedSourceSpec, context: AssetExecutionContext, result: dict[str, object]
) -> bool:
    """Join this native job backfill's receipts to the current complete generations."""
    from uuid import UUID

    from dagster._core.storage.tags import BACKFILL_ID_TAG

    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

    from .cleanup import preserve_primary_failure
    from .storage import SourceStore

    backfill_id = context.run.tags.get(BACKFILL_ID_TAG)
    if backfill_id is None:
        return True
    backfill = context.instance.get_backfill(backfill_id)
    if backfill is None:
        raise RuntimeError('Native backfill selection is missing.')
    if backfill.is_asset_backfill and spec.consumers:
        # Asset backfills natively wait for all upstream partitions before unpartitioned files.
        return True
    days = tuple(context.partition_keys) if backfill.is_asset_backfill else backfill.partition_names
    if not days or result['partition_key'] not in days:
        raise RuntimeError('Source partition is outside the native backfill selection.')
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    with preserve_primary_failure('disconnect', client.disconnect):
        store = SourceStore(client, settings.database, spec)
        store.execute(
            f'INSERT INTO {store.table("source_backfill_log")} VALUES',
            [
                (
                    spec.key,
                    backfill_id,
                    result['partition_key'],
                    result['revision'],
                    UUID(str(result['build_id'])),
                    result['generation'],
                    context.run.run_id,
                )
            ],
        )
        params: dict[str, object] = {'source': spec.key}
        completed = 0
        if spec.consumers:
            builds, params = store.complete_builds()
            completed, child = store.execute(
                f"""SELECT uniqExact(r.partition_key), countIf(
                    r.partition_key=%(partition)s AND r.revision=%(revision)s
                    AND r.build_id=%(build)s AND r.generation=%(generation)s
                    AND r.dagster_run_id=%(run)s)
                FROM {store.table('source_backfill_log')} r
                INNER JOIN {store.table('source_active_partitions')} a
                    USING (source_key, partition_key, revision, build_id, generation)
                INNER JOIN {builds} c USING (source_key, partition_key, revision, build_id)
                WHERE r.source_key=%(source)s AND r.backfill_id=%(backfill)s
                    AND r.partition_key IN %(days)s AND NOT a.provisional""",
                {**params, 'backfill': backfill_id, 'days': tuple(days),
                 'partition': result['partition_key'], 'revision': result['revision'],
                 'build': result['build_id'], 'generation': result['generation'],
                 'run': context.run.run_id},
            )[0]
            if not child:
                raise Failure('BACKFILL_RECEIPT_INVALID: Native child receipt lacks matching complete current coverage.',
                              allow_retries=False)
        proofs = store.execute(
            f'SELECT component, content_hash FROM {store.table("source_component_log")} '
            'WHERE source_key=%(source)s AND partition_key=%(partition)s '
            'AND revision=%(revision)s AND build_id=%(build)s',
            {**params, 'partition': result['partition_key'],
             'revision': result['revision'], 'build': result['build_id']},
        )
        record = store.record(spec.canonical.partition(str(result['partition_key'])))
        if record is None or (
            record.revision, str(record.build_id), record.generation
        ) != (str(result['revision']), str(result['build_id']), result['generation']) or store.missing_components(record) or tuple(sorted(
            (str(key), str(digest)) for key, digest in proofs
        )) != tuple(sorted(record.component_hashes)):
            raise Failure('BACKFILL_RECEIPT_INVALID: Native child component proofs do not match active coverage.',
                          allow_retries=False)
    if not spec.consumers:
        return True
    context.log.info(
        'source=%s phase=backfill_complete partitions=%s/%s backfill=%s',
        spec.key,
        completed,
        len(days),
        backfill_id,
    )
    return completed == len(days)
