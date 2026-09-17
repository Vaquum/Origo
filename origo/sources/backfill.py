from collections.abc import Callable
from typing import Protocol, cast

import dagster
from dagster import (
    AssetExecutionContext,
    ExecutorDefinition,
    Failure,
    JobDefinition,
    RetryRequested,
)

from .contracts import RevisionedSourceSpec, failure_code, failure_message, retryable_source_error


class _JobFactory(Protocol):
    def __call__(
        self, *, name: str, description: str, executor_def: ExecutorDefinition, tags: dict[str, str]
    ) -> Callable[[Callable[[], None]], JobDefinition]: ...


source_job = cast(_JobFactory, getattr(dagster, 'job'))


def execute_partition_backfill(
    spec: RevisionedSourceSpec,
    context: AssetExecutionContext,
    run_day: Callable[[str], dict[str, object]],
) -> dict[str, object]:
    from .prepare import prepare_source

    spec.require_enabled('backfill')
    days = tuple(context.partition_keys)
    if len(days) != 1:
        raise ValueError('Source backfills require one daily partition per native Dagster run.')
    day = days[0]
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
        if retryable_source_error(error):
            raise RetryRequested(
                max_retries=spec.orchestration.retry_count,
                seconds_to_wait=spec.orchestration.retry_delay,
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
    if backfill.is_asset_backfill:
        # Asset backfills natively wait for all upstream partitions before unpartitioned files.
        return True
    days = backfill.partition_names
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
        builds, params = store.complete_builds()
        completed = store.execute(
            f"""SELECT uniqExact(r.partition_key)
            FROM {store.table('source_backfill_log')} r
            INNER JOIN {store.table('source_active_partitions')} a
                USING (source_key, partition_key, revision, build_id, generation)
            INNER JOIN {builds} c USING (source_key, partition_key, revision, build_id)
            WHERE r.source_key=%(source)s AND r.backfill_id=%(backfill)s
                AND r.partition_key IN %(days)s AND NOT a.provisional""",
            {**params, 'backfill': backfill_id, 'days': tuple(days)},
        )[0][0]
    context.log.info(
        'source=%s phase=backfill_complete partitions=%s/%s backfill=%s',
        spec.key,
        completed,
        len(days),
        backfill_id,
    )
    return completed == len(days)
