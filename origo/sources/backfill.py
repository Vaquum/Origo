import hashlib
import os
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import dagster
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    DagsterEvent,
    DagsterEventType,
    ExecutorDefinition,
    Failure,
    JobDefinition,
    MetadataValue,
    RetryRequested,
)
from dagster._core.events import AssetMaterializationPlannedData

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
) -> None:
    from .prepare import prepare_source
    from .publication import publish_backfill

    spec.require_enabled('backfill')
    days = tuple(context.partition_keys)
    if not days:
        raise ValueError('Select at least one native source partition.')
    prepare_source(spec, context.instance)
    context.instance.add_run_tags(
        context.run.run_id,
        {
            'origo_source_key': spec.key,
            'origo_source_operation': 'backfill',
            'dagster/max_runtime': '0',
            'origo_source_start_date': days[0],
            'origo_source_end_date': days[-1],
        },
    )
    context.log.info('source=%s period=%s..%s days=%s', spec.key, days[0], days[-1], len(days))
    for consumer in spec.consumers:
        context.log.log_dagster_event(
            level='INFO',
            msg=f'Planned publication for {consumer.key}.',
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.ASSET_MATERIALIZATION_PLANNED.value,
                job_name=context.job_def.name,
                step_key=context.op_execution_context.op.name,
                event_specific_data=AssetMaterializationPlannedData(
                    AssetKey(f'publish_{spec.key}_{consumer.key}')
                ),
            ),
        )
    verified: list[dict[str, object]] = []
    for day in days:
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
        verified.append(result)
        version = hashlib.sha256(
            str((result['revision'], result['build_id'], result['generation'])).encode()
        ).hexdigest()
        context.log_event(
            AssetMaterialization(
                asset_key=f'build_{spec.key}_canonical_revision_origo',
                partition=day,
                metadata={
                    'source_state': MetadataValue.json(result),
                    'source_result': MetadataValue.json(result),
                },
                tags={'dagster/data_version': version},
            )
        )
    root = Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))

    def materialized(consumer: str, result: dict[str, object]) -> None:
        context.log_event(
            AssetMaterialization(
                asset_key=f'publish_{spec.key}_{consumer}',
                metadata={'source_result': MetadataValue.json(result)},
            )
        )

    publish_backfill(spec, verified, root, run_id=context.run.run_id, materialized=materialized)
