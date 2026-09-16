import hashlib
import os
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

import dagster
from dagster import (
    AssetMaterialization,
    Config,
    DynamicOut,
    DynamicOutput,
    ExecutorDefinition,
    Failure,
    JobDefinition,
    MetadataValue,
    OpExecutionContext,
    RetryRequested,
    in_process_executor,
    op,
)

from .contracts import RevisionedSourceSpec, failure_code, failure_message, retryable_source_error


class _JobFactory(Protocol):
    def __call__(
        self, *, name: str, description: str, executor_def: ExecutorDefinition, tags: dict[str, str]
    ) -> Callable[[Callable[[], None]], JobDefinition]: ...


_job = cast(_JobFactory, getattr(dagster, 'job'))


class BackfillConfig(Config):
    start_date: str = ''
    end_date: str = ''


def selected_days(spec: RevisionedSourceSpec, config: BackfillConfig) -> tuple[str, ...]:
    last = datetime.now(UTC).date() - timedelta(days=1)
    start = (
        date.fromisoformat(config.start_date) if config.start_date else spec.partitions.first_day
    )
    end = date.fromisoformat(config.end_date) if config.end_date else last
    if start < spec.partitions.first_day or end > last or start > end:
        raise ValueError(
            f'Select an inclusive UTC period within {spec.partitions.first_day} through {last}.'
        )
    return tuple(
        (start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)
    )


def build_backfill_job(
    spec: RevisionedSourceSpec, run_day: Callable[[str, str], dict[str, object]]
) -> JobDefinition:
    from .prepare import prepare_source

    @op(out=DynamicOut(str))
    def select_period(
        context: OpExecutionContext, config: BackfillConfig
    ) -> Iterator[DynamicOutput[str]]:
        spec.require_enabled('backfill')
        days = selected_days(spec, config)
        prepare_source(spec, context.instance)
        context.instance.add_run_tags(
            context.run_id,
            {
                'origo_source_start_date': days[0],
                'origo_source_end_date': days[-1],
            },
        )
        context.log.info('source=%s period=%s..%s days=%s', spec.key, days[0], days[-1], len(days))
        for day in days:
            yield DynamicOutput(day, mapping_key=day.replace('-', '_'))

    @op(pool=f'{spec.key}_heavy')
    def build_and_verify(context: OpExecutionContext, day: str) -> dict[str, object]:
        try:
            result = run_day(day, context.run_id)
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
        return result

    @op(pool=f'{spec.key}_heavy')
    def publish_files(context: OpExecutionContext, verified: list[dict[str, object]]) -> None:
        from .publication import publish_backfill

        root = Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))
        results = publish_backfill(spec, verified, root, run_id=context.run_id)
        for consumer, result in results.items():
            context.log_event(
                AssetMaterialization(
                    asset_key=f'publish_{spec.key}_{consumer}',
                    metadata={'source_result': MetadataValue.json(result)},
                )
            )
        context.add_output_metadata({'verified_days': len(verified), 'consumers': list(results)})

    @_job(
        name=f'backfill_{spec.key}_source_job',
        description='Choose inclusive start_date and end_date in Launchpad, then launch once. Empty dates select all closed daily archives. Success includes verified database projections and every declared file publication.',
        executor_def=in_process_executor,
        tags={
            'origo_source_key': spec.key,
            'origo_source_operation': 'backfill',
            'dagster/max_runtime': '0',
        },
    )
    def backfill() -> None:
        publish_files(select_period().map(build_and_verify).collect())

    return backfill
