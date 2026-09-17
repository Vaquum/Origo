from dagster import Config, DagsterInstance, OpExecutionContext, in_process_executor, op
from pydantic import Field

from origo.orchestration.recovery import recover_queue

from .backfill import source_job
from .prepare import configure_source_pool, prepare_source
from .registry import SOURCE_REGISTRY


class PreparationConfig(Config):
    # Queue recovery counts computed in main() before any captured op exists.
    queue_counts: dict[str, int] = Field(default_factory=dict)


@op
def prepare_registered_sources(context: OpExecutionContext, config: PreparationConfig) -> None:
    context.log.info('orchestration_recovery queue=%s', config.queue_counts)
    for spec in SOURCE_REGISTRY:
        context.log.info(
            'source=%s phase=preparation_started stage=%s', spec.key, spec.rollout_stage
        )
        prepare_source(spec, context.instance)
    context.add_output_metadata(
        {'registered_sources': [spec.key for spec in SOURCE_REGISTRY], **config.queue_counts}
    )


@source_job(
    name='prepare_revisioned_sources_job',
    description='Deployment applies registered source schemas and managed automation before the daemon starts.',
    executor_def=in_process_executor,
    tags={'origo_source_key': 'registered_sources', 'origo_source_operation': 'setup'},
)
def prepare_revisioned_sources_job() -> None:
    prepare_registered_sources()


def main() -> None:
    with DagsterInstance.get() as instance:
        for spec in SOURCE_REGISTRY:
            configure_source_pool(spec, instance)
        # Cancelling a redundant queued run initializes that run's event shard. Inside a
        # captured op the initialization log re-enters event storage under its lock and
        # deadlocks startup, so recovery runs here, before any DagsterLogManager exists.
        counts = recover_queue(instance)
        result = prepare_revisioned_sources_job.execute_in_process(
            instance=instance,
            run_config={
                'ops': {'prepare_registered_sources': {'config': {'queue_counts': counts}}}
            },
            raise_on_error=False,
        )
        if not result.success:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
