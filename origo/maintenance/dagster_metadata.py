"""Dagit entry point for operational metadata maintenance."""

import os
import selectors
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Protocol, cast

import dagster
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetsDefinition,
    DefaultScheduleStatus,
    Definitions,
    ExecutorDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    in_process_executor,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._serdes import serialize_value

from .protocol import OperationalMetadataMaintenanceConfig
from .worker import Outcome

if TYPE_CHECKING:
    _MaintenanceResult = MaterializeResult[None]
else:
    _MaintenanceResult = MaterializeResult


def deployed_config() -> OperationalMetadataMaintenanceConfig:
    mode = os.environ.get('ORIGO_METADATA_DRY_RUN', 'true')
    if mode not in ('true', 'false'):
        raise ValueError('ORIGO_METADATA_DRY_RUN must be true or false.')
    return OperationalMetadataMaintenanceConfig(
        dry_run=mode == 'true',
        metadata_budget_bytes=int(
            os.environ.get('ORIGO_OPERATIONAL_METADATA_BUDGET_BYTES', str(13 * 1024**3))
        ),
        max_runtime_seconds=int(os.environ.get('ORIGO_METADATA_MAX_RUNTIME_SECONDS', '600')),
    )


def _worker(
    context: AssetExecutionContext, config: OperationalMetadataMaintenanceConfig
) -> Outcome:
    with tempfile.NamedTemporaryFile(
        mode='w', prefix='origo-maintenance-', suffix='.json'
    ) as reference:
        reference.write(serialize_value(context.instance.get_ref()))
        reference.flush()
        return _worker_process(context, config, reference.name)


def _worker_process(
    context: AssetExecutionContext, config: OperationalMetadataMaintenanceConfig, reference: str
) -> Outcome:
    command = [
        sys.executable,
        '-m',
        'origo.maintenance.worker',
        '--config',
        config.model_dump_json(),
        '--instance-ref',
        reference,
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    if process.stdout is None:
        raise RuntimeError('Maintenance worker stdout is unavailable.')
    deadline = time.monotonic() + config.max_runtime_seconds
    outcome: Outcome | None = None
    tail: list[str] = []
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while process.poll() is None or selector.get_map():
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f'Maintenance exceeded {config.max_runtime_seconds} seconds; the journal will resume unfinished deletion stages.'
                    )
                for key, _ in selector.select(timeout=min(0.5, deadline - time.monotonic())):
                    line = process.stdout.readline()
                    if not line:
                        selector.unregister(key.fileobj)
                    elif line.startswith('RESULT\t'):
                        outcome = Outcome.model_validate_json(line.removeprefix('RESULT\t'))
                    else:
                        message = line.rstrip()
                        tail = ([*tail, message])[-25:]
                        context.log.info('%s', message)
        if process.wait() != 0 or outcome is None:
            raise RuntimeError('Maintenance worker failed:\n' + '\n'.join(tail))
        return outcome
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
        process.stdout.close()


@asset(
    group_name='operational_maintenance',
    pool='operational_metadata',
    check_specs=[
        AssetCheckSpec(
            name='operational_metadata_health', asset='maintain_operational_metadata', blocking=True
        )
    ],
)
def maintain_operational_metadata(
    context: AssetExecutionContext, config: OperationalMetadataMaintenanceConfig
) -> Iterator[AssetCheckResult | _MaintenanceResult]:
    try:
        outcome = _worker(context, config)
    except Exception as error:
        context.log.exception('Operational metadata maintenance failed.')
        yield AssetCheckResult(
            passed=False, check_name='operational_metadata_health', metadata={'error': str(error)}
        )
        raise Failure(str(error)) from error
    metadata = {'maintenance': MetadataValue.json(outcome.model_dump())}
    yield AssetCheckResult(
        passed=not outcome.violations, check_name='operational_metadata_health', metadata=metadata
    )
    if outcome.violations:
        context.log.error('Operational metadata health failed: %s', '; '.join(outcome.violations))
        raise Failure('; '.join(outcome.violations), metadata=metadata)
    yield MaterializeResult(value=None, metadata=metadata)


class _JobFactory(Protocol):
    def __call__(
        self,
        name: str,
        *,
        selection: list[AssetsDefinition],
        executor_def: ExecutorDefinition,
        config: dict[str, object],
    ) -> UnresolvedAssetJobDefinition: ...


_define_job = cast(_JobFactory, getattr(dagster, 'define_asset_job'))


_maintenance_definitions = Definitions(
    assets=[maintain_operational_metadata],
    jobs=[
        _define_job(
            'maintain_operational_metadata_job',
            selection=[maintain_operational_metadata],
            executor_def=in_process_executor,
            config={
                'ops': {'maintain_operational_metadata': {'config': deployed_config().model_dump()}}
            },
        )
    ],
)
maintain_operational_metadata_job = _maintenance_definitions.resolve_job_def(
    'maintain_operational_metadata_job'
)
operational_metadata_maintenance_schedule = ScheduleDefinition(
    name='operational_metadata_maintenance_schedule',
    job=maintain_operational_metadata_job,
    cron_schedule='*/10 * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
    run_config={
        'ops': {'maintain_operational_metadata': {'config': deployed_config().model_dump()}}
    },
)
