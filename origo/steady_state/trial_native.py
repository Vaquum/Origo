"""Native Dagster execution for owned local workload trials, not production setup.

Only the existing maintenance job is exposed in this code location. The trial uses
real gRPC workers and the repository's queued coordinator/launcher, with persistent
storage confined to an empty local directory.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import cast

import yaml
from dagster import AssetSpec, DagsterInstance, Definitions
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon

from origo.maintenance.dagster_metadata import (
    maintain_operational_metadata,
    maintain_operational_metadata_job,
)

ROOT = Path(__file__).resolve().parents[2]
defs = Definitions(
    assets=[
        maintain_operational_metadata,
        *[
            AssetSpec(key + '_provisional_feed')
            for key in (
                'binance_spot_trades',
                'binance_perp_trades',
                'binance_spot_aggtrades',
                'binance_perp_aggtrades',
            )
        ],
    ],
    jobs=[maintain_operational_metadata_job],
)


class NativeMaintenance:
    def __init__(self, root: Path) -> None:
        if not root.is_absolute() or root != root.resolve():
            raise PermissionError('Native trial storage must be an absolute, non-symlink path.')
        if root == Path('/') or any(
            root.is_relative_to(base)
            for base in (
                Path('/opt'),
                Path('/var/lib'),
                ROOT,
            )
        ):
            raise PermissionError(
                'Native trial storage must not overlap production or the checkout.'
            )
        if os.environ.get('CLICKHOUSE_HOST', '127.0.0.1') not in ('127.0.0.1', 'localhost', '::1'):
            raise PermissionError('Native trial refuses an inherited remote ClickHouse endpoint.')
        if root.exists() and any(root.iterdir()):
            raise FileExistsError('Native trial never reuses existing Dagster state.')
        self.root = root
        self._stack = ExitStack()
        self.instance: DagsterInstance | None = None
        self.workspace: WorkspaceProcessContext | None = None
        self.daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
        self.run_ids: list[str] = []
        self._old_home: str | None = None

    def __enter__(self) -> NativeMaintenance:
        self.root.mkdir(parents=True, exist_ok=True)
        document: object = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
        if not isinstance(document, dict):
            raise ValueError('The native queue declaration is not a mapping.')
        configuration = cast(dict[str, object], document)
        overrides = {
            key: configuration[key]
            for key in (
                'run_coordinator',
                'run_launcher',
                'run_monitoring',
                'concurrency',
            )
            if key in configuration
        }
        # Keep the actual Origo maintenance storage adapters, but replace every
        # production path with this trial's isolated directory before constructing.
        for key in (
            'local_artifact_storage',
            'run_storage',
            'event_log_storage',
            'schedule_storage',
            'compute_logs',
        ):
            value = configuration[key]
            if not isinstance(value, dict):
                raise ValueError('Native storage declarations must be mappings.')
            selected = dict(cast(dict[str, object], value))
            selected['config'] = {
                'base_dir': str(self.root / 'compute_logs' if key == 'compute_logs' else self.root)
            }
            overrides[key] = selected
        (self.root / 'dagster.yaml').write_text(yaml.safe_dump(overrides))
        self._old_home = os.environ.get('DAGSTER_HOME')
        os.environ['DAGSTER_HOME'] = str(self.root)
        try:
            self.instance = self._stack.enter_context(
                DagsterInstance.local_temp(str(self.root), overrides=overrides)
            )
            target = ModuleTarget(
                module_name='origo.steady_state.trial_native',
                attribute='defs',
                working_directory=str(ROOT),
                location_name='steady-state-trial',
            )
            self.workspace = self._stack.enter_context(
                WorkspaceProcessContext(self.instance, target)
            )
        except BaseException:
            self.close()
            raise
        return self

    def _state(self) -> tuple[DagsterInstance, WorkspaceProcessContext]:
        if self.instance is None or self.workspace is None:
            raise RuntimeError('The native trial code location has not started.')
        return self.instance, self.workspace

    def submit(self) -> str:
        instance, workspace = self._state()
        context = workspace.create_request_context()
        location = context.get_code_location('steady-state-trial')
        repository = location.get_repository('__repository__')
        remote = repository.get_full_job(maintain_operational_metadata_job.name)
        run = instance.create_run_for_job(
            maintain_operational_metadata_job,
            run_config={
                'ops': {
                    'maintain_operational_metadata': {
                        'config': {
                            'dry_run': True,
                            'max_runtime_seconds': 60,
                        }
                    }
                }
            },
            remote_job_origin=remote.get_remote_origin(),
            job_code_origin=remote.get_python_origin(),
            tags={'origo/steady_state_trial': str(self.root.name)},
        )
        instance.submit_run(run.run_id, context)
        self.run_ids.append(run.run_id)
        return run.run_id

    def step(self) -> None:
        _instance, workspace = self._state()
        iterate = cast(
            Callable[[WorkspaceProcessContext], Iterator[object]],
            getattr(self.daemon, 'run_iteration'),
        )
        for error in iterate(workspace):
            if error is not None:
                raise RuntimeError(f'The actual native queue iteration failed: {error}')

    def records(self) -> list[dict[str, object]]:
        instance, _workspace = self._state()
        rows: list[dict[str, object]] = []
        for record in instance.get_run_records():
            if record.dagster_run.run_id in self.run_ids:
                rows.append(
                    {
                        'run_id': record.dagster_run.run_id,
                        'job_name': record.dagster_run.job_name,
                        'status': record.dagster_run.status.value,
                        'created_at': record.create_timestamp.timestamp(),
                        'started_at': record.start_time,
                        'ended_at': record.end_time,
                        'queue_seconds': None
                        if record.start_time is None
                        else record.start_time - record.create_timestamp.timestamp(),
                    }
                )
        return rows

    def wait(self, run_id: str, *, timeout_seconds: float = 90) -> dict[str, object]:
        if run_id not in self.run_ids or not 0 < timeout_seconds <= 300:
            raise ValueError('Wait requires a run submitted by this trial and a bounded deadline.')
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            self.step()
            record = next((item for item in self.records() if item['run_id'] == run_id), None)
            if record is not None and record['status'] in ('SUCCESS', 'FAILURE', 'CANCELED'):
                return record
            time.sleep(0.2)
        raise TimeoutError('The native trial job did not reach its terminal deadline.')

    def close(self) -> None:
        if self.instance is not None:
            for run_id in self.run_ids:
                run = self.instance.get_run_by_id(run_id)
                if run is not None and not run.is_finished:
                    self.instance.run_launcher.terminate(run_id)
        self._stack.close()
        self.instance = self.workspace = None
        if self._old_home is None:
            os.environ.pop('DAGSTER_HOME', None)
        else:
            os.environ['DAGSTER_HOME'] = self._old_home

    def __exit__(self, *exception: object) -> None:
        self.close()
