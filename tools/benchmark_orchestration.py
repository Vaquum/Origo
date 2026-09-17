"""Exercise native Dagster launching with real source ingestion and file publication.

Runs only against an owned local ClickHouse and temporary Dagster storage. Compares
identical archive work with the baseline and proposed queue settings, with and
without concurrent routine ingestion/publication. No production endpoints are used.
"""

import argparse
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pyarrow.parquet as pq
import yaml
from dagster import (
    Config,
    Definitions,
    JobDefinition,
    OpExecutionContext,
    in_process_executor,
    job,
    op,
)
from dagster._core.test_utils import instance_for_test
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import PythonFileTarget
from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC as SPEC
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from tools.benchmark_source_backfill import (
    _legacy_setup,
    archive_evidence,
    clickhouse,
    throughput,
    worker,
)

ROOT = Path(__file__).resolve().parents[1]


class Work(Config):
    day: str
    archives: str
    database: str
    root: str


@op(pool='benchmark_backfill')
def verified_archive(context: OpExecutionContext, config: Work) -> None:
    env = {key: value for key, value in os.environ.items() if key.startswith('CLICKHOUSE_')}
    env['CLICKHOUSE_DATABASE'] = config.database
    result = worker(('revised', config.day, env, config.archives, config.root + '/locks'))
    Path(config.root, config.day + '.json').write_text(json.dumps(result))
    context.add_output_metadata({'rows': result['rows'], 'day': config.day})


@job(executor_def=in_process_executor)
def backfill_orchestration_evidence_job() -> None:
    verified_archive()


@op
def routine_ingestion_and_publication(context: OpExecutionContext, config: Work) -> None:
    env = {key: value for key, value in os.environ.items() if key.startswith('CLICKHOUSE_')}
    env['CLICKHOUSE_DATABASE'] = 'origo_routine_' + context.run_id.replace('-', '')
    os.environ.update(env)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        _legacy_setup(client, env['CLICKHOUSE_DATABASE'])
        result = worker(
            ('legacy', config.day, env, config.archives, config.root + '/routine-locks')
        )
        http = clickhouse_connect.get_client(
            host=env['CLICKHOUSE_HOST'],
            port=int(env['CLICKHOUSE_HTTP_PORT']),
            username=env['CLICKHOUSE_USER'],
            password=env['CLICKHOUSE_PASSWORD'],
        )
        try:
            table = http.query_arrow(
                f'SELECT * FROM {env["CLICKHOUSE_DATABASE"]}.binance_spot_klines ORDER BY datetime'
            )
            destination = Path(config.root, context.run_id + '.parquet')
            pq.write_table(table, destination, compression='zstd')
            assert pq.read_table(destination).equals(table)
            context.add_output_metadata(
                {'source_rows': result['rows'], 'published_rows': table.num_rows}
            )
        finally:
            http.close()
    finally:
        client.execute(f'DROP DATABASE IF EXISTS {env["CLICKHOUSE_DATABASE"]} SYNC')
        client.disconnect()


@job(executor_def=in_process_executor)
def routine_orchestration_evidence_job() -> None:
    routine_ingestion_and_publication()


defs = Definitions(jobs=[backfill_orchestration_evidence_job, routine_orchestration_evidence_job])


def scenario(
    root: Path, archives: Path, days: list[str], *, policy: str, mixed: bool
) -> dict[str, object]:
    root.mkdir()
    (root / 'instance').mkdir()
    config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
    overrides = {key: config[key] for key in ('run_coordinator', 'run_launcher', 'concurrency')}
    if policy == 'baseline':
        overrides['run_coordinator'] = {
            'module': 'dagster._core.run_coordinator.queued_run_coordinator',
            'class': 'QueuedRunCoordinator',
        }
        overrides['run_launcher'] = {'module': 'dagster', 'class': 'DefaultRunLauncher'}
    database = 'origo_evidence_' + uuid4().hex
    os.environ['CLICKHOUSE_DATABASE'] = database
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(SPEC, SourceStore(client, database, SPEC), root / 'locks', 'evidence')
    runtime.setup(anchor=datetime.strptime(min(days), '%Y-%m-%d').replace(tzinfo=UTC))
    try:
        with instance_for_test(temp_dir=str(root / 'instance'), overrides=overrides) as instance:
            instance.event_log_storage.set_concurrency_slots('benchmark_backfill', 8)
            target = PythonFileTarget(
                python_file=str(Path(__file__).resolve()),
                attribute='defs',
                working_directory=str(ROOT),
                location_name='evidence',
            )
            with WorkspaceProcessContext(instance, target) as workspace:
                context = workspace.create_request_context()
                location = context.get_code_location('evidence')
                repository = location.get_repository('__repository__')

                def submit(job_def: JobDefinition, day: str) -> str:
                    remote = repository.get_full_job(job_def.name)
                    step = (
                        'verified_archive'
                        if job_def.name.startswith('backfill_')
                        else 'routine_ingestion_and_publication'
                    )
                    run = instance.create_run_for_job(
                        job_def,
                        run_config={
                            'ops': {
                                step: {
                                    'config': {
                                        'day': day,
                                        'archives': str(archives),
                                        'database': database,
                                        'root': str(root),
                                    }
                                }
                            }
                        },
                        remote_job_origin=remote.get_remote_origin(),
                        job_code_origin=remote.get_python_origin(),
                        tags={'dagster/partition': day},
                    )
                    instance.submit_run(run.run_id, context)
                    return run.run_id

                backfill_ids = [submit(backfill_orchestration_evidence_job, day) for day in days]
                routine_ids = []
                # Submitted after the bulk queue: this is the starvation trigger.
                if mixed:
                    routine_ids.append(submit(routine_orchestration_evidence_job, '2017-08-17'))
                daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
                started = time.time()
                next_routine = started + 10
                deadline = started + 900
                while True:
                    for error in daemon.run_iteration(workspace):
                        if error is not None:
                            raise RuntimeError(str(error))
                    records = instance.get_run_records()
                    bulk_records = [r for r in records if r.dagster_run.run_id in backfill_ids]
                    if (
                        mixed
                        and time.time() >= next_routine
                        and any(not r.dagster_run.is_finished for r in bulk_records)
                    ):
                        routine_ids.append(submit(routine_orchestration_evidence_job, '2017-08-17'))
                        next_routine = time.time() + 10
                    if records and all(r.dagster_run.is_finished for r in records):
                        break
                    if time.time() > deadline:
                        raise TimeoutError('Native queue evidence exceeded 15 minutes.')
                    time.sleep(instance.run_coordinator.dequeue_interval_seconds)
                failed = [
                    r.dagster_run.run_id for r in records if r.dagster_run.status.value == 'FAILURE'
                ]
                if failed:
                    raise RuntimeError(f'Native evidence runs failed: {failed}')
                elapsed = max(r.end_time for r in bulk_records) - min(
                    r.start_time for r in bulk_records
                )
                rows = sum(json.loads((root / (day + '.json')).read_text())['rows'] for day in days)
                routine = [
                    r
                    for r in records
                    if r.dagster_run.run_id in routine_ids and r.start_time is not None
                ]
                proofs = client.execute(
                    f'SELECT partition_key,component,row_count,content_hash FROM {database}.source_component_log ORDER BY partition_key,component'
                )
                return {
                    'policy': policy,
                    'mixed': mixed,
                    'rows': rows,
                    'source_seconds': elapsed,
                    **throughput(rows, elapsed),
                    'routine_runs': len(routine),
                    'routine_max_queue_seconds': max(
                        (r.start_time - r.create_timestamp.timestamp() for r in routine), default=0
                    ),
                    'routine_max_completion_seconds': max(
                        (r.end_time - r.create_timestamp.timestamp() for r in routine), default=0
                    ),
                    'native_runs': [
                        {'job': r.dagster_run.job_name, 'start': r.start_time, 'end': r.end_time}
                        for r in records
                    ],
                    'component_proofs': proofs,
                }
    finally:
        client.execute(f'DROP DATABASE IF EXISTS {database} SYNC')
        client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archives', type=Path, required=True)
    parser.add_argument('--days', nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--memory', default='8g')
    parser.add_argument(
        '--policies', nargs='+', choices=['baseline', 'bounded'], default=['baseline', 'bounded']
    )
    args = parser.parse_args()
    evidence = archive_evidence(args.archives, sorted(set([*args.days, '2017-08-17'])))
    report = {
        'archives': evidence,
        'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'scenarios': [],
    }
    with (
        tempfile.TemporaryDirectory(prefix='origo-queue-evidence-') as temporary,
        clickhouse(args.memory),
    ):
        for policy in args.policies:
            for mixed in (False, True):
                result = scenario(
                    Path(temporary) / f'{policy}-{mixed}',
                    args.archives.resolve(),
                    args.days,
                    policy=policy,
                    mixed=mixed,
                )
                if (
                    report['scenarios']
                    and result['component_proofs'] != report['scenarios'][0]['component_proofs']
                ):
                    raise RuntimeError('Source component outputs changed between scenarios.')
                report['scenarios'].append(result)
                args.output.write_text(json.dumps(report, indent=2, default=str) + '\n')
                print(
                    json.dumps(
                        {
                            k: v
                            for k, v in result.items()
                            if k not in ('component_proofs', 'native_runs')
                        }
                    ),
                    flush=True,
                )


if __name__ == '__main__':
    main()
