from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    DefaultSensorStatus,
    Definitions,
    build_sensor_context,
)
from dagster._core.definitions.sensor_definition import SensorExecutionData

from origo.sources.bundle import SourceRunConfig, build_source_bundle, execute_source
from origo.sources.contracts import SourceBundle, SourceError
from origo.sources.profiles import consumer_base
from origo.sources.storage import SourceStore

from . import test_source_backfill_job as backfill

ready_job = backfill.ready_job
ReadyJob = tuple[SourceStore, DagsterInstance, SourceBundle]


def _canonical(env: ReadyJob) -> None:
    store, instance, source = env
    job = next(
        job for job in source.jobs
        if job.name == f'refresh_{store.spec.key}_canonical_source_job'
    )
    assert job.execute_in_process(instance=instance, partition_key='2020-01-01').success


def _evaluate(env: ReadyJob) -> SensorExecutionData:
    store, instance, source = env
    sensor = next(sensor for sensor in source.sensors if sensor.name == f'{store.spec.key}_mount_sensor')
    assert sensor.default_status == DefaultSensorStatus.RUNNING
    with build_sensor_context(
        instance=instance,
        definitions=Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors),
    ) as context:
        return sensor.evaluate_tick(context)


def _defer(env: ReadyJob, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _, _ = env
    # Use the captured 2020-01-01 archive; a zero cap makes its one real month
    # follow the same deferral path as the production 81-month mount.
    monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 0)
    with pytest.raises(SourceError, match='Mount render touches 1 months'):
        execute_source(store.spec, 'consumer_mount', SourceRunConfig(), run_id=str(uuid4()))


def test_only_deferred_mounts_enter_dagster_and_success_disarms_them(
    ready_job: ReadyJob,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store, instance, source = ready_job
    _canonical(ready_job)
    assert _evaluate(ready_job).run_requests == []
    token = store.snapshot(canonical_only=True).token
    publisher = next(job for job in source.jobs if job.name == f'publish_{store.spec.key}_mount_job')
    normal_identity = f'{store.spec.key}:consumer:mount:{token}'
    assert publisher.execute_in_process(
        instance=instance,
        tags={'origo_source_event': normal_identity, 'origo_source_attempt': '0'},
    ).success
    manifest_path = tmp_path / 'files' / store.spec.key / 'mount/latest.json'
    previous = json.loads(manifest_path.read_text())
    Path(previous['files'][0]['path']).unlink()
    _defer(ready_job, monkeypatch)
    first = _evaluate(ready_job).run_requests[0]
    repeated = _evaluate(ready_job).run_requests[0]
    assert first.run_key == repeated.run_key
    assert first.run_key is not None
    assert first.run_key.startswith(f'{store.spec.key}:consumer:mount:bulk:{token}:')
    assert first.tags['origo_source_state_token'] == token
    assert first.run_config == {
        'ops': {
            f'publish_{store.spec.key}_mount': {
                'config': {'partition_key': '', 'allow_full_history': True}
            }
        }
    }
    completed = publisher.execute_in_process(
        instance=instance, tags=first.tags, run_config=first.run_config
    )
    assert completed.success
    manifest = json.loads(manifest_path.read_text())
    assert manifest['state_token'] == token
    assert manifest['files']
    assert _evaluate(ready_job).run_requests == []
    assert store.execute(
        "SELECT failure_key FROM origo.source_failure_log WHERE error_code='RENDER_DEFERRED' "
        "GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED'"
    ) == []
    # A second outage of the same files is new work even after the successful
    # Dagster run is retired and only its durable receipt remains.
    identity = first.tags['origo_source_event']
    store.record_run_receipt(identity, 0, 'SUCCESS', completed.run_id)
    instance.delete_run(completed.run_id)
    Path(manifest['files'][0]['path']).unlink()
    _defer(ready_job, monkeypatch)
    second = _evaluate(ready_job).run_requests[0]
    _defer(ready_job, monkeypatch)
    assert _evaluate(ready_job).run_requests[0].run_key == second.run_key
    assert second.tags['origo_source_event'] != identity
    assert second.tags['origo_source_state_token'] == token
    assert second.tags['origo_source_attempt'] == '0'
    assert store.run_receipt(identity) == (0, 'SUCCESS', completed.run_id)
    assert publisher.execute_in_process(
        instance=instance, tags=second.tags, run_config=second.run_config
    ).success
    assert Path(manifest['files'][0]['path']).is_file()
    assert _evaluate(ready_job).run_requests == []


@pytest.mark.parametrize('prior_success', [False, True])
def test_deferred_mount_reuses_outstanding_guard_and_durable_retry_budget(
    ready_job: ReadyJob,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    prior_success: bool,
) -> None:
    store, instance, _ = ready_job
    _canonical(ready_job)
    _defer(ready_job, monkeypatch)
    spec = replace(
        store.spec, orchestration=replace(store.spec.orchestration, retry_count=1, retry_delay=3600)
    )
    source = build_source_bundle(spec)
    env = store, instance, source
    request = _evaluate(env).run_requests[0]
    publisher = next(job for job in source.jobs if job.name == f'publish_{store.spec.key}_mount_job')
    if prior_success:
        completed = publisher.execute_in_process(
            instance=instance, tags=request.tags, run_config=request.run_config
        )
        assert completed.success
        identity = request.tags['origo_source_event']
        store.record_run_receipt(identity, 0, 'SUCCESS', completed.run_id)
        instance.delete_run(completed.run_id)
        manifest = json.loads((tmp_path / 'files' / store.spec.key / 'mount/latest.json').read_text())
        Path(manifest['files'][0]['path']).unlink()
        _defer(env, monkeypatch)
        request = _evaluate(env).run_requests[0]
        assert request.tags['origo_source_event'] != identity
        assert request.tags['origo_source_attempt'] == '0'
    run = instance.create_run_for_job(
        publisher, status=DagsterRunStatus.STARTED, tags=request.tags, run_config=request.run_config
    )
    assert _evaluate(env).skip_message == 'A publication run for this consumer is still outstanding.'
    instance.report_run_failed(run, message='Deferred publication retry policy fault injection.')
    identity = request.tags['origo_source_event']
    store.record_run_receipt(identity, 0, 'FAILURE', run.run_id)
    instance.delete_run(run.run_id)
    _defer(env, monkeypatch)
    assert _evaluate(env).skip_message == 'Source retry delay has not elapsed.'
    elapsed = build_source_bundle(replace(spec, orchestration=replace(spec.orchestration, retry_delay=0)))
    env = store, instance, elapsed
    retry = _evaluate(env).run_requests[0]
    assert retry.tags['origo_source_attempt'] == '1'
    assert retry.tags['origo_source_event'] == identity
    assert retry.run_key != request.run_key
    retry_run = instance.create_run_for_job(publisher, tags=retry.tags, run_config=retry.run_config)
    instance.report_run_failed(retry_run, message='Deferred publication retry budget fault injection.')
    store.record_run_receipt(identity, 1, 'FAILURE', retry_run.run_id)
    instance.delete_run(retry_run.run_id)
    _defer(env, monkeypatch)
    exhausted = _evaluate(env)
    assert exhausted.run_requests == []
    assert exhausted.skip_message is not None
    assert exhausted.skip_message.startswith('Automatic source attempts exhausted;')


def test_deferred_mount_waits_for_the_native_backfill(
    ready_job: ReadyJob,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, instance, source = ready_job
    _canonical(ready_job)
    _defer(ready_job, monkeypatch)
    job = next(job for job in source.jobs if job.name == f'backfill_{store.spec.key}_source_job')
    run = instance.create_run_for_job(job, status=DagsterRunStatus.STARTED)
    blocked = _evaluate(ready_job)
    assert blocked.run_requests == []
    assert blocked.skip_message == 'The backfill job owns publication until its selected period completes.'
    instance.report_run_failed(run, message='Terminal backfills release publication.')
    assert len(_evaluate(ready_job).run_requests) == 1
