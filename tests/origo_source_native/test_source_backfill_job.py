import hashlib
import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from dagster import AssetKey, DagsterInstance, Definitions, build_sensor_context

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import capacity
from origo.sources.adapters import binance_daily
from origo.sources.backfill import BackfillConfig, selected_days
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import ConsumerSpec, Snapshot, SnapshotReader, SourceBundle
from origo.sources.prepare import prepare_source
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response

DAY = '2017-08-17'
ASSET = 'build_binance_spot_trades_canonical_revision_origo'


@pytest.fixture
def ready_job(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SourceStore, DagsterInstance, SourceBundle]]:
    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'files'))
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),)
    )
    monkeypatch.setattr(
        capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7)
    )
    spec = replace(
        BINANCE_SPOT_TRADES_SPEC,
        orchestration=replace(BINANCE_SPOT_TRADES_SPEC.orchestration, retry_count=0),
    )
    client = make_clickhouse_client(get_clickhouse_settings())
    (tmp_path / 'dagster').mkdir()
    try:
        with DagsterInstance.local_temp(str(tmp_path / 'dagster')) as instance:
            yield SourceStore(client, 'origo', spec), instance, build_source_bundle(spec)
    finally:
        client.disconnect()


def _config(end: str = DAY) -> dict[str, object]:
    return {'ops': {'select_period': {'config': {'start_date': DAY, 'end_date': end}}}}


def test_one_job_prepares_verifies_and_publishes_all_files(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, instance, bundle = ready_job
    assert store.execute('EXISTS TABLE origo.source_activation_log') == [(0,)]
    assert instance.all_instigator_state() == []
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))

    def no_copy(self: SourceStore, component: str, snapshot: Snapshot) -> list[tuple[object, ...]]:
        raise AssertionError(
            'Publication must query pinned projections without copying historical rows into Python.'
        )

    monkeypatch.setattr(SourceStore, 'rows', no_copy)
    result = job.execute_in_process(instance=instance, run_config=_config(), raise_on_error=False)
    assert result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert len(instance.all_instigator_state()) == len(bundle.sensors)
    assert store.canonical_verified()
    assert store.execute('SELECT min(successful) FROM origo.source_capacity_log') == [(1,)]
    token = store.snapshot().token
    for consumer in store.spec.consumers:
        root = tmp_path / 'files' / store.spec.key / consumer.key
        manifest = json.loads((root / 'latest.json').read_text())
        assert manifest['state_token'] == token
        assert len(manifest['files']) == (24 if consumer.key == 'arrow' else 12)
        for file in manifest['files']:
            path = root / 'versions' / manifest['version'] / file['path']
            assert hashlib.sha256(path.read_bytes()).hexdigest() == file['sha256']
        sensor = next(
            sensor
            for sensor in bundle.sensors
            if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
        )
        with build_sensor_context(
            instance=instance,
            definitions=Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors),
        ) as context:
            assert sensor.evaluate_tick(context).run_requests == []
    # Re-deployment restores code-owned sensor state without losing its cursor.
    state = instance.all_instigator_state()[0]
    from dagster._core.scheduler.instigation import InstigatorStatus

    instance.update_instigator_state(state.with_status(InstigatorStatus.STOPPED))
    with pytest.raises(RuntimeError, match='sensor is not prepared'):
        prepare_source(store.spec, instance, check=True)
    prepare_source(store.spec, instance)
    prepare_source(store.spec, instance, check=True)
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert store.snapshot().token == token


def test_file_failure_fails_job_and_retry_keeps_verified_generation(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, _bundle = ready_job
    original = store.spec.consumers[0]
    attempts = 0

    def interrupted(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError('Interrupted publication')
        original.publish(reader, snapshot, destination)

    spec = replace(
        store.spec, consumers=(ConsumerSpec(original.key, interrupted), *store.spec.consumers[1:])
    )
    job = next(job for job in build_source_bundle(spec).jobs if job.name.startswith('backfill_'))
    failed = job.execute_in_process(instance=instance, run_config=_config(), raise_on_error=False)
    assert not failed.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert not (tmp_path / 'files' / spec.key / 'parquet' / 'latest.json').exists()
    before = store.snapshot()
    retried = job.execute_in_process(instance=instance, run_config=_config(), raise_on_error=False)
    assert retried.success
    assert store.snapshot() == before
    assert instance.get_run_by_id(failed.run_id).status.value == 'FAILURE'


def test_unavailable_day_blocks_publication_and_preserves_completed_day(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    result = job.execute_in_process(
        instance=instance, run_config=_config('2017-08-18'), raise_on_error=False
    )
    assert not result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert store.canonical_verified()
    assert list((tmp_path / 'files').rglob('latest.json')) == []


def test_period_defaults_and_boundaries_are_shared_across_sources() -> None:
    spec = BINANCE_SPOT_TRADES_SPEC
    days = selected_days(spec, BackfillConfig())
    assert days[0] == DAY
    assert days[-1] == (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    assert selected_days(spec, BackfillConfig(start_date=DAY, end_date=DAY)) == (DAY,)
    for start, end in [('2017-08-16', DAY), ('2017-08-18', DAY), (DAY, '2999-01-01')]:
        with pytest.raises(ValueError, match='inclusive UTC period'):
            selected_days(spec, BackfillConfig(start_date=start, end_date=end))
    alternate = replace(spec, key='another_registered_source')
    job = next(
        job for job in build_source_bundle(alternate).jobs if job.name.startswith('backfill_')
    )
    assert job.name == 'backfill_another_registered_source_source_job'
    assert {node.name for node in job.nodes} == {
        'select_period',
        'build_and_verify',
        'publish_files',
    }
