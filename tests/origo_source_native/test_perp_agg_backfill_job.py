import hashlib
import json
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    DailyPartitionsDefinition,
    Definitions,
    build_sensor_context,
)

from dagster._core.definitions.sensor_definition import SensorExecutionData

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import capacity
from origo.sources.adapters import binance_perp_agg_daily
from origo.sources.binance_perp_aggtrades import BINANCE_PERP_AGGTRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import ConsumerSpec, Snapshot, SnapshotReader, SourceBundle
from origo.sources.prepare import prepare_source
from origo.sources.profiles import perp_agg_consumers
from origo.sources.profiles.formulas.perp_agg_series import SPECS as PERP_AGG_SERIES_SPECS
from origo.sources.storage import SourceStore

from .acceptance_cases import PERP_AGG_CASE
from .test_binance_perp_agg_daily_source_adapter import archive_response

DAY = '2019-12-31'
ASSET = 'build_binance_perp_aggtrades_canonical_revision_origo'


class FakeHfApi:
    """Records the Hugging Face calls a publication makes; nothing leaves the host."""

    calls: list[tuple[str, dict[str, object]]] = []

    def __init__(self, token: str) -> None:
        assert token

    def create_repo(self, **kwargs: object) -> None:
        self.calls.append(('create_repo', kwargs))

    def upload_folder(self, **kwargs: object) -> None:
        self.calls.append(('upload_folder', kwargs))


@pytest.fixture
def ready_job(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SourceStore, DagsterInstance, SourceBundle]]:
    monkeypatch.setattr(binance_perp_agg_daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'files'))
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(perp_agg_consumers, 'HfApi', FakeHfApi)
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),)
    )
    monkeypatch.setattr(
        capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7)
    )
    spec = replace(
        BINANCE_PERP_AGGTRADES_SPEC,
        orchestration=replace(BINANCE_PERP_AGGTRADES_SPEC.orchestration, retry_count=0),
    )
    client = make_clickhouse_client(get_clickhouse_settings())
    (tmp_path / 'dagster').mkdir()
    try:
        with DagsterInstance.local_temp(
            str(tmp_path / 'dagster'),
            overrides={'python_logs': {'managed_python_loggers': [''], 'python_log_level': 'INFO'}},
        ) as instance:
            yield SourceStore(client, 'origo', spec), instance, build_source_bundle(spec)
    finally:
        client.disconnect()
def _selection(end: str = DAY) -> dict[str, str]:
    return {'dagster/asset_partition_range_start': DAY, 'dagster/asset_partition_range_end': end}
def test_one_job_prepares_verifies_and_publishes_all_perp_agg_files(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, instance, bundle = ready_job
    assert {component.key for component in store.spec.components} == {
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
    }
    assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface_shadow'}
    assert store.execute('EXISTS TABLE origo.source_activation_log') == [(0,)]
    assert instance.all_instigator_state() == []
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))

    def no_copy(self: SourceStore, component: str, snapshot: Snapshot) -> list[tuple[object, ...]]:
        raise AssertionError(
            'Publication must query pinned projections without copying historical rows into Python.'
        )

    monkeypatch.setattr(SourceStore, 'rows', no_copy)
    result = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert len(instance.all_instigator_state()) == len(bundle.sensors) + len(bundle.schedules)
    assert store.canonical_ready()
    assert store.execute('SELECT min(successful) FROM origo.source_capacity_log') == [(1,)]
    token = store.snapshot().token
    for consumer in store.spec.consumers:
        root = tmp_path / 'files' / store.spec.key / consumer.key
        manifest = json.loads((root / 'latest.json').read_text())
        assert manifest['state_token'] == token
        # The public series start in 2020; the fixture day publishes an empty, current state.
        assert manifest['files'] == []
        assert manifest.get('month_tokens', {}) == {} and manifest.get('uploads', []) == []
        for file in manifest['files']:
            path = root / 'versions' / manifest['version'] / file['path']
            assert hashlib.sha256(path.read_bytes()).hexdigest() == file['sha256']
        sensor = next(
            (
                sensor
                for sensor in bundle.sensors
                if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
            ),
            None,
        )
        # A consumer that pins provisional rows is published by the provisional worker,
        # not by a sensor; canonical-only consumers keep theirs.
        assert (sensor is not None) == consumer.canonical_only
        if sensor is None:
            continue
        with build_sensor_context(
            instance=instance,
            definitions=Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors),
        ) as context:
            assert sensor.evaluate_tick(context).run_requests == []
    # The CANARY shadow consumer renders locally and never uploads.
    assert FakeHfApi.calls == []
    shadow = json.loads(
        (tmp_path / 'files' / store.spec.key / 'huggingface_shadow' / 'latest.json').read_text()
    )
    assert shadow['kind'] == 'huggingface_shadow' and shadow['uploads'] == []
    # Re-deployment restores code-owned sensor state without losing its cursor.
    state = instance.all_instigator_state()[0]
    from dagster._core.scheduler.instigation import InstigatorStatus

    instance.update_instigator_state(state.with_status(InstigatorStatus.STOPPED))
    with pytest.raises(RuntimeError, match='sensor or schedule is not prepared'):
        prepare_source(store.spec, instance, check=True)
    prepare_source(store.spec, instance)
    prepare_source(store.spec, instance, check=True)
    assert all(state.status.value == 'RUNNING' for state in instance.all_instigator_state())
    assert store.snapshot().token == token
def test_perp_agg_file_failure_fails_job_and_retry_keeps_verified_generation(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, _bundle = ready_job
    original = store.spec.consumers[1]
    attempts = 0

    def interrupted(
        reader: SnapshotReader,
        snapshot: Snapshot,
        destination: str,
        *,
        allow_full: bool = False,
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError('Interrupted publication')
        original.publish(reader, snapshot, destination)

    spec = replace(
        store.spec,
        consumers=(
            store.spec.consumers[0],
            ConsumerSpec(original.key, interrupted),
            *store.spec.consumers[2:],
        ),
    )
    job = next(job for job in build_source_bundle(spec).jobs if job.name.startswith('backfill_'))
    failed = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert not failed.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert not (tmp_path / 'files' / spec.key / 'huggingface_shadow' / 'latest.json').exists()
    manifest = tmp_path / 'files' / spec.key / 'mount' / 'latest.json'
    published = manifest.read_bytes()
    assert (
        instance.get_latest_materialization_event(AssetKey(f'publish_{spec.key}_mount')) is not None
    )
    before = store.snapshot()
    retried = job.execute_in_process(instance=instance, tags=_selection(), raise_on_error=False)
    assert retried.success
    assert store.snapshot() == before
    assert manifest.read_bytes() == published
    assert instance.get_run_by_id(failed.run_id).status.value == 'FAILURE'
def test_perp_agg_unavailable_day_requests_publication_and_preserves_completed_day(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, bundle = ready_job
    job = Definitions(
        assets=bundle.assets, jobs=bundle.jobs
    ).resolve_implicit_global_asset_job_def()
    assert job.execute_in_process(
        instance=instance, partition_key=DAY, asset_selection=[AssetKey(ASSET)]
    ).success
    result = job.execute_in_process(
        instance=instance,
        partition_key='2020-01-02',
        asset_selection=[AssetKey(ASSET)],
        raise_on_error=False,
    )
    assert not result.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY}
    assert store.canonical_ready()
    assert list((tmp_path / 'files').rglob('latest.json')) == []
    statuses = instance.get_status_by_partition(
        AssetKey(ASSET),
        [DAY, '2020-01-02'],
        DailyPartitionsDefinition(start_date=DAY, timezone='UTC'),
    )
    assert statuses is not None
    assert statuses[DAY].value == 'MATERIALIZED'
    assert statuses['2020-01-02'].value == 'FAILED'
    for sensor in bundle.sensors:
        if any(
            sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
            for consumer in store.spec.consumers
        ):
            with build_sensor_context(
                instance=instance,
                definitions=Definitions(
                    assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors
                ),
            ) as context:
                requests = sensor.evaluate_tick(context).run_requests
                assert len(requests) == 1
                assert requests[0].tags['origo_source_state_token'] == store.snapshot().token
def test_perp_agg_new_verified_data_automatically_requests_every_canonical_only_consumer(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    store, instance, bundle = ready_job
    backfill = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    assert backfill.execute_in_process(instance=instance, tags=_selection()).success
    previous_token = store.snapshot().token
    canonical = next(
        job for job in bundle.jobs if job.name == f'refresh_{store.spec.key}_canonical_source_job'
    )
    assert canonical.execute_in_process(instance=instance, partition_key='2020-01-01').success
    assert store.canonical_ready()
    assert store.snapshot().token != previous_token
    definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors)
    canonical_only = [consumer for consumer in store.spec.consumers if consumer.canonical_only]
    assert [consumer.key for consumer in canonical_only] == ['huggingface_shadow']
    for consumer in canonical_only:
        sensor = next(
            sensor
            for sensor in bundle.sensors
            if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
        )
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            requests = sensor.evaluate_tick(context).run_requests
        assert len(requests) == 1
        request = requests[0]
        assert request.tags['origo_source_state_token'] == store.snapshot().token
        publisher = next(
            job for job in bundle.jobs if job.name == f'publish_{store.spec.key}_{consumer.key}_job'
        )
        assert publisher.execute_in_process(
            instance=instance, run_config=request.run_config, tags=request.tags
        ).success
        manifest = json.loads(
            (tmp_path / 'files' / store.spec.key / consumer.key / 'latest.json').read_text()
        )
        assert manifest['state_token'] == store.snapshot().token
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            assert sensor.evaluate_tick(context).run_requests == []
def test_perp_agg_publication_follows_canonical_state_across_provisional_refreshes(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from origo.sources.contracts import Partition, StateRecord
    from origo.sources.lifecycle import SourceRuntime

    store, instance, bundle = ready_job
    SourceRuntime(store.spec, store, tmp_path / 'locks', 'test-setup').setup()
    generations = iter(range(1, 100))

    def provisional_refresh() -> None:
        # Scheduling state only: one more partial-day generation after the canonical day.
        start = datetime(2020, 1, 1, tzinfo=UTC)
        partition = Partition(
            start.strftime('%Y-%m-%dT%H:%M:%SZ'), start, start + timedelta(minutes=1), True
        )
        store.insert_activation(
            StateRecord(partition, next(generations), 'provisional', uuid4(), ()),
            'provisional-refresh',
        )

    def overlapping(
        publish: Callable[[SnapshotReader, Snapshot, str], None],
    ) -> Callable[[SnapshotReader, Snapshot, str], None]:
        def render(
            reader: SnapshotReader,
            snapshot: Snapshot,
            destination: str,
            *,
            allow_full: bool = False,
        ) -> None:
            provisional_refresh()
            publish(reader, snapshot, destination)

        return render

    provisional_refresh()
    spec = replace(
        store.spec,
        consumers=tuple(
            replace(consumer, publish=overlapping(consumer.publish))
            for consumer in store.spec.consumers
        ),
    )
    source = build_source_bundle(spec)
    backfill = next(job for job in source.jobs if job.name.startswith('backfill_'))
    assert backfill.execute_in_process(instance=instance, tags=_selection()).success
    canonical = store.snapshot(canonical_only=True).token
    assert store.snapshot().token != canonical
    for consumer in spec.consumers:
        manifest = json.loads(
            (tmp_path / 'files' / store.spec.key / consumer.key / 'latest.json').read_text()
        )
        assert manifest['state_token'] == canonical
        if consumer.canonical_only:
            assert manifest['pinned_token'] == canonical
        else:
            assert manifest['pinned_token'] not in (canonical, store.snapshot().token)
    definitions = Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors)
    sensors = {
        consumer.key: next(
            sensor
            for sensor in source.sensors
            if sensor.name == f'{store.spec.key}_{consumer.key}_sensor'
        )
        for consumer in spec.consumers
        if consumer.canonical_only
    }
    assert set(sensors) == {'huggingface_shadow'}
    # The consumer that pins provisional rows is the provisional worker's: it publishes
    # when the pinned state changed and a native backfill does not own publication.
    from origo.workers.dagster_reader import DagsterReader
    from origo.workers.provisional import ProvisionalFeed
    from origo.workers.receipts import ensure_monitoring_tables
    from origo.workers.report import Reporter

    ensure_monitoring_tables(store.client, 'origo')

    class _NoBackfill:
        def backfill_owns_publication(self, source_key: str) -> bool:
            return False

    worker = ProvisionalFeed(
        [spec],
        publication_root=tmp_path / 'files',
        reporter=cast(Reporter, object()),
        dagster=cast(DagsterReader, _NoBackfill()),
    )
    provisional_refresh()
    for consumer in sensors:
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            assert sensors[consumer].evaluate_tick(context).run_requests == []
    manifest_path = tmp_path / 'files' / spec.key / 'mount' / 'latest.json'
    published, unpublished = worker._publish(store, spec, datetime.now(UTC))
    assert (published, unpublished) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == canonical
    # The overlapping renderer refreshed the provisional state during the render, so the
    # files pin a state behind the current one and the next publication follows it; once
    # the state stops moving, the worker publishes nothing.
    assert pinned['pinned_token'] not in (canonical, store.snapshot().token)
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == canonical
    assert pinned['pinned_token'] == store.snapshot().token
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([], [])
    canonical_job = next(
        job for job in source.jobs if job.name == f'refresh_{store.spec.key}_canonical_source_job'
    )
    assert canonical_job.execute_in_process(instance=instance, partition_key='2020-01-01').success
    advanced = store.snapshot(canonical_only=True).token
    assert advanced != canonical
    for consumer in sensors:
        with build_sensor_context(instance=instance, definitions=definitions) as context:
            requests = sensors[consumer].evaluate_tick(context).run_requests
        assert len(requests) == 1
        assert requests[0].tags['origo_source_state_token'] == advanced
    assert worker._publish(store, store.spec, datetime.now(UTC)) == ([f'{spec.key}:mount'], [])
    pinned = json.loads(manifest_path.read_text())
    assert pinned['state_token'] == advanced
    assert pinned['pinned_token'] == store.snapshot().token
def test_perp_agg_canonical_day_retires_failed_provisional_intervals_inside_it(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from uuid import uuid4

    from origo.sources.failures import FailureLog
    from origo.sources.lifecycle import SourceRuntime

    store, _instance, _bundle = ready_job
    runtime = SourceRuntime(store.spec, store, tmp_path / 'locks', 'test-setup')
    runtime.setup()
    injected = FailureLog(store, tmp_path / 'locks', str(uuid4()))
    inside = f'{DAY}T10:15:00Z'
    injected.record(
        operation='component',
        scope='PARTITION',
        partition=inside,
        component='raw_latest',
        error_code='COMPONENT_CONTENT_INVALID',
    )
    injected.record(
        operation='provisional', scope='PARTITION', partition=inside, error_code='RUN_FAILED'
    )
    outside = '2019-09-09T00:03:00Z'
    injected.record(
        operation='provisional', scope='PARTITION', partition=outside, error_code='RUN_FAILED'
    )
    runtime.build(DAY)
    assert store.execute(
        'SELECT partition_key, argMax(event_type, event_time), argMax(details_json, event_time) '
        'FROM origo.source_failure_log GROUP BY failure_key, partition_key ORDER BY partition_key',
    ) == [
        (outside, 'FAILED', '{}'),
        (inside, 'RECOVERED', '{"reason": "superseded by the canonical day"}'),
        (inside, 'RECOVERED', '{"reason": "superseded by the canonical day"}'),
    ]
@pytest.mark.parametrize('status', ['FAILED', 'COMPLETED_FAILED', 'CANCELED'])
def test_perp_agg_native_backfill_failure_releases_publication(
    tmp_path: Path, status: str
) -> None:
    from dagster import DagsterRunStatus
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill

    from origo.sources.prepare import backfill_active, backfill_owns_publication

    spec = BINANCE_PERP_AGGTRADES_SPEC
    bundle = build_source_bundle(spec)
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        for day, run_status in (
            ('2020-01-02', DagsterRunStatus.FAILURE),
            ('2020-01-01', DagsterRunStatus.SUCCESS),
        ):
            instance.create_run_for_job(
                job,
                status=run_status,
                tags={
                    'dagster/asset_partition_range_start': day,
                    'dagster/asset_partition_range_end': day,
                },
            )
        # Terminal runs alone never hold publication.
        assert not backfill_active(instance, spec)
        assert not backfill_owns_publication(instance, spec)
        instance.add_backfill(
            PartitionBackfill(
                backfill_id='separate-gaps',
                status=BulkActionStatus[status],
                from_failure=False,
                tags={},
                backfill_timestamp=1.0,
                asset_selection=[AssetKey(ASSET)],
            )
        )
        # A terminal native selection does not hold it either.
        assert not backfill_active(instance, spec)
        assert not backfill_owns_publication(instance, spec)
def test_perp_agg_native_job_backfill_waits_for_own_selected_generations(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle], tmp_path: Path
) -> None:
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._core.storage.tags import BACKFILL_ID_TAG

    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    # An earlier verified generation must not satisfy a new backfill's queued day.
    assert job.execute_in_process(instance=instance, partition_key='2020-01-01').success
    root = tmp_path / 'files' / store.spec.key
    previous = {
        consumer.key: (root / consumer.key / 'latest.json').read_bytes()
        for consumer in store.spec.consumers
    }
    parent = PartitionBackfill(
        backfill_id='real-native-job',
        status=BulkActionStatus.REQUESTED,
        from_failure=False,
        tags={},
        backfill_timestamp=datetime.now(UTC).timestamp(),
        asset_selection=list(job.asset_layer.executable_asset_keys),
        partition_names=[DAY, '2020-01-01'],
    )
    assert not parent.is_asset_backfill
    instance.add_backfill(parent)
    first = job.execute_in_process(
        instance=instance,
        partition_key=DAY,
        tags={BACKFILL_ID_TAG: parent.backfill_id},
    )
    assert first.success
    assert instance.get_materialized_partitions(AssetKey(ASSET)) == {DAY, '2020-01-01'}
    assert any(event.is_step_skipped for event in first.all_events)
    for consumer in store.spec.consumers:
        assert (root / consumer.key / 'latest.json').read_bytes() == previous[consumer.key]
    last = job.execute_in_process(
        instance=instance,
        partition_key='2020-01-01',
        tags={BACKFILL_ID_TAG: parent.backfill_id},
    )
    assert last.success
    for consumer in store.spec.consumers:
        assert (
            json.loads((root / consumer.key / 'latest.json').read_text())['state_token']
            == store.snapshot().token
        )
    assert store.execute('SELECT uniqExact(partition_key) FROM origo.source_backfill_log') == [(2,)]


def test_perp_agg_huggingface_upload_records_every_rendered_series(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import polars as pl

    from origo.sources.profiles import perp_agg_consumers
    from origo.sources.profiles.formulas import perp_agg_huggingface as agg_snapshot

    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    assert job.execute_in_process(instance=instance, tags=_selection()).success
    columns = [c.name for c in next(c for c in store.spec.components if c.key == 'time').columns]
    real = store.execute(f'SELECT {", ".join(columns)} FROM {store.component_table("time")} LIMIT 2')
    assert len(real) == 2
    frame = pl.DataFrame(real, schema=columns, orient='row')
    monkeypatch.setattr(agg_snapshot, 'get_perp_agg_klines_from_1m_projection', lambda **kwargs: frame)
    monkeypatch.setattr(
        agg_snapshot, 'get_perp_agg_dollar_klines', lambda **kwargs: frame.clear()
    )
    destination = str(tmp_path / 'files' / store.spec.key / 'huggingface')
    perp_agg_consumers._huggingface(store, store.snapshot(), destination)
    assert [call for call, _ in FakeHfApi.calls] == ['create_repo', 'upload_folder'] * 6
    manifest = json.loads((Path(destination) / 'latest.json').read_text())
    assert manifest['kind'] == 'huggingface'
    assert len(manifest['uploads']) == len(manifest['files']) == 6
    assert len({entry['repo_id'] for entry in manifest['uploads']}) == 6


def test_perp_agg_huggingface_shadow_renders_locally_without_uploading(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import polars as pl

    from origo.sources.profiles import perp_agg_consumers
    from origo.sources.profiles.formulas import perp_agg_huggingface as agg_snapshot

    store, instance, bundle = ready_job
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    assert job.execute_in_process(instance=instance, tags=_selection()).success
    columns = [c.name for c in next(c for c in store.spec.components if c.key == 'time').columns]
    real = store.execute(f'SELECT {", ".join(columns)} FROM {store.component_table("time")} LIMIT 2')
    assert len(real) == 2
    frame = pl.DataFrame(real, schema=columns, orient='row')
    monkeypatch.setattr(agg_snapshot, 'get_perp_agg_klines_from_1m_projection', lambda **kwargs: frame)
    monkeypatch.setattr(
        agg_snapshot, 'get_perp_agg_dollar_klines', lambda **kwargs: frame.clear()
    )
    destination = str(tmp_path / 'files' / store.spec.key / 'huggingface_shadow')
    perp_agg_consumers._huggingface_shadow(store, store.snapshot(), destination)
    assert FakeHfApi.calls == []
    manifest = json.loads((Path(destination) / 'latest.json').read_text())
    assert manifest['kind'] == 'huggingface_shadow'
    assert manifest['uploads'] == [] and len(manifest['files']) == 6


def test_perp_agg_mount_renders_post_cutoff_month_and_sweeps_only_own_staging(
    ready_job: tuple[SourceStore, DagsterInstance, SourceBundle],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import time
    from uuid import uuid4

    import polars as pl

    from origo.sources import publication
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.profiles.formulas.perp_agg_series import SPECS

    store, _instance, _bundle = ready_job
    day = '2024-04-20'
    runtime = SourceRuntime(store.spec, store, tmp_path / 'locks', str(uuid4()))
    runtime.setup(anchor=datetime(2024, 4, 20, tzinfo=UTC))
    runtime.build(day)
    parquet = tmp_path / 'parquet'
    own = parquet / '.binance_perp_aggtrades-staging-stale'
    own.mkdir(parents=True)
    (own / 'x.parquet').write_bytes(b'x')
    foreign = parquet / '.staging-foreign'
    foreign.mkdir(parents=True)
    (foreign / 'x.parquet').write_bytes(b'x')
    old = time.time() - 7200
    os.utime(own, (old, old))
    os.utime(foreign, (old, old))
    mount = tmp_path / store.spec.key / 'mount'
    snapshot = runtime.publish('mount', str(mount))
    assert not own.exists()
    assert (foreign / 'x.parquet').is_file()
    assert not list(parquet.glob('.binance_perp_aggtrades-staging-*'))
    manifest = json.loads((mount / 'latest.json').read_text())
    assert manifest['state_token'] == snapshot.token == manifest['pinned_token']
    assert list(manifest['month_tokens']) == ['2024-04']
    assert len(manifest['files']) == 24
    for entry in manifest['files']:
        assert Path(entry['path']).is_absolute() and Path(entry['path']).is_file()
    for series in SPECS:
        month = parquet / series.sub_path / '2024/04.parquet'
        assert pl.read_parquet(month).height > 0
        assert (tmp_path / 'arrow' / series.name / 'latest.arrow').is_file()
    written = {
        entry['path']: Path(entry['path']).stat().st_mtime_ns for entry in manifest['files']
    }
    with monkeypatch.context() as patch:
        patch.setattr(publication, 'publication_current', lambda *a, **k: False)
        runtime.publish('mount', str(mount))
    repeated = json.loads((mount / 'latest.json').read_text())
    assert {e['path']: Path(e['path']).stat().st_mtime_ns for e in repeated['files']} == written


def test_source_inventory_is_explicit() -> None:
    assert PERP_AGG_CASE.inventory == (
        'perp_agg_time_1m',
        'perp_agg_time_15m',
        'perp_agg_time_30m',
        'perp_agg_time_1h',
        'perp_agg_time_2h',
        'perp_agg_time_4h',
        'perp_agg_dollar_1M',
        'perp_agg_dollar_15M',
        'perp_agg_dollar_30M',
        'perp_agg_dollar_60M',
        'perp_agg_dollar_120M',
        'perp_agg_dollar_240M',
    )
    assert tuple(series.name for series in PERP_AGG_SERIES_SPECS) == PERP_AGG_CASE.inventory
