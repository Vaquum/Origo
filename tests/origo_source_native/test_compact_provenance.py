"""Retention and lossless provenance checks driven by real Binance archive runs."""

import hashlib
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import AssetKey, DagsterInstance, DagsterRun, RunsFilter

from origo.maintenance import archive, retention
from origo.maintenance.event_storage import OrigoSqliteEventLogStorage
from origo.maintenance.protocol import OperationalMetadataMaintenanceConfig
from origo.maintenance.roles import PROJECTION_JOBS, SOURCE_JOBS, run_role
from origo.maintenance.sqlite import Layout
from origo.sources.registry import SOURCE_REGISTRY

from .test_dagster_metadata_maintenance import (
    PARTITIONS,
    execute_archive,
    metadata_instance,
    planned,
)

# Importing the real-instance fixture makes it available to this test module.
__all__ = ['metadata_instance']


def _source(instance: DagsterInstance) -> str:
    return execute_archive(instance, tags={'origo_source_key': SOURCE_REGISTRY[0].key})


def test_source_and_mixed_runs_are_never_retired(metadata_instance: DagsterInstance) -> None:
    for job in SOURCE_JOBS:
        assert run_role(DagsterRun(job_name=job)) == 'source'
    for job in PROJECTION_JOBS:
        assert run_role(DagsterRun(job_name=job)) == 'projection'
    assert run_role(DagsterRun(job_name='create_tdw_database_job')) == 'unknown'
    assert (
        run_role(
            DagsterRun(
                job_name='build_bar_store_arrow_job',
                tags={'origo_source_key': SOURCE_REGISTRY[0].key},
            )
        )
        == 'source'
    )
    run_id = _source(metadata_instance)
    layout, journal, candidate = planned(metadata_instance, run_id)
    policy = OperationalMetadataMaintenanceConfig(metadata_budget_bytes=1024**3)
    record = metadata_instance.get_run_records(RunsFilter(run_ids=[run_id]), limit=1)[0]
    assert (
        retention.protection(
            metadata_instance,
            layout,
            record,
            policy,
            time.time() + 365 * 86400,
            time.monotonic() + 10,
        )
        == 'source_provenance'
    )
    assert (
        retention.reclaim(
            metadata_instance,
            layout,
            candidate,
            journal,
            layout.runs.parent / 'guard.json',
            policy,
            time.monotonic() + 10,
        )
        == 0
    )
    assert metadata_instance.get_run_by_id(run_id) is not None
    assert layout.shard(run_id).exists()
    with pytest.raises(RuntimeError, match='cannot be retired'):
        metadata_instance.delete_run(run_id)
    assert metadata_instance.get_run_by_id(run_id) is not None


def test_compacted_source_preserves_dagster_reads(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    run_id = _source(instance)
    storage = instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    layout = Layout.from_instance(instance)
    key = AssetKey('metadata_proof_archive')
    before = instance.get_records_for_run(run_id)
    stats = instance.get_run_stats(run_id)
    steps = instance.get_run_step_stats(run_id)
    state = instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS)
    assert storage.archive_run(run_id, time.monotonic() + 10) > 0
    assert not layout.shard(run_id).exists()
    assert instance.get_run_by_id(run_id) is not None
    assert instance.get_records_for_run(run_id) == before
    assert instance.get_run_stats(run_id) == stats
    assert instance.get_run_step_stats(run_id) == steps
    assert instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS) == state
    first = instance.get_records_for_run(run_id, limit=3)
    rest = instance.get_records_for_run(run_id, cursor=first.cursor)
    assert [*first.records, *rest.records] == before.records
    assert instance.get_records_for_run(run_id, ascending=False).records == list(
        reversed(before.records)
    )
    assert run_id in storage.get_all_run_ids()
    assert not layout.shard(run_id).exists(), 'Reads must not recreate per-run files.'


def test_source_compaction_recovers_after_interruption(
    metadata_instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = _source(metadata_instance)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    layout = Layout.from_instance(metadata_instance)
    before = metadata_instance.get_records_for_run(run_id)
    original = Path.unlink

    def interrupted(path: Path, missing_ok: bool = False) -> None:
        if path == layout.shard(run_id):
            raise OSError('Controlled crash after durable archive commit.')
        original(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', interrupted)
        with pytest.raises(OSError, match='Controlled crash'):
            storage.archive_run(run_id, time.monotonic() + 10)
    assert layout.shard(run_id).exists()
    assert metadata_instance.get_records_for_run(run_id) == before
    assert storage.archive_run(run_id, time.monotonic() + 10) > 0
    assert not layout.shard(run_id).exists()
    assert metadata_instance.get_records_for_run(run_id) == before


def test_late_source_event_rehydrates_without_losing_history(
    metadata_instance: DagsterInstance,
) -> None:
    run_id = _source(metadata_instance)
    instance = metadata_instance
    storage = instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    layout = Layout.from_instance(instance)
    before = instance.get_records_for_run(run_id).records
    storage.archive_run(run_id, time.monotonic() + 10)
    run = instance.get_run_by_id(run_id)
    assert run is not None
    instance.report_engine_event('Late source execution diagnostic.', dagster_run=run)
    after = instance.get_records_for_run(run_id).records
    assert after[: len(before)] == before and len(after) == len(before) + 1
    assert layout.shard(run_id).exists()
    assert archive.read_image(layout.events.parent, run_id, time.monotonic() + 10) is None
    storage.archive_run(run_id, time.monotonic() + 10)
    assert instance.get_records_for_run(run_id).records == after


def test_corrupt_source_archive_is_an_error(metadata_instance: DagsterInstance) -> None:
    run_id = _source(metadata_instance)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    layout = Layout.from_instance(metadata_instance)
    storage.archive_run(run_id, time.monotonic() + 10)
    with sqlite3.connect(archive.archive_path(layout.events.parent)) as database:
        database.execute("UPDATE source_runs SET sha256='corrupted' WHERE run_id=?", (run_id,))
    with pytest.raises(ValueError, match='checksum'):
        metadata_instance.get_records_for_run(run_id)
    assert not layout.shard(run_id).exists()


def test_projection_retirement_preserves_current_state(
    metadata_instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = execute_archive(metadata_instance)
    layout, journal, candidate = planned(metadata_instance, run_id)
    key = AssetKey('metadata_proof_archive')
    before = metadata_instance.fetch_materializations(key, limit=1).records
    state = metadata_instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS)
    policy = OperationalMetadataMaintenanceConfig(metadata_budget_bytes=1024**3)
    future = time.time() + 2 * 3600
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    assert (
        retention.reclaim(
            metadata_instance,
            layout,
            candidate,
            journal,
            layout.runs.parent / 'projection.json',
            policy,
            time.monotonic() + 10,
        )
        > 0
    )
    assert metadata_instance.get_run_by_id(run_id) is None
    assert metadata_instance.fetch_materializations(key, limit=1).records == before
    assert metadata_instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS) == state


def test_shared_compaction_releases_physical_space(
    metadata_instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    from origo.maintenance.compaction import incremental_compaction, initialize_compaction
    from origo.maintenance.sqlite import allocated

    ids = [execute_archive(metadata_instance) for _ in range(12)]
    layout = Layout.from_instance(metadata_instance)
    key = AssetKey('metadata_proof_archive')
    expected = metadata_instance.fetch_materializations(key, limit=1).records
    for path in (layout.runs, layout.events, layout.schedules):
        initialize_compaction(path, time.monotonic() + 20, 1)
    before = allocated(layout.runs) + allocated(layout.events)
    future = time.time() + 2 * 3600
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    policy = OperationalMetadataMaintenanceConfig(metadata_budget_bytes=1024**3)
    for run_id in ids:
        _, journal, candidate = planned(metadata_instance, run_id)
        retention.reclaim(
            metadata_instance,
            layout,
            candidate,
            journal,
            layout.runs.parent / 'compact.json',
            policy,
            time.monotonic() + 20,
        )
    assert metadata_instance.fetch_materializations(key, limit=1).records == expected
    for path in (layout.runs, layout.events, layout.schedules):
        incremental_compaction(path, time.monotonic() + 20, 1)
    assert allocated(layout.runs) + allocated(layout.events) < before
    assert metadata_instance.fetch_materializations(key, limit=1).records == expected


def test_capacity_report_counts_all_retained_storage(metadata_instance: DagsterInstance) -> None:
    import os

    from origo.maintenance.worker import directory_bytes

    run_id = _source(metadata_instance)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    layout = Layout.from_instance(metadata_instance)
    before = directory_bytes(layout, time.monotonic() + 20)
    storage.archive_run(run_id, time.monotonic() + 20)
    after = directory_bytes(layout, time.monotonic() + 20)
    archive_path = archive.archive_path(layout.events.parent)
    assert layout.artifact_root is not None
    roots = {
        layout.runs.parent,
        layout.events.parent,
        layout.schedules.parent,
        layout.compute,
        layout.artifact_root,
    }
    outer_roots = [
        root
        for root in roots
        if not any(root != other and root.is_relative_to(other) for other in roots)
    ]
    expected = 0
    for root in outer_roots:
        for directory, _, files in os.walk(root):
            expected += Path(directory).stat().st_blocks * 512
            expected += sum((Path(directory) / name).stat().st_blocks * 512 for name in files)
    assert after == expected and after < before
    assert after >= archive_path.stat().st_blocks * 512 > 0
    assert metadata_instance.get_records_for_run(run_id).records


def test_source_archive_supports_dagster_upgrade(metadata_instance: DagsterInstance) -> None:
    run_id = _source(metadata_instance)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    before = metadata_instance.get_records_for_run(run_id)
    storage.archive_run(run_id, time.monotonic() + 10)
    storage.upgrade()
    assert metadata_instance.get_records_for_run(run_id) == before
    assert not Layout.from_instance(metadata_instance).shard(run_id).exists()


def test_shared_json_compaction_preserves_queries_and_updates(
    metadata_instance: DagsterInstance,
) -> None:
    from origo.maintenance.codec import decode_json

    instance = metadata_instance
    run_id = _source(instance)
    layout = Layout.from_instance(instance)
    run_before = instance.get_run_by_id(run_id)
    records_before = instance.get_run_records(RunsFilter(run_ids=[run_id]))
    key = AssetKey('metadata_proof_archive')
    assets_before = instance.fetch_materializations(key, limit=20)
    storage = instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    storage.archive_run(run_id, time.monotonic() + 10)
    with sqlite3.connect(layout.runs) as database:
        body = database.execute('SELECT run_body FROM runs WHERE run_id=?', (run_id,)).fetchone()[0]
        assert isinstance(body, bytes) and run_id in decode_json(body)
    with sqlite3.connect(layout.events) as database:
        payloads = database.execute(
            'SELECT event FROM event_logs WHERE run_id=?', (run_id,)
        ).fetchall()
        assert payloads and all(isinstance(row[0], bytes) for row in payloads)
    assert instance.get_run_by_id(run_id) == run_before
    assert instance.get_run_records(RunsFilter(run_ids=[run_id])) == records_before
    assert instance.fetch_materializations(key, limit=20) == assets_before
    instance.add_run_tags(run_id, {'provenance_note': 'Verified original archive'})
    updated = instance.get_run_by_id(run_id)
    assert updated is not None and updated.tags['provenance_note'] == 'Verified original archive'
    assert instance.fetch_materializations(key, limit=20) == assets_before
    assert instance.get_records_for_run(run_id).records


def test_corrupt_shared_json_fails_loudly(metadata_instance: DagsterInstance) -> None:
    run_id = _source(metadata_instance)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    storage.archive_run(run_id, time.monotonic() + 10)
    layout = Layout.from_instance(metadata_instance)
    with sqlite3.connect(layout.runs) as database:
        payload = database.execute(
            'SELECT run_body FROM runs WHERE run_id=?', (run_id,)
        ).fetchone()[0]
        damaged = payload[:20] + bytes([payload[20] ^ 1]) + payload[21:]
        database.execute('UPDATE runs SET run_body=? WHERE run_id=?', (damaged, run_id))
    with pytest.raises(ValueError, match='checksum'):
        metadata_instance.get_run_by_id(run_id)
    assert archive.read_image(layout.events.parent, run_id, time.monotonic() + 10) is not None


@pytest.mark.parametrize('partition', ['2017-08-17', '2020-01-01'])
def test_packed_io_round_trip_and_overwrite(tmp_path: Path, partition: str) -> None:
    from dagster import build_input_context, build_output_context
    from upath import UPath

    from origo.maintenance.io_manager import PackedFilesystemIOManager
    from origo.maintenance.outputs import MAX_OUTPUT_BYTES

    from .test_dagster_metadata_maintenance import ARCHIVES

    manager = PackedFilesystemIOManager(str(tmp_path))
    path = UPath(tmp_path / 'archive')
    output = build_output_context(asset_key=AssetKey('archive'))
    value = (ARCHIVES / f'BTCUSDT-trades-{partition}.zip').read_bytes()
    metadata = {'archive_sha256': hashlib.sha256(value).hexdigest()}
    for expected in (metadata, value, metadata):
        manager.dump_to_path(output, expected, path)
        assert manager.has_output(output)
        reopened = PackedFilesystemIOManager(str(tmp_path))
        assert reopened.load_from_path(build_input_context(), path) == expected
        assert path.exists() == (isinstance(expected, bytes) and len(expected) > MAX_OUTPUT_BYTES)
    manager.unlink(path)
    assert not manager.has_output(output)


def test_existing_output_migration_verifies_before_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dagster import build_input_context, build_output_context
    from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager
    from upath import UPath

    from origo.maintenance.io_manager import PackedFilesystemIOManager
    from origo.maintenance.outputs import OutputStore, pack_existing_assets

    from .test_dagster_metadata_maintenance import ARCHIVES

    payload = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip').read_bytes()
    path = tmp_path / 'archive'
    output = build_output_context(asset_key=AssetKey('archive'))
    legacy = PickledObjectFilesystemIOManager(base_dir=str(tmp_path))
    legacy.dump_to_path(output, payload, UPath(path))
    original = path.read_bytes()
    unlink = Path.unlink

    def interrupted(target: Path, missing_ok: bool = False) -> None:
        if target == path:
            raise OSError('Controlled interruption after output commit.')
        unlink(target, missing_ok=missing_ok)

    store = OutputStore(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', interrupted)
        with pytest.raises(OSError, match='Controlled interruption'):
            pack_existing_assets(store, [['archive']], time.monotonic() + 10)
    assert path.read_bytes() == original == store.read('archive')
    assert pack_existing_assets(store, [['archive']], time.monotonic() + 10) == 1
    assert not path.exists()
    manager = PackedFilesystemIOManager(str(tmp_path))
    assert manager.load_from_path(build_input_context(), UPath(path)) == payload
    with sqlite3.connect(store.path) as database:
        database.execute('UPDATE outputs SET payload=?', (payload,))
        database.commit()
    with pytest.raises(ValueError, match='checksum'):
        manager.load_from_path(build_input_context(), UPath(path))


def test_packed_io_is_used_by_normal_asset_materialization(
    metadata_instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib

    from dagster import asset, materialize

    from origo.maintenance.outputs import OutputStore

    from .test_dagster_metadata_maintenance import ARCHIVES

    monkeypatch.setenv('DAGSTER_DEFAULT_IO_MANAGER_MODULE', 'origo.maintenance.io_manager')
    monkeypatch.setenv('DAGSTER_DEFAULT_IO_MANAGER_ATTRIBUTE', 'packed_io_manager')
    monkeypatch.delenv('DAGSTER_DEFAULT_IO_MANAGER_SILENCE_FAILURES', raising=False)
    payload = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip').read_bytes()

    @asset
    def archive_bytes() -> bytes:
        return payload

    @asset
    def archive_digest(archive_bytes: bytes) -> str:
        return hashlib.sha256(archive_bytes).hexdigest()

    result = materialize([archive_bytes, archive_digest], instance=metadata_instance)
    assert result.success
    store = OutputStore(Path(metadata_instance.storage_directory()))
    assert store.read('archive_bytes') is not None
    assert store.read('archive_digest') is not None
    handled = [
        event.dagster_event.event_specific_data.metadata
        for event in metadata_instance.all_logs(result.run_id)
        if event.dagster_event is not None
        and event.dagster_event.event_type_value == 'HANDLED_OUTPUT'
    ]
    assert {value['storage_key'].value for value in handled} == {'archive_bytes', 'archive_digest'}
    assert all(value['path'].value == str(store.path) for value in handled)
    assert not (store.root / 'archive_bytes').exists()
    assert not (store.root / 'archive_digest').exists()
    assert (
        metadata_instance.get_latest_materialization_event(AssetKey('archive_digest')) is not None
    )


def test_output_migration_rejects_uncommitted_different_file(tmp_path: Path) -> None:
    from origo.maintenance.outputs import OutputStore, pack_existing_assets

    from .test_dagster_metadata_maintenance import ARCHIVES

    store = OutputStore(tmp_path)
    path = tmp_path / 'archive'
    original = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip').read_bytes()
    path.write_bytes(original)
    with store.lock('archive'):
        store.pack(path)
    interrupted = hashlib.sha256(original).hexdigest().encode()
    path.write_bytes(interrupted)
    with pytest.raises(RuntimeError, match='Raw and committed packed output differ'):
        pack_existing_assets(store, [['archive']], time.monotonic() + 10)
    assert path.read_bytes() == interrupted
    assert store.read('archive') == original


def test_run_scoped_outputs_keep_filesystem_lifetime(tmp_path: Path) -> None:
    from dagster import build_output_context
    from upath import UPath

    from origo.maintenance.io_manager import PackedFilesystemIOManager

    from .test_dagster_metadata_maintenance import ARCHIVES

    manager = PackedFilesystemIOManager(str(tmp_path))
    output = build_output_context(step_key='archive', name='result')
    path = UPath(tmp_path / 'result')
    value = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip').read_bytes()
    manager.dump_to_path(output, value, path)
    assert path.exists()
    assert manager.store.read('result') is None


def test_packed_asset_can_become_partitioned(tmp_path: Path) -> None:
    from dagster import build_output_context
    from upath import UPath

    from origo.maintenance.io_manager import PackedFilesystemIOManager

    from .test_dagster_metadata_maintenance import ARCHIVES

    manager = PackedFilesystemIOManager(str(tmp_path))
    path = UPath(tmp_path / 'archive')
    output = build_output_context(asset_key=AssetKey('archive'))
    value = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip').read_bytes()
    manager.dump_to_path(output, value, path)
    assert manager.store.read('archive') is not None
    manager._handle_transition_to_partitioned_asset(output, path)
    assert manager.store.read('archive') is None
    path.mkdir()
    manager.dump_to_path(output, value, path / '2017-08-17')
    assert manager.store.read('archive/2017-08-17') is not None


def test_archiving_one_run_does_not_block_another_run(
    metadata_instance: DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from origo.maintenance import event_storage

    first = _source(metadata_instance)
    second = _source(metadata_instance)
    second_run = metadata_instance.get_run_by_id(second)
    assert second_run is not None
    entered = threading.Event()
    release = threading.Event()
    original = event_storage.snapshot_image

    def slow_snapshot(path: Path, deadline: float) -> bytes:
        if path.stem == first:
            entered.set()
            if not release.wait(10):
                raise TimeoutError('Test did not release the paused source snapshot.')
        return original(path, deadline)

    monkeypatch.setattr(event_storage, 'snapshot_image', slow_snapshot)
    storage = metadata_instance.event_log_storage
    assert isinstance(storage, OrigoSqliteEventLogStorage)
    with ThreadPoolExecutor(max_workers=2) as executor:
        archive_future = executor.submit(storage.archive_run, first, time.monotonic() + 15)
        assert entered.wait(3)
        try:
            write = executor.submit(
                metadata_instance.report_engine_event,
                'Unrelated source run remains writable during compaction.',
                dagster_run=second_run,
            )
            write.result(timeout=2)
            assert any(
                'Unrelated source run remains writable' in row.event_log_entry.message
                for row in metadata_instance.get_records_for_run(second).records
            )
        finally:
            release.set()
        assert archive_future.result(timeout=5) > 0
    assert metadata_instance.get_records_for_run(first).records


def test_capacity_does_not_count_the_application_tree(tmp_path: Path) -> None:
    from origo.maintenance.worker import directory_bytes

    from .test_dagster_metadata_maintenance import ARCHIVES

    application = tmp_path / 'application'
    storage = application / 'storage'
    storage.mkdir(parents=True)
    home = tmp_path / 'instance'
    home.mkdir()
    with DagsterInstance.local_temp(
        str(home),
        overrides={
            'local_artifact_storage': {
                'module': 'dagster._core.storage.root',
                'class': 'LocalArtifactStorage',
                'config': {'base_dir': str(application)},
            }
        },
    ) as instance:
        layout = Layout.from_instance(instance)
        assert layout.artifact_root == storage
        layout.compute.mkdir(parents=True, exist_ok=True)
        before = directory_bytes(layout, time.monotonic() + 10)
        fixture = ARCHIVES / 'BTCUSDT-trades-2017-08-17.zip'
        (application / fixture.name).write_bytes(fixture.read_bytes())
        assert directory_bytes(layout, time.monotonic() + 10) == before
        (storage / fixture.name).write_bytes(fixture.read_bytes())
        assert directory_bytes(layout, time.monotonic() + 10) > before
