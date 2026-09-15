from __future__ import annotations

import fcntl
import hashlib
import json
import multiprocessing
import os
import time
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone, tzinfo
from multiprocessing.synchronize import Event
from typing import Self
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest
from dagster import DagsterInstance, ExecuteInProcessResult, materialize

os.environ.setdefault('CLICKHOUSE_PASSWORD', 'import-guard')

import origo.assets.build_depth_snapshot_store_arrow as depth_store
from origo.assets.build_bar_store_arrow import BarSeriesBuild, series_store_dir
from origo.assets.build_depth_snapshot_store_arrow import (
    DEPTH20_SOURCE_JOB_NAME,
    DEPTH200_SOURCE_JOB_NAME,
    DEPTH_SNAPSHOT_PARTITIONS,
    DEPTH_SNAPSHOT_SERIES,
    LATEST_MANIFEST_NAME,
    build_depth_snapshot_frame,
    build_depth_snapshot_store_arrow,
    depth_snapshot_chunk_relative_path,
    depth_snapshot_store_partition_run_request,
    minute_start_from_partition_key,
    publish_depth_snapshot_chunk,
    spec_for_depth_snapshot_series,
)

FIXTURES = Path(__file__).parent / 'fixtures' / 'depth_arrow_retention'
BASE = datetime(2026, 9, 15, 8, tzinfo=timezone.utc)
_ACTUAL_CUTOFF = depth_store._retention_cutoff
SOURCE_PARTITION_KEY = '2026-09-15T08:20:00+0000'


def _key(index: int) -> str:
    return (BASE + timedelta(minutes=index)).strftime('%Y-%m-%dT%H:%M:%S%z')


def _fixture(series: str, index: int) -> Path:
    return FIXTURES / series / f'{BASE + timedelta(minutes=index):%Y%m%dT%H%M%SZ}.arrow'


def _build(series: str, index: int) -> BarSeriesBuild:
    frame = pl.read_ipc(_fixture(series, index), memory_map=False)
    return BarSeriesBuild(frame, source_rows=frame.height, dropped_duplicate_ts=0)


def _rows(series: str, index: int) -> list[tuple[object, ...]]:
    return [
        (
            r['ts'],
            r['source_timestamp_ms'],
            r['last_update_id'],
            [(v['price'], v['qty']) for v in r['bids']],
            [(v['price'], v['qty']) for v in r['asks']],
        )
        for r in _build(series, index).df.to_dicts()
    ]


class FakeClickHouseClient:
    def __init__(self, rows: Sequence[tuple[object, ...]]) -> None:
        self.rows = rows
        self.queries: list[str] = []
        self.disconnected = False

    def execute(
        self, query: str, params: object | None = None, settings: object | None = None
    ) -> object:
        self.queries.append(query)
        return self.rows

    def disconnect(self) -> None:
        self.disconnected = True


@pytest.fixture(autouse=True)
def _recorded_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(depth_store, '_retention_cutoff', lambda: BASE + timedelta(minutes=5))


def _publish(series: str, index: int) -> depth_store.DepthSnapshotChunkPublish:
    return publish_depth_snapshot_chunk(series, _key(index), _build(series, index))


def _chunk(series: str, index: int) -> Path:
    return series_store_dir(series) / depth_snapshot_chunk_relative_path(
        BASE + timedelta(minutes=index)
    )


def _seed(series: str, indices: Sequence[int], latest: int) -> None:
    directory = series_store_dir(series)
    for index in indices:
        target = _chunk(series, index)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_fixture(series, index).read_bytes())
    manifest = {
        'series': series,
        'source_partition_key': _key(latest),
        'chunk': str(_chunk(series, latest).relative_to(directory)),
        'rows': 2,
        'source_rows': 2,
        'dropped_duplicate_ts': 0,
        'version': hashlib.sha256(_fixture(series, latest).read_bytes()).hexdigest()[:16],
        'updated_at_unix_ns': int((BASE + timedelta(minutes=latest)).timestamp() * 1e9),
    }
    (directory / LATEST_MANIFEST_NAME).write_text(json.dumps(manifest))


def _materialize(
    partition: str, dry_run: bool = False, instance: DagsterInstance | None = None
) -> ExecuteInProcessResult:
    return materialize(
        [build_depth_snapshot_store_arrow],
        partition_key='depth20_snapshots',
        instance=instance,
        run_config={
            'ops': {
                'build_depth_snapshot_store_arrow': {
                    'config': {'source_partition_key': partition, 'dry_run': dry_run}
                }
            }
        },
    )


def test_depth_snapshot_series_cover_depth20_and_depth200() -> None:
    assert DEPTH_SNAPSHOT_SERIES == ('depth20_snapshots', 'depth200_snapshots')
    assert set(DEPTH_SNAPSHOT_PARTITIONS.get_partition_keys()) == set(DEPTH_SNAPSHOT_SERIES)
    for series, depth in zip(DEPTH_SNAPSHOT_SERIES, (20, 200)):
        spec = spec_for_depth_snapshot_series(series)
        assert spec.depth == depth and spec.table_name == f'binance_spot_{series}'


def test_build_depth_snapshot_frame_shapes_fixed_size_books_and_zero_copy_buffers() -> None:
    rows = _rows('depth20_snapshots', 20)
    client = FakeClickHouseClient([rows[1], rows[0], rows[1]])
    build = build_depth_snapshot_frame(
        client,
        'origo',
        spec_for_depth_snapshot_series('depth20_snapshots'),
        BASE + timedelta(minutes=20),
    )
    assert build.df.equals(_build('depth20_snapshots', 20).df)
    assert build.source_rows == 3 and build.dropped_duplicate_ts == 1
    assert '2026-09-15 08:20:00.000' in client.queries[0]
    table = build.df.to_arrow()
    assert table['ts'].chunk(0).to_numpy(zero_copy_only=True).tolist() == [r[0] for r in rows]
    for side in ['bids', 'asks']:
        array = table[side].chunk(0)
        assert isinstance(array, pa.FixedSizeListArray) and array.type.list_size == 20
        assert len(array.values.field('price').to_numpy(zero_copy_only=True)) == 40


def test_build_depth_snapshot_frame_rejects_empty_source() -> None:
    with pytest.raises(RuntimeError, match='No source rows'):
        build_depth_snapshot_frame(
            FakeClickHouseClient([]),
            'origo',
            spec_for_depth_snapshot_series('depth20_snapshots'),
            BASE,
        )


def test_build_depth_snapshot_frame_rejects_wrong_book_depth() -> None:
    with pytest.raises(RuntimeError, match='Expected 200 bids levels'):
        build_depth_snapshot_frame(
            FakeClickHouseClient(_rows('depth20_snapshots', 20)),
            'origo',
            spec_for_depth_snapshot_series('depth200_snapshots'),
            BASE,
        )


def test_asset_publishes_depth_snapshot_arrow_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeClickHouseClient(_rows('depth20_snapshots', 20))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    monkeypatch.setattr(depth_store, 'make_clickhouse_client', lambda settings: client)
    with DagsterInstance.ephemeral() as instance:
        result = _materialize(SOURCE_PARTITION_KEY, instance=instance)
        events = instance.get_records_for_run(result.run_id).records
        assert any(
            'expired_files=0 reclaimed_file_bytes=0' in r.event_log_entry.message for r in events
        )
    assert result.success and client.disconnected
    assert pl.read_ipc(_chunk('depth20_snapshots', 20)).equals(_build('depth20_snapshots', 20).df)


def test_asset_publishes_single_record_batch_ipc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    _publish('depth200_snapshots', 20)
    with pa.memory_map(str(_chunk('depth200_snapshots', 20)), 'r') as mapping:
        reader = pa_ipc.open_file(mapping)
        assert reader.num_record_batches == 1
        assert reader.read_all().equals(_build('depth200_snapshots', 20).df.to_arrow())


def test_depth_snapshot_publish_keeps_latest_manifest_monotonic_for_backfilled_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    assert _publish('depth20_snapshots', 21).status == 'published'
    assert _publish('depth20_snapshots', 20).status == 'skipped_not_newer'
    assert _chunk('depth20_snapshots', 20).exists()
    assert json.loads((tmp_path / 'depth20_snapshots/latest.json').read_text())[
        'source_partition_key'
    ] == _key(21)


@pytest.mark.parametrize('series', DEPTH_SNAPSHOT_SERIES)
def test_depth_retention_window_and_latest_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, series: str
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    _seed(series, [0, 4, 5, 20], latest=4)
    before = _chunk(series, 4).read_bytes()
    assert _publish(series, 0).status == 'skipped_expired'
    assert not _chunk(series, 0).exists()
    assert _chunk(series, 4).read_bytes() == before
    assert _chunk(series, 5).exists()
    directory = tmp_path / series
    (directory / 'chunks/2025/12/31/23').mkdir(parents=True)
    _publish(series, 20)
    assert not _chunk(series, 4).exists() and not (directory / 'chunks/2025').exists()
    assert _chunk(series, 5).read_bytes() == _fixture(series, 5).read_bytes()
    manifest_path = directory / LATEST_MANIFEST_NAME
    assert set(json.loads(manifest_path.read_text())) == {
        'series',
        'source_partition_key',
        'chunk',
        'rows',
        'source_rows',
        'dropped_duplicate_ts',
        'version',
        'updated_at_unix_ns',
    }
    _seed(series, [0, 20], latest=20)
    manifest_path.write_text('{broken')
    with pytest.raises(json.JSONDecodeError):
        _publish(series, 21)
    assert _chunk(series, 0).exists()
    _seed(series, [0, 20], latest=20)
    _chunk(series, 20).unlink()
    with pytest.raises(FileNotFoundError):
        _publish(series, 21)
    assert _chunk(series, 0).exists()
    _seed(series, [0, 20], latest=20)
    wrong_schema = (FIXTURES / 'bar.arrow').read_bytes()
    _chunk(series, 20).write_bytes(wrong_schema)
    manifest = json.loads(manifest_path.read_text())
    manifest['version'] = hashlib.sha256(wrong_schema).hexdigest()[:16]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match='Invalid latest depth IPC'):
        _publish(series, 21)
    assert _chunk(series, 0).exists()
    _seed(series, [0, 20], latest=20)
    original_unlink = Path.unlink

    def fail_expiry(path: Path, missing_ok: bool = False) -> None:
        if path == _chunk(series, 0):
            raise PermissionError('recorded chunk deletion denied')
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, 'unlink', fail_expiry)
    with pytest.raises(PermissionError, match='deletion denied'):
        _publish(series, 21)


@pytest.mark.parametrize('series', DEPTH_SNAPSHOT_SERIES)
def test_expired_depth_backfill_does_not_recreate_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, series: str
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    _publish(series, 20)
    manifest = (tmp_path / series / LATEST_MANIFEST_NAME).read_bytes()
    assert _publish(series, 4) == depth_store.DepthSnapshotChunkPublish(
        'skipped_expired', None, None
    )
    assert not _chunk(series, 4).exists()
    assert (tmp_path / series / LATEST_MANIFEST_NAME).read_bytes() == manifest
    assert _publish(series, 5).status == 'skipped_not_newer'
    assert _chunk(series, 5).read_bytes() == _fixture(series, 5).read_bytes()
    before = {p: p.read_bytes() for p in tmp_path.rglob('*.arrow')}
    _publish(series, 5)
    assert before == {p: p.read_bytes() for p in tmp_path.rglob('*.arrow')}


def _concurrent_publish(root: str, series: str, index: int, ready: Event) -> None:
    os.environ['LOCAL_ARROW_DIR'] = root
    depth_store._retention_cutoff = lambda: BASE + timedelta(minutes=5)
    ready.set()
    _publish(series, index)


@pytest.mark.parametrize('series', DEPTH_SNAPSHOT_SERIES)
def test_concurrent_depth_publish_and_prune_keep_manifest_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, series: str
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    monkeypatch.setenv('POLARS_MAX_THREADS', '2')
    _seed(series, [0, 5], latest=5)
    expired = _chunk(series, 0)
    with pa.memory_map(str(expired), 'r') as mapping:
        reader = pa_ipc.open_file(mapping)
        ctx = multiprocessing.get_context('spawn')
        ready = [ctx.Event() for _ in range(4)]
        processes = [
            ctx.Process(target=_concurrent_publish, args=(str(tmp_path), series, index, event))
            for index, event in zip([20, 21, 35, 5], ready)
        ]
        with (tmp_path / series / f'.{series}.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            for process in processes:
                process.start()
            assert all(event.wait(10) for event in ready)
            time.sleep(0.05)
            assert expired.exists()
            assert json.loads((tmp_path / series / LATEST_MANIFEST_NAME).read_text())[
                'source_partition_key'
            ] == _key(5)
        deadline = time.monotonic() + 30
        while any(process.is_alive() for process in processes):
            manifest = json.loads((tmp_path / series / LATEST_MANIFEST_NAME).read_text())
            with pa.memory_map(str(tmp_path / series / manifest['chunk']), 'r') as current:
                assert pa_ipc.open_file(current).num_record_batches == 1
            assert time.monotonic() < deadline
            time.sleep(0.01)
        for process in processes:
            process.join(timeout=1)
            assert process.exitcode == 0
        assert not expired.exists()
        assert reader.read_all().equals(_build(series, 0).df.to_arrow())
    assert json.loads((tmp_path / series / LATEST_MANIFEST_NAME).read_text())[
        'source_partition_key'
    ] == _key(35)


@pytest.mark.parametrize('series', DEPTH_SNAPSHOT_SERIES)
def test_depth_retention_covers_repair_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, origo_definitions_module: object, series: str
) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    lookback = getattr(origo_definitions_module, 'DEPTH_SOURCE_LOOKBACK_MINUTES')
    assert lookback == 15
    _publish(series, 34)
    for index in range(35 - lookback, 35):
        assert _publish(series, index).status in {'published', 'skipped_not_newer'}
        assert _chunk(series, index).exists()
    expected = _chunk(series, 22).read_bytes()
    _chunk(series, 22).unlink()
    _publish(series, 22)
    assert _chunk(series, 22).read_bytes() == expected
    _publish(series, 35)
    assert all(_chunk(series, index).exists() for index in range(20, 35))


def _tree(root: Path) -> dict[str, bytes | str]:
    return {
        str(p.relative_to(root)): str(p.readlink()) if p.is_symlink() else p.read_bytes()
        for p in root.rglob('*')
        if p.is_symlink() or p.is_file()
    }


def test_depth_retention_leaves_other_stores_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / 'arrow'
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(root))
    preserved: list[Path] = []
    for relative, fixture in [
        ('time_4h/version.arrow', 'bar.arrow'),
        ('conduit/time_15m/version.arrow', 'prediction.arrow'),
        ('depth20_snapshots/legacy.arrow', 'legacy_depth.arrow'),
    ]:
        p = root / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes((FIXTURES / fixture).read_bytes())
        latest = p.parent / 'latest.arrow'
        latest.symlink_to(p.name)
        preserved.extend([p, latest])
    _seed('depth20_snapshots', [0, 20], latest=20)
    chunks = root / 'depth20_snapshots/chunks'
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / _fixture('depth20_snapshots', 0).name).write_bytes(
        _fixture('depth20_snapshots', 0).read_bytes()
    )
    (chunks / 'outside').symlink_to(outside, target_is_directory=True)
    unrelated = chunks / 'notes.arrow'
    unrelated.write_bytes((FIXTURES / 'bar.arrow').read_bytes())
    (chunks / 'unrelated').mkdir()
    preserved.extend([unrelated, chunks / 'outside'])
    before = {p: str(p.readlink()) if p.is_symlink() else p.read_bytes() for p in preserved}
    outside_before = _tree(outside)
    _publish('depth20_snapshots', 20)
    assert before == {p: str(p.readlink()) if p.is_symlink() else p.read_bytes() for p in preserved}
    assert _tree(outside) == outside_before and (chunks / 'unrelated').exists()
    snapshot = _tree(tmp_path)
    client = FakeClickHouseClient(_rows('depth20_snapshots', 20))
    monkeypatch.setattr(depth_store, 'make_clickhouse_client', lambda settings: client)
    assert _materialize(_key(20), dry_run=True).success
    assert _tree(tmp_path) == snapshot


def test_depth_fixture_provenance() -> None:
    for record in json.loads((FIXTURES / 'provenance.json').read_text())['records']:
        assert (
            hashlib.sha256((FIXTURES / record['file']).read_bytes()).hexdigest()
            == record['fixture_sha256']
        )
        assert record['selected_row_indices'] == [0, 1]
        if 'partition' in record:
            minute = minute_start_from_partition_key(record['partition'])
            assert all(
                int(minute.timestamp() * 1e9)
                <= v
                < int((minute + timedelta(minutes=1)).timestamp() * 1e9)
                for v in pl.read_ipc(FIXTURES / record['file'])['ts']
            )


def test_depth_snapshot_partition_run_request_maps_source_jobs() -> None:
    depth20 = depth_snapshot_store_partition_run_request(
        DEPTH20_SOURCE_JOB_NAME,
        'run-20',
        SOURCE_PARTITION_KEY,
    )
    depth200 = depth_snapshot_store_partition_run_request(
        DEPTH200_SOURCE_JOB_NAME,
        'run-200',
        SOURCE_PARTITION_KEY,
    )

    assert depth20.partition_key == 'depth20_snapshots'
    assert depth20.run_key == 'depth20_snapshots:run-20'
    assert (
        depth20.run_config['ops']['build_depth_snapshot_store_arrow']['config'][
            'source_partition_key'
        ]
        == SOURCE_PARTITION_KEY
    )
    assert depth200.partition_key == 'depth200_snapshots'
    assert depth200.run_key == 'depth200_snapshots:run-200'
    assert (
        depth200.run_config['ops']['build_depth_snapshot_store_arrow']['config'][
            'source_partition_key'
        ]
        == SOURCE_PARTITION_KEY
    )

    with pytest.raises(ValueError, match='Unknown depth snapshot source job: other_job'):
        depth_snapshot_store_partition_run_request('other_job', 'run-other', SOURCE_PARTITION_KEY)


def test_definitions_wires_depth_snapshot_arrow_job(origo_definitions_module: object) -> None:
    job = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow_job')
    asset_def = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow')

    assert job.name == 'build_depth_snapshot_store_arrow_job'
    assert asset_def.partitions_def.get_partition_keys() == list(DEPTH_SNAPSHOT_SERIES)


def test_definitions_wires_depth_snapshot_arrow_sensor_to_depth_source_jobs(
    origo_definitions_module: object,
) -> None:
    sensor = getattr(origo_definitions_module, 'depth_snapshot_store_source_sensor')
    arrow_job = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow_job')
    depth20_job = getattr(origo_definitions_module, 'refresh_binance_spot_depth20_data_source_job')
    depth200_job = getattr(
        origo_definitions_module, 'refresh_binance_spot_depth200_data_source_job'
    )

    assert sensor.name == 'depth_snapshot_store_source_sensor'
    assert arrow_job.name == 'build_depth_snapshot_store_arrow_job'
    assert depth20_job.name == DEPTH20_SOURCE_JOB_NAME
    assert depth200_job.name == DEPTH200_SOURCE_JOB_NAME
    assert sensor.job_name == arrow_job.name
    assert {job.name for job in sensor._monitored_jobs} == {
        DEPTH20_SOURCE_JOB_NAME,
        DEPTH200_SOURCE_JOB_NAME,
    }


def test_expired_asset_skips_source_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))

    def unexpected_client(settings: object) -> FakeClickHouseClient:
        raise AssertionError('Expired invocation must not query source data')

    monkeypatch.setattr(depth_store, 'make_clickhouse_client', unexpected_client)
    result = _materialize(_key(4))
    assert result.success
    assert (
        result.output_for_node('build_depth_snapshot_store_arrow')['outcome'] == 'skipped_expired'
    )
    assert list(tmp_path.iterdir()) == []


def test_cutoff_uses_thirty_completed_utc_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    class RecordedClock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> Self:
            return cls.fromtimestamp((BASE + timedelta(minutes=35, seconds=59)).timestamp(), tz)

    monkeypatch.setattr(depth_store, 'datetime', RecordedClock)
    assert _ACTUAL_CUTOFF() == BASE + timedelta(minutes=5)
