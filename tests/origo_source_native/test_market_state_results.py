from __future__ import annotations

import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from origo.query import market_state_reader, market_state_results
from origo.query.market_state import (
    CELLS_SCHEMA,
    METADATA_KEY,
    QUERY_SETTINGS,
    SUMMARY_SCHEMA,
    parse_request,
    write_result,
)
from origo.query.market_state_reader import open_file, read_table
from origo.query.market_state_results import (
    FLOOR_MARGIN_BYTES,
    QUERY_RESERVATION_BYTES,
    DiskSample,
    ResultStore,
    StorageFull,
)
from origo.sources.lifecycle import SourceRuntime
from origo.workers.market_state_api import ApiServer, MarketStateApi, serve, source_floor
from origo.workers.report import Reporter

from .test_market_state_query import DAY1, DAY2, MINUTES, built, cube  # noqa: F401

HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS
GIB = 1024**3


@dataclass
class Clock:
    now: int

    def __call__(self) -> int:
        return self.now

    def advance(self, nanoseconds: int) -> None:
        self.now += nanoseconds


def roomy(path: Path) -> DiskSample:
    return DiskSample(total=10**13, free=10**13, inodes=10**7, free_inodes=10**7)


def published(store: ResultStore, runtime: SourceRuntime, body: bytes = b'{}') -> tuple[str, Path, Path]:
    result_id = str(uuid4())
    staging = store.register(result_id)
    write_result(runtime, parse_request(body), staging, result_id=result_id, guard=lambda staged: None)
    cells, summary = store.publish(result_id)
    return result_id, cells, summary


def last_access(store: ResultStore, result_id: str, name: str) -> int | None:
    connection = sqlite3.connect(store.database)
    try:
        row = connection.execute(
            'SELECT last_access_ns FROM files WHERE result_id = ? AND name = ?', (result_id, name)
        ).fetchone()
    finally:
        connection.close()
    return None if row is None else int(row[0])


@pytest.fixture
def service(tmp_path: Path) -> Iterator[tuple[ResultStore, Clock, str, ApiServer]]:
    clock = Clock(1_900_000_000_000_000_000)
    store = ResultStore(tmp_path / 'market-state', clock=clock, disk=roomy)
    api = MarketStateApi(store, Reporter('http://127.0.0.1:9', timeout_seconds=0.2), tmp_path / 'locks')
    server = serve(api, port=0)
    try:
        yield store, clock, f'http://127.0.0.1:{server.server_address[1]}', server
    finally:
        server.shutdown()
        server.server_close()


def post(url: str, route: str, body: object) -> tuple[int, dict[str, object]]:
    request = urllib.request.Request(
        url + route, data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'}, method='POST'
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def test_publication_survives_each_crash_window(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = built(cube, DAY1)
    clock = Clock(1_900_000_000_000_000_000)
    root = tmp_path / 'market-state'
    store = ResultStore(root, clock=clock, disk=roomy)
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'keep').write_text('foreign')
    foreign = [
        root / 'staging' / 'notes.txt',
        root / 'staging' / str(uuid4()),
        root / 'results' / str(uuid4()),
    ]
    foreign[0].write_text('foreign')
    for directory in foreign[1:]:
        directory.mkdir()
        (directory / 'cells.arrow').write_text('foreign')
    links = [root / 'staging' / str(uuid4()), root / 'results' / str(uuid4())]
    for link in links:
        link.symlink_to(outside, target_is_directory=True)
    # Interrupted before registration: nothing belongs to the store.
    assert ResultStore(root, clock=clock, disk=roomy).recover() == 0
    # Registered and interrupted while writing.
    interrupted = str(uuid4())
    write_result(runtime, parse_request(b'{}'), store.register(interrupted), result_id=interrupted, guard=lambda staged: None)
    # Retirement committed, then the unlink was interrupted.
    retiring, retiring_cells, _ = published(store, runtime)
    clock.advance(12 * HOUR_NS)
    done, done_cells, _ = published(store, runtime)
    clock.advance(12 * HOUR_NS)
    unlink = Path.unlink

    def refuse(self: Path, missing_ok: bool = False) -> None:
        raise OSError('simulated crash before the unlink')

    monkeypatch.setattr(Path, 'unlink', refuse)
    with pytest.raises(OSError):
        store.expire()
    monkeypatch.setattr(Path, 'unlink', unlink)
    assert retiring_cells.is_file() and store.access(retiring, 'cells.arrow') is None
    # The published transaction committed, then the rename was interrupted.
    forward = str(uuid4())
    write_result(runtime, parse_request(b'{}'), store.register(forward), result_id=forward, guard=lambda staged: None)
    rename = Path.rename

    def crash(self: Path, target: Path) -> Path:
        raise OSError('simulated crash between the lifecycle commit and the rename')

    monkeypatch.setattr(Path, 'rename', crash)
    with pytest.raises(OSError):
        store.publish(forward)
    monkeypatch.setattr(Path, 'rename', rename)
    # A publication that fails after its commit and rename is discarded whole: its paths
    # were never returned, so neither rows nor files outlive it.
    failed = str(uuid4())
    write_result(runtime, parse_request(b'{}'), store.register(failed), result_id=failed, guard=lambda staged: None)
    real_sync = market_state_results._sync

    def sync(path: Path) -> None:
        if path == store.results:
            raise OSError('simulated fsync failure after the rename')
        real_sync(path)

    monkeypatch.setattr(market_state_results, '_sync', sync)
    with pytest.raises(OSError):
        store.publish(failed)
    monkeypatch.setattr(market_state_results, '_sync', real_sync)
    assert (root / 'results' / failed).is_dir()
    # A crash in the middle of that discard leaves a registration recovery finishes.
    rmtree = market_state_results.shutil.rmtree

    def interrupted_rmtree(path: object, *args: object, **kwargs: object) -> None:
        raise OSError('simulated crash while removing a discarded result')

    monkeypatch.setattr(market_state_results.shutil, 'rmtree', interrupted_rmtree)
    with pytest.raises(OSError):
        store.discard(failed)
    monkeypatch.setattr(market_state_results.shutil, 'rmtree', rmtree)
    assert (root / 'results' / failed).is_dir() and store.access(failed, 'cells.arrow') is None
    assert store.usage()[1] > 0  # retired but still on disk: counted until removed
    # A restart rolls every registered step back or forward.
    restarted = ResultStore(root, clock=clock, disk=roomy)
    assert restarted.recover() == 1
    assert not (root / 'staging' / interrupted).exists()
    assert (root / 'results' / forward / 'cells.arrow').is_file()
    assert restarted.access(forward, 'cells.arrow') is not None
    assert not retiring_cells.exists() and not (root / 'results' / retiring).exists()
    assert not (root / 'results' / failed).exists() and restarted.access(failed, 'cells.arrow') is None
    assert done_cells.is_file() and restarted.access(done, 'summary.arrow') is not None
    assert foreign[0].read_text() == 'foreign'
    assert all((directory / 'cells.arrow').read_text() == 'foreign' for directory in foreign[1:])
    assert all(link.is_symlink() for link in links) and (outside / 'keep').read_text() == 'foreign'


def test_results_reopen_with_declared_schemas(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1, minutes=MINUTES)
    store = ResultStore(tmp_path / 'market-state', disk=roomy)
    result_id, cells, summary = published(store, runtime, b'{"t1": "2021-01-02T00:00:00Z", "tR": 900}')
    for path, schema in ((cells, CELLS_SCHEMA), (summary, SUMMARY_SCHEMA)):
        reader = ipc.open_file(path)
        assert reader.schema.equals(schema, check_metadata=False)
        meta = json.loads(reader.schema.metadata[METADATA_KEY.encode()])
        assert meta['schema_version'] == 1 and meta['result_id'] == result_id
        assert meta['grid'] == {
            't0': '2021-01-01T00:00:00.000000+00:00', 'tR': 900.0, 'pR': 125.0,
            'time_exponent': 4, 'price_exponent': 0,
        }
        assert [pin[0] for pin in meta['pins']] == list(MINUTES)
        assert meta['state_token'] == ipc.open_file(summary).read_all().to_pylist()[0]['state_token']
        assert path.stat().st_mode & 0o777 == 0o644
    assert cells.parent.stat().st_mode & 0o777 == 0o755


def test_reader_renews_on_every_read_and_only_then(
    cube: SourceRuntime, tmp_path: Path, service: tuple[ResultStore, Clock, str, ApiServer]
) -> None:
    runtime = built(cube, DAY1)
    store, clock, url, _ = service
    created = clock.now
    result_id, cells, summary = published(store, runtime)
    assert last_access(store, result_id, 'cells.arrow') == last_access(store, result_id, 'summary.arrow') == created
    # Opening the file at the OS level, seeking, stat and close never renew; nor do
    # cleanup scans, bookkeeping, recovery or health checks.
    clock.advance(HOUR_NS)
    handle = market_state_reader._RenewingFile(str(cells), url)
    handle.seek(0, 2)
    handle.seek(0)
    cells.stat()
    handle.close()
    assert store.expire() == 0
    store.usage()
    ResultStore(store.root, clock=clock, disk=roomy).recover()
    with urllib.request.urlopen(url + '/healthz', timeout=5) as health:
        assert health.status == 200
    # Plain pyarrow and memory-mapped reads work but do not renew (A01).
    assert pa.OSFile(str(cells)).read(6) == b'ARROW1'
    assert pa.memory_map(str(cells)).read(6) == b'ARROW1'
    assert last_access(store, result_id, 'cells.arrow') == created
    # A read through the cube reader renews that file only; each file has its own clock.
    assert read_table(str(cells), url=url).num_rows > 0
    renewed = clock.now
    assert last_access(store, result_id, 'cells.arrow') == renewed
    assert last_access(store, result_id, 'summary.arrow') == created
    # Reads through an existing handle keep renewing; the footer read at open is a read.
    reader = open_file(str(summary), url=url)
    assert last_access(store, result_id, 'summary.arrow') == renewed
    clock.advance(2 * HOUR_NS)
    assert reader.read_all().num_rows == 1
    assert last_access(store, result_id, 'summary.arrow') == clock.now
    # A read after the deadline but before retirement still renews.
    clock.advance(25 * HOUR_NS)
    assert read_table(str(cells), url=url).num_rows > 0
    assert store.expire() == 1
    assert cells.is_file() and not summary.exists()
    # Access state persists across a restart of the service.
    renewal = last_access(store, result_id, 'cells.arrow')
    restarted = ResultStore(store.root, clock=clock, disk=roomy)
    assert restarted.recover() == 0
    assert last_access(restarted, result_id, 'cells.arrow') == renewal
    # A never-accessed result expires exactly 24 hours after its creation.
    idle_id, idle_cells, _ = published(store, runtime)
    clock.advance(DAY_NS - 1)
    assert store.expire() == 0 and idle_cells.is_file()
    clock.advance(1)
    # The idle result's two files go, and so does the first cells file, 24 hours after its
    # late renewal.
    assert store.expire() == 3 and not idle_cells.exists() and not idle_cells.parent.exists()
    assert store.access(idle_id, 'cells.arrow') is None


def test_cleanup_and_reads_race_safely(
    cube: SourceRuntime, tmp_path: Path, service: tuple[ResultStore, Clock, str, ApiServer]
) -> None:
    runtime = built(cube, DAY1)
    store, clock, url, _ = service
    result_id, cells, summary = published(store, runtime)
    # Read wins: a renewal holding the write lock commits before the retirement can start.
    clock.advance(DAY_NS + 1)
    renewal = sqlite3.connect(store.database, isolation_level=None, timeout=30)
    renewal.execute('BEGIN IMMEDIATE')
    removed: list[int] = []
    cleanup = threading.Thread(target=lambda: removed.append(store.expire()))
    cleanup.start()
    time.sleep(0.3)
    assert cleanup.is_alive()
    renewal.execute(
        'UPDATE files SET last_access_ns = ? WHERE result_id = ? AND name = ?', (clock.now, result_id, 'cells.arrow')
    )
    renewal.execute('COMMIT')
    renewal.close()
    cleanup.join(10)
    assert removed == [1] and cells.is_file() and not summary.exists()
    # Deletion wins: the retirement commits first, so a read through a handle opened
    # earlier raises FileNotFoundError before any bytes reach the caller.
    second, second_cells, _ = published(store, runtime)
    handle = market_state_reader._RenewingFile(str(second_cells), url)
    clock.advance(DAY_NS + 1)
    store.expire()
    buffer = bytearray(6)
    with pytest.raises(FileNotFoundError):
        handle.readinto(buffer)
    assert buffer == bytearray(6)
    handle.close()
    assert store.access(second, 'cells.arrow') is None
    # Renewal is keyed by the last two path components, so a volume mounted elsewhere works.
    third, third_cells, _ = published(store, runtime)
    elsewhere = tmp_path / 'mounted-elsewhere'
    elsewhere.symlink_to(store.root, target_is_directory=True)
    clock.advance(HOUR_NS)
    assert read_table(str(elsewhere / 'results' / third / 'cells.arrow'), url=url).num_rows > 0
    assert last_access(store, third, 'cells.arrow') == clock.now
    for path in ('/x/not-a-uuid/cells.arrow', f'/x/{third}/other.arrow', 'results/../lifecycle.sqlite', f'/x/{third.upper()}/cells.arrow'):
        assert post(url, '/v1/market-state/access', {'path': path}) == (
            400, {'error': 'invalid_request', 'reason': 'invalid_path'}
        )
    assert post(url, '/v1/market-state/access', {'path': f'/x/{uuid4()}/cells.arrow'}) == (410, {'error': 'gone'})
    # A registered result directory replaced by a symlink never leads cleanup to foreign files.
    fourth, fourth_cells, _ = published(store, runtime)
    outside = tmp_path / 'foreign'
    outside.mkdir()
    for name in ('cells.arrow', 'summary.arrow'):
        (outside / name).write_text('foreign')
    for child in fourth_cells.parent.iterdir():
        child.unlink()
    fourth_cells.parent.rmdir()
    fourth_cells.parent.symlink_to(outside, target_is_directory=True)
    clock.advance(DAY_NS + 1)
    store.expire()
    assert [path.read_text() for path in sorted(outside.iterdir())] == ['foreign', 'foreign']
    assert store.access(fourth, 'cells.arrow') is None


def test_admission_keeps_the_source_reserve(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = built(cube, DAY1)
    total = 1000 * GIB
    # Without measurements the 30% term governs; a source whose measured working set x 2 x
    # canonical concurrency exceeds 30% raises the floor above it.
    assert source_floor(runtime.store, total, QUERY_SETTINGS) == (total * 3 + 9) // 10 + FLOOR_MARGIN_BYTES
    runtime.store.execute(
        'INSERT INTO origo.source_capacity_log VALUES',
        [('binance_spot_trades', 'volume', 30 * GIB, 'capacity-probe', 1, datetime.now(UTC))],
    )
    floor = source_floor(runtime.store, total, QUERY_SETTINGS)
    assert floor == 30 * GIB * 2 * 8 + FLOOR_MARGIN_BYTES
    free = {'bytes': floor + QUERY_RESERVATION_BYTES}

    def disk(path: Path) -> DiskSample:
        return DiskSample(total=total, free=free['bytes'], inodes=10**6, free_inodes=10**6)

    store = ResultStore(tmp_path / 'market-state', disk=disk, budget_bytes=10 * GIB)
    kept, kept_cells, kept_summary = published(store, runtime)
    before = (kept_cells.read_bytes(), kept_summary.read_bytes())
    # Two simultaneous admissions near the floor: the check and the reservation are one step,
    # so the first holds its reservation and the second is refused before either writes.
    free['bytes'] = floor + QUERY_RESERVATION_BYTES + QUERY_RESERVATION_BYTES // 2
    first, second = str(uuid4()), str(uuid4())
    store.admit(first, 0, floor)
    with pytest.raises(StorageFull) as near:
        store.admit(second, 0, floor)
    assert near.value.floor == floor
    store.discard(first)
    store.admit(second, 0, floor)
    store.discard(second)
    # The byte budget refuses a result growing past it while it is written.
    with pytest.raises(StorageFull):
        store.admit(first, 11 * GIB, floor)
    # The inode reserve refuses too.
    store2 = ResultStore(tmp_path / 'other', disk=lambda path: DiskSample(total, 10**13, 1000, 99))
    with pytest.raises(StorageFull):
        store2.admit(str(uuid4()), 0, 0)
    # Refusals never evict unexpired results.
    assert (kept_cells.read_bytes(), kept_summary.read_bytes()) == before
    assert store.access(kept, 'cells.arrow') is not None
    # A failed unlink keeps its bytes counted and renewals of other files working.
    store.clock = lambda: time.time_ns() + 2 * DAY_NS
    live, live_cells, _ = published(store, runtime)
    size = store.usage()[1]
    def fault(self: Path, missing_ok: bool = False) -> None:
        raise OSError('disk fault')

    monkeypatch.setattr(Path, 'unlink', fault)
    with pytest.raises(OSError):
        store.expire()
    assert store.usage()[1] == size and kept_cells.is_file()
    assert store.access(live, 'cells.arrow') is not None
