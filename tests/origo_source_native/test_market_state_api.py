from __future__ import annotations

import ast
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import market_state, market_state_reader
from origo.query.market_state_reader import MarketStateError, query, read_table
from origo.query.market_state_results import DiskSample, ResultStore
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.workers import market_state_api
from origo.workers.market_state_api import FEED, ApiHandler, ApiServer, MarketStateApi, main, serve
from origo.workers.monitor import DELIVERY_LAG_SECONDS, MARKET_STATE_API_FEED
from origo.workers.report import Reporter
from origo.workers.runtime import heartbeat_path, touch_heartbeat

from .test_market_state_query import DAY1, built, cube  # noqa: F401
from .test_monitor import _monitor, recorder  # noqa: F401


def roomy(path: Path) -> DiskSample:
    return DiskSample(total=10**13, free=10**13, inodes=10**7, free_inodes=10**7)


@dataclass
class RecordingReporter(Reporter):
    calls: list[tuple[str, Mapping[str, object]]] = field(default_factory=list)
    base_url: str = 'http://127.0.0.1:9'
    timeout_seconds: float = 0.2

    def materialized(self, asset_key: str, *, partition: str | None, metadata: Mapping[str, object]) -> bool:
        self.calls.append((asset_key, dict(metadata)))
        return True


@dataclass
class Service:
    api: MarketStateApi
    store: ResultStore
    server: ApiServer
    reporter: RecordingReporter

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self.server.server_address[1]}'

    def post(self, route: str, raw: bytes) -> tuple[int, dict[str, Any], dict[str, str]]:
        request = urllib.request.Request(self.url + route, data=raw, headers={'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read()), dict(response.headers)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read()), dict(error.headers)


@pytest.fixture
def service(cube: SourceRuntime, tmp_path: Path) -> Iterator[Service]:
    built(cube, DAY1)
    store = ResultStore(tmp_path / 'market-state', disk=roomy)
    reporter = RecordingReporter()
    api = MarketStateApi(store, reporter, cube.lock_root, interrupted=1)
    server = serve(api, port=0)
    api.port = server.server_address[1]
    try:
        yield Service(api, store, server, reporter)
    finally:
        server.shutdown()
        server.server_close()


def receipts(series: str) -> list[tuple[Any, ...]]:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        return client.execute(
            'SELECT status, error_code, error, rows, sha256 FROM origo.worker_minute_log '
            'WHERE feed = %(feed)s AND series = %(series)s ORDER BY recorded_at',
            {'feed': FEED, 'series': series},
        )
    finally:
        client.disconnect()


def test_http_contract_and_supported_caller(
    service: Service, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = query(t1='2021-01-01T00:57:11.25Z', t2='2021-01-01T01:00:00Z', tR=225, pR=250, url=service.url)
    response = dict(result.response)
    assert set(response) == {
        'result_id', 'cells', 'summary', 'expires_after_seconds', 'expires_at', 'effective', 'clipped',
        'data_cutoff', 'canonical_through', 'last_column_unfinished', 'state_token', 'cell_count',
    }
    root = service.store.root
    assert response['cells'] == str(root / 'results' / result.result_id / 'cells.arrow')
    assert response['summary'] == str(root / 'results' / result.result_id / 'summary.arrow')
    assert response['expires_after_seconds'] == 86_400
    assert response['effective'] == {
        't1': '2021-01-01T00:57:11.250000+00:00', 't2': '2021-01-01T01:00:00.000000+00:00',
        'p1': 28875.0, 'p2': 29125.0, 'tR': 225.0, 'pR': 250.0,
    }
    assert response['data_cutoff'] == '2021-01-02T00:00:00.000000+00:00'
    assert response['clipped'] == {'t1': False, 't2': False} and response['last_column_unfinished'] is False
    assert datetime.fromisoformat(str(response['expires_at'])) - datetime.now(UTC) > timedelta(hours=23)
    # The supported caller mounts the volume anywhere and swaps the returned prefix.
    mount = tmp_path / 'consumer-mount'
    mount.symlink_to(root, target_is_directory=True)
    cells = read_table(result.cells.replace(str(root), str(mount)), url=service.url)
    summary = read_table(result.summary.replace(str(root), str(mount)), url=service.url).to_pylist()
    assert cells.num_rows == response['cell_count'] and summary[0]['result_id'] == result.result_id
    # Every error has its declared shape.
    status, body, _ = service.post('/v1/market-state/query', b'{"x": 1}')
    assert (status, body['error'], body['reason'], body['field']) == (400, 'invalid_request', 'unknown_field', 'x')
    status, body, _ = service.post('/v1/market-state/query', b'{')
    assert (status, body['reason']) == (400, 'invalid_json')
    with pytest.raises(MarketStateError) as zone:
        query(t1='2021-01-01', url=service.url)
    assert zone.value.status == 400 and zone.value.body['reason'] == 'time_zone_required'
    status, body, _ = service.post('/v1/market-state/query', b'{"t1": "2022-01-01T00:00:00Z"}')
    assert (status, body) == (409, {
        'error': 'outside_coverage', 'history_start': '2021-01-01T00:00:00.000000+00:00',
        'data_cutoff': '2021-01-02T00:00:00.000000+00:00',
    })
    service.api.queries.acquire()
    service.api.queries.acquire()
    try:
        status, body, headers = service.post('/v1/market-state/query', b'{}')
    finally:
        service.api.queries.release()
        service.api.queries.release()
    assert (status, body, headers['Retry-After']) == (503, {'error': 'busy'}, '5')
    monkeypatch.setattr(market_state, 'FENCE_WAIT_SECONDS', 1.0)
    with source_lock(service.api.lock_root, 'binance_spot_trades', 'heavy'):
        status, body, headers = service.post('/v1/market-state/query', b'{}')
    assert (status, body, headers['Retry-After']) == (503, {'error': 'source_maintenance'}, '5')
    service.store.budget_bytes = 0
    status, body, _ = service.post('/v1/market-state/query', b'{}')
    service.store.budget_bytes = 64 * 1024**3
    assert status == 507 and body['error'] == 'result_storage_full' and body['budget_bytes'] == 0
    assert set(body) == {'error', 'used_bytes', 'budget_bytes', 'free_bytes', 'floor_bytes'}

    def broken() -> object:
        raise ConnectionError('ClickHouse went away')

    monkeypatch.setattr(market_state, '_connect', broken)
    status, body, _ = service.post('/v1/market-state/query', b'{}')
    assert (status, body) == (500, {'error': 'export_failed', 'reason': 'ConnectionError'})
    assert list((root / 'staging').iterdir()) == []
    status, body, _ = service.post('/v1/market-state/other', b'{}')
    assert (status, body) == (404, {'error': 'not_found'})


def test_request_bounds_keep_the_service_responsive(
    service: Service, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ApiHandler, 'timeout', 1)
    port = service.server.server_address[1]
    first = query(url=service.url)
    renew = json.dumps({'path': first.cells}).encode()
    # A client that sends nothing is dropped after the socket timeout; others are served meanwhile.
    stalled = socket.create_connection(('127.0.0.1', port))
    assert service.post('/v1/market-state/access', renew)[0] == 200
    stalled.settimeout(5)
    assert stalled.recv(1) == b''
    stalled.close()
    # Oversized bodies are refused without reading them.
    connection = socket.create_connection(('127.0.0.1', port))
    connection.sendall(b'POST /v1/market-state/query HTTP/1.1\r\nHost: x\r\nContent-Length: 70000\r\n\r\n')
    connection.settimeout(5)
    assert b' 400 ' in connection.recv(4096).split(b'\r\n', 1)[0]
    connection.close()
    # Connections beyond the 32 slots are closed at once.
    idle = [socket.create_connection(('127.0.0.1', port)) for _ in range(32)]
    time.sleep(0.3)
    extra = socket.create_connection(('127.0.0.1', port))
    extra.settimeout(5)
    assert extra.recv(1) == b''
    for sock in (*idle, extra):
        sock.close()
    time.sleep(1.5)
    # With both query slots busy, renewals and health still answer.
    service.api.queries.acquire()
    service.api.queries.acquire()
    try:
        assert service.post('/v1/market-state/access', renew)[0] == 200
        with urllib.request.urlopen(service.url + '/healthz', timeout=5) as health:
            assert health.status == 200
    finally:
        service.api.queries.release()
        service.api.queries.release()
    # A client that disconnects before publication leaves no published result.
    published_before = set((service.store.root / 'results').iterdir())
    original = market_state_api.write_result

    def slow(*args: Any, **kwargs: Any) -> dict[str, object]:
        time.sleep(1.0)
        return original(*args, **kwargs)

    monkeypatch.setattr(market_state_api, 'write_result', slow)
    gone = socket.create_connection(('127.0.0.1', port))
    gone.sendall(b'POST /v1/market-state/query HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\n{}')
    gone.close()
    time.sleep(3.0)
    monkeypatch.setattr(market_state_api, 'write_result', original)
    assert set((service.store.root / 'results').iterdir()) == published_before
    assert list((service.store.root / 'staging').iterdir()) == []
    # An export whose transport stalls fails within its bound and frees its slot and staging.
    shift = market_state._shift
    monkeypatch.setattr(market_state, 'RECEIVE_TIMEOUT_SECONDS', 1)
    monkeypatch.setattr(
        market_state, '_shift', lambda column, exponent: f'bitShiftRight({column} + sleepEachRow(0.5), {exponent})'
    )
    started = time.monotonic()
    status, body, _ = service.post('/v1/market-state/query', b'{}')
    assert (status, body['error']) == (500, 'export_failed') and time.monotonic() - started < 10
    assert list((service.store.root / 'staging').iterdir()) == []
    monkeypatch.setattr(market_state, 'RECEIVE_TIMEOUT_SECONDS', 90)
    monkeypatch.setattr(market_state, '_shift', shift)
    assert query(url=service.url).response['cell_count'] == first.response['cell_count']


def test_tick_reports_receipts_heartbeat_and_live_asset(
    service: Service, recorder: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ok = query(url=service.url)
    service.api.queries.acquire()
    service.api.queries.acquire()
    service.post('/v1/market-state/query', b'{}')
    service.api.queries.release()
    service.api.queries.release()
    service.store.budget_bytes = 0
    service.post('/v1/market-state/query', b'{}')
    service.store.budget_bytes = 64 * 1024**3
    service.post('/v1/market-state/query', b'{"x": 1}')
    outcome = service.api.tick(datetime.now(UTC))
    assert outcome.failed == ('binance_spot_trades:query',)
    assert receipts('binance_spot_trades:cleanup') == [('OK', '', '', 0, '')]
    assert receipts('binance_spot_trades:query') == [(
        'FAILED', 'EXPORT_INTERRUPTED',
        'failed=1:EXPORT_INTERRUPTED failed=1:RESULT_STORAGE_FULL ok=1 rejected=1:QUERY_BUSY',
        4, ok.response['state_token'],
    )]
    asset, metadata = service.reporter.calls[-1]
    assert asset == 'market_state_query_service'
    assert (metadata['queries_ok'], metadata['queries_rejected'], metadata['queries_failed'], metadata['queries_invalid']) == (1, 1, 2, 1)
    assert (metadata['results'], metadata['files_reclaimed']) == (1, 0)
    # One receipt per series per tick; a tick without queries writes only the cleanup receipt.
    service.api.tick(datetime.now(UTC) + timedelta(minutes=1))
    assert len(receipts('binance_spot_trades:query')) == 1 and len(receipts('binance_spot_trades:cleanup')) == 2
    # Receipts that cannot be written stay for the next tick.
    settings = get_clickhouse_settings()
    service.api.queries.acquire()
    service.api.queries.acquire()
    service.post('/v1/market-state/query', b'{}')
    service.api.queries.release()
    service.api.queries.release()
    monkeypatch.setattr(market_state_api, 'get_clickhouse_settings', lambda: type(settings)(settings.host, 1, settings.user, settings.password, settings.database))
    service.api.tick(datetime.now(UTC) + timedelta(minutes=2))
    monkeypatch.setattr(market_state_api, 'get_clickhouse_settings', get_clickhouse_settings)
    service.api.tick(datetime.now(UTC) + timedelta(minutes=3))
    assert receipts('binance_spot_trades:query')[-1][:3] == ('REJECTED', 'QUERY_BUSY', 'rejected=1:QUERY_BUSY')
    # A Dagit outage never blocks the tick.
    silent = MarketStateApi(service.store, Reporter('http://127.0.0.1:9', timeout_seconds=0.5), service.api.lock_root, port=service.api.port)
    assert silent.tick(datetime.now(UTC)).processed == ('binance_spot_trades:cleanup',)
    # A query server that stops answering makes the tick exit for a restart.
    exits: list[int] = []
    monkeypatch.setattr(market_state_api.os, '_exit', lambda code: exits.append(code))
    stopped = MarketStateApi(service.store, service.reporter, service.api.lock_root, port=9)
    stopped.tick(datetime.now(UTC))
    assert exits == [3]
    # The monitor expects the heartbeat from first start and reports the FAILED receipt.
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        monitor = _monitor(recorder, tmp_path / 'monitor', client=client)
        heartbeat = heartbeat_path(monitor.heartbeat_dir, MARKET_STATE_API_FEED)
        heartbeat.unlink()
        assert MARKET_STATE_API_FEED == FEED and heartbeat in monitor._heartbeats()
        found = monitor.tick(datetime.now(UTC) + timedelta(seconds=DELIVERY_LAG_SECONDS + 30))
    finally:
        client.disconnect()
    assert f'heartbeat_stale:{FEED}' in found.failed
    assert f'receipt_failed:{FEED}:binance_spot_trades:query' in found.failed
    # ``--check`` needs a fresh heartbeat and an answering /healthz.
    monkeypatch.setenv('ORIGO_HEARTBEAT_DIR', str(tmp_path / 'beats'))
    assert main(['--check', '--port', str(service.api.port)]) == 1
    touch_heartbeat(heartbeat_path(tmp_path / 'beats', FEED))
    assert main(['--check', '--port', str(service.api.port)]) == 0
    assert main(['--check', '--port', '9']) == 1


def test_reader_module_needs_only_stdlib_and_pyarrow() -> None:
    source = Path(market_state_reader.__file__).read_text()
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split('.')[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split('.')[0])
        elif isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'import_module':
            argument = node.args[0]
            assert isinstance(argument, ast.Constant) and isinstance(argument.value, str)
            imported.add(argument.value.split('.')[0])
    allowed = set(sys.stdlib_module_names) | {'__future__', '_typeshed', 'pyarrow'}
    assert imported <= allowed, imported - allowed
    assert 'origo' not in imported
