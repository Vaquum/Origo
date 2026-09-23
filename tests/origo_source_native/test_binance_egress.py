from __future__ import annotations

import multiprocessing
import os
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing.queues import Queue as ProcessQueue
from multiprocessing.synchronize import Event
from pathlib import Path
from queue import Queue

import pytest
import requests

from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters import binance_perp_rest as perp
from origo.sources.adapters.binance_provisional import BinanceProvisionalBase
from origo.sources.contracts import Partition, Revision, SourceError

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/rest/trades/locator.json'
URL = 'https://fapi.binance.com/fapi/v1/historicalTrades'
DEDICATED = ('37.27.112.140', '37.27.112.144')


def test_parallel_minutes_keep_immutable_egress_and_native_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    barrier = threading.Barrier(4)
    observed: Queue[tuple[str, str | None]] = Queue()
    body = FIXTURE.read_bytes()

    def request(
        url: str,
        *,
        params: Mapping[str, str | int],
        headers: Mapping[str, str],
        weight: int,
        egress_ip: str | None = None,
    ) -> daily.Response:
        observed.put((str(params['minute']), egress_ip))
        return daily.Response(body, {}, 200, egress_ip)

    def observe(
        adapter: BinanceProvisionalBase,
        partition: Partition,
        previous_evidence: str | None,
    ) -> Revision:
        # Observe transport selection only; do not reinterpret fixture rows as other minutes.
        barrier.wait(timeout=10)
        for _ in range(2):
            adapter._get_response(URL, {'minute': partition.key}, {}, 200)
            barrier.wait(timeout=10)
        raise RuntimeError('transport observed')

    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', ','.join(DEDICATED))
    monkeypatch.setattr(perp, 'get_response', request)
    monkeypatch.setattr(BinanceProvisionalBase, 'fetch', observe)
    adapter = perp.BinancePerpProvisional()
    partitions = [adapter.partition(f'2026-09-16T20:0{minute}:00Z') for minute in range(4)]

    def fetch(partition: Partition) -> None:
        with pytest.raises(RuntimeError, match='transport observed'):
            adapter.fetch(partition)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(fetch, partitions))
    assert adapter.egress_ip is None
    with pytest.raises(FrozenInstanceError):
        setattr(adapter, 'egress_ip', DEDICATED[0])
    results = [observed.get_nowait() for _ in range(8)]
    for partition in partitions:
        expected = DEDICATED[int(partition.start.timestamp()) // 60 % 2]
        assert [ip for key, ip in results if key == partition.key] == [expected, expected]
    # Retrying the same minute keeps its assignment, including a singleton pool.
    barrier = threading.Barrier(1)
    fetch(partitions[0])
    assert [observed.get_nowait()[1] for _ in range(2)] == [DEDICATED[0]] * 2
    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', DEDICATED[1])
    fetch(partitions[0])
    assert [observed.get_nowait()[1] for _ in range(2)] == [DEDICATED[1]] * 2
    monkeypatch.delenv('ORIGO_BINANCE_PERP_EGRESS_IPS')
    fetch(partitions[0])
    assert [observed.get_nowait()[1] for _ in range(2)] == [None, None]


@pytest.mark.parametrize(
    'pool',
    [
        '',
        ' ',
        '37.27.112.140,',
        '37.27.112.140,37.27.112.140',
        '37.27.112.167',
        '37.27.112.0140',
        '::1',
        '37.27.112.140, 37.27.112.144',
        '37.27.112.140,37.27.112.144,37.27.112.140',
    ],
)
def test_invalid_egress_pool_fails_before_fetch(
    pool: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', pool)
    adapter = perp.BinancePerpProvisional()
    with pytest.raises(ValueError, match='distinct dedicated IPv4'):
        adapter.fetch(adapter.partition('2026-09-16T20:00:00Z'))


def test_ip_budgets_persist_across_pool_changes_and_isolate_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [1000.0]
    outcomes = [(418, '1200'), (200, None), (429, '60'), (200, None), (200, None)]
    called: list[str | None] = []

    def request(
        url: str,
        params: Mapping[str, str | int] | None,
        headers: Mapping[str, str] | None,
        egress_ip: str | None = None,
    ) -> requests.Response:
        called.append(egress_ip)
        status, retry = outcomes.pop(0)
        response = requests.Response()
        response.status_code = status
        response._content = FIXTURE.read_bytes()
        if retry is not None:
            response.headers['Retry-After'] = retry
        return response

    def fetch(
        adapter: BinanceProvisionalBase,
        partition: Partition,
        previous_evidence: str | None,
    ) -> Revision:
        adapter._get_response(URL, {}, {}, 200)
        raise RuntimeError('transport observed')

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.delenv('ORIGO_WORKER_HEARTBEAT', raising=False)
    monkeypatch.setattr(daily, '_request', request)
    monkeypatch.setattr(daily.time, 'time', lambda: clock[0])
    monkeypatch.setattr(daily.time, 'sleep', sleep)
    monkeypatch.setattr(BinanceProvisionalBase, 'fetch', fetch)
    partition = perp.BinancePerpProvisional().partition('2026-09-16T20:00:00Z')
    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', ','.join(DEDICATED))
    with pytest.raises(SourceError, match=r'HTTP 418.*37\.27\.112\.140'):
        perp.BinancePerpProvisional().fetch(partition)
    banned_path = daily._budget_state_file(tmp_path, URL, DEDICATED[0])
    saved = banned_path.read_text()
    assert saved == '2200.000000 2200.000000'
    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', DEDICATED[1])
    with pytest.raises(RuntimeError, match='transport observed'):
        perp.BinancePerpProvisional().fetch(partition)
    assert clock[0] == 1000.0
    with pytest.raises(SourceError, match=r'HTTP 429.*37\.27\.112\.144'):
        perp.BinancePerpProvisional().fetch(partition)
    assert clock[0] == pytest.approx(1000 + 200 / 24)
    limited_path = daily._budget_state_file(tmp_path, URL, DEDICATED[1])
    limited_state = limited_path.read_text()
    deadline = float(limited_state.split()[0])
    assert deadline == pytest.approx(clock[0] + 60)
    monkeypatch.setenv('ORIGO_BINANCE_PERP_EGRESS_IPS', ','.join(DEDICATED))
    with pytest.raises(SourceError, match=r'circuit.*37\.27\.112\.140'):
        perp.BinancePerpProvisional().fetch(partition)
    assert banned_path.read_text() == saved
    assert limited_path.read_text() == limited_state
    assert called == [DEDICATED[0], DEDICATED[1], DEDICATED[1]]
    # A fresh adapter reuses the persisted healthy-IP cooldown after pool restoration.
    with pytest.raises(RuntimeError, match='transport observed'):
        perp.BinancePerpProvisional().fetch(
            perp.BinancePerpProvisional().partition('2026-09-16T20:01:00Z')
        )
    assert clock[0] == pytest.approx(deadline)
    monkeypatch.delenv('ORIGO_BINANCE_PERP_EGRESS_IPS')
    legacy = tmp_path / 'binance_rest_budget.fapi_binance_com.state'
    legacy.write_text(f'{clock[0] + 10} 0')
    with pytest.raises(RuntimeError, match='transport observed'):
        perp.BinancePerpProvisional().fetch(partition)
    assert clock[0] == pytest.approx(deadline + 10)
    assert called[-1] is None and not outcomes
    assert banned_path.read_text() == saved
    assert daily._budget_state_file(tmp_path, 'https://api1.binance.com', DEDICATED[1]) == (
        daily._budget_state_file(tmp_path, 'https://api.binance.com', DEDICATED[1])
    )


@contextmanager
def fixture_server() -> Iterator[tuple[str, Queue[tuple[str, str]]]]:
    events: Queue[tuple[str, str]] = Queue()
    body = FIXTURE.read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            events.put(('start', self.client_address[0]))
            time.sleep(0.05)
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            events.put(('end', self.client_address[0]))
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/', events
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.skipif(
    sys.platform == 'darwin', reason='Multiple loopback addresses require Linux CI.'
)
def test_bound_transport_observes_source_and_fails_without_rerouting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HTTP_PROXY', 'http://127.0.0.1:1')
    monkeypatch.setenv('ALL_PROXY', 'http://127.0.0.1:1')
    monkeypatch.setenv('NO_PROXY', '')
    ips = ('127.0.0.2', '127.0.0.3')
    with fixture_server() as (url, events):
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(lambda ip: daily.get_response(url, egress_ip=ip), ips))
        assert [response.egress_ip for response in responses] == list(ips)
        assert all(response.body == FIXTURE.read_bytes() for response in responses)
        observed = [events.get(timeout=5) for _ in range(4)]
        assert sorted(ip for event, ip in observed if event == 'start') == sorted(ips)
        with pytest.raises(SourceError, match=r'did not complete.*192\.0\.2\.123'):
            daily.get_response(url, egress_ip='192.0.2.123')
        assert events.empty()


def test_bound_transport_keeps_tls_timeout_and_names_failed_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def failed(
        session: requests.Session,
        url: str,
        *,
        params: object,
        headers: object,
        timeout: tuple[int, int],
    ) -> requests.Response:
        calls.append(url)
        assert session.trust_env is False and session.verify is True
        assert timeout == (5, 30)
        for scheme in ('http://', 'https://'):
            adapter = session.adapters[scheme]
            assert adapter.poolmanager.connection_pool_kw['source_address'] == (DEDICATED[0], 0)
        raise requests.ConnectionError('binding unavailable')

    monkeypatch.setattr(requests.Session, 'get', failed)
    with pytest.raises(SourceError, match=r'did not complete.*37\.27\.112\.140'):
        daily.get_response(URL, egress_ip=DEDICATED[0])
    assert calls == [URL]
    with pytest.raises(ValueError):
        daily.get_response(URL, egress_ip='../not-an-ip')
    assert calls == [URL]


def budget_process(url: str, root: str, start: Event, results: ProcessQueue[str]) -> None:
    os.environ['ORIGO_SOURCE_LOCK_DIR'] = root
    os.environ.pop('ORIGO_WORKER_HEARTBEAT', None)
    results.put('ready')
    assert start.wait(timeout=15)
    response = daily.get_response(url, weight=1, egress_ip='127.0.0.1')
    assert response.body == FIXTURE.read_bytes()
    results.put('done')


def test_host_ip_budget_serializes_across_processes(tmp_path: Path) -> None:
    context = multiprocessing.get_context('spawn')
    start = context.Event()
    results: ProcessQueue[str] = context.Queue()
    with fixture_server() as (url, events):
        processes = [
            context.Process(target=budget_process, args=(url, str(tmp_path), start, results))
            for _ in range(2)
        ]
        try:
            for process in processes:
                process.start()
            assert [results.get(timeout=15) for _ in processes] == ['ready', 'ready']
            start.set()
            assert [results.get(timeout=15) for _ in processes] == ['done', 'done']
            assert [events.get(timeout=5)[0] for _ in range(4)] == ['start', 'end', 'start', 'end']
            for process in processes:
                process.join(timeout=5)
                assert process.exitcode == 0
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            results.close()
