"""The market state query service: local HTTP queries, cube-reader renewals and expiry (PRD-0022).

``POST /v1/market-state/query`` writes one pinned sparse result and answers with its paths
(``origo.query.market_state``); ``POST /v1/market-state/access`` is the cube reader's renewal
before each read (``origo.query.market_state_reader``); ``GET /healthz`` answers ``ok``. Two
queries run at once and a third is refused with 503 rather than queued; renewals need no
query slot and no ClickHouse, so they keep working while ClickHouse restarts.

The service is an observed worker. Every minute its tick expires files idle for 24 hours,
writes one ``binance_spot_trades:cleanup`` receipt and, when queries ended since the last
tick, one aggregated ``binance_spot_trades:query`` receipt, materializes
``market_state_query_service`` in Dagit, probes its own ``/healthz`` and touches the
heartbeat the monitor expects. A failed or interrupted export makes that receipt ``FAILED``,
which ``workers_alive`` reports.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import socket
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Final, Literal, cast
from uuid import uuid4

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.market_state import Request, RequestError, iso, parse_request, write_result
from origo.query.market_state_results import (
    FLOOR_MARGIN_BYTES,
    IDLE_EXPIRY_SECONDS,
    MAX_CONCURRENT_QUERIES,
    RESULT_ROOT,
    ResultStore,
    StorageFull,
    parse_result_path,
)
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.capacity import CAPACITY_TOTAL_RESERVE_TENTHS, CAPACITY_WORKING_SET_FACTOR
from origo.sources.contracts import Client, SourceError, failure_code
from origo.sources.lifecycle import SourceRuntime
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.workers.receipts import ensure_monitoring_tables, record_receipt
from origo.workers.report import Reporter
from origo.workers.runtime import (
    WATCHDOG_EXIT_CODE,
    TickOutcome,
    check_heartbeat,
    heartbeat_directory,
    heartbeat_path,
    run_forever,
    touch_heartbeat,
)

FEED: Final = 'market_state_api'
ASSET: Final = 'market_state_query_service'
QUERY_SERIES: Final = 'binance_spot_trades:query'
CLEANUP_SERIES: Final = 'binance_spot_trades:cleanup'
PORT: Final = 8486
CONNECTION_SLOTS: Final = 32
REQUEST_TIMEOUT_SECONDS: Final = 10
PROBE_TIMEOUT_SECONDS: Final = 5
DEFAULT_WEBSERVER_URL: Final = 'http://dagit:3000'
DEFAULT_LOCK_ROOT: Final = '/opt/origo/locks'

log = logging.getLogger('origo.workers.market_state_api')

Status = Literal['OK', 'REJECTED', 'FAILED']

Answer = tuple[int, dict[str, object], dict[str, str]]
_PRECEDENCE: Final[tuple[Status, ...]] = ('FAILED', 'REJECTED', 'OK')


class MarketStateApi:
    """The query service's work and its minute tick (``origo.workers.runtime.Feed``)."""

    name = FEED
    lookback_minutes = 0

    def __init__(
        self,
        store: ResultStore,
        reporter: Reporter,
        lock_root: Path,
        *,
        interrupted: int = 0,
        port: int = PORT,
    ) -> None:
        self.store, self.reporter, self.lock_root, self.port = store, reporter, lock_root, port
        self.queries = threading.BoundedSemaphore(MAX_CONCURRENT_QUERIES)
        self._lock = threading.Lock()
        self._outcomes: Counter[tuple[Status, str]] = Counter({('FAILED', 'EXPORT_INTERRUPTED'): interrupted} if interrupted else {})
        self._invalid = 0
        self._token = ''
        self._monitoring_ready = False

    def query(self, raw: bytes, deliver: Callable[[Answer], bool]) -> None:
        """Answer one query through ``deliver``, which reports whether the client got it.

        A published result whose answer could not be sent is discarded at once: nobody holds
        its paths. A client that closed while the answer still fit the socket buffer cannot
        be told apart from one that half-closed its write side and is still reading, so its
        result is kept and expires 24 hours later.
        """
        answer = self._answer(raw)
        delivered = deliver(answer)
        status, payload, _ = answer
        if status != 200:
            return
        result_id = str(payload['result_id'])
        if not delivered:
            self.store.discard(result_id)
            self._count('REJECTED', 'CLIENT_DISCONNECTED')
            return
        with self._lock:
            self._outcomes[('OK', '')] += 1
            self._token = str(payload['state_token'])

    def _answer(self, raw: bytes) -> Answer:
        try:
            request = parse_request(raw)
        except RequestError as error:
            self._count_invalid()
            return error.status, error.body(), {}
        if not self.queries.acquire(blocking=False):
            self._count('REJECTED', 'QUERY_BUSY')
            return 503, {'error': 'busy'}, {'Retry-After': '5'}
        try:
            return self._export(request)
        finally:
            self.queries.release()

    def access(self, raw: bytes) -> Answer:
        try:
            value = json.loads(raw)
        except (ValueError, RecursionError):
            return 400, {'error': 'invalid_request', 'reason': 'invalid_path'}, {}
        path = cast(dict[str, object], value).get('path') if isinstance(value, dict) else None
        parsed = parse_result_path(path) if isinstance(path, str) and len(path) <= 4096 else None
        if parsed is None:
            return 400, {'error': 'invalid_request', 'reason': 'invalid_path'}, {}
        deadline = self.store.access(*parsed)
        if deadline is None:
            return 410, {'error': 'gone'}, {}
        return 200, {'expires_at': iso(deadline)}, {}

    def tick(self, now: datetime) -> TickOutcome:
        minute = now.replace(second=0, microsecond=0)
        started = time.monotonic()
        failed: list[str] = []
        cleanup: tuple[Status, str, str]
        try:
            reclaimed = self.store.expire()
            cleanup = ('OK', '', '')
        except Exception as error:
            log.exception('market state cleanup failed')
            reclaimed = 0
            cleanup = ('FAILED', 'CLEANUP_FAILED', f'{type(error).__name__}: {error}')
            failed.append(CLEANUP_SERIES)
        status, code, message = cleanup
        with self._lock:
            outcomes, invalid, token = Counter(self._outcomes), self._invalid, self._token
        receipts: list[tuple[str, int, str, int, Status, str, str]] = [
            (CLEANUP_SERIES, reclaimed, '', _elapsed(started), status, code, message)
        ]
        if outcomes:
            worst: Status = next(level for level in _PRECEDENCE if any(key[0] == level for key in outcomes))
            first = '' if worst == 'OK' else next(key[1] for key in sorted(outcomes) if key[0] == worst)
            summary = ' '.join(f'{level.lower()}={count}' + (f':{reason}' if reason else '') for (level, reason), count in sorted(outcomes.items()))
            receipts.append((QUERY_SERIES, sum(outcomes.values()), token, 0, worst, first, summary))
            if worst == 'FAILED':
                failed.append(QUERY_SERIES)
        if self._write_receipts(minute, receipts):
            with self._lock:
                self._outcomes.subtract(outcomes)
                self._outcomes = +self._outcomes
                self._invalid -= invalid
        results, size = self.store.usage()
        self.reporter.materialized(
            ASSET,
            partition=None,
            metadata={
                'minute': minute.isoformat(),
                'results': results,
                'bytes': size,
                'queries_ok': sum(count for (level, _), count in outcomes.items() if level == 'OK'),
                'queries_rejected': sum(count for (level, _), count in outcomes.items() if level == 'REJECTED'),
                'queries_failed': sum(count for (level, _), count in outcomes.items() if level == 'FAILED'),
                'queries_invalid': invalid,
                'files_reclaimed': reclaimed,
                'rss_bytes': _rss_bytes(),
            },
        )
        if not _healthy(self.port):
            log.error('market state query server stopped answering; exiting for a restart')
            os._exit(WATCHDOG_EXIT_CODE)
        return TickOutcome(FEED, minute, (CLEANUP_SERIES,), tuple(failed))

    def _export(self, request: Request) -> Answer:
        result_id = str(uuid4())
        try:
            runtime, client = _runtime(self.lock_root)
            try:
                answer, cells, summary = self._publish(runtime, request, result_id)
            finally:
                client.disconnect()
        except RequestError as error:
            self._count_invalid()
            return error.status, error.body(), {}
        except StorageFull as error:
            self._count('FAILED', 'RESULT_STORAGE_FULL')
            log.error('market state result storage full: %s', error)
            return 507, {
                'error': 'result_storage_full', 'used_bytes': error.used, 'budget_bytes': error.budget,
                'free_bytes': error.free, 'floor_bytes': error.floor,
            }, {}
        except SourceError as error:
            if error.code != 'SOURCE_MAINTENANCE':
                return self._failed(error)
            self._count('REJECTED', 'SOURCE_MAINTENANCE')
            return 503, {'error': 'source_maintenance'}, {'Retry-After': '5'}
        except Exception as error:
            return self._failed(error)
        created = datetime.now(UTC)
        return 200, {
            **answer,
            'cells': str(cells),
            'summary': str(summary),
            'expires_after_seconds': IDLE_EXPIRY_SECONDS,
            'expires_at': iso(created + timedelta(seconds=IDLE_EXPIRY_SECONDS)),
        }, {}

    def _publish(
        self, runtime: SourceRuntime, request: Request, result_id: str
    ) -> tuple[dict[str, object], Path, Path]:
        """Admit, write and publish one result; any failure discards the unreturned result."""
        runtime.require_shared_mount()
        floor = source_floor(runtime.store, self.store.disk(self.store.root).total)
        self.store.admit(result_id, 0, floor)
        try:
            staging = self.store.register(result_id)
            answer = write_result(
                runtime, request, staging, result_id=result_id,
                guard=lambda staged: self.store.admit(result_id, staged, floor),
            )
            cells, summary = self.store.publish(result_id)
        except BaseException:
            self.store.discard(result_id)
            raise
        return answer, cells, summary

    def _failed(self, error: Exception) -> Answer:
        log.exception('market state export failed')
        self._count('FAILED', 'EXPORT_FAILED')
        return 500, {'error': 'export_failed', 'reason': failure_code(error)}, {}

    def _count(self, status: Status, code: str) -> None:
        with self._lock:
            self._outcomes[(status, code)] += 1

    def _count_invalid(self) -> None:
        with self._lock:
            self._invalid += 1

    def _write_receipts(
        self, minute: datetime, receipts: Sequence[tuple[str, int, str, int, Status, str, str]]
    ) -> bool:
        try:
            settings = get_clickhouse_settings()
            client = cast(Client, make_clickhouse_client(settings))
            try:
                if not self._monitoring_ready:
                    ensure_monitoring_tables(client, settings.database)
                    self._monitoring_ready = True
                for series, rows, token, duration_ms, status, code, message in receipts:
                    record_receipt(
                        client, settings.database, feed=FEED, series=series, minute=minute, rows=rows,
                        sha256=token, duration_ms=duration_ms, status=status, error_code=code, error=message,
                    )
            finally:
                client.disconnect()
        except Exception:
            log.exception('market state receipts failed; their outcomes stay for the next tick')
            return False
        return True



def source_floor(store: SourceStore, total_bytes: int) -> int:
    """The free space admission keeps: the largest source capacity reserve plus a margin.

    Each source's reserve is ``capacity.check``'s, read-only: the larger of 30% of the
    filesystem and twice its largest measured working set times its canonical concurrency.
    """
    rows = store.execute(
        f'SELECT source_key, max(working_set_bytes) FROM {store.table("source_capacity_log")} GROUP BY source_key'
    )
    measured = {str(row[0]): int(str(row[1])) for row in rows}
    reserve = (total_bytes * CAPACITY_TOTAL_RESERVE_TENTHS + 9) // 10
    for spec in SOURCE_REGISTRY:
        working = measured.get(spec.key, 0) * CAPACITY_WORKING_SET_FACTOR * spec.orchestration.canonical_concurrency
        reserve = max(reserve, working)
    return reserve + FLOOR_MARGIN_BYTES


class ApiServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, address: tuple[str, int], api: MarketStateApi) -> None:
        self.api = api
        self.slots = threading.BoundedSemaphore(CONNECTION_SLOTS)
        super().__init__(address, ApiHandler)

    def process_request(self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]) -> None:
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        super().process_request(request, client_address)

    def process_request_thread(self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class ApiHandler(BaseHTTPRequestHandler):
    timeout = REQUEST_TIMEOUT_SECONDS

    def do_POST(self) -> None:
        api = cast(ApiServer, self.server).api
        try:
            declared = self.headers.get('Content-Length')
            length = -1 if declared is None or self.headers.get('Transfer-Encoding') else int(declared)
            if length < 0 or length > 65_536:
                # Chunked or unsized bodies are refused rather than read as an empty request.
                self.close_connection = True
                self._send(400, {'error': 'invalid_request', 'reason': 'invalid_json', 'detail': 'A Content-Length body of at most 65536 bytes is required.'}, {})
                return
            raw = self.rfile.read(length)
            answer: Answer
            if self.path == '/v1/market-state/query':
                api.query(raw, lambda reply: self._send(*reply))
                return
            if self.path == '/v1/market-state/access':
                answer = api.access(raw)
            else:
                answer = 404, {'error': 'not_found'}, {}
        except (OSError, ValueError) as error:
            self.close_connection = True
            log.warning('market state request unreadable: %s', type(error).__name__)
            return
        except Exception:
            log.exception('market state request failed: %s', self.path)
            answer = 500, {'error': 'internal_error'}, {}
        self._send(*answer)

    def do_GET(self) -> None:
        if self.path == '/healthz':
            self._send(200, {'status': 'ok'}, {})
        else:
            self._send(404, {'error': 'not_found'}, {})

    def _send(self, status: int, payload: Mapping[str, object], headers: Mapping[str, str]) -> bool:
        """Send one JSON answer; ``False`` when the client was gone."""
        body = json.dumps(payload, allow_nan=False).encode()
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        except OSError as error:
            self.close_connection = True
            log.warning('market state client disconnected: %s', type(error).__name__)
            return False
        return True

    def log_message(self, format: str, *args: object) -> None:
        log.debug(format, *args)


def serve(api: MarketStateApi, *, port: int) -> ApiServer:
    """Serve queries, renewals and health on a daemon thread."""
    server = ApiServer(('0.0.0.0', port), api)
    threading.Thread(target=server.serve_forever, name='market-state-http', daemon=True).start()
    return server


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m origo.workers.market_state_api',
        description='Serve market state cube queries and expire their result files.',
    )
    parser.add_argument('--check', action='store_true', help='healthcheck: heartbeat fresh and /healthz answering')
    parser.add_argument('--port', type=int, default=PORT)
    parser.add_argument('--root', type=Path, default=RESULT_ROOT)
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    heartbeat = heartbeat_path(heartbeat_directory(), FEED)
    if arguments.check:
        return check_heartbeat(heartbeat) or (0 if _healthy(arguments.port) else 1)
    touch_heartbeat(heartbeat)
    store = ResultStore(arguments.root)
    interrupted = store.recover()
    if interrupted:
        log.error('market state queries interrupted by the previous process: %d', interrupted)
    api = MarketStateApi(
        store,
        Reporter(os.environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)),
        Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', DEFAULT_LOCK_ROOT)),
        interrupted=interrupted,
        port=arguments.port,
    )
    serve(api, port=arguments.port)
    run_forever(api, heartbeat=heartbeat)


def _runtime(lock_root: Path) -> tuple[SourceRuntime, Client]:
    settings = get_clickhouse_settings()
    client = cast(Client, make_clickhouse_client(settings))
    store = SourceStore(client, settings.database, BINANCE_SPOT_TRADES_SPEC)
    return SourceRuntime(BINANCE_SPOT_TRADES_SPEC, store, lock_root, str(uuid4())), client


def _healthy(port: int) -> bool:
    """Whether the query server's accept loop is alive.

    A 200 from ``/healthz`` proves it, and so does a connection the server accepts and then
    drops, cleanly or with a reset: it sheds connections beyond its slots only while it is
    accepting. A refused connection, or no answer within the probe timeout, means the server
    is gone or wedged.
    """
    try:
        probe = socket.create_connection(('127.0.0.1', port), timeout=PROBE_TIMEOUT_SECONDS)
    except OSError:
        return False
    answer = b''
    with probe:
        try:
            probe.sendall(b'GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n')
            # A status line may arrive in fragments; read until it is complete or the peer closes.
            while len(answer) < 12:
                fragment = probe.recv(12 - len(answer))
                if not fragment:
                    break
                answer += fragment
        except (ConnectionResetError, BrokenPipeError):
            return True
        except OSError:
            return False
    return answer == b'' or answer.startswith(b'HTTP/1.0 200') or answer.startswith(b'HTTP/1.1 200')


def _elapsed(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage if sys.platform == 'darwin' else usage * 1024


if __name__ == '__main__':
    sys.exit(main())
