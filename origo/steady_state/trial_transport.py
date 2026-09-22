"""Local archive replay boundary; these responses are never HTTP-capture provenance."""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import threading
import time
from collections.abc import Mapping
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, urlsplit

import polars as pl
import requests

from origo.sources.adapters import binance_daily


def digest(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path: Path, value: object) -> None:
    """Commit a checkpoint only after its complete bytes have been flushed."""
    import os

    pending = path.with_suffix(path.suffix + '.pending')
    with pending.open('w') as stream:
        json.dump(value, stream, sort_keys=True, default=str)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    pending.replace(path)


def restrict_network(ports: set[int]) -> None:
    """Install a process-lifetime socket fence before importing application setup."""

    def audit(event: str, arguments: tuple[object, ...]) -> None:
        if event in ('socket.connect', 'socket.sendto'):
            address = arguments[-1]
            if not isinstance(address, tuple):
                raise PermissionError('Trial refuses non-owned socket destinations.')
            address = cast(tuple[object, ...], address)
            if len(address) < 2:
                raise PermissionError('Trial refuses non-owned socket destinations.')
            host, port = address[:2]
            if host != '127.0.0.1' or port not in ports:
                raise PermissionError(f'Trial refuses socket destination {address!r}.')
        if event == 'socket.getaddrinfo' and arguments[0] != '127.0.0.1':
            raise PermissionError('Trial refuses external DNS resolution.')

    sys.addaudithook(audit)


class Tape:
    def __init__(self, document: dict[str, object]) -> None:
        self.document = document
        path = Path(str(document['tape_path']))
        if digest(path) != document['tape_sha256']:
            raise ValueError('Immutable replay tape digest changed.')
        self.frame = pl.read_ipc(path, memory_map=False)
        self.aggregate = str(document['source_key']).endswith('aggtrades')
        self.identity = 'a' if self.aggregate else 'id'
        self.timestamp = 'T' if self.aggregate else 'time'
        self.ids = self.frame.get_column(self.identity)
        self.times = self.frame.get_column(self.timestamp)

    def page(self, params: Mapping[str, str], *, recent: bool, now: datetime) -> bytes:
        limit = int(params['limit'])
        if recent:
            stop = int(self.times.search_sorted(int(now.timestamp() * 1000), side='right'))
            start = max(0, stop - limit)
        else:
            start = (
                int(self.ids.search_sorted(int(params['fromId'])))
                if 'fromId' in params
                else int(self.times.search_sorted(int(params['startTime'])))
            )
            stop = min(start + limit, self.frame.height)
        frame = self.frame.slice(start, stop - start)
        frame = frame.filter(pl.col(self.timestamp) <= int(now.timestamp() * 1000))
        if 'endTime' in params:
            frame = frame.filter(pl.col(self.timestamp) <= int(params['endTime']))
        return frame.write_json().encode()


class ReplayServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        tapes: dict[str, Tape],
        origins: dict[str, datetime],
        log: Path,
        *,
        bind_and_activate: bool = True,
        locators: dict[str, Tape] | None = None,
    ) -> None:
        self.tapes, self.origins, self.log = tapes, origins, log
        self.locators = dict(locators or {})
        self.started: float | None = None
        self.mutex = threading.Lock()
        super().__init__(('127.0.0.1', 0), ReplayHandler, bind_and_activate=bind_and_activate)

    def source_now(self, source: str) -> datetime:
        if self.started is None:
            raise RuntimeError('Replay has not been admitted.')
        return self.origins[source] + timedelta(seconds=time.monotonic() - self.started)

    def response(self, source: str, path: str, params: dict[str, str]) -> bytes:
        if source not in self.tapes:
            raise ValueError('Undeclared source.')
        if params.get('symbol') != 'BTCUSDT':
            raise ValueError('Only the original BTCUSDT request shape is supported.')
        prefix = '/api/v3/' if '_spot_' in source else '/fapi/v1/'
        endpoint = path.removeprefix(prefix)
        if not path.startswith(prefix) or endpoint not in (
            'aggTrades',
            'historicalTrades',
            'trades',
        ):
            raise ValueError('Unsupported provider endpoint.')
        limit = int(params.get('limit', '0'))
        maximum = 500 if endpoint == 'historicalTrades' and '_perp_' in source else 1000
        if not 1 <= limit <= maximum:
            raise ValueError('Unsupported original page limit.')
        fields = set(params)
        if endpoint == 'trades':
            if source != 'binance_perp_trades' or fields != {'symbol', 'limit'} or limit != 1000:
                raise ValueError('Unsupported recent-trade shape.')
        elif endpoint == 'historicalTrades':
            if fields != {'symbol', 'limit', 'fromId'}:
                raise ValueError('Unsupported historical-trade shape.')
        elif fields not in (
            {'symbol', 'limit', 'fromId'},
            {'symbol', 'limit', 'startTime'},
            {'symbol', 'limit', 'startTime', 'endTime'},
        ):
            raise ValueError('Unsupported aggregate-trade shape.')
        tape = self.tapes[source]
        if endpoint == 'aggTrades' and not tape.aggregate:
            candidate = source.removesuffix('trades') + 'aggtrades'
            locator = self.locators.get(source, self.tapes.get(candidate))
            if locator is None or locator.document['day'] != tape.document['day']:
                raise LookupError(
                    'Missing authentic aggregate locator archive for this raw-trade day.'
                )
            tape = locator
        if endpoint != 'aggTrades' and tape.aggregate:
            raise ValueError('Aggregate events cannot substitute for raw trades.')
        return tape.page(params, recent=endpoint == 'trades', now=self.source_now(source))


class ReplayHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        server = cast(ReplayServer, self.server)
        parsed = urlsplit(self.path)
        parts = parsed.path.split('/', 2)
        status, body = 200, b''
        began = time.monotonic()
        try:
            if len(parts) != 3:
                raise ValueError('Source-qualified replay path required.')
            query = parse_qs(parsed.query, strict_parsing=True)
            if any(len(values) != 1 for values in query.values()):
                raise ValueError('Repeated parameters are unsupported.')
            body = server.response(parts[1], '/' + parts[2], {k: v[0] for k, v in query.items()})
        except (ValueError, LookupError, RuntimeError) as error:
            status = 422
            body = json.dumps({'error': str(error)}).encode()
        with server.mutex, server.log.open('a') as log:
            log.write(
                json.dumps(
                    {
                        'observed_at': datetime.now(UTC).isoformat(),
                        'path': self.path,
                        'status': status,
                        'body_sha256': hashlib.sha256(body).hexdigest(),
                        'response_bytes': len(body),
                        'elapsed_seconds': time.monotonic() - began,
                        'kind': 'derived_official_archive_replay',
                        'http_capture': False,
                        'error': body.decode() if status != 200 else None,
                    }
                )
                + '\n'
            )
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        # This endpoint is explicitly unavailable, never a fake Dagster success.
        self.send_error(503, 'Native Dagster is not wired into this trial.')

    def log_message(self, format: str, *args: object) -> None:
        # HTTP facts are retained above, without BaseHTTPRequestHandler stderr noise.
        logging.getLogger(__name__).debug(format, *args)


def install_transport(source: str, port: int, costs: Path) -> None:
    """Change only the wire destination, downstream of unchanged production pacing."""
    session = requests.Session()
    session.trust_env = False
    mutex = threading.Lock()
    original = binance_daily.get_response
    admitted: ContextVar[bool] = ContextVar('trial_request_admitted', default=False)

    def request(
        url: str, params: Mapping[str, str | int] | None, headers: Mapping[str, str] | None
    ) -> requests.Response:
        parsed = urlsplit(url)
        expected = 'api.binance.com' if '_spot_' in source else 'fapi.binance.com'
        if parsed.scheme != 'https' or parsed.netloc != expected or parsed.query:
            raise PermissionError('Only declared production request identities can be replayed.')
        admitted.set(True)
        return session.get(
            f'http://127.0.0.1:{port}/{source}{parsed.path}',
            params=params,
            headers=headers,
            timeout=(5, 30),
            allow_redirects=False,
        )

    def measured(
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
        weight: int = 0,
        lane: binance_daily.Lane = 'shared',
    ) -> binance_daily.Response:
        from dataclasses import asdict

        began = time.monotonic()
        token = admitted.set(False)
        document: dict[str, object] = {
            'url': url,
            'params': params,
            'weight': weight,
            'lane': lane,
            'observed_at': datetime.now(UTC).isoformat(),
            'authenticated_shape': bool(headers and headers.get('X-MBX-APIKEY')),
            'http_capture': False,
        }
        try:
            response = original(url, params=params, headers=headers, weight=weight, lane=lane)
            document.update(asdict(response.cost))
            document.update(status=response.status, response_bytes=len(response.body))
            return response
        except Exception as error:
            document['error'] = repr(error)
            raise
        finally:
            document['admitted'] = admitted.get()
            document['requested_weight'] = weight
            document['weight'] = weight if admitted.get() else 0
            admitted.reset(token)
            document['elapsed_seconds'] = time.monotonic() - began
            with mutex, costs.open('a') as stream:
                stream.write(json.dumps(document) + '\n')

    setattr(binance_daily, '_request', request)
    # Modules import get_response by value; replace all four adapter seams plus capture.
    from origo.sources.adapters import (
        binance_perp_agg_rest,
        binance_perp_rest,
        binance_spot_agg_rest,
        binance_spot_rest,
    )
    from origo.workers import trade_capture

    for module in (
        binance_perp_agg_rest,
        binance_perp_rest,
        binance_spot_agg_rest,
        binance_spot_rest,
        trade_capture,
    ):
        setattr(module, 'get_response', measured)
