from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import aiohttp
from aiohttp import web

from origo.sources.adapters.binance_daily import Response, get_response
from origo.sources.adapters.book_spool import (
    Market,
    SealedMinute,
    atomic_write,
    object_mapping,
    reconcile_spool_bytes,
    seal_minute,
    spool_bytes,
    utc_millisecond,
)
from origo.sources.contracts import SourceError, failure_code
from origo.sources.locking import source_lock

from .runtime import (
    HEARTBEAT_MAX_AGE_SECONDS,
    heartbeat_directory,
    heartbeat_is_fresh,
    heartbeat_path,
    start_watchdog,
    touch_heartbeat,
)

BOOK_SEED_DEPTH = {'spot': 5000, 'perp': 1000}
BOOK_SEED_WEIGHT = {'spot': 250, 'perp': 20}
BOOK_MAX_SEED_ATTEMPTS_PER_HOUR = 3
BOOK_MAX_CONNECTION_ATTEMPTS_PER_5M = 5
SEED_URL = {
    'spot': 'https://api.binance.com/api/v3/depth',
    'perp': 'https://fapi.binance.com/fapi/v1/depth',
}
STREAM_URL = {
    'spot': 'wss://stream.binance.com:9443/ws/btcusdt@depth@100ms',
    'perp': 'wss://fstream.binance.com/public/ws/btcusdt@depth@100ms',
}
BOOK_ROTATION_SECONDS = 23 * 3600
BOOK_MAX_EVENT_AGE_SECONDS = 5
STATUS_MAX_BYTES = 16 * 1024
FRAME_MAX_BYTES = 1024**2
log = logging.getLogger(__name__)
Level = tuple[Decimal, Decimal]


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError('Book clocks and update IDs require nonnegative integers.')
    return value


def _levels(value: object, *, zeros: bool) -> tuple[Level, ...]:
    if not isinstance(value, list):
        raise ValueError('Book levels must be an array.')
    levels: list[Level] = []
    for item in cast(list[object], value):
        if not isinstance(item, list) or len(cast(list[object], item)) != 2:
            raise ValueError('A book level requires price and quantity.')
        price, quantity = cast(list[object], item)
        if not isinstance(price, str) or not isinstance(quantity, str):
            raise ValueError('Book values require decimal strings.')
        p, q = Decimal(price), Decimal(quantity)
        if not p.is_finite() or not q.is_finite() or p <= 0 or q < 0 or (not zeros and q == 0):
            raise ValueError('Book values must be finite and positive, except deletion quantities.')
        levels.append((p, q))
    if len({p for p, _ in levels}) != len(levels):
        raise ValueError('A book frame contains duplicate price levels.')
    return tuple(levels)


@dataclass(frozen=True)
class DepthEvent:
    event_ms: int
    first: int
    last: int
    previous: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    @classmethod
    def parse(cls, raw: bytes, market: Market) -> DepthEvent:
        if len(raw) > FRAME_MAX_BYTES:
            raise ValueError('Book frame exceeds its bound.')
        value = object_mapping(json.loads(raw))
        if value.get('e') != 'depthUpdate' or value.get('s') != 'BTCUSDT':
            raise ValueError('The capture owns BTCUSDT diff-depth events only.')
        event = cls(
            _integer(value.get('E')),
            _integer(value.get('U')),
            _integer(value.get('u')),
            _integer(value.get('pu')) if market == 'perp' else None,
            _levels(value.get('b'), zeros=True),
            _levels(value.get('a'), zeros=True),
        )
        if event.first > event.last:
            raise ValueError('Book frame update IDs are reversed.')
        return event


class DiffBook:
    def __init__(self, market: Market, seed: bytes) -> None:
        self.market = market
        value = object_mapping(json.loads(seed))
        self.last = _integer(value.get('lastUpdateId'))
        bids, asks = (_levels(value.get(side), zeros=False) for side in ('bids', 'asks'))
        if len(bids) < 200 or len(asks) < 200:
            raise SourceError('BOOK_SEED_TOO_SHALLOW', 'Seed does not prove a 200-level book.')
        if bids != tuple(sorted(bids, reverse=True)) or asks != tuple(sorted(asks)):
            raise ValueError('Seed levels are not strictly ordered.')
        self.bids = dict(bids)
        self.asks = dict(asks)
        self.bid_floor, self.ask_ceiling = bids[-1][0], asks[-1][0]
        self.event_ms: int | None = None
        self.verified = False
        self.cached: tuple[tuple[Level, ...], tuple[Level, ...]] | None = None

    def bridges(self, event: DepthEvent) -> bool:
        if self.verified:
            return (
                event.previous == self.last
                if self.market == 'perp'
                else event.first <= self.last + 1 <= event.last
            )
        return (
            event.first <= self.last <= event.last
            if self.market == 'perp'
            else event.first <= self.last + 1 <= event.last
        )

    def obsolete(self, event: DepthEvent) -> bool:
        return (
            event.last <= self.last
            if self.verified or self.market == 'spot'
            else event.last < self.last
        )

    def top(self, depth: int) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
        if self.cached is not None and depth <= 200:
            return self.cached[0][:depth], self.cached[1][:depth]
        bids = tuple(sorted(self.bids.items(), reverse=True)[:depth])
        asks = tuple(sorted(self.asks.items())[:depth])
        if (
            len(bids) != depth
            or len(asks) != depth
            or bids[-1][0] < self.bid_floor
            or asks[-1][0] > self.ask_ceiling
        ):
            raise SourceError(
                'BOOK_KNOWN_DEPTH_EXHAUSTED', 'Top depth crosses the seeded known region.'
            )
        if bids[0][0] >= asks[0][0]:
            raise SourceError('BOOK_CROSSED', 'Reconstructed book is crossed.')
        if depth == 200:
            self.cached = (bids, asks)
        return bids, asks

    def apply(self, event: DepthEvent) -> bool:
        if self.obsolete(event):
            return False
        if not self.bridges(event):
            self.verified = False
            raise SourceError(
                'BOOK_SEQUENCE_GAP', 'Diff stream does not bridge the last update ID.'
            )
        if self.event_ms is not None and event.event_ms < self.event_ms:
            self.verified = False
            raise SourceError('BOOK_EVENT_TIME_REGRESSION', 'Exchange event time moved backwards.')
        self.cached = None
        for side, updates in ((self.bids, event.bids), (self.asks, event.asks)):
            for price, quantity in updates:
                # Updates outside the seeded region cannot prove a complete top200.
                if (side is self.bids and price < self.bid_floor) or (
                    side is self.asks and price > self.ask_ceiling
                ):
                    continue
                if quantity:
                    side[price] = quantity
                else:
                    side.pop(price, None)
        self.last, self.event_ms = event.last, event.event_ms
        try:
            self.top(200)
        except SourceError:
            self.verified = False
            raise
        self.verified = True
        return True


def _strings(levels: tuple[Level, ...]) -> list[list[str]]:
    return [[str(price), str(quantity)] for price, quantity in levels]


class BookSampler:
    def __init__(self, root: Path, market: Market) -> None:
        self.root = root
        self.market: Market = market
        self.book: DiffBook | None = None
        self.next_grid: int | None = None
        self.minute: int | None = None
        self.lines: list[bytes] = []
        self.counts = {20: 0, 200: 0}
        self.first_id: int | None = None
        self.last_seal: datetime | None = None
        self.last_received: datetime | None = None

    def invalidate(self) -> None:
        self.book = None
        self.next_grid = None
        self.minute = None
        self.lines.clear()
        self.counts = {20: 0, 200: 0}
        self.first_id = None

    def seed(self, payload: bytes) -> None:
        self.invalidate()
        self.book = DiffBook(self.market, payload)

    def _seal(self) -> None:
        book = self.book
        if self.counts == {20: 600, 200: 60} and book is not None and self.minute is not None:
            payload = b''.join(self.lines)
            assert self.first_id is not None
            minute: SealedMinute = {
                'market': self.market,
                'minute_start': utc_millisecond(self.minute).isoformat(),
                'samples_depth20': 600,
                'samples_depth200': 60,
                'first_update_id': self.first_id,
                'last_update_id': book.last,
                'payload_sha256': hashlib.sha256(payload).hexdigest(),
            }
            seal_minute(self.root, minute, payload)
            self.last_seal = utc_millisecond(self.minute + 60000)
        self.lines.clear()
        self.counts = {20: 0, 200: 0}
        self.first_id = None

    def _sample_until(self, event_ms: int) -> None:
        book = self.book
        assert book is not None and book.event_ms is not None and self.next_grid is not None
        while self.next_grid < event_ms:
            grid = self.next_grid
            minute = grid // 60000 * 60000
            if minute != self.minute:
                self._seal()
                self.minute = minute
            if self.first_id is None:
                self.first_id = book.last
            depths = (20, 200) if grid % 1000 == 0 else (20,)
            for depth in depths:
                bids, asks = book.top(depth)
                row = {
                    'depth': depth,
                    'datetime_ms': grid,
                    'source_timestamp_ms': book.event_ms,
                    'last_update_id': book.last,
                    'bids': _strings(bids),
                    'asks': _strings(asks),
                }
                self.lines.append(
                    json.dumps(row, separators=(',', ':'), allow_nan=False).encode() + b'\n'
                )
                self.counts[depth] += 1
            self.next_grid += 100
        if self.minute is not None and event_ms >= self.minute + 60000:
            self._seal()
            self.minute = None

    def accept(self, event: DepthEvent, *, received_at: datetime) -> bool:
        book = self.book
        if book is None:
            raise SourceError('BOOK_UNSEEDED', 'Diff event arrived without a seed.')
        if book.obsolete(event):
            return False
        if not book.bridges(event) or (
            book.event_ms is not None and event.event_ms < book.event_ms
        ):
            self.invalidate()
            raise SourceError('BOOK_SEQUENCE_GAP', 'Book continuity was lost before sampling.')
        if (
            not 0
            <= (received_at - utc_millisecond(event.event_ms)).total_seconds()
            <= BOOK_MAX_EVENT_AGE_SECONDS
        ):
            self.invalidate()
            raise SourceError(
                'BOOK_EVENT_STALE', 'Exchange event is stale or ahead of the local clock.'
            )
        if book.verified and book.event_ms is not None and self.next_grid is not None:
            self._sample_until(event.event_ms)
        try:
            applied = book.apply(event)
        except SourceError:
            self.invalidate()
            raise
        if self.next_grid is None:
            self.next_grid = ((event.event_ms + 99) // 100) * 100
        self.last_received = received_at
        return applied

    def top20(self, now: datetime) -> Mapping[str, object]:
        book = self.book
        if (
            book is None
            or not book.verified
            or book.event_ms is None
            or self.last_received is None
            or not 0 <= (now - self.last_received).total_seconds() <= BOOK_MAX_EVENT_AGE_SECONDS
            or not 0
            <= (now - utc_millisecond(book.event_ms)).total_seconds()
            <= BOOK_MAX_EVENT_AGE_SECONDS
        ):
            raise SourceError(
                'BOOK_UNAVAILABLE', 'No fresh sequence-verified local book is available.'
            )
        bids, asks = book.top(20)
        return {
            't': book.event_ms,
            'd': {'lastUpdateId': book.last, 'bids': _strings(bids), 'asks': _strings(asks)},
        }


class AttemptBudget:
    def __init__(
        self, root: Path, market: Market, *, clock: Callable[[], float] = time.time
    ) -> None:
        if not root.is_absolute():
            raise ValueError('Attempt limits require an absolute shared lock mount.')
        self.root, self.market, self.clock = root, market, clock
        self.path = root / f'book_capture_{market}.attempts.json'

    def _read(self) -> dict[str, object]:
        if not self.path.exists():
            return {
                'seed': [],
                'connection': [],
                'seed_weight': 0,
                'seed_total': 0,
                'connection_total': 0,
                'last_error': None,
            }
        with self.path.open('rb') as stream:
            raw = stream.read(STATUS_MAX_BYTES + 1)
        if len(raw) > STATUS_MAX_BYTES:
            raise ValueError('Book attempts exceed the evidence bound.')
        value = dict(object_mapping(json.loads(raw)))
        for kind in ('seed', 'connection'):
            stamps = value.get(kind)
            if not isinstance(stamps, list) or any(
                type(t) not in (int, float)
                or not math.isfinite(cast(float, t))
                or cast(float, t) < 0
                for t in cast(list[object], stamps)
            ):
                raise ValueError('Persisted book attempt timestamps are invalid.')
        for name in ('seed_weight', 'seed_total', 'connection_total'):
            _integer(value.get(name))
        return value

    def _reserve(self, kind: str) -> dict[str, object]:
        value = self._read()
        now = self.clock()
        window, maximum = (
            (3600, BOOK_MAX_SEED_ATTEMPTS_PER_HOUR)
            if kind == 'seed'
            else (300, BOOK_MAX_CONNECTION_ATTEMPTS_PER_5M)
        )
        stamps = [float(t) for t in cast(list[float], value[kind]) if t > now - window]
        if any(t > now for t in stamps):
            raise SourceError(
                'BOOK_ATTEMPT_CLOCK_REGRESSION', 'Clock moved behind a durable attempt.'
            )
        if len(stamps) >= maximum:
            raise SourceError(
                'BOOK_ATTEMPT_LIMIT', f'{kind} attempts paused until {stamps[0] + window:.3f}.'
            )
        value[kind] = [*stamps, now]
        total = f'{kind}_total'
        value[total] = _integer(value[total]) + 1
        if kind == 'seed':
            value['seed_weight'] = _integer(value['seed_weight']) + BOOK_SEED_WEIGHT[self.market]
        value['last_error'] = None
        atomic_write(self.path, json.dumps(value, separators=(',', ':'), allow_nan=False).encode())
        return value

    def connect(self) -> None:
        with source_lock(self.root, f'book_capture_{self.market}', 'attempts', wait=True):
            self._reserve('connection')

    def seed(self) -> Response:
        # Keep the reservation lock through transport: duplicate processes cannot seed together.
        with source_lock(self.root, f'book_capture_{self.market}', 'attempts', wait=True):
            value = self._reserve('seed')
            try:
                response = get_response(
                    SEED_URL[self.market],
                    params={'symbol': 'BTCUSDT', 'limit': BOOK_SEED_DEPTH[self.market]},
                    weight=BOOK_SEED_WEIGHT[self.market],
                    egress_ip=None,
                )
            except SourceError as error:
                value['last_error'] = error.code
                atomic_write(self.path, json.dumps(value, separators=(',', ':')).encode())
                raise
            return response

    def evidence(self) -> Mapping[str, object]:
        # Reservations publish by atomic rename; status reads never block a seed in flight.
        return self._read()


def top20_app(
    sampler: BookSampler, token: str, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> web.Application:
    if sampler.market != 'spot' or not token:
        raise ValueError('Only the spot capture serves authenticated /top20.')

    async def top20(request: web.Request) -> web.Response:
        if not hmac.compare_digest(
            request.headers.get('Authorization', '').encode(), f'Bearer {token}'.encode()
        ):
            raise web.HTTPUnauthorized()
        try:
            payload = sampler.top20(clock())
        except SourceError as error:
            raise web.HTTPServiceUnavailable(text=error.code) from error
        return web.json_response(payload, headers={'Cache-Control': 'no-store'})

    app = web.Application(client_max_size=1024)
    app.router.add_get('/top20', top20)
    return app


def status_path(directory: Path, market: Market) -> Path:
    return directory / f'book_capture_{market}.status.json'


def publish_status(
    sampler: BookSampler, budget: AttemptBudget, path: Path, error: str | None
) -> None:
    book = sampler.book
    status = {
        'schema_version': 1,
        'market': sampler.market,
        'committed_at': datetime.now(UTC).isoformat(),
        'book_verified': book is not None and book.verified,
        'last_event_at': utc_millisecond(book.event_ms).isoformat()
        if book is not None and book.event_ms is not None
        else None,
        'last_received_at': sampler.last_received.isoformat() if sampler.last_received else None,
        'last_seal_at': sampler.last_seal.isoformat() if sampler.last_seal else None,
        'spool_bytes': spool_bytes(sampler.root, sampler.market),
        'attempts': budget.evidence(),
        'error_code': error,
    }
    payload = json.dumps(status, separators=(',', ':'), allow_nan=False).encode()
    if len(payload) > STATUS_MAX_BYTES:
        raise ValueError('Book status exceeds its 16 KiB bound.')
    atomic_write(path, payload)


def check_capture(directory: Path, market: Market, now: datetime) -> int:
    if not heartbeat_is_fresh(
        heartbeat_path(directory, f'book_capture_{market}'),
        max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS,
        now=now.timestamp(),
    ):
        return 1
    return 0


@dataclass(frozen=True)
class StreamPacket:
    stream_id: int
    received_at: datetime
    event: DepthEvent | None = None
    error: SourceError | None = None


async def _receive(
    session: aiohttp.ClientSession,
    market: Market,
    stream_id: int,
    queue: asyncio.Queue[StreamPacket],
) -> None:
    try:
        async with session.ws_connect(
            STREAM_URL[market], autoping=True, max_msg_size=FRAME_MAX_BYTES
        ) as socket:
            while True:
                message = await socket.receive(timeout=10)
                received = datetime.now(UTC)
                if message.type == aiohttp.WSMsgType.TEXT:
                    if not isinstance(message.data, str):
                        raise ValueError('Diff-depth text frame has an invalid payload.')
                    event = DepthEvent.parse(message.data.encode(), market)
                    await queue.put(StreamPacket(stream_id, received, event=event))
                elif message.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.ERROR,
                ):
                    raise SourceError(
                        'BOOK_STREAM_CLOSED', 'Diff-depth stream closed before planned handover.'
                    )
                else:
                    raise ValueError(f'Unexpected diff-depth message type: {message.type}.')
    except (aiohttp.ClientError, TimeoutError, SourceError, ValueError) as error:
        failure = (
            error
            if isinstance(error, SourceError)
            else SourceError('BOOK_STREAM_FAILED', str(error))
        )
        log.error('%s stream %s failed: %s', market, stream_id, failure)
        await queue.put(StreamPacket(stream_id, datetime.now(UTC), error=failure))


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)
    if isinstance(result[0], BaseException) and not isinstance(result[0], asyncio.CancelledError):
        raise result[0]


async def run_capture(
    sampler: BookSampler, budget: AttemptBudget, directory: Path, *, stopping: asyncio.Event
) -> None:
    await asyncio.to_thread(reconcile_spool_bytes, sampler.root, sampler.market)
    market = sampler.market
    heartbeat = heartbeat_path(directory, f'book_capture_{market}')
    status = status_path(directory, market)
    queue: asyncio.Queue[StreamPacket] = asyncio.Queue(maxsize=512)
    sequence = 0
    failures = 0
    backoff_until = 0.0
    last_publish = 0.0
    rotation_after = 0.0
    active: int | None = None
    standby: int | None = None
    tasks: dict[int, asyncio.Task[None]] = {}
    opened: dict[int, float] = {}

    async def connect(session: aiohttp.ClientSession) -> int:
        nonlocal sequence
        if len(tasks) >= 2:
            raise ValueError('Capture may hold at most two connections during handover.')
        await asyncio.to_thread(budget.connect)
        sequence += 1
        tasks[sequence] = asyncio.create_task(_receive(session, market, sequence, queue))
        opened[sequence] = time.monotonic()
        return sequence

    async def retire(stream_id: int) -> None:
        await _cancel(tasks.pop(stream_id))
        del opened[stream_id]

    async def initialize() -> None:
        seed = asyncio.create_task(asyncio.to_thread(budget.seed))
        while not seed.done():
            await asyncio.wait({seed}, timeout=1)
            touch_heartbeat(heartbeat)
            publish_status(sampler, budget, status, 'BOOK_INITIALIZING')
        sampler.seed((await seed).body)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=10)
    ) as session:
        try:
            while not stopping.is_set():
                touch_heartbeat(heartbeat)
                if active is None and time.monotonic() < backoff_until:
                    await asyncio.sleep(min(1.0, backoff_until - time.monotonic()))
                    continue
                try:
                    if active is None:
                        active = await connect(session)
                    if (
                        standby is None
                        and sampler.book is not None
                        and sampler.book.verified
                        and time.monotonic() >= rotation_after
                        and time.monotonic() - opened[active] >= BOOK_ROTATION_SECONDS
                    ):
                        try:
                            standby = await connect(session)
                        except (SourceError, aiohttp.ClientError, OSError) as error:
                            # A failed overlap must not discard a still-verified active book.
                            log.warning('%s rotation delayed: %s', market, error)
                            rotation_after = time.monotonic() + 60
                            publish_status(sampler, budget, status, failure_code(error))
                    try:
                        packet = await asyncio.wait_for(queue.get(), timeout=1)
                    except TimeoutError:
                        publish_status(
                            sampler,
                            budget,
                            status,
                            None if sampler.book and sampler.book.verified else 'BOOK_INITIALIZING',
                        )
                        continue
                    if packet.stream_id not in tasks:
                        continue
                    if packet.error is not None:
                        if packet.stream_id == standby:
                            await retire(packet.stream_id)
                            standby = None
                            rotation_after = time.monotonic() + 60
                            publish_status(sampler, budget, status, packet.error.code)
                            continue
                        raise packet.error
                    event = packet.event
                    assert event is not None
                    if packet.stream_id == standby:
                        book = sampler.book
                        if book is None or book.obsolete(event) or not book.bridges(event):
                            continue
                        assert active is not None
                        await retire(active)
                        active, standby = standby, None
                    if sampler.book is None:
                        await initialize()
                    sampler.accept(event, received_at=packet.received_at)
                    failures = 0
                    if time.monotonic() - last_publish >= 1:
                        publish_status(sampler, budget, status, None)
                        last_publish = time.monotonic()
                except (SourceError, aiohttp.ClientError, OSError, ValueError) as error:
                    sampler.invalidate()
                    log.error('%s capture paused: %s', market, error)
                    publish_status(sampler, budget, status, failure_code(error))
                    for stream_id in tuple(tasks):
                        await retire(stream_id)
                    active, standby = None, None
                    failures += 1
                    backoff_until = time.monotonic() + random.uniform(
                        1, min(60, 2 ** min(failures, 6))
                    )
        finally:
            sampler.invalidate()
            for stream_id in tuple(tasks):
                await retire(stream_id)
            publish_status(sampler, budget, status, 'BOOK_CAPTURE_STOPPED')


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Capture Binance BTCUSDT books without REST polling.'
    )
    parser.add_argument('--market', choices=('spot', 'perp'), required=True)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    market = cast(Market, args.market)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    directory = heartbeat_directory()
    if args.check:
        return check_capture(directory, market, datetime.now(UTC))
    root = Path(os.environ.get('ORIGO_BOOK_SPOOL_ROOT', '/var/lib/origo-book-spool'))
    lock_root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute() or not lock_root.is_absolute():
        raise ValueError('Capture requires absolute persistent spool and shared lock mounts.')
    token = os.environ['ORIGO_BOOK_TOP20_TOKEN'] if market == 'spot' else ''
    heartbeat = heartbeat_path(directory, f'book_capture_{market}')
    touch_heartbeat(heartbeat)
    start_watchdog(heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS)

    async def serve() -> None:
        import signal

        stopping = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stopping.set)
        sampler = BookSampler(root, market)
        budget = AttemptBudget(lock_root, market)
        runner: web.AppRunner | None = None
        if market == 'spot':
            runner = web.AppRunner(top20_app(sampler, token), access_log=None)
            await runner.setup()
            await web.TCPSite(runner, '0.0.0.0', 8088).start()
        try:
            await run_capture(sampler, budget, directory, stopping=stopping)
        finally:
            if runner is not None:
                await runner.cleanup()

    with source_lock(lock_root, f'book_capture_{market}', 'owner'):
        asyncio.run(serve())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
