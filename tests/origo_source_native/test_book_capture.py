from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import requests
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from origo.sources.adapters import binance_daily
from origo.sources.adapters.book_local import LocalBookCanonical, LocalBookProvisional
from origo.sources.adapters.book_spool import (
    Market,
    object_mapping,
    payload_rows,
    read_payload,
    sealed_minutes,
)
from origo.sources.contracts import SourceError
from origo.workers import book_capture


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_resync_attempts_are_bounded_and_exhaustion_leaves_a_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
) -> None:
    now = [1000.0]
    attempts: list[tuple[str, int, str | None]] = []

    def unavailable(
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
        weight: int = 0,
        egress_ip: str | None = None,
    ) -> binance_daily.Response:
        attempts.append((url, weight, egress_ip))
        assert params == {'symbol': 'BTCUSDT', 'limit': book_capture.BOOK_SEED_DEPTH[market]}
        raise SourceError('PROVIDER_TRANSPORT_FAILED', 'Controlled unavailable transport.')

    monkeypatch.setattr(book_capture, 'get_response', unavailable)
    sampler = book_capture.BookSampler(tmp_path / 'spool', market)
    budget = book_capture.AttemptBudget(tmp_path / 'locks', market, clock=lambda: now[0])
    for _ in range(3):
        with pytest.raises(SourceError, match='Controlled unavailable'):
            budget.seed()
        now[0] += 1
    replacement = book_capture.AttemptBudget(tmp_path / 'locks', market, clock=lambda: now[0])
    with pytest.raises(SourceError) as limited:
        replacement.seed()
    assert limited.value.code == 'BOOK_ATTEMPT_LIMIT'
    assert (
        attempts
        == [(book_capture.SEED_URL[market], book_capture.BOOK_SEED_WEIGHT[market], None)] * 3
    )
    assert replacement.evidence()['seed_total'] == 3
    assert replacement.evidence()['seed_weight'] == 3 * book_capture.BOOK_SEED_WEIGHT[market]
    assert sampler.book is None and sampler.last_seal is None
    assert not (tmp_path / 'spool').exists()
    now[0] = 4600
    with pytest.raises(SourceError, match='Controlled unavailable'):
        replacement.seed()
    assert len(attempts) == 4
    for _ in range(5):
        replacement.connect()
    with pytest.raises(SourceError) as reconnect_limit:
        replacement.connect()
    assert reconnect_limit.value.code == 'BOOK_ATTEMPT_LIMIT'
    now[0] += 300
    replacement.connect()
    assert replacement.evidence()['connection_total'] == 6


@pytest.mark.parametrize('market', ['spot', 'perp'])
@pytest.mark.parametrize('status', [429, 418])
def test_capture_shares_existing_budget_and_preserves_provider_cooldown_across_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
    status: int,
) -> None:
    now = [1000.0]
    delays: list[float] = []
    calls: list[tuple[str, float]] = []

    def sleep(seconds: float) -> None:
        delays.append(seconds)
        now[0] += seconds

    def throttled(
        url: str,
        params: Mapping[str, str | int] | None,
        headers: Mapping[str, str] | None,
        egress_ip: str | None = None,
    ) -> requests.Response:
        assert egress_ip is None
        calls.append((url, now[0]))
        response = requests.Response()
        response.status_code = status
        response.headers['Retry-After'] = '120'
        response.headers['X-MBX-USED-WEIGHT-1M'] = '0'
        return response

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.setattr(binance_daily, 'time', SimpleNamespace(time=lambda: now[0], sleep=sleep))
    monkeypatch.setattr(binance_daily, '_request', throttled)
    budget = book_capture.AttemptBudget(tmp_path, market, clock=lambda: now[0])
    with pytest.raises(SourceError) as first:
        budget.seed()
    assert first.value.code == f'PROVIDER_HTTP_{status}'
    family = 'api_binance_com' if market == 'spot' else 'fapi_binance_com'
    ledger = tmp_path / f'binance_rest_budget.{family}.state'
    next_request, circuit = map(float, ledger.read_text().split())
    assert next_request == 1120 and circuit == (1120 if status == 418 else 0)
    existing_caller = (
        'https://api1.binance.com/api/v3/aggTrades'
        if market == 'spot'
        else 'https://fapi.binance.com/fapi/v1/aggTrades'
    )
    with pytest.raises(SourceError) as existing:
        binance_daily.get_response(existing_caller, weight=4)
    if status == 418:
        assert existing.value.code == 'PROVIDER_RATE_CIRCUIT'
        assert len(calls) == 1
    else:
        assert existing.value.code == 'PROVIDER_HTTP_429'
        assert calls[1][1] == 1120 and 120 in delays
    replacement = book_capture.AttemptBudget(tmp_path, market, clock=lambda: now[0])
    with pytest.raises(SourceError):
        replacement.seed()
    if status == 418:
        assert len(calls) == 1
    else:
        assert calls[-1][1] == 1240
    assert replacement.evidence()['seed_total'] == 2
    assert tuple(tmp_path.glob('binance_rest_budget.*.state')) == (ledger,)


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / 'fixtures/binance'


def recording_root(market: Market) -> Path:
    return FIXTURE_ROOT / ('spot' if market == 'spot' else 'futures') / 'books'


def recorded_packets(
    market: Market, *, latest: bool = True
) -> tuple[tuple[int, datetime, bytes], ...]:
    root = recording_root(market)
    seed_index = 2 if market == 'spot' and latest else 1
    evidence = object_mapping(
        json.loads((root / f'snapshot-{seed_index:02d}.evidence.json').read_bytes())
    )
    start = datetime.fromisoformat(str(evidence['started_at'])) - timedelta(seconds=2)
    packets: list[tuple[int, datetime, bytes]] = []
    for path in sorted((root / 'raw').rglob('*.jsonl.gz')):
        with gzip.open(path, 'rb') as stream:
            for line in stream:
                value = object_mapping(json.loads(line))
                received = datetime.fromisoformat(str(value['received_at']))
                if received >= start:
                    packets.append(
                        (int(str(value['stream_id'])), received, str(value['raw']).encode())
                    )
    return tuple(sorted(packets, key=lambda packet: packet[1]))


def recorded_events(market: Market) -> Iterator[tuple[datetime, book_capture.DepthEvent]]:
    for stream, received, raw in recorded_packets(market):
        if stream != 99:
            yield received, book_capture.DepthEvent.parse(raw, market)


def seed_payload(market: Market) -> bytes:
    return (
        recording_root(market) / ('snapshot-02.json' if market == 'spot' else 'snapshot-01.json')
    ).read_bytes()


def replay(root: Path, market: Market) -> book_capture.BookSampler:
    sampler = book_capture.BookSampler(root, market)
    sampler.seed(seed_payload(market))
    for received, event in recorded_events(market):
        sampler.accept(event, received_at=received)
    return sampler


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_recorded_diff_streams_match_independent_books_for_spot_and_perp(
    tmp_path: Path,
    market: Market,
) -> None:
    root = recording_root(market)
    manifest = object_mapping(json.loads((root / 'provenance.json').read_bytes()))
    for relative, expected in object_mapping(manifest['sha256']).items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
    sampler = book_capture.BookSampler(tmp_path, market)
    sampler.seed(seed_payload(market))
    shadow = book_capture.DiffBook(market, (root / 'snapshot-02.json').read_bytes())
    matched_rest = False
    partials: dict[int, tuple[tuple[book_capture.Level, ...], tuple[book_capture.Level, ...]]] = {}
    states: dict[int, tuple[tuple[book_capture.Level, ...], tuple[book_capture.Level, ...]]] = {}
    for stream, received, raw in recorded_packets(market):
        if stream == 99:
            partial = object_mapping(json.loads(raw))
            partials[int(str(partial['lastUpdateId']))] = (
                tuple(
                    (Decimal(str(pair[0])), Decimal(str(pair[1])))
                    for pair in cast(list[list[str]], partial['bids'])
                ),
                tuple(
                    (Decimal(str(pair[0])), Decimal(str(pair[1])))
                    for pair in cast(list[list[str]], partial['asks'])
                ),
            )
            continue
        event = book_capture.DepthEvent.parse(raw, market)
        sampler.accept(event, received_at=received)
        book = sampler.book
        assert book is not None
        if book.verified:
            bids, asks = book.top(200)
            assert book.top(20) == (bids[:20], asks[:20])
            states[book.last] = book.top(20)
        if market == 'perp' and not matched_rest and not shadow.obsolete(event):
            shadow.apply(event)
            if shadow.verified and book.verified and shadow.last == book.last:
                assert shadow.top(200) == book.top(200)
                matched_rest = True
    if market == 'perp':
        assert matched_rest
    else:
        overlap = states.keys() & partials.keys()
        assert len(overlap) >= 1000
        assert all(states[last] == partials[last] for last in overlap)


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_minute_seals_on_event_time_and_a_gap_or_resync_leaves_it_missing(
    tmp_path: Path,
    market: Market,
) -> None:
    sampler = replay(tmp_path / 'complete', market)
    assert sampler.last_seal is not None
    start = sampler.last_seal - timedelta(minutes=1)
    minute = sealed_minutes(sampler.root, market, start, sampler.last_seal)[0]
    rows = tuple(payload_rows(read_payload(sampler.root, minute), minute))
    assert len(rows) == 660 and sum(row[0] == 20 for row in rows) == 600
    assert all(int(str(row[2])) <= int(cast(datetime, row[1]).timestamp() * 1000) for row in rows)
    events = tuple(recorded_events(market))
    broken = book_capture.BookSampler(tmp_path / 'gap', market)
    broken.seed(seed_payload(market))
    verified = 0
    skipped = False
    for received, event in events:
        assert broken.book is not None
        if broken.book.verified and not skipped and verified > 100:
            skipped = True
            continue
        try:
            applied = broken.accept(event, received_at=received)
        except SourceError as error:
            assert error.code == 'BOOK_SEQUENCE_GAP'
            break
        verified += applied
    else:
        pytest.fail('Dropping a real update must expose sequence loss.')
    assert skipped and broken.book is None
    assert not tuple((broken.root / market).rglob('*.seal.json'))
    # Resynchronization starts another partial minute; a REST seed alone has no event clock.
    broken.seed(seed_payload(market))
    assert broken.book is not None and broken.book.event_ms is None
    assert broken.next_grid is None and broken.counts == {20: 0, 200: 0}


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_connection_handover_before_the_24_hour_limit_loses_no_minute(
    tmp_path: Path,
    market: Market,
) -> None:
    root = recording_root(market)
    handover = object_mapping(json.loads((root / 'handover-02.json').read_bytes()))
    switch_ms = int(str(handover['event_ms']))
    sampler = replay(tmp_path, market)
    start = book_capture.utc_millisecond(switch_ms).replace(second=0, microsecond=0)
    assert sealed_minutes(tmp_path, market, start, start + timedelta(minutes=1))
    assert sampler.book is not None and sampler.book.verified
    assert book_capture.BOOK_ROTATION_SECONDS < 24 * 3600


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_sampling_readers_local_replay_and_verified_handover_make_no_rest_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> binance_daily.Response:
        pytest.fail('A local reader/replay/handover must not call Binance REST.')

    monkeypatch.setattr(book_capture, 'get_response', forbidden)
    sampler = replay(tmp_path, market)
    assert sampler.last_received is not None and sampler.last_seal is not None
    if market == 'spot':
        for _ in range(100):
            assert sampler.top20(sampler.last_received)['t']
    monkeypatch.setenv('ORIGO_BOOK_SPOOL_ROOT', str(tmp_path))
    key = (sampler.last_seal - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%MZ')
    provisional = LocalBookProvisional(market)
    assert len(tuple(provisional.fetch(provisional.partition(key)).rows())) == 660
    with pytest.raises(SourceError) as missing:
        LocalBookCanonical(market).fetch(LocalBookCanonical(market).partition(key[:10]))
    assert missing.value.code == 'BOOK_MINUTES_MISSING'


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_seeded_known_depth_exhaustion_stops_samples_and_uses_bounded_resync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
) -> None:
    # Withhold the deeper recorded seed levels; do not invent price/quantity updates.
    seed = dict(object_mapping(json.loads(seed_payload(market))))
    for side in ('bids', 'asks'):
        seed[side] = cast(list[object], seed[side])[:200]
    sampler = book_capture.BookSampler(tmp_path / 'spool', market)
    sampler.seed(json.dumps(seed).encode())
    for received, event in recorded_events(market):
        try:
            sampler.accept(event, received_at=received)
        except SourceError as error:
            assert error.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
            break
    else:
        pytest.fail('Recorded movement must exhaust the withheld seeded region.')
    assert sampler.book is None and sampler.last_seal is None
    calls: list[str] = []

    def offline(url: str, **kwargs: object) -> binance_daily.Response:
        calls.append(url)
        return binance_daily.Response(seed_payload(market), {}, 200)

    monkeypatch.setattr(book_capture, 'get_response', offline)
    budget = book_capture.AttemptBudget(tmp_path / 'locks', market)
    for _ in range(3):
        sampler.seed(budget.seed().body)
    with pytest.raises(SourceError) as limited:
        budget.seed()
    assert limited.value.code == 'BOOK_ATTEMPT_LIMIT' and len(calls) == 3


def test_top20_serves_the_binsim_payload_and_rejects_a_missing_token(tmp_path: Path) -> None:
    sampler = replay(tmp_path, 'spot')
    assert sampler.last_received is not None
    now = sampler.last_received

    async def verify() -> None:
        async with TestClient(
            TestServer(book_capture.top20_app(sampler, 'fixture-token', clock=lambda: now))
        ) as client:
            assert (await client.get('/top20')).status == 401
            assert (
                await client.get('/top20', headers={'Authorization': 'Bearer wrong'})
            ).status == 401
            for _ in range(10):
                response = await client.get(
                    '/top20', headers={'Authorization': 'Bearer fixture-token'}
                )
                assert response.status == 200
                payload: object = await response.json()
                assert object_mapping(payload) == sampler.top20(now)
                assert response.headers['Cache-Control'] == 'no-store'
            sampler.invalidate()
            assert (
                await client.get('/top20', headers={'Authorization': 'Bearer fixture-token'})
            ).status == 503

    asyncio.run(verify())


def test_failed_rotation_keeps_verified_book_without_a_rest_seed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sampler = book_capture.BookSampler(tmp_path / 'spool', 'spot')
    sampler.seed(seed_payload('spot'))
    events = iter(recorded_events('spot'))
    for _ in range(100):
        received, event = next(events)
        sampler.accept(event, received_at=received)
    assert sampler.book is not None and sampler.book.verified
    budget = book_capture.AttemptBudget(tmp_path / 'locks', 'spot')
    connections: list[int] = []

    def connect() -> None:
        connections.append(1)
        if len(connections) > 1:
            raise SourceError('BOOK_ATTEMPT_LIMIT', 'Controlled failed overlap admission.')

    def forbidden(*args: object, **kwargs: object) -> binance_daily.Response:
        pytest.fail('Failed rotation must retain the initialized book without reseeding.')

    monkeypatch.setattr(budget, 'connect', connect)
    monkeypatch.setattr(book_capture, 'get_response', forbidden)
    monkeypatch.setattr(book_capture, 'BOOK_ROTATION_SECONDS', 0)

    async def verify() -> None:
        stopping = asyncio.Event()

        async def receive(
            session: object,
            market: Market,
            stream: int,
            queue: asyncio.Queue[book_capture.StreamPacket],
        ) -> None:
            for _ in range(10):
                stamp, frame = next(events)
                await queue.put(book_capture.StreamPacket(stream, stamp, frame))
                await asyncio.sleep(0.001)
            assert sampler.book is not None and sampler.book.verified
            stopping.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(book_capture, '_receive', receive)
        await asyncio.wait_for(
            book_capture.run_capture(sampler, budget, tmp_path / 'heartbeats', stopping=stopping),
            timeout=2,
        )

    asyncio.run(verify())
    assert len(connections) == 2 and budget.evidence()['seed_total'] == 0


def test_capture_shutdown_with_real_frames_does_not_hang(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packets = [
        (stamp, raw, book_capture.DepthEvent.parse(raw, 'spot'))
        for stream, stamp, raw in recorded_packets('spot')
        if stream != 99
    ][:100]
    stamps = {event.last: stamp for stamp, _, event in packets}
    sampler = book_capture.BookSampler(tmp_path / 'spool', 'spot')
    accept = sampler.accept

    def recorded_clock(event: book_capture.DepthEvent, *, received_at: datetime) -> bool:
        # Transport runs locally now; market and receive timestamps stay those recorded.
        return accept(event, received_at=stamps[event.last])

    monkeypatch.setattr(sampler, 'accept', recorded_clock)
    seeds: list[str] = []

    def seed(url: str, **kwargs: object) -> binance_daily.Response:
        seeds.append(url)
        return binance_daily.Response(seed_payload('spot'), {}, 200)

    monkeypatch.setattr(book_capture, 'get_response', seed)
    monkeypatch.setattr(book_capture, 'BOOK_ROTATION_SECONDS', 0.1)

    async def verify() -> None:
        sockets: set[web.WebSocketResponse] = set()
        stopping = asyncio.Event()

        async def handler(request: web.Request) -> web.WebSocketResponse:
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            sockets.add(socket)
            try:
                async for _ in socket:
                    raise RuntimeError('The book subscription sends no application messages.')
            finally:
                sockets.remove(socket)
            return socket

        async def send() -> None:
            while not sockets:
                await asyncio.sleep(0.001)
            for _, raw, _ in packets:
                for socket in tuple(sockets):
                    if not socket.closed:
                        await socket.send_str(raw.decode())
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.05)
            assert sampler.book is not None and sampler.book.verified
            stopping.set()

        app = web.Application()
        app.router.add_get('/ws', handler)
        async with TestServer(app) as server:
            monkeypatch.setitem(
                book_capture.STREAM_URL,
                'spot',
                str(server.make_url('/ws')).replace('http:', 'ws:', 1),
            )
            sender = asyncio.create_task(send())
            budget = book_capture.AttemptBudget(tmp_path / 'locks', 'spot')
            await asyncio.wait_for(
                book_capture.run_capture(
                    sampler, budget, tmp_path / 'heartbeats', stopping=stopping
                ),
                timeout=4,
            )
            await sender
            assert 2 <= int(str(budget.evidence()['connection_total'])) <= 5

    asyncio.run(verify())
    assert len(seeds) == 1


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_provisional_admission_waits_for_actual_seals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    sampler = replay(tmp_path / 'spool', market)
    assert sampler.last_seal is not None
    day = sampler.last_seal.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes = sealed_minutes(sampler.root, market, day, day + timedelta(days=1))
    assert len(minutes) == 2
    adapter = LocalBookProvisional(market)
    monkeypatch.setenv('ORIGO_BOOK_SPOOL_ROOT', str(sampler.root))
    keys = tuple(datetime.fromisoformat(m['minute_start']).strftime('%Y-%m-%dT%H:%MZ') for m in minutes)
    # The just-closed unsealed minute and every earlier capture gap stay ineligible.
    for now in (sampler.last_seal + timedelta(minutes=1), sampler.last_seal + timedelta(hours=37)):
        assert tuple(p.key for p in adapter.candidates(now, day, ())) == keys
        covered = (adapter.partition(keys[0]),)
        assert tuple(p.key for p in adapter.candidates(now, day, covered)) == keys[1:]
    assert adapter.candidates(day + timedelta(days=1), day + timedelta(days=1), ()) == ()


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_spool_status_and_sealing_do_not_walk_retained_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    from origo.sources.adapters.book_spool import (
        reconcile_spool_bytes, remove_spool_payloads, seal_minute, spool_bytes,
    )
    from origo.sources.locking import source_lock

    sampler = replay(tmp_path / 'recorded', market)
    assert sampler.last_seal is not None
    day = sampler.last_seal.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes = sealed_minutes(sampler.root, market, day, day + timedelta(days=1))
    payloads = tuple(read_payload(sampler.root, minute) for minute in minutes)
    root = tmp_path / 'capture'
    assert reconcile_spool_bytes(root, market) == 0

    def forbidden_walk(*args: object, **kwargs: object) -> Iterator[Path]:
        pytest.fail('Capture status and sealing must not walk retained history.')

    monkeypatch.setattr(Path, 'rglob', forbidden_walk)
    directory = root / market / day.strftime('%Y-%m-%d')
    for minute, payload in zip(minutes, payloads, strict=True):
        seal_minute(root, minute, payload)
        expected = sum(path.stat().st_size for path in directory.iterdir())
        assert spool_bytes(root, market) == expected
        seal_minute(root, minute, payload)
        assert spool_bytes(root, market) == expected
    with source_lock(root, f'book_spool_{market}', 'sealed'):
        remove_spool_payloads(root, market, directory)
    assert spool_bytes(root, market) == sum(path.stat().st_size for path in directory.iterdir())


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_spool_startup_repairs_interrupted_reservations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    from origo.sources.adapters import book_spool

    sampler = replay(tmp_path / 'recorded', market)
    assert sampler.last_seal is not None
    start = sampler.last_seal - timedelta(minutes=1)
    minute = sealed_minutes(sampler.root, market, start, sampler.last_seal)[0]
    payload = read_payload(sampler.root, minute)
    root = tmp_path / 'interrupted'
    book_spool.reconcile_spool_bytes(root, market)
    atomic = book_spool.atomic_write

    def interrupted(path: Path, body: bytes) -> None:
        if path.suffix == '.gz':
            raise OSError('Injected interruption after durable byte reservation.')
        atomic(path, body)

    with monkeypatch.context() as fault:
        fault.setattr(book_spool, 'atomic_write', interrupted)
        with pytest.raises(OSError, match='Injected interruption'):
            book_spool.seal_minute(root, minute, payload)
    assert book_spool.spool_bytes(root, market) > 0
    assert book_spool.reconcile_spool_bytes(root, market) == 0
    book_spool.seal_minute(root, minute, payload)
    before = book_spool.spool_bytes(root, market)
    assert book_spool.reconcile_spool_bytes(root, market) == before
    assert book_spool.read_payload(root, minute) == payload


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_spool_limit_preserves_unacknowledged_real_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    from origo.sources.adapters import book_spool

    sampler = replay(tmp_path / 'recorded', market)
    assert sampler.last_seal is not None
    day = sampler.last_seal.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes = sealed_minutes(sampler.root, market, day, day + timedelta(days=1))
    assert len(minutes) == 2
    payloads = tuple(read_payload(sampler.root, minute) for minute in minutes)
    root = tmp_path / 'bounded'
    book_spool.seal_minute(root, minutes[0], payloads[0])
    used = book_spool.spool_bytes(root, market)
    monkeypatch.setattr(book_spool, 'BOOK_SPOOL_MAX_BYTES', used)
    with pytest.raises(SourceError) as full:
        book_spool.seal_minute(root, minutes[1], payloads[1])
    assert full.value.code == 'BOOK_SPOOL_FULL'
    assert book_spool.spool_bytes(root, market) == used
    assert book_spool.read_payload(root, minutes[0]) == payloads[0]
    assert book_spool.sealed_minutes(root, market, day, day + timedelta(days=1)) == minutes[:1]


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_sampling_rejects_a_stale_recorded_book_before_sealing(
    tmp_path: Path, market: Market,
) -> None:
    class QuietSampler(book_capture.BookSampler):
        def advance_clock(self, event_ms: int) -> None:
            self._sample_until(event_ms)

    sampler = QuietSampler(tmp_path, market)
    sampler.seed(seed_payload(market))
    events = tuple(recorded_events(market))
    for received, event in events:
        if sampler.accept(event, received_at=received):
            break
    assert sampler.book is not None and sampler.book.event_ms is not None
    # Withhold subsequent real updates; the later recorded clock cannot certify a quiet grid.
    later_event = events[-1][1]
    assert later_event.event_ms - sampler.book.event_ms > 5000
    with pytest.raises(SourceError) as stale:
        sampler.advance_clock(later_event.event_ms)
    assert stale.value.code == 'BOOK_EVENT_STALE'
    assert sampler.book is None and sampler.lines == [] and sampler.last_seal is None
    assert not tuple((tmp_path / market).rglob('*.seal.json'))


@pytest.mark.parametrize('market', ['spot', 'perp'])
@pytest.mark.parametrize('lag_updates', [0, 3])
def test_later_standby_handover_preserves_real_minutes_without_reseeding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market, lag_updates: int,
) -> None:
    expected = replay(tmp_path / 'expected', market)
    sampler = book_capture.BookSampler(tmp_path / 'capture', market)
    sampler.seed(seed_payload(market))
    events = tuple(recorded_events(market))
    for stamp, event in events[:100]:
        sampler.accept(event, received_at=stamp)
    assert sampler.book is not None and sampler.book.verified
    remaining = [(stamp, event) for stamp, event in events[100:] if event.last > sampler.book.last]
    budget = book_capture.AttemptBudget(tmp_path / 'locks', market)
    monkeypatch.setattr(book_capture, 'BOOK_ROTATION_SECONDS', 0)

    def forbidden(*args: object, **kwargs: object) -> binance_daily.Response:
        pytest.fail('Delayed standby must hand over without a REST seed.')

    monkeypatch.setattr(book_capture, 'get_response', forbidden)

    async def verify() -> None:
        stopping = asyncio.Event()
        retired = asyncio.Event()
        receivers: dict[int, asyncio.Queue[book_capture.StreamPacket]] = {}

        async def receive(
            session: object, market: Market, stream: int,
            queue: asyncio.Queue[book_capture.StreamPacket],
        ) -> None:
            receivers[stream] = queue
            if stream == 2:
                # Advance only the operating rotation interval, never the market clocks.
                monkeypatch.setattr(book_capture, 'BOOK_ROTATION_SECONDS', 23 * 3600)
            try:
                await asyncio.Event().wait()
            finally:
                if stream == 1:
                    retired.set()

        async def applied(last: int) -> None:
            while sampler.book is None or sampler.book.last < last:
                await asyncio.sleep(0)

        async def send() -> None:
            while len(receivers) < 2:
                await asyncio.sleep(0)
            # A real frame from before overlap cannot justify retiring the active stream.
            stamp, old = events[99]
            await receivers[2].put(book_capture.StreamPacket(2, stamp, old))
            await asyncio.sleep(0.01)
            assert not retired.is_set()
            # The active stream always leads; standby arrives one or several updates later.
            for stamp, event in remaining[:lag_updates + 1]:
                await receivers[1].put(book_capture.StreamPacket(1, stamp, event))
                await applied(event.last)
            stamp, duplicate = remaining[0]
            await receivers[2].put(book_capture.StreamPacket(2, stamp, duplicate))
            await asyncio.wait_for(retired.wait(), timeout=1)
            assert sampler.book is not None
            assert sampler.book.last == remaining[lag_updates][1].last
            for stamp, event in remaining[1:]:
                await receivers[2].put(book_capture.StreamPacket(2, stamp, event))
                await applied(event.last)
            assert sampler.book is not None and sampler.book.verified
            assert expected.book is not None
            assert sampler.book.top(200) == expected.book.top(200)
            assert sampler.last_seal == expected.last_seal
            stopping.set()

        monkeypatch.setattr(book_capture, '_receive', receive)
        sender = asyncio.create_task(send())
        try:
            await asyncio.wait_for(
                book_capture.run_capture(sampler, budget, tmp_path / 'heartbeats', stopping=stopping),
                timeout=10,
            )
            await sender
        finally:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    asyncio.run(verify())
    assert budget.evidence()['seed_total'] == 0
    assert budget.evidence()['connection_total'] == 2
    assert expected.last_seal is not None
    day = expected.last_seal.replace(hour=0, minute=0, second=0, microsecond=0)
    expected_minutes = sealed_minutes(expected.root, market, day, day + timedelta(days=1))
    actual_minutes = sealed_minutes(sampler.root, market, day, day + timedelta(days=1))
    assert actual_minutes == expected_minutes
    assert all(read_payload(sampler.root, minute) == read_payload(expected.root, minute)
               for minute in expected_minutes)


@pytest.mark.parametrize('market', ['spot', 'perp'])
@pytest.mark.parametrize('field', [0, 1])
def test_malformed_decimal_frame_reports_failure_and_reconnects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market, field: int,
) -> None:
    packets = [(stamp, raw, book_capture.DepthEvent.parse(raw, market))
               for stream, stamp, raw in recorded_packets(market) if stream != 99][:150]
    stamps = {event.last: stamp for stamp, _, event in packets}
    invalid = dict(object_mapping(json.loads(packets[0][1])))
    levels = cast(list[list[str]], invalid['b'])
    # Protocol corruption is rejected; no invented market value can enter the book.
    levels[0][field] = 'not-a-decimal'
    sampler = book_capture.BookSampler(tmp_path / 'spool', market)
    seeds: list[str] = []

    def seed(url: str, **kwargs: object) -> binance_daily.Response:
        seeds.append(url)
        return binance_daily.Response(seed_payload(market), {}, 200)

    monkeypatch.setattr(book_capture, 'get_response', seed)
    def immediate_retry(lower: float, upper: float) -> float:
        return 0.0

    monkeypatch.setattr(book_capture.random, 'uniform', immediate_retry)

    async def verify() -> None:
        stopping = asyncio.Event()
        connections: list[int] = []
        accept = sampler.accept

        def recorded_clock(event: book_capture.DepthEvent, *, received_at: datetime) -> bool:
            applied = accept(event, received_at=stamps[event.last])
            if applied:
                assert len(connections) == 2 and sampler.book is not None and sampler.book.verified
                stopping.set()
            return applied

        async def handler(request: web.Request) -> web.WebSocketResponse:
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            connections.append(1)
            if len(connections) == 1:
                await socket.send_str(json.dumps(invalid))
            else:
                for _, raw, _ in packets:
                    await socket.send_str(raw.decode())
            async for _ in socket:
                raise RuntimeError('Capture sends no application messages.')
            return socket

        monkeypatch.setattr(sampler, 'accept', recorded_clock)
        app = web.Application()
        app.router.add_get('/ws', handler)
        async with TestServer(app) as server:
            monkeypatch.setitem(book_capture.STREAM_URL, market,
                                str(server.make_url('/ws')).replace('http:', 'ws:', 1))
            budget = book_capture.AttemptBudget(tmp_path / 'locks', market)
            await asyncio.wait_for(
                book_capture.run_capture(sampler, budget, tmp_path / 'heartbeats', stopping=stopping),
                timeout=3,
            )
            assert budget.evidence()['connection_total'] == 2
            assert budget.evidence()['seed_total'] == 1

    asyncio.run(verify())
    assert seeds == [book_capture.SEED_URL[market]]
