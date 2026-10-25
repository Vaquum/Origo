from __future__ import annotations

import hashlib
import json
import tarfile
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import cast
from uuid import uuid4

import polars as pl
import pyarrow as pa
import pytest
from dagster import DagsterInstance, Definitions, RunRequest, build_sensor_context

from origo import law
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.law_catalog import build_catalog
from origo.sources import capacity, dagit
from origo.sources.adapters import render_book
from origo.sources.adapters.render_book import (
    Market,
    RenderBookCache,
    RenderBookCanonical,
    RenderBookEndpoint,
    RenderBookProvisional,
)
from origo.sources.binance_top20 import top20_source
from origo.sources.bundle import SourceRunConfig, build_source_bundle
from origo.sources.contracts import (
    RevisionedSourceSpec,
    RolloutStage,
    SourceError,
)
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles import top20_arrow
from origo.sources.storage import SourceStore
from origo.workers import provisional as worker
from origo.workers.receipts import ensure_monitoring_tables

from .test_provisional_worker import _Dagster, _Reporter
from .test_provisional_worker import _feed as worker_feed

FIXTURE = Path(__file__).parents[1] / 'fixtures' / 'render_book'
HOUR = '2026-10-24T13Z'
START = datetime(2026, 10, 24, 13, tzinfo=UTC)
MINUTE = '2026-10-24T13:21:00Z'


@dataclass
class RecordedFeed:
    replica: str
    market: Market
    entries: tuple[dict[str, object], ...]
    payloads: Mapping[int, bytes]
    omitted: set[int] = field(default_factory=set)
    calls: list[str] = field(default_factory=list)

    def read(self, path: str, limit: int) -> bytes:
        self.calls.append(path)
        if '?after=' in path or '?tail=' in path:
            tail = '?tail=' in path
            after = 0 if tail else int(path.rsplit('=', 1)[1])
            entries = [
                m
                for m in self.entries
                if int(str(m['id'])) > after and int(str(m['id'])) not in self.omitted
            ][:100]
            if tail:
                entries = entries[-2:]
            payload = json.dumps(
                {
                    'collector': self.replica,
                    'market': self.market,
                    'segments': entries,
                    'pruned_through': 0,
                    'has_more': False,
                    'next_cursor': entries[-1]['id'] if entries else after,
                    'tail': tail,
                }
            ).encode()
        else:
            payload = self.payloads[int(path.rsplit('/', 1)[1])]
        assert len(payload) <= limit
        return payload


@pytest.fixture(scope='module')
def recordings() -> dict[tuple[str, Market], RecordedFeed]:
    provenance = json.loads((FIXTURE / 'provenance.json').read_text())
    result: dict[tuple[str, Market], RecordedFeed] = {}
    for file in provenance['files']:
        path = FIXTURE / file['file']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == file['sha256']
        with tarfile.open(path) as archive:
            contents: dict[str, bytes] = {}
            for member in archive.getmembers():
                stream = archive.extractfile(member)
                assert stream is not None
                contents[member.name] = stream.read()
        entries = tuple(
            json.loads(contents[name]) for name in sorted(contents) if name.endswith('.json')
        )
        payloads = {
            int(name.removesuffix('.ndjson')): value
            for name, value in contents.items()
            if name.endswith('.ndjson')
        }
        key = file['replica'], file['market']
        result[key] = RecordedFeed(*key, entries, payloads)
    return result


def _feed(
    recordings: dict[tuple[str, Market], RecordedFeed], replica: str, market: Market
) -> RecordedFeed:
    source = recordings[replica, market]
    return RecordedFeed(replica, market, source.entries, source.payloads)


@dataclass
class RecordedServer:
    url: str
    feeds: dict[tuple[str, Market], RecordedFeed]
    offline: set[str] = field(default_factory=set)
    events: list[str] = field(default_factory=list)


@pytest.fixture
def recorded_server(recordings: dict[tuple[str, Market], RecordedFeed]) -> Iterator[RecordedServer]:
    state = RecordedServer('', {key: _feed(recordings, *key) for key in recordings})

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            replica, path = self.path[1:].split('/', 1)
            path = '/' + path
            state.events.append(replica + path)
            if self.headers.get('Authorization') != 'Bearer local-proof-token':
                self.send_error(401)
            elif replica in state.offline:
                self.send_error(503)
            elif path == '/expired':
                self.send_error(410)
            elif path == '/slow':
                time.sleep(0.2)  # Deliberate transport stall; no fabricated observation.
            else:
                market = cast(Market, path.split('/')[2].split('?')[0])
                payload = state.feeds[replica, market].read(path, 64 * 1024**2)
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f'http://127.0.0.1:{server.server_port}'
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _gap(feed: RecordedFeed, minute: int) -> set[int]:
    start = int((START + timedelta(minutes=minute)).timestamp()) * 1_000_000
    return {
        int(str(entry['id']))
        for entry in feed.entries
        if int(str(entry['min_exchange_time'])) < start + 60_000_000
        and int(str(entry['max_exchange_time'])) >= start
    }


def _expected(
    recordings: dict[tuple[str, Market], RecordedFeed], market: Market
) -> dict[int, dict[str, object]]:
    groups: dict[int, list[dict[str, object]]] = {}
    for replica in ('A', 'B'):
        for payload in recordings[replica, market].payloads.values():
            for line in payload.splitlines():
                row = cast(dict[str, object], json.loads(line))
                second = int(str(row['exchange_time'])) // 1_000_000
                if int(START.timestamp()) <= second < int(START.timestamp()) + 3600:
                    groups.setdefault(second, []).append(row)
    return {
        second: max(
            rows,
            key=lambda row: (
                int(str(row['exchange_time'])),
                int(str(row['update_id'])),
                row['collector'] == 'A',
                str(row['session']),
                int(str(row['sequence'])),
            ),
        )
        for second, rows in groups.items()
    }


@pytest.fixture(params=['spot', 'perp'])
def runtime(
    request: pytest.FixtureRequest,
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[SourceRuntime]:
    market = cast(Market, request.param)
    monkeypatch.setenv('ORIGO_RENDER_BOOK_ROOT', str(tmp_path / 'cache'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    declared = top20_source(market)
    spec = replace(
        declared,
        rollout_stage=RolloutStage.CANARY,
        orchestration=replace(declared.orchestration, retry_count=0),
    )
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    client = make_clickhouse_client(get_clickhouse_settings())
    instance = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        instance.setup()
        yield instance
    finally:
        client.disconnect()


def test_hourly_recovery_and_activation(
    runtime: SourceRuntime,
    recordings: dict[tuple[str, Market], RecordedFeed],
    record_property: Callable[[str, object], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A skipped provisional minute is restored by the real hourly build and repair path."""
    canonical = cast(RenderBookCanonical, runtime.spec.canonical)
    cache = canonical.cache
    market = cache.market
    for replica in ('A', 'B'):
        feed = _feed(recordings, replica, market)
        assert cache.sync(replica, feed.read) == 61
        reads = len(feed.calls)
        assert cache.sync(replica, feed.read) == 0
        assert (
            len(feed.calls) == reads + 1
        )  # Inventory only; immutable bodies are not fetched again.

    for minute in (20, 22):
        runtime.build(f'2026-10-24T13:{minute}:00Z', provisional=True)
    raw_view = runtime.store.table(runtime.spec.names.prefix + '_depth20_current')
    metric_view = runtime.store.table(runtime.spec.names.prefix + '_depth20_1m_current')
    assert runtime.store.execute(
        f"SELECT count() FROM {raw_view} WHERE toStartOfMinute(datetime)='2026-10-24 13:21:00'"
    ) == [(0,)]
    activation_history = runtime.store.execute(
        'SELECT partition_key, revision, build_id, generation FROM origo.source_activation_log '
        'WHERE provisional=1 ORDER BY partition_key'
    )
    started = time.monotonic()
    record = runtime.build(HOUR)
    record_property('canonical_build_seconds', time.monotonic() - started)
    assert record.generation == 1
    assert runtime.store.execute(f'SELECT count(), uniqExact(datetime) FROM {raw_view}') == [
        (3600, 3600)
    ]
    assert runtime.store.execute(f'SELECT count() FROM {metric_view}') == [(60,)]
    # Genuine activated rows certify the hourly law; deliberate proof faults do not.
    monkeypatch.setattr(law, 'SOURCE_REGISTRY', (runtime.spec,))
    query = law._Queries(runtime.store.client)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(record.build_id))
    assert len(proofs) == 1 and proofs[0].verdict()['status'] == 'PASS'
    shortened = replace(proofs[0], components=tuple(
        (key, count - 1, digest, completed) if key == 'depth20' else (key, count, digest, completed)
        for key, count, digest, completed in proofs[0].components
    ))
    assert shortened.verdict()['reason'] == 'book_component_grid_incomplete'
    before_due = START + timedelta(hours=1, minutes=4, seconds=59)
    now = START + timedelta(hours=1, minutes=5)
    assert law._c1([], runtime.spec.key, before_due)['status'] == 'NOT_DUE'
    assert law._c1([], runtime.spec.key, now)['reason'] == 'canonical_hour_missing'
    c1 = law._c1(proofs, runtime.spec.key, now)
    assert c1['status'] == 'PASS' and c1['evidence']['canonical_interval'] == 'hour'
    c2 = law._c2(query, 'origo', proofs, runtime.spec.key, now + timedelta(hours=2))
    assert c2['status'] == 'FAIL' and c2['reason'] == 'canonical_hour_missing'
    assert c2['evidence']['expected_hours'] == 2 and c2['evidence']['valid_hours'] == 1
    assert c2['evidence']['first_invalid_hour'] == '2026-10-24T14Z'
    edges = law._edge(query, 'origo', runtime.spec.key, record.partition.end)
    r1 = law._r1(query, 'origo', runtime.spec, record.partition.end, edges, proofs)
    assert r1['status'] == 'PASS'
    assert r1['evidence']['rows_depth20'] == 60 and r1['evidence']['rows_depth20_1m'] == 1
    gates = {gate['id']: gate for gate in build_catalog('0' * 40)['gates']}
    assert gates[f'law.C1:{runtime.spec.key}']['thresholds'] == {'delay_seconds': 300}
    assert 'canonical hour' in gates[f'law.C2:{runtime.spec.key}']['condition']
    assert runtime.store.execute(
        f"SELECT count() FROM {raw_view} WHERE toStartOfMinute(datetime)='2026-10-24 13:21:00'"
    ) == [(60,)]
    actual = [
        json.loads(str(row[0]))
        for row in runtime.store.execute(
            f'SELECT original_snapshot FROM {raw_view} ORDER BY datetime'
        )
    ]
    assert actual == list(_expected(recordings, market).values())
    stored = runtime.store.execute(
        'SELECT exchange_time_us, observed_at_us, last_update_id, collector, session, sequence, '
        f'transaction_time_us, first_update_id, previous_update_id, bids, asks FROM {raw_view} ORDER BY datetime'
    )
    for row, original in zip(stored, actual, strict=True):
        assert row[:9] == tuple(
            original.get(key, 0)
            for key in (
                'exchange_time',
                'observed_at',
                'update_id',
                'collector',
                'session',
                'sequence',
                'transaction_time',
                'first_update_id',
                'previous_update_id',
            )
        )
        assert row[9:] == tuple(
            [(float(price), float(quantity)) for price, quantity in original[side]]
            for side in ('bids', 'asks')
        )
    minute_ends = {int(str(row['exchange_time'])) // 60_000_000: row for row in actual}
    metrics = runtime.store.execute(
        'SELECT source_timestamp_ms, book_mid_price, book_spread_bps, book_bid_depth_20_notional, '
        f'book_ask_depth_20_notional, book_imbalance_20 FROM {metric_view} ORDER BY datetime'
    )
    for row, original in zip(metrics, minute_ends.values(), strict=True):
        bids, asks = original['bids'], original['asks']
        mid = (float(bids[0][0]) + float(asks[0][0])) / 2
        bid_value = sum(float(p) * float(q) for p, q in bids)
        ask_value = sum(float(p) * float(q) for p, q in asks)
        assert row[0] == int(original['exchange_time']) // 1000
        assert row[1:] == pytest.approx(
            (
                mid,
                (float(asks[0][0]) - float(bids[0][0])) / mid * 10000,
                bid_value,
                ask_value,
                (bid_value - ask_value) / (bid_value + ask_value),
            ),
            rel=1e-12,
        )
    started = time.monotonic()
    assert runtime.build(HOUR) == runtime.reconcile(HOUR) == record
    record_property('unchanged_build_and_reconciliation_seconds', time.monotonic() - started)
    assert (
        runtime.store.execute(
            'SELECT partition_key, revision, build_id, generation FROM origo.source_activation_log '
            'WHERE provisional=1 ORDER BY partition_key'
        )
        == activation_history
    )

    table = runtime.store.component_table('depth20')
    runtime.store.execute(
        f"ALTER TABLE {table} DELETE WHERE partition_key='{HOUR}' AND toStartOfMinute(datetime)='2026-10-24 13:21:00' SETTINGS mutations_sync=2"
    )
    with pytest.raises(SourceError) as failure:
        runtime.reconcile(HOUR)
    assert failure.value.code == 'RETAINED_CONTENT_INVALID'
    started = time.monotonic()
    repaired = runtime.repair(HOUR)
    record_property('repair_seconds', time.monotonic() - started)
    assert repaired.generation > record.generation
    assert repaired.component_hashes == record.component_hashes
    assert runtime.store.execute(f'SELECT count(), uniqExact(datetime) FROM {raw_view}') == [
        (3600, 3600)
    ]
    assert [
        json.loads(str(row[0]))
        for row in runtime.store.execute(
            f'SELECT original_snapshot FROM {raw_view} ORDER BY datetime'
        )
    ] == actual
    assert runtime.reconcile(HOUR) == repaired

    # Actual retention verifies the accepted hour and preserves unaccepted overlap.
    before = set(cache.root.glob(f'{market}/*/*/*.ndjson'))
    assert canonical.audit_maintenance(runtime, START + timedelta(days=3), lambda p: p.key) > 0
    remaining = set(cache.root.glob(f'{market}/*/*/*.ndjson'))
    assert remaining and remaining < before
    assert runtime.reconcile(HOUR) == runtime.build(HOUR) == repaired
    runtime.store.execute(
        f"ALTER TABLE {table} DELETE WHERE partition_key='{HOUR}' AND datetime='2026-10-24 13:21:00' SETTINGS mutations_sync=2"
    )
    with pytest.raises(FileNotFoundError):
        runtime.repair(HOUR)


@pytest.mark.parametrize('market', ['spot', 'perp'])
@pytest.mark.parametrize('condition', ['A_gap', 'B_gap', 'complementary', 'shared', 'A_offline'])
def test_replica_gap_conditions(
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
    market: Market,
    condition: str,
) -> None:
    """Only genuine complementary observations can complete a missing source minute."""
    cache = RenderBookCache(tmp_path, market)
    for replica in ('A', 'B'):
        feed = _feed(recordings, replica, market)
        if condition in ('A_gap', 'complementary', 'shared') and replica == 'A':
            feed.omitted = _gap(feed, 21)
        if condition in ('B_gap', 'complementary', 'shared') and replica == 'B':
            feed.omitted = _gap(feed, 21 if condition == 'shared' else 41)
        if condition == 'A_offline' and replica == 'A':

            def unavailable(path: str, limit: int) -> bytes:
                raise OSError('deliberate replica outage')

            with pytest.raises(OSError, match='deliberate replica outage'):
                cache.sync(replica, unavailable)
            assert cache.cursor(replica) == 0
        else:
            cache.sync(replica, feed.read)
    adapter = RenderBookCanonical(cache)
    partition = adapter.partition(HOUR)
    if condition == 'shared':
        with pytest.raises(SourceError) as failure:
            adapter.fetch(partition)
        assert failure.value.code == 'RENDER_SECONDS_MISSING'
        assert not (tmp_path / market / 'hours' / f'{HOUR}.json').exists()
    else:
        revision = adapter.fetch(partition)
        rows = tuple(revision.rows())
        assert len(rows) == len({row[0] for row in rows}) == 3600
        originals = {
            json.dumps(json.loads(line), sort_keys=True)
            for replica in ('A', 'B')
            for payload in recordings[replica, market].payloads.values()
            for line in payload.splitlines()
        }
        for row in rows:
            assert json.dumps(json.loads(str(row[-1])), sort_keys=True) in originals
        evidence = json.loads(revision.evidence_json)
        assert evidence['redundant_seconds'] < 3600
        assert (
            RenderBookCanonical(RenderBookCache(tmp_path, market)).fetch(partition).key
            == revision.key
        )


def test_transport_restart_and_expiry(
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed transfer and lost cursor acknowledgement never skip or refetch committed bytes."""
    cache = RenderBookCache(tmp_path, 'spot')
    feed = _feed(recordings, 'A', 'spot')
    broken = int(str(feed.entries[21]['id']))

    def interrupted(path: str, limit: int) -> bytes:
        if path.endswith(f'/{broken:020}'):
            raise OSError('deliberately unavailable minute')
        return feed.read(path, limit)

    with pytest.raises(OSError, match='deliberately unavailable'):
        cache.sync('A', interrupted)
    assert cache.cursor('A') == int(str(feed.entries[20]['id']))
    restarted = RenderBookCache(tmp_path, 'spot')
    assert restarted.sync('A', feed.read) == 40
    assert restarted.cursor('A') == int(str(feed.entries[-1]['id']))
    bodies = [path for path in feed.calls if '?after=' not in path]
    assert len(bodies) == len(set(bodies)) == 61

    def expired(path: str, limit: int) -> bytes:
        return json.dumps(
            {'collector': 'A', 'market': 'spot', 'pruned_through': cache.cursor('A') + 1}
        ).encode()

    with pytest.raises(SourceError) as failure:
        restarted.sync('A', expired)
    assert failure.value.code == 'RENDER_EXPIRED'
    retained = RenderBookCanonical(restarted)
    assert retained.fetch(retained.partition(HOUR)).row_count == 3600

    other = RenderBookCache(tmp_path / 'ack-loss', 'spot')
    first = _feed(recordings, 'A', 'spot')
    real_write = render_book.atomic_write

    def lose_ack(path: Path, payload: bytes) -> None:
        if path.name == 'cursor.json':
            raise OSError('deliberate crash before cursor acknowledgement')
        real_write(path, payload)

    with monkeypatch.context() as fault:
        fault.setattr(render_book, 'atomic_write', lose_ack)
        with pytest.raises(OSError, match='deliberate crash'):
            other.sync('A', first.read)
    assert other.cursor('A') == 0
    assert RenderBookCache(other.root, 'spot').sync('A', first.read) == 61
    assert len([path for path in first.calls if '?after=' not in path]) == 61

    bounded = RenderBookCache(tmp_path / 'bounded', 'spot')
    replay = _feed(recordings, 'A', 'spot')
    assert bounded.sync('A', replay.read, max_segments=2) == 2
    assert bounded.cursor('A') == int(str(replay.entries[1]['id']))
    assert bounded.sync('A', replay.read, max_segments=2) == 2
    assert bounded.cursor('A') == int(str(replay.entries[3]['id']))
    assert len([path for path in replay.calls if '?after=' not in path]) == 4

    live = RenderBookCache(tmp_path / 'live', 'spot')
    live_feed = _feed(recordings, 'A', 'spot')
    assert live.sync_tail('A', live_feed.read) == (2, 0)
    assert live.cursor('A') == 0
    provisional = RenderBookProvisional(live)
    assert provisional.fetch(provisional.partition('2026-10-24T13:59:00Z')).row_count == 60
    assert live.sync('A', live_feed.read, max_segments=2) == 2
    assert live.cursor('A') == int(str(live_feed.entries[1]['id']))
    assert len([p for p in live_feed.calls if '?' not in p]) == 4

    full = RenderBookCache(tmp_path / 'full', 'spot')
    with monkeypatch.context() as limit:
        limit.setattr(render_book, 'MAX_REPLICA_CACHE_BYTES', 1)
        with pytest.raises(SourceError) as failure:
            full.sync('A', _feed(recordings, 'A', 'spot').read)
    assert failure.value.code == 'RENDER_CACHE_FULL'
    assert full.cursor('A') == 0


def test_integrity_conflict_and_pin(
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
) -> None:
    """Bad bytes and conflicting identities fail; an accepted pin survives later B imports."""
    cache = RenderBookCache(tmp_path, 'spot')
    feed = _feed(recordings, 'A', 'spot')

    def corrupt(path: str, limit: int) -> bytes:
        payload = feed.read(path, limit)
        return payload if '?after=' in path else payload[:-1]

    with pytest.raises(SourceError) as failure:
        cache.sync('A', corrupt)
    assert failure.value.code == 'RENDER_INTEGRITY'
    assert cache.cursor('A') == 0
    cache.sync('A', feed.read)
    adapter = RenderBookCanonical(cache)
    partition = adapter.partition(HOUR)
    accepted = adapter.fetch(partition)
    later = _feed(recordings, 'B', 'spot')
    cache.sync('B', later.read)
    assert adapter.fetch(partition).key == accepted.key
    assert json.loads(adapter.fetch(partition).evidence_json)['replica_seconds'] == {
        'A': 3600,
        'B': 0,
    }

    # Deliberate protocol fault: one observation identity appears twice with different receipt time.
    faulty = _feed(recordings, 'A', 'spot')
    metadata = dict(faulty.entries[1])
    identifier = int(str(metadata['id']))
    lines = faulty.payloads[identifier].splitlines()
    duplicate = json.loads(lines[20])
    duplicate['observed_at'] += 1
    lines.insert(21, json.dumps(duplicate, separators=(',', ':')).encode())
    payload = b'\n'.join(lines) + b'\n'
    metadata.update(
        count=len(lines), bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()
    )
    faulty.entries = tuple(metadata if m['id'] == identifier else m for m in faulty.entries)
    faulty.payloads = {**faulty.payloads, identifier: payload}
    conflict = RenderBookCache(tmp_path / 'conflict', 'spot')
    conflict.sync('A', faulty.read)
    with pytest.raises(SourceError) as failure:
        RenderBookCanonical(conflict).fetch(partition)
    assert failure.value.code == 'RENDER_CONFLICT'
    assert not (conflict.root / 'spot' / 'hours' / f'{HOUR}.json').exists()


def test_provisional_fallback_and_late_recovery(
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
) -> None:
    """B restores A's missing minute before finality, without replacing the receipt clock."""
    cache = RenderBookCache(tmp_path, 'perp')
    a = _feed(recordings, 'A', 'perp')
    a.omitted = _gap(a, 21)
    cache.sync('A', a.read)
    provisional = RenderBookProvisional(cache)
    minute = provisional.partition(MINUTE)
    canonical = RenderBookCanonical(cache)
    hour = canonical.partition(HOUR)
    for adapter, partition in ((provisional, minute), (canonical, hour)):
        with pytest.raises(SourceError) as failure:
            adapter.fetch(partition)
        assert failure.value.code == 'RENDER_SECONDS_MISSING'
    b = _feed(recordings, 'B', 'perp')
    cache.sync('B', b.read)
    recovered = provisional.fetch(minute)
    rows = tuple(recovered.rows())
    assert len(rows) == 60 and {row[4] for row in rows} == {'B'}
    for row in rows:
        original = json.loads(str(row[-1]))
        assert (row[1], row[2], row[3]) == (
            original['exchange_time'],
            original['observed_at'],
            original['update_id'],
        )
    assert canonical.fetch(hour).row_count == 3600
    assert canonical.candidate(START + timedelta(hours=1, minutes=4)).start == START - timedelta(
        hours=1
    )
    assert canonical.candidate(START + timedelta(hours=1, minutes=5)).start == START
    candidates = provisional.candidates(START + timedelta(hours=1), START, (minute,))
    assert len(candidates) == 8
    assert candidates[0].start == START + timedelta(minutes=59)
    assert candidates[1].start == START
    assert minute not in candidates
    with pytest.raises(ValueError, match='exact UTC minute'):
        provisional.partition('2026-10-24T13:21:01Z')


def test_top20_sources_are_isolated_until_rollout() -> None:
    from origo.sources.registry import SOURCE_REGISTRY

    sources = tuple(spec for spec in SOURCE_REGISTRY if spec.key.endswith('_top20_v1'))
    assert {spec.key for spec in sources} == {'binance_spot_top20_v1', 'binance_perp_top20_v1'}
    for spec in sources:
        assert spec.rollout_stage == RolloutStage.DORMANT
        assert spec.partitions.start == START and spec.partitions.interval == 'hour'
        assert {c.key for c in spec.components} == {
            'depth20', 'depth20_1m', 'depth20_latest', 'depth20_1m_latest',
        }


def test_native_recovery_discovers_unrequested_hour_and_retries_new_input(
    runtime: SourceRuntime,
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A never-requested hour builds automatically when late genuine replica input heals it."""
    cache = cast(RenderBookCanonical, runtime.spec.canonical).cache
    a = _feed(recordings, 'A', cache.market)
    a.omitted = _gap(a, 21)
    cache.sync('A', a.read)
    assert runtime.store.execute('SELECT count() FROM origo.source_discovery_log') == [(0,)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return START + timedelta(hours=1, minutes=6)

    monkeypatch.setattr(dagit, 'datetime', Clock)
    # Isolated test filesystems use their real free space, with a 1 MiB test reserve.
    monkeypatch.setattr(capacity, '_volumes', lambda owner: (capacity._Volume('local-test', tmp_path),))
    monkeypatch.setattr(capacity, 'CAPACITY_RESERVE_BYTES', 1024**2)
    source = build_source_bundle(runtime.spec)
    definitions = Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors)
    sensor = next(s for s in source.sensors if s.name.endswith('_reconciliation_sensor'))
    asset = f'build_{runtime.spec.key}_canonical_revision_origo'
    job = next(j for j in source.jobs if j.name == f'refresh_{runtime.spec.key}_canonical_source_job')
    (tmp_path / 'dagster-recovery').mkdir()
    with DagsterInstance.local_temp(str(tmp_path / 'dagster-recovery')) as instance:
        def requests() -> list[RunRequest]:
            with build_sensor_context(instance=instance, definitions=definitions) as context:
                return [r for r in sensor.evaluate_tick(context).run_requests if r.partition_key]

        request = next(r for r in requests() if r.partition_key == HOUR)
        assert request.run_config['ops'][asset]['config']['reconcile_only'] is False
        failed = job.execute_in_process(
            instance=instance, partition_key=HOUR, tags=request.tags,
            run_config=request.run_config, raise_on_error=False,
        )
        assert not failed.success
        assert instance.get_run_by_id(failed.run_id).tags['origo_source_verdict'] == 'RENDER_SECONDS_MISSING'
        assert requests() == []
        cache.sync('B', _feed(recordings, 'B', cache.market).read)
        recovered = next(r for r in requests() if r.partition_key == HOUR)
        assert recovered.tags['origo_source_authority'] != request.tags['origo_source_authority']
        assert job.execute_in_process(
            instance=instance, partition_key=HOUR, tags=recovered.tags,
            run_config=recovered.run_config,
        ).success
        assert runtime.store.record(cast(RenderBookCanonical, runtime.spec.canonical).partition(HOUR))
        assert requests() == []


def test_http_retrieval_uses_authentication_and_transfer_bounds(
    recorded_server: RecordedServer,
    tmp_path: Path,
) -> None:
    """The real HTTP reader imports genuine files and rejects oversized/error responses."""
    base = recorded_server.url + '/A'
    endpoint = RenderBookEndpoint(base, 'local-proof-token')
    cache = RenderBookCache(tmp_path, 'spot')
    assert cache.sync('A', endpoint.read) == 61
    assert RenderBookCanonical(cache).fetch(RenderBookCanonical(cache).partition(HOUR)).row_count == 3600
    for reader, path, limit, expected in (
        (RenderBookEndpoint(base, 'wrong').read, '/segments/spot?after=0', 1024**2, 'RENDER_HTTP'),
        (endpoint.read, '/expired', 1024**2, 'RENDER_EXPIRED'),
        (endpoint.read, '/segments/spot?after=0', 1, 'RENDER_TRANSFER_BOUND'),
        (
            replace(endpoint, deadline=time.monotonic() + 0.05).read,
            '/slow', 1024**2, 'RENDER_IMPORT_BUDGET',
        ),
    ):
        with pytest.raises(SourceError) as failure:
            reader(path, limit)
        assert failure.value.code == expected


def test_worker_imports_current_minute_before_bounded_recovery(
    runtime: SourceRuntime,
    recorded_server: RecordedServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unavailable replica and an old cursor cannot block the real minute worker's live input."""
    cache = cast(RenderBookCanonical, runtime.spec.canonical).cache
    ensure_monitoring_tables(runtime.store.client, runtime.store.database)
    recorded_server.offline.add('A')
    for replica in ('A', 'B'):
        monkeypatch.setenv(f'ORIGO_RENDER_BOOK_{replica}_URL', recorded_server.url + '/' + replica)
        monkeypatch.setenv(f'ORIGO_RENDER_BOOK_{replica}_TOKEN', 'local-proof-token')
    execute = worker.execute_source

    def build(
        spec: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str,
    ) -> dict[str, object]:
        recorded_server.events.append(f'build:{config.partition_key}')
        return execute(spec, operation, config, run_id=run_id)

    monkeypatch.setattr(worker, 'execute_source', build)
    feed = worker_feed(runtime.spec, tmp_path, _Dagster(), _Reporter())
    outcome = feed.tick(START + timedelta(hours=1))
    minute = '2026-10-24T13:59:00Z'
    assert f'{runtime.spec.key}:{minute}' in outcome.processed
    assert recorded_server.events.index(f'build:{minute}') < recorded_server.events.index(
        f'B/segments/{cache.market}?after=0'
    )
    assert cache.cursor('A') == 0
    assert cache.cursor('B') == int(str(recorded_server.feeds['B', cache.market].entries[7]['id']))
    raw = runtime.store.table(runtime.spec.names.prefix + '_depth20_current')
    assert runtime.store.execute(
        f"SELECT count(), uniqExact(collector), any(collector) FROM {raw} "
        "WHERE datetime>='2026-10-24 13:59:00' AND datetime<'2026-10-24 14:00:00'"
    ) == [(60, 1, 'B')]


@pytest.mark.parametrize('runtime', ['spot'], indirect=True)
def test_spot_arrow_preserves_pinned_rows_and_recovers_corrupt_output(
    runtime: SourceRuntime,
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real canonical activation rejects a stale render; retry repairs the bounded Arrow file."""
    cache = cast(RenderBookCanonical, runtime.spec.canonical).cache
    for replica in ('A', 'B'):
        cache.sync(replica, _feed(recordings, replica, 'spot').read)
    for minute in (20, 22):
        runtime.build(f'2026-10-24T13:{minute}:00Z', provisional=True)
    destination = tmp_path / 'source-files' / runtime.spec.key / 'arrow'
    target = tmp_path / 'arrow' / runtime.spec.key / 'latest.arrow'
    runtime.publish('arrow', str(destination))
    original_file = target.read_bytes()
    original_manifest = (destination / 'latest.json').read_bytes()
    assert pl.read_ipc(target).height == 120
    runtime.build('2026-10-24T13:23:00Z', provisional=True)
    render = top20_arrow._frame

    def activate_hour(
        reader: top20_arrow._BookReader, snapshot: top20_arrow.Snapshot,
        start: datetime, end: datetime,
    ) -> pl.DataFrame:
        frame = render(reader, snapshot, start, end)
        runtime.build(HOUR)
        return frame

    with monkeypatch.context() as transition:
        transition.setattr(top20_arrow, '_frame', activate_hour)
        with pytest.raises(SourceError) as failure:
            runtime.publish('arrow', str(destination))
    assert failure.value.code == 'GENERATION_CHANGED'
    assert target.read_bytes() == original_file
    assert (destination / 'latest.json').read_bytes() == original_manifest
    runtime.publish('arrow', str(destination))
    with pa.ipc.open_file(target) as arrow:
        assert arrow.num_record_batches == 1
        table = arrow.read_all()
        assert table.schema.field('bids').type.list_size == 20
    rows = table.to_pylist()
    expected = list(_expected(recordings, 'spot').values())[-1800:]
    assert len(rows) == 1800
    for row, original in zip(rows, expected, strict=True):
        assert row['canonical'] is True
        assert json.loads(row['original_snapshot']) == original
        assert row['ts'] == original['exchange_time'] * 1000
        assert row['source_timestamp_ms'] == original['exchange_time'] // 1000
        assert row['observed_at_us'] == original['observed_at']
        for side in ('bids', 'asks'):
            assert [(level['price'], level['qty']) for level in row[side]] == [
                (float(price), float(qty)) for price, qty in original[side]
            ]
    unchanged = target.stat().st_mtime_ns
    runtime.publish('arrow', str(destination))
    assert target.stat().st_mtime_ns == unchanged
    # Deliberate file corruption, preserving the source generations and manifest.
    target.write_bytes(target.read_bytes() + b'x')
    runtime.publish('arrow', str(destination))
    repaired = pl.read_ipc(target)
    assert repaired.equals(pl.from_arrow(table))
    manifest = json.loads((destination / 'latest.json').read_bytes())
    assert manifest['files'][0]['sha256'] == hashlib.sha256(target.read_bytes()).hexdigest()
