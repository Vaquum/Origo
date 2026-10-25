from __future__ import annotations

import hashlib
import json
import tarfile
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import cast
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import render_book
from origo.sources.adapters.book_spool import Market
from origo.sources.adapters.render_book import (
    RenderBookCache,
    RenderBookCanonical,
    RenderBookEndpoint,
    RenderBookProvisional,
)
from origo.sources.contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceError,
    SourceNames,
    SourceReadPolicy,
)
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.top20 import top20_components
from origo.sources.storage import SourceStore

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
        if '?after=' in path:
            after = int(path.rsplit('=', 1)[1])
            entries = [
                m
                for m in self.entries
                if int(str(m['id'])) > after and int(str(m['id'])) not in self.omitted
            ][:100]
            payload = json.dumps(
                {
                    'collector': self.replica,
                    'market': self.market,
                    'segments': entries,
                    'pruned_through': 0,
                    'has_more': False,
                    'next_cursor': entries[-1]['id'] if entries else after,
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
    cache = RenderBookCache(tmp_path / 'cache', market)
    key = f'binance_{market}_top20_v1'
    spec = RevisionedSourceSpec(
        key=key,
        rollout_stage=RolloutStage.CANARY,
        schema_version=1,
        names=SourceNames(key),
        partitions=PartitionPolicy(START.date(), interval='hour', first_hour=13),
        canonical=RenderBookCanonical(cache),
        provisional=RenderBookProvisional(cache),
        components=top20_components(),
        consumers=(),
        orchestration=OrchestrationSpec('5 * * * *', '* * * * *', '30 * * * *', retry_count=0),
        read_policy=SourceReadPolicy.AVAILABLE,
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
        if condition == 'A_offline' and replica == 'A':
            feed.omitted = {int(str(entry['id'])) for entry in feed.entries}
        if condition in ('A_gap', 'complementary', 'shared') and replica == 'A':
            feed.omitted = _gap(feed, 21)
        if condition in ('B_gap', 'complementary', 'shared') and replica == 'B':
            feed.omitted = _gap(feed, 21 if condition == 'shared' else 41)
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
    assert len(provisional.candidates(START + timedelta(hours=1), START, (minute,))) == 59
    with pytest.raises(ValueError, match='exact UTC minute'):
        provisional.partition('2026-10-24T13:21:01Z')


def test_adapter_does_not_register_or_replace_sources() -> None:
    from origo.sources.registry import SOURCE_REGISTRY

    assert not any('top20_v1' in spec.key for spec in SOURCE_REGISTRY)
    assert {spec.key for spec in SOURCE_REGISTRY if spec.key.endswith('_book')} == {
        'binance_spot_book',
        'binance_perp_book',
    }


def test_http_retrieval_uses_authentication_and_transfer_bounds(
    recordings: dict[tuple[str, Market], RecordedFeed],
    tmp_path: Path,
) -> None:
    """The real HTTP reader imports genuine files and rejects oversized/error responses."""
    feed = _feed(recordings, 'A', 'spot')

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.headers.get('Authorization') != 'Bearer local-proof-token':
                self.send_error(401)
            elif self.path == '/expired':
                self.send_error(410)
            else:
                payload = feed.read(self.path, 64 * 1024**2)
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    endpoint = RenderBookEndpoint(base, 'local-proof-token')
    try:
        cache = RenderBookCache(tmp_path, 'spot')
        assert cache.sync('A', endpoint.read) == 61
        assert (
            RenderBookCanonical(cache).fetch(RenderBookCanonical(cache).partition(HOUR)).row_count
            == 3600
        )
        for reader, path, limit, expected in (
            (
                RenderBookEndpoint(base, 'wrong').read,
                '/segments/spot?after=0',
                1024**2,
                'RENDER_HTTP',
            ),
            (endpoint.read, '/expired', 1024**2, 'RENDER_EXPIRED'),
            (endpoint.read, '/segments/spot?after=0', 1, 'RENDER_TRANSFER_BOUND'),
        ):
            with pytest.raises(SourceError) as failure:
                reader(path, limit)
            assert failure.value.code == expected
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
