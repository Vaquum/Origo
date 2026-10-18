from __future__ import annotations

import fcntl
import gzip
import hashlib
import itertools
import json
import shutil
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from origo.sources.adapters import book_vendor as vendor
from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.book_spool import Market
from origo.sources.contracts import Partition, RevisionedSourceSpec, SourceError
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

REAL_HOUR = '2026-10-04T09Z'
_ORIGINAL_HASHES = {
    ('spot', '2026-10-04T09Z'): '60485683304b70774fa690ea695bb70686e7322c14fc6575a43b47793498004c',
    ('spot', '2026-10-04T10Z'): '67d928a15251a2a4201f6bc61633344d7c1f204789f588f70a04886376e36f98',
    ('perp', '2026-10-04T09Z'): 'a81bddd28166b90d60741b12de960891fc59d066c649818bb67a270eb9ae04d4',
    ('perp', '2026-10-04T10Z'): 'a56dd6dfdb48744dce9fe9c4ed72614f8dc2fafbc0364c56d98c46e227f2260d',
    ('spot', '2026-09-24T22Z'): 'c17204fc6275a822136a26bc37d0e4acfaf5f73201e05fea800c815754000a38',
    ('spot', '2026-09-24T23Z'): '7608ef20cc157128e3ecd9508ab335d9543e6d3e440926d1af8455ddaf3f6873',
    ('perp', '2026-09-24T22Z'): '32019e1483c700155f546b029710d61e2f69bb2fcd575acd30872545610cec75',
    ('perp', '2026-09-24T23Z'): 'da24eac19523b788af4ee4082da27c4176172130e86b0d6f5bfd2612c26c4721',
    ('perp', '2026-09-24T00Z'): 'ff1b4f7041101af2b595f80063f71cad190cec531c40ca9e6bdabbfb16d93ca4',
    ('perp', '2026-09-24T01Z'): '57f930164e1cf2c046375ab2b012dcb3706be3ff8ba7d52591a70cd605e5cdf2',
    ('spot', '2026-09-29T09Z'): '31628678f151487c07167854417f7bd42f6ced08fd747bd37b4b56b26fae63ad',
    ('spot', '2026-09-29T10Z'): '7e7574d5d6f788d8d893c88278e8aaeeba29597cddb8a7836164aa47712721f3',
}
_MINUTE_HASHES = {
    'spot': 'd8d884160b44755251b3208e67c91c7e37102cbcfcb73fc7633832e27bbc93ac',
    'perp': '194362e76263a438c157b8b546cc6689c7968f97148f2d725e498e883335b7ef',
}


@pytest.fixture(scope='session')
def vendor_session_root(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> Path:
    base = tmp_path_factory.getbasetemp()
    return base.parent if worker_id != 'master' else base


def _original_archive(
    root: Path, market: Market, partition: Partition,
) -> tuple[Market, bytes, dict[str, str]]:
    import requests

    name = f'vendor-{market}-{partition.key}'
    with (root / f'{name}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = root / f'{name}.input'
        headers = root / f'{name}.headers.json'
        if not path.exists():
            response = requests.get(
                vendor.CRYPTOHFT_URL,
                params={'file': vendor._file(market, partition)}, timeout=(5, 60),
            )
            response.raise_for_status()
            path.write_bytes(response.content)
            headers.write_text(json.dumps(dict(response.headers)))
        body = path.read_bytes()
        expected = _ORIGINAL_HASHES.get((market, partition.key))
        if expected is not None:
            assert hashlib.sha256(body).hexdigest() == expected
        return market, body, cast(dict[str, str], json.loads(headers.read_text()))


@pytest.fixture(scope='session', params=['spot', 'perp'])
def original_vendor_archive(
    request: pytest.FixtureRequest, vendor_session_root: Path,
) -> tuple[Market, bytes, dict[str, str]]:
    """Real private inputs fetched from the vendor, never committed or published.

    This integration input is deliberately unavailable rather than substituted
    when the actual provider cannot serve it. No Binance request is made.
    """
    market = cast(Market, request.param)
    # Versioned test preparation, not operator configuration. Only this vendor
    # host is contacted, once per market for the complete test session.
    partition = vendor.hour_partition(REAL_HOUR)
    return _original_archive(vendor_session_root, market, partition)


@pytest.fixture(scope='session')
def original_vendor_following_archive(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    vendor_session_root: Path,
) -> tuple[Market, bytes, dict[str, str]]:
    market = original_vendor_archive[0]
    partition = vendor.hour_partition('2026-10-04T10Z')
    return _original_archive(vendor_session_root, market, partition)


@pytest.fixture()
def vendor_transport(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]], monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[Market, bytes, dict[str, str]]]:
    market, body, headers = original_vendor_archive
    expected_file = vendor._file(market, vendor.hour_partition(REAL_HOUR))

    def captured(
        url: str, *, params: object = None, headers: object = None, weight: int = 0
    ) -> Response:
        assert url == vendor.CRYPTOHFT_URL
        assert weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        captured = original_vendor_archive if params == {'file': expected_file} else original_vendor_following_archive
        assert params in ({'file': expected_file}, {'file': vendor._file(market, vendor.hour_partition('2026-10-04T10Z'))})
        return Response(
            captured[1][:1] if headers else captured[1], captured[2], 206 if headers else 200
        )

    monkeypatch.setattr(vendor, 'get_response', captured)
    yield market, body, headers


def test_original_vendor_hour_builds_all_book_grids_without_binance(
    original_vendor_revision: tuple[Market, vendor.Revision],
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
) -> None:
    market, revision = original_vendor_revision
    assert market == original_vendor_archive[0]
    partition = vendor.hour_partition(REAL_HOUR)
    current_hash = hashlib.sha256(original_vendor_archive[1]).hexdigest()
    following_hash = hashlib.sha256(original_vendor_following_archive[1]).hexdigest()
    evidence = json.loads(revision.evidence_json)
    assert evidence['input_sha256'] == current_hash
    assert evidence['following_input_sha256'] == [['2026-10-04T10Z', following_hash]]
    assert revision.content_hash == hashlib.sha256(json.dumps(
        [current_hash, [], [['2026-10-04T10Z', following_hash]]], separators=(',', ':')
    ).encode()).hexdigest()
    assert json.loads(revision.evidence_json)['grid_evidence']['terminal_exchange_event_ms'] >= int(partition.end.timestamp() * 1000) - 100
    minutes = evidence['grid_evidence']['sealed_minutes']
    assert len(minutes) == 60
    assert hashlib.sha256(json.dumps(
        [(minute['minute_start'], minute['payload_sha256']) for minute in minutes],
        separators=(',', ':'),
    ).encode()).hexdigest() == _MINUTE_HASHES[market]
    counts = {20: 0, 200: 0}
    first: dict[int, datetime] = {}
    last: dict[int, datetime] = {}
    for row in revision.rows():
        depth = cast(int, row[0])
        first.setdefault(depth, cast(datetime, row[1]))
        last[depth] = cast(datetime, row[1])
        counts[depth] += 1
    assert counts == {20: 36000, 200: 3600}
    assert first == {20: partition.start, 200: partition.start}
    assert last == {
        20: partition.end - timedelta(milliseconds=100),
        200: partition.end - timedelta(seconds=1),
    }


def test_hourly_book_partitions_reuse_native_source_jobs() -> None:
    from dagster import Definitions, HourlyPartitionsDefinition
    from origo.sources.bundle import build_source_bundle
    from origo.sources.registry import SOURCE_REGISTRY

    for spec in SOURCE_REGISTRY:
        assert spec.partitions.interval == ('hour' if spec.key.endswith('_book') else 'day')
        if not spec.key.endswith('_book'):
            continue
        bundle = build_source_bundle(spec)
        defs = Definitions(
            assets=bundle.assets,
            jobs=bundle.jobs,
            schedules=bundle.schedules,
            sensors=bundle.sensors,
        )
        job = defs.get_job_def(f'backfill_{spec.key}_source_job')
        assert isinstance(job.partitions_def, HourlyPartitionsDefinition)
        assert REAL_HOUR in job.partitions_def.get_partition_keys(
            datetime(2026, 10, 4, 11, tzinfo=UTC)
        )
        assert (
            len(
                vendor.CryptoHFTBookHourly('spot')
                .candidate(datetime(2026, 10, 4, 10, 15, tzinfo=UTC))
                .key
            )
            == 14
        )
        assert spec.orchestration.canonical_cron == '15 * * * *'
        assert spec.orchestration.audit_cron == '5,20,35,50 * * * *'


@pytest.mark.parametrize(
    'key',
    [
        '2026-10-04T09:00:00Z',
        '2026-10-04',
        '2026-10-04T09:01:00Z',
        '2026-10-04T09:00Z',
        '2026-10-04T09:00:01Z',
    ],
)
def test_hourly_adapter_rejects_nonhour_keys(key: str) -> None:
    with pytest.raises(ValueError):
        vendor.hour_partition(key)


def test_missing_vendor_checkpoint_cannot_activate_a_partial_hour(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path
) -> None:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    market, body, _ = original_vendor_archive
    # Delete real checkpoint rows; preserve every update and timestamp.
    archive = pq.ParquetFile(pa.BufferReader(body))
    path = tmp_path / 'missing-checkpoint.parquet'
    with pq.ParquetWriter(path, archive.schema_arrow, compression='zstd') as writer:
        for batch in archive.iter_batches(batch_size=vendor.CRYPTOHFT_BATCH_ROWS):
            table = pa.Table.from_batches([batch])
            writer.write_table(table.filter(pc.equal(table['event_type'], 'update')))
    with pytest.raises(SourceError) as failure:
        vendor.replay_hour(path, market, vendor.hour_partition(REAL_HOUR), tmp_path / 'grid')
    assert failure.value.code == 'BOOK_VENDOR_SEED_MISSING'


def test_truncated_exchange_tail_cannot_complete_an_hour(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    market, body, _ = original_vendor_archive
    partition = vendor.hour_partition(REAL_HOUR)
    cutoff = int(partition.end.timestamp() * 1000) - 1000
    archive = pq.ParquetFile(pa.BufferReader(body))
    path = tmp_path / 'truncated-tail.parquet'
    # Remove actual final-second records; retain all other original values/clocks.
    with pq.ParquetWriter(path, archive.schema_arrow, compression='zstd') as writer:
        for batch in archive.iter_batches(batch_size=vendor.CRYPTOHFT_BATCH_ROWS):
            table = pa.Table.from_batches([batch])
            writer.write_table(table.filter(pc.less(table['event_time'], cutoff)))
    with pytest.raises(SourceError) as failure:
        vendor.replay_hour(path, market, partition, tmp_path / 'grid')
    assert failure.value.code == 'BOOK_VENDOR_TAIL_MISSING'


@pytest.fixture(scope='session')
def original_vendor_preparation(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    vendor_session_root: Path,
) -> tuple[Market, vendor.Revision, Path]:
    market, body, headers = original_vendor_archive

    def captured(
        url: str, *, params: object = None, headers: object = None, weight: int = 0
    ) -> Response:
        assert url == vendor.CRYPTOHFT_URL
        current = {'file': vendor._file(market, vendor.hour_partition(REAL_HOUR))}
        following = {'file': vendor._file(market, vendor.hour_partition('2026-10-04T10Z'))}
        assert params in (current, following)
        assert weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        captured = original_vendor_archive if params == current else original_vendor_following_archive
        return Response(
            captured[1][:1] if headers else captured[1], captured[2], 206 if headers else 200
        )

    root = vendor_session_root
    grid = root / f'verified-book-{market}'
    metadata = root / f'verified-book-{market}.json'
    with (root / f'verified-book-{market}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not metadata.exists():
            with pytest.MonkeyPatch.context() as patch:
                lock_root = root / f'vendor-scratch-{market}'
                patch.setenv('ORIGO_SOURCE_LOCK_DIR', str(lock_root))
                patch.setattr(vendor, 'get_response', captured)
                revision = vendor.CryptoHFTBookHourly(market).fetch(vendor.hour_partition(REAL_HOUR))
                assert revision.key == vendor.CryptoHFTBookHourly(market).discover(
                    vendor.hour_partition(REAL_HOUR)
                )
            original, = (lock_root / f'book_vendor_{market}').glob('hour-*/grid')
            shutil.copytree(original, grid)
            metadata.write_text(json.dumps([
                revision.key, revision.content_hash, revision.evidence_json, revision.row_count,
            ]))
        values = cast(list[object], json.loads(metadata.read_text()))
        local = vendor.fetch_sealed_partition(grid, market, vendor.hour_partition(REAL_HOUR))
        revision = replace(
            local, key=str(values[0]), content_hash=str(values[1]),
            evidence_json=str(values[2]), row_count=int(str(values[3])),
        )
    return market, revision, grid


@pytest.fixture(scope='session')
def original_vendor_revision(
    original_vendor_preparation: tuple[Market, vendor.Revision, Path],
) -> tuple[Market, vendor.Revision]:
    market, revision, _ = original_vendor_preparation
    return market, revision


@pytest.fixture(scope='session')
def original_vendor_spool(
    original_vendor_preparation: tuple[Market, vendor.Revision, Path],
) -> Path:
    return original_vendor_preparation[2]


def test_hourly_and_provisional_keys_cannot_share_partition_identity() -> None:
    for spec in (BINANCE_SPOT_BOOK_SPEC, BINANCE_PERP_BOOK_SPEC):
        canonical = spec.canonical.partition(REAL_HOUR)
        assert spec.provisional is not None
        minute = spec.provisional.partition(canonical.start.strftime('%Y-%m-%dT%H:%M:%SZ'))
        assert minute.start == canonical.start and minute.key != canonical.key
        with pytest.raises(ValueError):
            spec.provisional.partition(canonical.key)
        with pytest.raises(ValueError):
            spec.canonical.partition(minute.key)


@pytest.mark.parametrize('deletion', [False, True])
def test_same_id_snapshot_checks_observed_prices_outside_seed(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
    deletion: bool,
) -> None:
    import json
    from decimal import Decimal
    from origo.workers.book_capture import BOOK_SEED_DEPTH, DepthEvent, DiffBook, parse_book_integer

    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'
    path.write_bytes(body)
    frames = iter(vendor._frames((path,)))
    checkpoint = next(frames)
    sides = {side: sorted([(str(row['price']), str(row['quantity'])) for row in checkpoint
                           if row['side'] == side], key=lambda pair: Decimal(pair[0]),
                          reverse=side == 'bid') for side in ('bid', 'ask')}
    seed = {'lastUpdateId': checkpoint[0]['last_update_id'], 'bids': sides['bid'], 'asks': sides['ask']}
    reference = DiffBook(market, json.dumps(seed).encode())
    reference.verified = True
    seed['bids'], seed['asks'] = (sides[side][:min(BOOK_SEED_DEPTH[market], len(sides[side]) // 2)]
                                 for side in ('bid', 'ask'))
    book = DiffBook(market, json.dumps(seed).encode())
    book.verified = True
    for frame in frames:
        first = frame[0]
        event = DepthEvent(parse_book_integer(first['event_time']),
                           parse_book_integer(first['first_update_id']),
                           parse_book_integer(first['final_update_id']),
                           parse_book_integer(first['prev_final_update_id']) if market == 'perp' else None,
                           tuple((Decimal(str(row['price'])), Decimal(str(row['quantity'])))
                                 for row in frame if row['side'] == 'bid'),
                           tuple((Decimal(str(row['price'])), Decimal(str(row['quantity'])))
                                 for row in frame if row['side'] == 'ask'))
        previous = {'bids': dict(reference.bids), 'asks': dict(reference.asks)}
        reference.apply(event)
        book.apply(event)
        for name, updates, observed in (('bids', event.bids, book.bid_observed),
                                        ('asks', event.asks, book.ask_observed)):
            for price, quantity in updates:
                before = previous[name].get(price, Decimal(0))
                if price not in observed or (quantity == 0) != deletion or before == quantity:
                    continue
                if (name == 'bids' and price < reference.bid_floor) or (name == 'asks' and price > reference.ask_ceiling):
                    continue
                current = {'bids': dict(reference.bids), 'asks': dict(reference.asks)}
                # Fault injection restores this price's actual preceding quantity;
                # every other value comes from the unchanged recorded update stream.
                if before:
                    current[name][price] = before
                else:
                    del current[name][price]
                payload = {'lastUpdateId': book.last,
                           **{side: [[str(p), str(q)] for p, q in sorted(levels.items(), reverse=side == 'bids')]
                              for side, levels in current.items()}}
                with pytest.raises(SourceError) as failure:
                    book.refresh(DiffBook(market, json.dumps(payload).encode()))
                assert failure.value.code == 'BOOK_SNAPSHOT_MISMATCH'
                assert book.top(200) == reference.top(200)
                return
    pytest.fail('Original recording lacks the required outside-boundary update.')


def test_real_hour_survives_original_seed_region_without_reseeding(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    import json
    from decimal import Decimal
    from origo.workers.book_capture import (
        BOOK_SEED_DEPTH, BookSampler, DepthEvent, DiffBook, parse_book_integer, utc_millisecond,
    )

    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'
    path.write_bytes(body)
    following = tmp_path / 'following.parquet'
    following.write_bytes(original_vendor_following_archive[1])
    frames = iter(vendor._frames((path, following)))
    checkpoint = next(frames)
    initial = checkpoint[0]
    sides = {side: sorted([(str(row['price']), str(row['quantity'])) for row in checkpoint
                           if row['side'] == side], key=lambda pair: Decimal(pair[0]),
                          reverse=side == 'bid') for side in ('bid', 'ask')}
    seed = {'lastUpdateId': initial['last_update_id'], 'bids': sides['bid'], 'asks': sides['ask']}
    full = DiffBook(market, json.dumps(seed).encode())
    full.verified = True
    seed['bids'] = sides['bid'][:BOOK_SEED_DEPTH[market]]
    seed['asks'] = sides['ask'][:BOOK_SEED_DEPTH[market]]
    sampler = BookSampler(tmp_path / 'grid', market)
    partition = vendor.hour_partition(REAL_HOUR)
    sampler.checkpoint(json.dumps(seed).encode(), parse_book_integer(initial['event_time']),
                       int(partition.start.timestamp() * 1000))
    assert sampler.book is not None
    original_floor, original_ceiling = sampler.book.bid_floor, sampler.book.ask_ceiling
    beyond = 0
    applied = 0
    for frame in frames:
        first = frame[0]
        if first['event_type'] == 'snapshot':
            continue  # Prove live continuity without using another HCD checkpoint.
        event = DepthEvent(parse_book_integer(first['event_time']),
                           parse_book_integer(first['first_update_id']),
                           parse_book_integer(first['final_update_id']),
                           parse_book_integer(first['prev_final_update_id']) if market == 'perp' else None,
                           tuple((Decimal(str(row['price'])), Decimal(str(row['quantity'])))
                                 for row in frame if row['side'] == 'bid'),
                           tuple((Decimal(str(row['price'])), Decimal(str(row['quantity'])))
                                 for row in frame if row['side'] == 'ask'))
        full.apply(event)
        sampler.accept(event, received_at=utc_millisecond(parse_book_integer(first['received_time']) // 1000000))
        assert sampler.book is not None and sampler.book.verified
        assert sampler.book.last == full.last == event.last
        assert sampler.book.top(200) == full.top(200)
        bids, asks = sampler.book.top(200)
        assert sampler.book.top(20) == (bids[:20], asks[:20])
        beyond += bids[-1][0] < original_floor or asks[-1][0] > original_ceiling
        applied += 1
        if event.event_ms >= int(partition.end.timestamp() * 1000):
            break
    assert sampler.book is not None and sampler.book.event_ms is not None
    assert sampler.book.event_ms >= int(partition.end.timestamp() * 1000)
    sampler.finish(int(partition.end.timestamp() * 1000))
    revision = vendor.fetch_sealed_partition(sampler.root, market, partition)
    assert revision.row_count == 36000
    assert applied > 10000
    if market == 'perp':
        assert beyond > 10000


def test_vendor_jwt_authentication_rotates_without_url_credentials(monkeypatch, caplog):
    clock = [1000.0]
    calls = []
    minted = [0]
    invalid = [False]
    reject = [0]

    def transport(url, *, params=None, headers=None, weight=0, method='GET'):
        calls.append((url, params, dict(headers or {}), weight, method))
        assert 'api_key=' not in url
        if url == vendor.CRYPTOHFT_TOKEN_URL:
            assert method == 'POST' and headers == {'X-API-Key': 'control-key'}
            if invalid[0]:
                raise SourceError('PROVIDER_HTTP_401', 'Provider returned HTTP 401.')
            minted[0] += 1
            return Response(json.dumps({'jwt_token': f'control-token-{minted[0]}',
                                        'expires_in': 100, 'token_type': 'Bearer'}).encode(), {}, 200)
        assert method == 'GET' and 'X-API-Key' not in (headers or {})
        if reject[0]:
            reject[0] -= 1
            raise SourceError('PROVIDER_HTTP_401', 'Provider returned HTTP 401.')
        return Response(b'provider-protocol-body', {'ETag': 'same-object'}, 206 if 'Range' in (headers or {}) else 200)

    monkeypatch.setattr(vendor, '_token', None)
    monkeypatch.setattr(vendor.time, 'time', lambda: clock[0])
    monkeypatch.setattr(vendor, 'get_response', transport)
    monkeypatch.setenv('CRYPTOHFTDATA_API_KEY', 'control-key')
    partition = vendor.hour_partition(REAL_HOUR)
    for market in ('spot', 'perp'):
        for metadata in (False, True):
            vendor._response(market, partition, metadata=metadata)
    assert minted == [1]
    clock[0] = 1091
    vendor._response('spot', partition, metadata=True)
    assert minted == [2]
    reject[0] = 1
    vendor._response('perp', partition, metadata=False)
    assert minted == [3]
    reject[0] = 2
    with pytest.raises(SourceError, match='HTTP 401'):
        vendor._response('spot', partition, metadata=False)
    assert minted == [4] and reject == [0]
    # A fresh native process has no token; a rejected key never falls back anonymously.
    monkeypatch.setattr(vendor, '_token', None)
    invalid[0] = True
    before = len(calls)
    with pytest.raises(SourceError) as failure:
        vendor._response('spot', partition, metadata=True)
    assert failure.value.code == 'PROVIDER_HTTP_401'
    assert len(calls) == before + 1 and calls[-1][0] == vendor.CRYPTOHFT_TOKEN_URL
    assert all(weight == 2 for _, _, _, weight, _ in calls)
    # Only an archive 404 can mean unpublished; token mint/refresh failures stay loud.
    recent = vendor.hour_partition(datetime.now(UTC).strftime('%Y-%m-%dT%HZ'))
    for refresh in (False, True):
        monkeypatch.setattr(vendor, '_token', None)
        token_calls = [0]

        def token_unavailable(url, **kwargs):
            if url == vendor.CRYPTOHFT_TOKEN_URL:
                token_calls[0] += 1
                if not refresh or token_calls[0] > 1:
                    raise SourceError('PROVIDER_HTTP_404', 'Provider returned HTTP 404.')
                return Response(b'{"jwt_token":"control-token","expires_in":100}', {}, 200)
            raise SourceError('PROVIDER_HTTP_401', 'Provider returned HTTP 401.')

        monkeypatch.setattr(vendor, 'get_response', token_unavailable)
        with pytest.raises(SourceError) as failure:
            vendor._response('spot', recent, metadata=True)
        assert failure.value.code == 'PROVIDER_HTTP_404'
        assert token_calls == [2 if refresh else 1]
    monkeypatch.setattr(vendor, 'get_response', transport)
    assert 'control-key' not in caplog.text and 'control-token' not in caplog.text
    # The key is confined to the token endpoint even if it returns a redirect.
    import requests
    from origo.sources.adapters import binance_daily as daily
    sent = {}

    def post(url, **kwargs):
        sent.update(kwargs)
        value = requests.Response()
        value.status_code = 302
        value._content = b''
        value._content_consumed = True
        value.headers['Location'] = 'https://storage.example/object'
        return value

    monkeypatch.setattr(daily.requests, 'post', post)
    with daily._request(vendor.CRYPTOHFT_TOKEN_URL, None, {'X-API-Key': 'control-key'},
                        method='POST') as redirected:
        assert redirected.status_code == 302
    assert sent['allow_redirects'] is False
    monkeypatch.delenv('CRYPTOHFTDATA_API_KEY')
    calls.clear()
    vendor._response('spot', partition, metadata=False)
    assert calls == [(vendor.CRYPTOHFT_URL, {'file': vendor._file('spot', partition)}, {}, 40, 'GET')]


def test_vendor_authenticated_requests_keep_one_shared_budget(tmp_path, monkeypatch):
    import requests
    from origo.sources.adapters import binance_daily as daily

    clock = [1000.0]
    calls = []
    responses = []

    def response(status=200, headers=None, body=b'protocol'):
        value = requests.Response()
        value.status_code = status
        value.headers.update(headers or {})
        value._content = body
        value._content_consumed = True
        return value

    def request(url, params, headers, egress_ip=None, method='GET'):
        calls.append((url, dict(headers or {}), method, clock[0]))
        if url == vendor.CRYPTOHFT_TOKEN_URL:
            value = response(body=b'{"jwt_token":"control-token","expires_in":14400}')
        else:
            value = response(headers={'ETag': 'recorded-object'})
            value.history = [response(302, {'RateLimit-Remaining': '0', 'RateLimit-Reset': '2',
                                           'RateLimit-Policy': 'q=60;w=60'})]
        responses.append(value)
        return value

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.setenv('CRYPTOHFTDATA_API_KEY', 'control-key')
    monkeypatch.setattr(vendor, '_token', None)
    monkeypatch.setattr(daily, '_request', request)
    monkeypatch.setattr(daily.time, 'time', lambda: clock[0])
    monkeypatch.setattr(daily.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    partition = vendor.hour_partition(REAL_HOUR)
    first = vendor._response('spot', partition, metadata=True)
    vendor._response('perp', partition, metadata=False)
    assert len(list(tmp_path.glob('*.state'))) == 1
    assert calls[0][0] == vendor.CRYPTOHFT_TOKEN_URL
    assert calls[1][3] >= 1000.1 and calls[2][3] >= calls[1][3] + 2
    assert daily._read_budget((tmp_path / 'binance_rest_budget.api_cryptohftdata_com.state').open())[1] >= clock[0] + 2
    assert vendor._identity('spot', partition, first) == vendor._identity(
        'spot', partition, Response(b'', {'ETag': 'recorded-object'}, 206))
    # Exercise requests' real cross-host redirect preparation without a network socket.
    prepared = requests.Request('GET', vendor.CRYPTOHFT_URL,
                                headers={'Authorization': 'Bearer control-token', 'Range': 'bytes=0-0'}).prepare()
    redirect = response(302, {'Location': 'https://storage.example/object'})
    redirect.url = vendor.CRYPTOHFT_URL
    redirect.request = prepared
    with requests.Session() as session:
        next_request = next(session.resolve_redirects(redirect, prepared, yield_requests=True))
    assert 'Authorization' not in next_request.headers and 'X-API-Key' not in next_request.headers
    assert next_request.headers['Range'] == 'bytes=0-0'
    assert all(r._content_consumed for r in responses)


def test_snapshot_presence_reads_only_original_required_columns(
    vendor_session_root, tmp_path, monkeypatch,
):
    import pyarrow.parquet as pq

    partition = vendor.hour_partition('2025-06-28T05Z')
    _, body, _ = _original_archive(vendor_session_root, 'perp', partition)
    raw = tmp_path / 'original.parquet'
    raw.write_bytes(body)
    path = vendor._prepare_archive(raw, tmp_path / 'unwrapped')
    original = vendor._archive_rows
    observed = []

    def rows(path, *, columns=vendor._COLUMNS):
        observed.append(columns)
        yield from original(path, columns=columns)

    monkeypatch.setattr(vendor, '_archive_rows', rows)
    for start in (0, int(partition.end.timestamp() * 1000)):
        expected = any(row['event_type'] == 'snapshot' and int(str(row['event_time'])) <= start
                       for row in original(path))
        assert vendor._has_snapshot(path, start) == expected
    assert observed == [('event_type', 'event_time')] * 2
    table = pq.read_table(path)
    damaged = tmp_path / 'missing-column.parquet'
    pq.write_table(table.drop(['quantity']), damaged)
    with pytest.raises(SourceError) as failure:
        vendor._has_snapshot(damaged, 0)
    assert failure.value.code == 'BOOK_VENDOR_SCHEMA'
    for value, expected in [('123', True), (123, True)]:
        monkeypatch.setattr(vendor, '_archive_rows', lambda *a, **k: iter([
            {'event_type': 'snapshot', 'event_time': value}]))
        assert vendor._has_snapshot(path, 123) == expected
    monkeypatch.setattr(vendor, '_archive_rows', lambda *a, **k: iter([
        {'event_type': 'snapshot', 'event_time': None}]))
    with pytest.raises(ValueError):
        vendor._has_snapshot(path, 123)


@pytest.fixture(scope='session', params=['spot', 'perp'])
def nominal_archives(request: pytest.FixtureRequest, vendor_session_root: Path) -> tuple[Market, Path, Path]:
    market = cast(Market, request.param)
    paths = []
    for key in ('2026-09-24T22Z', '2026-09-24T23Z'):
        _original_archive(vendor_session_root, market, vendor.hour_partition(key))
        paths.append(vendor_session_root / f'vendor-{market}-{key}.input')
    return market, paths[0], paths[1]


def _checkpoint_payload(frame: list[dict[str, object]]) -> bytes:
    from decimal import Decimal

    sides = {side: sorted([[str(row['price']), str(row['quantity'])] for row in frame if row['side'] == side],
                          key=lambda level: Decimal(level[0]), reverse=side == 'bid') for side in ('bid', 'ask')}
    return json.dumps({'lastUpdateId': frame[0]['last_update_id'], 'bids': sides['bid'], 'asks': sides['ask']}).encode()


def test_nominal_archive_snapshot_uses_real_exchange_clock(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path,
) -> None:
    market, current, following = nominal_archives
    partition = vendor.hour_partition('2026-09-24T22Z')
    root = tmp_path / 'grids'
    revision = vendor.replay_hour(current, market, partition, root, following=(following,))
    assert revision.row_count == 36000
    evidence = json.loads(revision.evidence_json)
    assert len(evidence['sealed_minutes']) == 60
    seals = evidence['sealed_minutes']
    digest = hashlib.sha256(json.dumps([seal['payload_sha256'] for seal in seals]).encode()).hexdigest()
    assert digest == {
        'spot': 'd84a112d98d4c36287129ce30b4cfad280765b45a7e12e1f7324e5884b354949',
        'perp': '78a1bc9c390297723b6e6d75cf6fa46813de131f8fb12e6750696415f236e626',
    }[market]
    rows = sorted(root.rglob('*.seal.gz'))
    assert rows
    first = json.loads(gzip.decompress(rows[0].read_bytes()).splitlines()[0])
    assert first['datetime_ms'] == 1790287200000
    assert (first['source_timestamp_ms'], first['last_update_id']) == {
        'spot': (1790287199914, 100634608796), 'perp': (1790287199988, 11650503406048),
    }[market]
    for path in rows:
        for raw in gzip.decompress(path.read_bytes()).splitlines():
            row = json.loads(raw)
            assert row['source_timestamp_ms'] <= row['datetime_ms']
    assert evidence['terminal_exchange_event_ms'] >= 1790290799900
    assert evidence['final_grid_ms'] == 1790290799900
    if market == 'spot':
        assert (evidence['terminal_exchange_event_ms'], evidence['terminal_update_id']) == (1790290800014, 100636094069)
        assert seals[0]['first_update_id'] == 100634608796


def test_nominal_archive_frame_inventory(nominal_archives: tuple[Market, Path, Path]) -> None:
    from origo.workers.book_capture import parse_book_integer

    market, current, following = nominal_archives
    expected = {'spot': ((45437, 100634608787), (45765, 100636094046)),
                'perp': ((34663, 11650503398982), (35824, 11650828724707))}[market]
    identities = set()
    for path, (count, cursor), event_ms in zip((current, following), expected, (1790287200000, 1790290800000), strict=True):
        frames = vendor._frames((path,))
        prefix = list(itertools.takewhile(lambda frame: frame[0]['event_type'] == 'snapshot', frames))
        assert len(prefix) == 1 and len(prefix[0]) == count
        assert len({vendor._frame_key(row) for row in prefix[0]}) == 1
        first = prefix[0][0]
        assert parse_book_integer(first['event_time']) == event_ms
        assert parse_book_integer(first['received_time']) == event_ms * 1000000
        assert parse_book_integer(first['last_update_id']) == cursor
        identities.add((event_ms, cursor))
    assert vendor._nominal_snapshots((current, following)) == identities


def test_nominal_archive_requires_initial_bridge(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, current, following = nominal_archives
    partition = vendor.hour_partition('2026-09-24T22Z')
    # Withhold the entire current input; the following file's untouched bridge is valid but late.
    with pytest.raises(SourceError, match='initial grid') as late:
        vendor.replay_hour(following, market, partition, tmp_path / 'late')
    assert late.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'
    frames = vendor._frames

    def prefix(paths: tuple[Path, ...], *, legacy: bool = False) -> Iterator[list[dict[str, object]]]:
        yield from itertools.takewhile(lambda frame: frame[0]['event_type'] == 'snapshot', frames(paths, legacy=legacy))

    monkeypatch.setattr(vendor, '_frames', prefix)
    with pytest.raises(SourceError) as absent:
        vendor.replay_hour(current, market, partition, tmp_path / 'absent')
    assert absent.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'


def test_nominal_archive_requires_real_tail(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, current, following = nominal_archives
    frames = vendor._frames
    limit = [0]

    def subset(paths: tuple[Path, ...], *, legacy: bool = False) -> Iterator[list[dict[str, object]]]:
        for path in paths:
            yield from frames((path,), legacy=legacy) if path == current else itertools.islice(frames((path,), legacy=legacy), 1 + limit[0])

    monkeypatch.setattr(vendor, '_frames', subset)
    partition = vendor.hour_partition('2026-09-24T22Z')
    with pytest.raises(SourceError) as missing:
        vendor.replay_hour(current, market, partition, tmp_path / 'missing', following=(following,))
    assert missing.value.code == 'BOOK_VENDOR_TAIL_MISSING'
    limit[0] = 1 if market == 'spot' else 2
    result = vendor.replay_hour(current, market, partition, tmp_path / 'witness', following=(following,))
    terminal = json.loads(result.evidence_json)['terminal_exchange_event_ms']
    assert 1790290799900 <= terminal < 1790290800000
    assert len(json.loads(result.evidence_json)['sealed_minutes']) == 60


def test_nominal_archive_preserves_native_gap(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, current, following = nominal_archives
    frames = vendor._frames

    def missing_bridge(paths: tuple[Path, ...], *, legacy: bool = False) -> Iterator[list[dict[str, object]]]:
        for index, frame in enumerate(frames(paths, legacy=legacy)):
            if index != 1:
                yield frame

    monkeypatch.setattr(vendor, '_frames', missing_bridge)
    with pytest.raises(SourceError) as gap:
        vendor.replay_hour(current, market, vendor.hour_partition('2026-09-24T22Z'), tmp_path, following=(following,))
    assert gap.value.code == 'BOOK_SEQUENCE_GAP'


def test_nominal_archive_real_checkpoint_after_clockless_state(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    from origo.workers.book_capture import BookSampler, parse_book_integer

    market, body, _ = original_vendor_archive
    path = tmp_path / 'real.parquet'
    path.write_bytes(body)
    frame = next(vendor._frames((path,)))
    payload = _checkpoint_payload(frame)
    event_ms = parse_book_integer(frame[0]['event_time'])
    start_ms = int(vendor.hour_partition(REAL_HOUR).start.timestamp() * 1000)
    sampler = BookSampler(tmp_path / 'grids', market)
    # Controlled replay state uses the unmodified real checkpoint payload, with its clock not yet proven.
    vendor._archive_checkpoint(sampler, payload, event_ms, start_ms, nominal=True)
    assert sampler.book is not None and sampler.book.event_ms is None
    vendor._archive_checkpoint(sampler, payload, event_ms, start_ms, nominal=False)
    assert sampler.book.event_ms == event_ms and sampler.next_grid == start_ms
    vendor._archive_checkpoint(sampler, payload, event_ms, start_ms, nominal=True)
    assert sampler.book.event_ms == event_ms and sampler.lines == []
    late = BookSampler(tmp_path / 'late', market)
    vendor._archive_checkpoint(late, payload, event_ms, start_ms - 3600000, nominal=True)
    with pytest.raises(SourceError) as missing:
        vendor._archive_checkpoint(late, payload, event_ms, start_ms - 3600000, nominal=False)
    assert missing.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'


def test_replay_policy_invalidates_complete_revision_identity(
    vendor_transport: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, body, headers = vendor_transport
    partition = vendor.hour_partition(REAL_HOUR)
    after = vendor.hour_partition('2026-10-04T10Z')
    current = vendor._identity(market, partition, Response(body, headers, 200))
    dependencies = [(after.key, vendor._identity(market, after, Response(
        original_vendor_following_archive[1], original_vendor_following_archive[2], 200)))]
    old = 'cryptohftdata-v2:' + hashlib.sha256(json.dumps([current, dependencies], separators=(',', ':')).encode()).hexdigest()
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    path = vendor._dependency_path(market, partition)
    vendor.atomic_write(path, json.dumps({'current': current, 'dependencies': dependencies,
        'rejection': {'policy': 1, 'code': 'BOOK_EVENT_STALE', 'revision': old}}).encode())
    adapter = vendor.CryptoHFTBookHourly(market)
    discovered = adapter.discover(partition)
    assert discovered == 'cryptohftdata-v2:' + hashlib.sha256(json.dumps(
        [vendor._REPLAY_POLICY_VERSION, current, dependencies], separators=(',', ':')).encode()).hexdigest()
    assert discovered != old and not adapter.rejected_revision(partition, discovered)
    vendor._write_dependencies(market, partition, current, dependencies, rejected='BOOK_VENDOR_CLOCK_UNPROVEN')
    assert adapter.rejected_revision(partition, discovered)
    with monkeypatch.context() as changed:
        changed.setattr(vendor, '_REJECTION_POLICY_VERSION', vendor._REJECTION_POLICY_VERSION + 1)
        assert adapter.discover(partition) == discovered
        assert not adapter.rejected_revision(partition, discovered)
    with monkeypatch.context() as changed:
        changed.setattr(vendor, '_REPLAY_POLICY_VERSION', vendor._REPLAY_POLICY_VERSION + 1)
        assert adapter.discover(partition) != discovered
        assert not adapter.rejected_revision(partition, adapter.discover(partition))
    assert {'BOOK_VENDOR_CLOCK_UNPROVEN', 'BOOK_VENDOR_TAIL_MISSING'} <= vendor._REJECTION_CODES


def test_nominal_archive_preserves_original_freshness(vendor_session_root: Path, tmp_path: Path) -> None:
    paths = []
    for key in ('2026-09-24T00Z', '2026-09-24T01Z'):
        _original_archive(vendor_session_root, 'perp', vendor.hour_partition(key))
        paths.append(vendor_session_root / f'vendor-perp-{key}.input')
    with pytest.raises(SourceError) as stale:
        vendor.replay_hour(paths[0], 'perp', vendor.hour_partition('2026-09-24T00Z'), tmp_path, following=(paths[1],))
    assert stale.value.code == 'BOOK_EVENT_STALE'


def test_real_clock_archive_preserves_original_sequence_gap(vendor_session_root: Path, tmp_path: Path) -> None:
    paths = []
    for key in ('2026-09-29T09Z', '2026-09-29T10Z'):
        _original_archive(vendor_session_root, 'spot', vendor.hour_partition(key))
        paths.append(vendor_session_root / f'vendor-spot-{key}.input')
    with pytest.raises(SourceError) as gap:
        vendor.replay_hour(paths[0], 'spot', vendor.hour_partition('2026-09-29T09Z'), tmp_path, following=(paths[1],))
    assert gap.value.code == 'BOOK_SEQUENCE_GAP'
