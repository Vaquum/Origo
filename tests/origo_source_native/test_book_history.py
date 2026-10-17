from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from origo.sources.adapters import book_vendor as vendor
from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.book_spool import Market
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.bundle import SourceRunConfig, _execute_operation
from origo.sources.contracts import Partition, SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_book_vendor import (
    _original_archive, original_vendor_archive as original_vendor_archive,
    vendor_session_root as vendor_session_root,
)


@dataclass(frozen=True)
class OriginalArchive:
    partition: Partition
    body: bytes
    headers: dict[str, str]


@pytest.fixture(scope='session')
def original_historical_archives(vendor_session_root: Path) -> dict[Market, tuple[OriginalArchive, ...]]:
    originals: dict[Market, tuple[OriginalArchive, ...]] = {}
    for market, hour in (('spot', 7), ('perp', 6)):
        captured: list[OriginalArchive] = []
        for prior in (hour - 1, hour, hour + 1):
            partition = vendor.hour_partition(f'2025-06-28T{prior:02d}Z')
            _, body, headers = _original_archive(vendor_session_root, cast(Market, market), partition)
            captured.append(OriginalArchive(partition, body, headers))
        originals[cast(Market, market)] = tuple(captured)
    return originals


def _transport(archives: tuple[OriginalArchive, ...], market: Market, monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], dict[str, str]]:
    files = {vendor._file(market, archive.partition): archive for archive in archives}
    visited: list[str] = []
    rewritten: dict[str, str] = {}

    def recorded(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> Response:
        assert url == vendor.CRYPTOHFT_URL and weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        assert isinstance(params, dict)
        file = str(cast(dict[str, object], params)['file'])
        visited.append(file)
        archive = files[file]
        original_headers = dict(archive.headers)
        if file in rewritten:
            original_headers['ETag'] = rewritten[file]
        return Response(archive.body[:1] if headers else archive.body, original_headers, 206 if headers else 200)

    monkeypatch.setattr(vendor, 'get_response', recorded)
    return visited, rewritten


def test_original_historical_hour_replays_preceding_vendor_snapshot(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market: Market = 'perp'
    archives = original_historical_archives[market]
    visited, rewritten = _transport(archives, market, monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    assert all(archive.body[:4] == b'\x28\xb5\x2f\xfd' for archive in archives)
    adapter = vendor.CryptoHFTBookHourly(market)
    partition = archives[1].partition
    standalone = adapter.discover(partition)
    revision = adapter.fetch(partition)
    assert revision.key != standalone
    assert len(tuple((tmp_path / 'locks' / 'book_vendor_perp').glob('hour-*'))) == 1
    adapter.revalidate(partition, revision)
    evidence = json.loads(revision.evidence_json)
    assert evidence['input_sha256'] == hashlib.sha256(archives[1].body).hexdigest()
    assert evidence['prelude_input_sha256'] == [[archives[0].partition.key, hashlib.sha256(archives[0].body).hexdigest()]]
    assert evidence['following_input_sha256'] == [[archives[2].partition.key, hashlib.sha256(archives[2].body).hexdigest()]]
    assert evidence['grid_evidence']['terminal_exchange_event_ms'] >= int(partition.end.timestamp() * 1000) - 100
    assert len(evidence['grid_evidence']['sealed_minutes']) == 60
    counts = {20: 0, 200: 0}
    first: dict[int, object] = {}
    last: dict[int, object] = {}
    for row in revision.rows():
        depth = cast(int, row[0])
        counts[depth] += 1
        first.setdefault(depth, row[1])
        last[depth] = row[1]
    assert counts == {20: 36000, 200: 3600}
    assert first == {20: partition.start, 200: partition.start}
    assert last == {20: partition.end - timedelta(milliseconds=100), 200: partition.end - timedelta(seconds=1)}
    assert set(visited) == {vendor._file(market, archive.partition) for archive in archives}
    # Provider metadata changes only; original market records are untouched.
    rewritten[vendor._file(market, archives[0].partition)] = 'revised-provider-object'
    assert adapter.discover(partition) != revision.key
    with pytest.raises(SourceError) as failure:
        adapter.revalidate(partition, revision)
    assert failure.value.code == 'OFFICIAL_REVISION_CHANGED'
    rewritten.clear()
    rewritten[vendor._file(market, archives[2].partition)] = 'revised-closing-object'
    with pytest.raises(SourceError) as failure:
        adapter.revalidate(partition, revision)
    assert failure.value.code == 'OFFICIAL_REVISION_CHANGED'


def test_original_spot_history_rejects_unproven_depth(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    archives = original_historical_archives['spot']
    visited, _ = _transport(archives, 'spot', monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    with pytest.raises(SourceError) as failure:
        vendor.CryptoHFTBookHourly('spot').fetch(archives[1].partition)
    assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    assert set(visited) == {vendor._file('spot', archive.partition) for archive in archives}
    retained = tuple((tmp_path / 'locks' / 'book_vendor_spot').iterdir())
    assert retained == (vendor._dependency_path('spot', archives[1].partition),)
    assert retained[0].stat().st_size <= vendor.BOOK_DEPENDENCY_MAX_BYTES
    adapter = vendor.CryptoHFTBookHourly('spot')
    assert adapter.rejected_revision(archives[1].partition, adapter.discover(archives[1].partition))


def test_legacy_snapshot_received_first_follows_its_native_update(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]], tmp_path: Path,
) -> None:
    archive = original_historical_archives['perp'][1]
    path = tmp_path / 'original.bin'
    path.write_bytes(archive.body)
    path = vendor._prepare_archive(path, tmp_path)
    updates: dict[int, int] = {}
    received_first = 0
    for frame in vendor._frames((path,), legacy=True):
        first = frame[0]
        if first['event_type'] == 'update':
            updates[int(str(first['final_update_id']))] = int(str(first['received_time']))
        elif int(str(first['last_update_id'])) in updates:
            received_first += int(str(first['received_time'])) < updates[int(str(first['last_update_id']))]
    assert received_first == 1


def test_legacy_collector_clock_cannot_close_exchange_hour(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections.abc import Iterator

    archives = original_historical_archives['perp']
    paths: list[Path] = []
    for archive in archives:
        path = tmp_path / (archive.partition.key + '.bin')
        path.write_bytes(archive.body)
        paths.append(vendor._prepare_archive(path, tmp_path))
    partition = archives[1].partition
    original_frames = vendor._frames
    replaced = [False]

    def delayed_collector(paths: tuple[Path, ...], *, legacy: bool = False) -> Iterator[list[dict[str, object]]]:
        for frame in original_frames(paths, legacy=legacy):
            first = frame[0]
            # Alter only one REST snapshot's collector metadata; original native
            # update IDs, exchange clocks and every price/quantity remain intact.
            if (not replaced[0] and first['event_type'] == 'snapshot'
                    and int(str(first['event_time'])) >= int(partition.start.timestamp() * 1000)):
                frame = [dict(row, event_time=int(partition.end.timestamp() * 1000)) for row in frame]
                replaced[0] = True
            yield frame

    monkeypatch.setattr(vendor, '_frames', delayed_collector)
    revision = vendor.replay_hour(paths[1], 'perp', partition, tmp_path / 'grid',
                                  preludes=(paths[0],), following=(paths[2],))
    assert replaced[0]
    assert len(json.loads(revision.evidence_json)['sealed_minutes']) == 60
    counts = {20: 0, 200: 0}
    for row in revision.rows():
        counts[cast(int, row[0])] += 1
    assert counts == {20: 36000, 200: 3600}


def test_initial_legacy_snapshot_uses_matching_original_exchange_clock(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]], tmp_path: Path,
) -> None:
    from decimal import Decimal
    from origo.workers.book_capture import BookSampler, parse_book_integer

    archive = original_historical_archives['perp'][1]
    path = tmp_path / 'original.bin'
    path.write_bytes(archive.body)
    path = vendor._prepare_archive(path, tmp_path)
    update: list[dict[str, object]] = []
    for frame in vendor._frames((path,), legacy=True):
        first = frame[0]
        if first['event_type'] == 'update':
            update = frame
        elif update and first['last_update_id'] == update[0]['final_update_id']:
            seed = {'lastUpdateId': first['last_update_id'], **{
                name: sorted([[str(row['price']), str(row['quantity'])] for row in frame if row['side'] == side],
                             key=lambda level: Decimal(level[0]), reverse=side == 'bid')
                for name, side in (('bids', 'bid'), ('asks', 'ask'))}}
            seed['bids'] = seed['bids'][:200]
            seed['asks'] = seed['asks'][:200]
            bid_floor, ask_ceiling = Decimal(seed['bids'][-1][0]), Decimal(seed['asks'][-1][0])
            outside = [row for row in update if (
                Decimal(str(row['price'])) < bid_floor if row['side'] == 'bid'
                else Decimal(str(row['price'])) > ask_ceiling)]
            if not outside:
                continue
            payload = json.dumps(seed).encode()
            actual_exchange_clock = parse_book_integer(update[0]['event_time'])
            sampler = BookSampler(tmp_path / 'grid', 'perp')
            vendor._bind_boundary_seed(sampler, payload, update, actual_exchange_clock, 'perp')
            assert sampler.book is not None and sampler.book.verified
            assert sampler.book.event_ms == actual_exchange_clock
            assert sampler.book.last == parse_book_integer(first['last_update_id'])
            for row in outside:
                price, quantity = Decimal(str(row['price'])), Decimal(str(row['quantity']))
                levels = sampler.book.bids if row['side'] == 'bid' else sampler.book.asks
                observed = sampler.book.bid_observed if row['side'] == 'bid' else sampler.book.ask_observed
                assert levels.get(price, Decimal(0)) == quantity
                assert price in observed or (price >= sampler.book.bid_floor if row['side'] == 'bid' else price <= sampler.book.ask_ceiling)
            assert actual_exchange_clock != parse_book_integer(first['event_time'])
            with pytest.raises(SourceError) as failure:
                vendor._bind_boundary_seed(sampler, payload, update, actual_exchange_clock + 1, 'perp')
            assert failure.value.code == 'BOOK_VENDOR_SEED_MISSING'
            break
    else:
        pytest.fail('Original input has no same-ID update/snapshot pair.')


@pytest.fixture()
def rejection_runtime(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[SourceRuntime]:
    from origo.sources import capacity

    market = original_vendor_archive[0]
    spec = BINANCE_SPOT_BOOK_SPEC if market == 'spot' else BINANCE_PERP_BOOK_SPEC
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    # Control only capacity metadata; replay and canonical operations stay real.
    monkeypatch.setattr(capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),))
    monkeypatch.setattr(capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7))
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup()
        yield runtime
    finally:
        client.disconnect()


def test_rejected_book_revision_identity_and_retry(
    rejection_runtime: SourceRuntime,
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = rejection_runtime
    adapter = runtime.spec.canonical
    assert isinstance(adapter, vendor.CryptoHFTBookHourly)
    archives = original_historical_archives[adapter.market]
    partition = archives[1].partition
    visited, rewritten = _transport(archives, adapter.market, monkeypatch)
    path = vendor._dependency_path(adapter.market, partition)
    replay = vendor.replay_hour
    attempts: list[str] = []
    fault = ['BOOK_SEQUENCE_GAP']

    def rejected(*args: object, **kwargs: object) -> vendor.Revision:
        attempts.append(fault[0])
        raise SourceError(fault[0], 'Deliberate replay verdict fault; original vendor bytes unchanged.')

    monkeypatch.setattr(vendor, 'replay_hour', rejected)

    def retry() -> None:
        with pytest.raises(SourceError) as failure:
            _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
        assert failure.value.code == fault[0]

    # Discovery and fetch can observe different provider ETags.
    discovered = adapter.discover(partition)
    rewritten[vendor._file(adapter.market, partition)] = 'controlled-fetch-time-etag'
    # Backfill does not enroll previously unrequested failed hours in audit.
    retry()
    assert runtime.store.execute('SELECT count() FROM origo.source_discovery_log') == [(0,)]
    assert _execute_operation(runtime, 'audit', SourceRunConfig()) == {'corrected_partitions': []}
    assert len(attempts) == 1
    value = json.loads(path.read_text())
    assert value['current'] == vendor._identity(adapter.market, partition, Response(archives[1].body,
        {**archives[1].headers, 'ETag': 'controlled-fetch-time-etag'}, 200))
    assert not adapter.rejected_revision(partition, discovered)
    assert value['dependencies'] == [[a.partition.key, vendor._identity(adapter.market, a.partition, Response(a.body, a.headers, 200))]
                                     for a in (archives[0], archives[2])]
    assert not tuple(path.parent.glob('hour-*'))
    runtime.discover(partition)
    assert adapter.rejected_revision(partition, adapter.discover(partition))
    # Explicit native retry bypasses automatic suppression for unchanged inputs.
    retry()
    assert len(attempts) == 2
    for archive, code in zip(archives, ('BOOK_EVENT_STALE', 'BOOK_KNOWN_DEPTH_EXHAUSTED', 'BOOK_SEQUENCE_GAP'), strict=True):
        rewritten[vendor._file(adapter.market, archive.partition)] = 'controlled-provider-etag-' + archive.partition.key
        fault[0] = code
        assert not adapter.rejected_revision(partition, adapter.discover(partition))
        with pytest.raises(ExceptionGroup):
            _execute_operation(runtime, 'audit', SourceRunConfig())
        assert adapter.rejected_revision(partition, adapter.discover(partition))
    before = len(attempts)
    monkeypatch.setattr(vendor, '_REJECTION_POLICY_VERSION', vendor._REJECTION_POLICY_VERSION + 1)
    runtime = replace(runtime, run_id=str(uuid4()), spec=replace(runtime.spec, canonical=vendor.CryptoHFTBookHourly(adapter.market)))
    assert not runtime.spec.canonical.rejected_revision(partition, runtime.discover(partition))
    with pytest.raises(ExceptionGroup):
        _execute_operation(runtime, 'audit', SourceRunConfig())
    assert len(attempts) == before + 1
    before = len(attempts)
    path.unlink()
    with pytest.raises(ExceptionGroup):
        _execute_operation(runtime, 'audit', SourceRunConfig())
    assert len(attempts) == before + 1
    # Incomplete dependency evidence cannot suppress; malformed evidence fails visibly.
    value = json.loads(path.read_text())
    value['dependencies'] = value['dependencies'][:-1]
    path.write_text(json.dumps(value))
    assert not adapter.rejected_revision(partition, adapter.discover(partition))
    path.write_text('{')
    with pytest.raises(ValueError):
        adapter.discover(partition)
    path.unlink()
    for code in ('PROVIDER_TRANSPORT_FAILED', 'BOOK_VENDOR_SCHEMA'):
        fault[0] = code
        retry()
        assert not path.exists()
        assert not adapter.rejected_revision(partition, adapter.discover(partition))
    def interrupted(*args: object, **kwargs: object) -> vendor.Revision:
        raise OSError('Deliberate filesystem interruption, with original vendor bytes unchanged.')

    monkeypatch.setattr(vendor, 'replay_hour', interrupted)
    with pytest.raises(OSError):
        _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
    assert not path.exists()
    frames = vendor._frames

    def early_failure(*args: object, **kwargs: object) -> Iterator[list[dict[str, object]]]:
        raise SourceError('BOOK_SEQUENCE_GAP', 'Deliberate early failure before dependency payloads are fetched.')

    monkeypatch.setattr(vendor, '_frames', early_failure)
    with pytest.raises(SourceError):
        _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
    assert not path.exists()
    monkeypatch.setattr(vendor, '_frames', frames)
    assert visited
    # A real successful replay clears rejected evidence; ordinary activation recovers.
    monkeypatch.setattr(vendor, 'replay_hour', replay)
    if adapter.market == 'spot':
        with pytest.raises(SourceError, match='depth'):
            _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
        assert adapter.rejected_revision(partition, adapter.discover(partition))
    else:
        result = _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
        assert result['partition_key'] == partition.key
        assert 'rejection' not in json.loads(path.read_text())
        assert runtime.store.execute("SELECT failure_key FROM origo.source_failure_log WHERE blocking_scope='PARTITION' GROUP BY failure_key HAVING argMax(event_type,event_time)='FAILED'") == []
        path.write_text('{')
        with pytest.raises(ValueError):
            _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
