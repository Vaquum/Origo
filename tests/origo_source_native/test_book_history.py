from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from origo.sources.adapters import book_vendor as vendor
from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.book_spool import Market
from origo.sources.contracts import Partition, SourceError


@dataclass(frozen=True)
class OriginalArchive:
    partition: Partition
    body: bytes
    headers: dict[str, str]


@pytest.fixture(scope='session')
def original_historical_archives(tmp_path_factory: pytest.TempPathFactory) -> dict[Market, tuple[OriginalArchive, ...]]:
    import requests

    originals: dict[Market, tuple[OriginalArchive, ...]] = {}
    root = tmp_path_factory.mktemp('original-historical-books')
    for market, hour in (('spot', 7), ('perp', 6)):
        captured: list[OriginalArchive] = []
        for prior in (hour - 1, hour, hour + 1):
            partition = vendor.hour_partition(f'2025-06-28T{prior:02d}Z')
            response = requests.get(vendor.CRYPTOHFT_URL, params={'file': vendor._file(cast(Market, market), partition)}, timeout=(5, 60))
            response.raise_for_status()
            (root / f'{market}-{prior:02d}.bin').write_bytes(response.content)
            captured.append(OriginalArchive(partition, response.content, dict(response.headers)))
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
    assert not tuple((tmp_path / 'locks' / 'book_vendor_spot').iterdir())


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
