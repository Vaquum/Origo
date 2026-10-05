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
        for prior in (hour - 1, hour):
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
    partition = archives[-1].partition
    standalone = adapter.discover(partition)
    revision = adapter.fetch(partition)
    assert revision.key != standalone
    adapter.revalidate(partition, revision)
    evidence = json.loads(revision.evidence_json)
    assert evidence['input_sha256'] == hashlib.sha256(archives[-1].body).hexdigest()
    assert evidence['prelude_input_sha256'] == [[archives[0].partition.key, hashlib.sha256(archives[0].body).hexdigest()]]
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


def test_original_spot_history_rejects_unproven_depth(
    original_historical_archives: dict[Market, tuple[OriginalArchive, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    archives = original_historical_archives['spot']
    visited, _ = _transport(archives, 'spot', monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    with pytest.raises(SourceError) as failure:
        vendor.CryptoHFTBookHourly('spot').fetch(archives[-1].partition)
    assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    assert set(visited) == {vendor._file('spot', archive.partition) for archive in archives}
    assert not (tmp_path / 'locks' / 'book_vendor_spot').exists()
