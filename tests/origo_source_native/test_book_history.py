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
    OriginalRecording, _original_archive, _fault_columnar,
    original_reconstruction_archives as original_reconstruction_archives,
    original_vendor_archive as original_vendor_archive,
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


def _transport(archives: tuple[OriginalArchive, ...], market: Market, monkeypatch: pytest.MonkeyPatch,
               *, max_prelude_hours: int = 1) -> tuple[list[str], dict[str, str]]:
    # Existing H-1/H/H+1 fixtures own one prefix; expansion owners opt into the production bound.
    monkeypatch.setattr(vendor, 'BOOK_MAX_PRELUDE_HOURS', max_prelude_hours)
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
    # This negative proves the recorded06Z prefix, not exhaustion of all24hours.
    monkeypatch.setattr(vendor, 'BOOK_MAX_PRELUDE_HOURS', 1)
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
    archives = original_historical_archives['perp']
    paths: list[Path] = []
    for archive in archives:
        path = tmp_path / (archive.partition.key + '.bin')
        path.write_bytes(archive.body)
        paths.append(vendor._prepare_archive(path, tmp_path))
    partition = archives[1].partition
    replaced = [False]

    def delayed_collector(events, levels, prices):
        import polars as pl
        selected = events.filter((pl.col('event_type') == 'snapshot')
            & (pl.col('event_time') >= int(partition.start.timestamp()*1000)))['order'][0]
        replaced[0] = True
        # Deliberate collector-clock fault; native IDs and all levels remain original.
        return events.with_columns(pl.when(pl.col('order') == selected)
            .then(pl.lit(int(partition.end.timestamp()*1000)))
            .otherwise(pl.col('event_time')).alias('event_time')), levels, prices

    _fault_columnar(monkeypatch, delayed_collector)
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
        # This identity scenario deliberately exercises all three original dependencies.
        if not kwargs.get('preludes'):
            raise SourceError('BOOK_VENDOR_SEED_MISSING', 'Controlled prefix requirement.')
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
    value['dependencies'] = [['invalid-hour', 'invalid-identity']]
    path.write_text(json.dumps(value))
    rewritten[vendor._file(adapter.market, partition)] = 'changed-current-after-stale-corrupt-evidence'
    assert not adapter.rejected_revision(partition, adapter.discover(partition))
    with pytest.raises(ExceptionGroup):
        _execute_operation(runtime, 'audit', SourceRunConfig())
    assert adapter.rejected_revision(partition, adapter.discover(partition))
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
    prepare = vendor._prepare_archive

    def early_failure(*args: object, **kwargs: object) -> Path:
        raise SourceError('BOOK_SEQUENCE_GAP', 'Deliberate early failure before dependency payloads are fetched.')

    monkeypatch.setattr(vendor, '_prepare_archive', early_failure)
    with pytest.raises(SourceError):
        _execute_operation(runtime, 'canonical', SourceRunConfig(partition_key=partition.key, automatic_capacity=True))
    assert not path.exists()
    monkeypatch.setattr(vendor, '_prepare_archive', prepare)
    assert visited
    if adapter.market == 'spot':
        # A fresh missing dependency uses the same lifecycle suppression and retry contract.
        path.unlink(missing_ok=True)
        monkeypatch.setattr(vendor, 'BOOK_MAX_PRELUDE_HOURS', 24)
        calls = _missing_prelude_transport(archives, monkeypatch)
        fault[0] = 'BOOK_KNOWN_DEPTH_EXHAUSTED'
        retry()
        runtime.discover(partition)
        before_attempts, before_calls = len(attempts), len(calls)
        assert _execute_operation(runtime, 'audit', SourceRunConfig()) == {'corrected_partitions': []}
        assert len(attempts) == before_attempts
        assert all(metadata for _, metadata in calls[before_calls:])
        assert runtime.store.record(partition) is None
        assert runtime.store.execute("SELECT count() FROM (SELECT failure_key FROM origo.source_failure_log WHERE blocking_scope='PARTITION' GROUP BY failure_key HAVING argMax(event_type,event_time)='FAILED')")[0][0] > 0
        retry()
        assert len(attempts) > before_attempts
        _transport(archives, adapter.market, monkeypatch)
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



def _recorded_inputs(values: tuple[OriginalRecording, ...]) -> tuple[OriginalArchive, ...]:
    return tuple(OriginalArchive(vendor.hour_partition(key), body, headers) for key, body, headers in values)


def _assert_reconstructed_hour(revision: vendor.Revision, partition: Partition, expected: str) -> None:
    evidence = json.loads(revision.evidence_json)
    assert len(evidence['grid_evidence']['sealed_minutes']) == 60
    assert evidence['grid_evidence']['terminal_exchange_event_ms'] >= int(partition.end.timestamp() * 1000) - 100
    rows = {20: 0, 200: 0}
    first: dict[int, object] = {}
    last: dict[int, object] = {}
    for row in revision.rows():
        depth = int(str(row[0])); rows[depth] += 1
        first.setdefault(depth, row[1]); last[depth] = row[1]
    assert rows == {20: 36000, 200: 3600}
    assert first == {20: partition.start, 200: partition.start}
    assert last == {20: partition.end - timedelta(milliseconds=100), 200: partition.end - timedelta(seconds=1)}
    minutes = evidence['grid_evidence']['sealed_minutes']
    local = 'local-book-binary-v1:' + hashlib.sha256(json.dumps(
        {'market': evidence['grid_evidence']['market'], 'sealed_minutes': minutes}, separators=(',', ':'),
    ).encode()).hexdigest()
    assert local == expected


def _verify_expanded_identity(adapter: vendor.CryptoHFTBookHourly, partition: Partition,
                              revision: vendor.Revision, archives: tuple[OriginalArchive, ...],
                              rewritten: dict[str, str]) -> None:
    evidence = json.loads(revision.evidence_json)
    expected = [(a.partition.key, hashlib.sha256(a.body).hexdigest())
                for a in archives if a.partition.key < partition.key]
    assert evidence['prelude_input_sha256'] == [list(x) for x in expected]
    assert evidence['following_input_sha256'] == [[archives[-1].partition.key, hashlib.sha256(archives[-1].body).hexdigest()]]
    assert adapter.discover(partition) == revision.key
    for archive in archives:
        file = vendor._file('spot', archive.partition)
        rewritten[file] = 'changed-etag-for-original-unchanged-body'
        assert adapter.discover(partition) != revision.key
        with pytest.raises(SourceError) as failure:
            adapter.revalidate(partition, revision)
        assert failure.value.code == 'OFFICIAL_REVISION_CHANGED'
        rewritten.clear()


def test_update_only_hour_recovers_with_bounded_prefix(
    original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    archives = _recorded_inputs(original_reconstruction_archives['update-only'])
    visited, rewritten = _transport(archives, 'spot', monkeypatch, max_prelude_hours=24)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    adapter = vendor.CryptoHFTBookHourly('spot'); partition = vendor.hour_partition('2025-07-15T20Z')
    attempts: list[tuple[int, str]] = []
    original = vendor.replay_hour

    def observed(path: Path, market: Market, partition: Partition, root: Path, *,
                 preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> vendor.Revision:
        try:
            value = original(path, market, partition, root, preludes=preludes, following=following)
        except SourceError as error:
            attempts.append((len(preludes), error.code))
            raise
        attempts.append((len(preludes), 'accepted'))
        return value

    monkeypatch.setattr(vendor, 'replay_hour', observed)
    revision = adapter.fetch(partition)
    assert attempts == [(0, 'BOOK_KNOWN_DEPTH_EXHAUSTED'), (1, 'BOOK_KNOWN_DEPTH_EXHAUSTED'), (2, 'accepted')]
    _assert_reconstructed_hour(revision, partition, 'local-book-binary-v1:feb1c48b8a290bc7797075469d77c51bd1715e1710a57196afd9fef69f73d1c4')
    assert len(tuple((tmp_path / 'locks' / 'book_vendor_spot').glob('hour-*'))) == 1
    assert set(visited) == {vendor._file('spot', a.partition) for a in archives}
    _verify_expanded_identity(adapter, partition, revision, archives[:2] + archives[3:], rewritten)
    # Repeated component/hash reads retain exactly the accepted scratch grid.
    assert len(tuple(revision.rows())) == 39600


@pytest.mark.parametrize('anchor_kind', ['update', 'snapshot'])
def test_zero_prelude_opening_gap_fetches_the_real_bridge(
    original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, anchor_kind: str,
) -> None:
    import polars as pl

    case = 'update-only' if anchor_kind == 'update' else 'shallow'
    archives = list(_recorded_inputs(original_reconstruction_archives[case]))
    current_index = 2 if anchor_kind == 'update' else 5
    prior_index = current_index - 1
    partition = archives[current_index].partition
    start = int(partition.start.timestamp() * 1000)
    frames = []
    for index in (0, prior_index, current_index):
        root = tmp_path / str(index)
        root.mkdir()
        path = root / 'input'
        path.write_bytes(archives[index].body)
        frames.append(pl.read_parquet(vendor._prepare_archive(path, root)))
    prefix, prior, current = frames
    updates = prior.filter(pl.col('event_type') == 'update')
    anchor_id = updates['final_update_id'].unique().sort()[-2]
    anchor = updates.filter(pl.col('final_update_id') == anchor_id)
    if anchor_kind == 'snapshot':
        snapshots = prefix.filter(pl.col('event_type') == 'snapshot')
        anchor = snapshots.filter(pl.col('received_time') == snapshots['received_time'].max())
    bridge = current.filter(pl.col('event_time') < start)
    assert anchor.height and bridge.height
    # Move complete genuine messages across the file boundary; values/clocks/IDs stay exact.
    for index, frame in (
        (prior_index, pl.concat([prior, bridge])),
        (current_index, pl.concat([anchor, current.filter(pl.col('event_time') >= start)])),
    ):
        path = tmp_path / f'reassociated-{index}.parquet'
        frame.write_parquet(path)
        archives[index] = replace(archives[index], body=path.read_bytes())
    _transport(tuple(archives), 'spot', monkeypatch, max_prelude_hours=24)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    attempts: list[tuple[int, str]] = []
    replay = vendor.replay_hour

    def observed(path: Path, market: Market, partition: Partition, root: Path, *,
                 preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> vendor.Revision:
        try:
            revision = replay(path, market, partition, root, preludes=preludes, following=following)
        except SourceError as error:
            attempts.append((len(preludes), error.code))
            raise
        attempts.append((len(preludes), 'accepted'))
        return revision

    monkeypatch.setattr(vendor, 'replay_hour', observed)
    revision = vendor.CryptoHFTBookHourly('spot').fetch(partition)
    expected = [(0, 'BOOK_VENDOR_SEED_MISSING'), (1, 'BOOK_KNOWN_DEPTH_EXHAUSTED')]
    if anchor_kind == 'snapshot':
        expected.append((2, 'BOOK_KNOWN_DEPTH_EXHAUSTED'))
    expected.append((2 if anchor_kind == 'update' else 4, 'accepted'))
    assert attempts == expected
    _assert_reconstructed_hour(
        revision, partition,
        'local-book-binary-v1:' + (
            'feb1c48b8a290bc7797075469d77c51bd1715e1710a57196afd9fef69f73d1c4'
            if anchor_kind == 'update' else
            '58dcc9d61aec399f5dde8036cf74e0091e45af455e17c191b18da4c87a210b9a'
        ),
    )


def test_shallow_original_seed_recovers_without_fabricated_depth(
    original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from .test_book_vendor import _checkpoint_payload
    from origo.workers.book_capture import DiffBook
    recordings = original_reconstruction_archives['shallow']
    archives = _recorded_inputs(recordings)
    seed_path = tmp_path / 'actual100.bin'; seed_path.write_bytes(archives[0].body)
    seed_path = vendor._prepare_archive(seed_path, tmp_path)
    frame = next(frame for frame in vendor._frames((seed_path,), legacy=True) if frame[0]['event_type'] == 'snapshot')
    payload = _checkpoint_payload(frame)
    assert len(json.loads(payload)['bids']) == len(json.loads(payload)['asks']) == 100
    with pytest.raises(SourceError) as failure:
        DiffBook('spot', payload)
    assert failure.value.code == 'BOOK_SEED_TOO_SHALLOW'
    partition = vendor.hour_partition('2026-07-31T17Z')
    warmup = DiffBook('spot', payload, archive_start_ms=int(partition.start.timestamp() * 1000))
    with pytest.raises(SourceError) as unknown:
        warmup.top(200)
    assert unknown.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    visited, rewritten = _transport(archives[1:], 'spot', monkeypatch, max_prelude_hours=24)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    adapter = vendor.CryptoHFTBookHourly('spot')
    attempts: list[tuple[int, str]] = []
    original = vendor.replay_hour

    def observed(path: Path, market: Market, partition: Partition, root: Path, *,
                 preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> vendor.Revision:
        try:
            value = original(path, market, partition, root, preludes=preludes, following=following)
        except SourceError as error:
            attempts.append((len(preludes), error.code))
            raise
        attempts.append((len(preludes), 'accepted'))
        return value

    monkeypatch.setattr(vendor, 'replay_hour', observed)
    revision = adapter.fetch(partition)
    assert attempts == [(0, 'BOOK_KNOWN_DEPTH_EXHAUSTED'), (1, 'BOOK_KNOWN_DEPTH_EXHAUSTED'), (2, 'BOOK_KNOWN_DEPTH_EXHAUSTED'), (4, 'accepted')]
    _assert_reconstructed_hour(revision, partition, 'local-book-binary-v1:58dcc9d61aec399f5dde8036cf74e0091e45af455e17c191b18da4c87a210b9a')
    assert set(visited) == {vendor._file('spot', a.partition) for a in archives[1:]}
    selected = tuple(a for a in archives[1:] if a.partition.key != partition.key)
    _verify_expanded_identity(adapter, partition, revision, selected, rewritten)


@pytest.mark.parametrize('case', ['interrupted', 'bounded-exhaustion', 'partial-grid-cleanup'])
def test_archive_fetch_retains_only_completed_rejection(
    case: str, original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    vendor_session_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.workers import book_capture
    assert (vendor.BOOK_MAX_PRELUDE_HOURS, vendor.BOOK_MAX_FOLLOWING_HOURS,
            vendor.CRYPTOHFT_MAX_FILE_BYTES, vendor.BOOK_MAX_PRELUDE_BYTES,
            book_capture.BOOK_MAX_TRACKED_PRICES) == (24, 1, 256 * 1024**2, 1024**3, 100000)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    archives = _recorded_inputs(original_reconstruction_archives['update-only'])
    _transport(archives, 'spot', monkeypatch, max_prelude_hours=24)
    adapter = vendor.CryptoHFTBookHourly('spot'); partition = vendor.hour_partition('2025-07-15T20Z')
    if case == 'interrupted':
        original = vendor.get_response

        def interrupted(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> Response:
            if params == {'file': vendor._file('spot', archives[0].partition)}:
                raise SourceError('PROVIDER_HTTP_503', 'Deliberately interrupted the next original-prefix fetch.')
            return original(url, params=params, headers=headers, weight=weight)

        monkeypatch.setattr(vendor, 'get_response', interrupted)
        with pytest.raises(SourceError) as failure:
            adapter.fetch(partition)
        assert failure.value.code == 'PROVIDER_HTTP_503'
        assert not vendor._dependency_path('spot', partition).exists()
        assert not adapter.rejected_revision(partition, adapter.discover(partition))
        assert not tuple((tmp_path / 'locks').rglob('hour-*'))
    elif case == 'bounded-exhaustion':
        with monkeypatch.context() as limits:
            limits.setattr(vendor, 'BOOK_MAX_PRELUDE_HOURS', 1)
            with pytest.raises(SourceError) as failure:
                adapter.fetch(partition)
            assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
            discovered = adapter.discover(partition)
            assert adapter.rejected_revision(partition, discovered)
            memo = json.loads(vendor._dependency_path('spot', partition).read_text())
            assert memo['rejection']['policy'] == vendor._REJECTION_POLICY_VERSION
            assert [x[0] for x in memo['dependencies']] == ['2025-07-15T19Z', '2025-07-15T21Z']
        assert not tuple((tmp_path / 'locks').rglob('hour-*'))
        current = archives[2]
        with monkeypatch.context() as limits:
            limits.setattr(vendor, 'CRYPTOHFT_MAX_FILE_BYTES', len(current.body) - 1)
            with pytest.raises(SourceError) as file_bound:
                adapter.fetch(partition)
            assert file_bound.value.code == 'BOOK_VENDOR_FILE_BOUND'
        path = tmp_path / 'actual-current.bin'; path.write_bytes(current.body)
        prepared = vendor._prepare_archive(path, tmp_path)
        with monkeypatch.context() as limits:
            limits.setattr(vendor, 'BOOK_MAX_PRELUDE_BYTES', prepared.stat().st_size)
            with pytest.raises(SourceError) as combined_bound:
                adapter.fetch(partition)
            assert combined_bound.value.code == 'BOOK_VENDOR_PRELUDE_BOUND'
        with monkeypatch.context() as limits:
            limits.setattr(book_capture, 'BOOK_MAX_TRACKED_PRICES', 1)
            # The live collector's price cap no longer constrains bounded archive arrays.
            accepted = adapter.fetch(partition)
            assert accepted.row_count == 36000
            assert len(json.loads(accepted.evidence_json)['grid_evidence']['sealed_minutes']) == 60
        assert 'rejection' not in json.loads(vendor._dependency_path('spot', partition).read_text())
    else:
        originals = []
        for key in ('2026-10-04T09Z', '2026-10-04T10Z'):
            _, body, headers = _original_archive(vendor_session_root, 'spot', vendor.hour_partition(key))
            originals.append(OriginalArchive(vendor.hour_partition(key), body, headers))
        _transport(tuple(originals), 'spot', monkeypatch, max_prelude_hours=24)
        partition = originals[0].partition; end = int(partition.end.timestamp() * 1000)
        replay = vendor.replay_hour; written = []

        def missing_tail(events, levels, prices):
            import polars as pl
            return events.filter(pl.col('event_time') < end-1000), levels, prices

        def observed(path: Path, market: Market, partition: Partition, root: Path, *,
                     preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> vendor.Revision:
            try:
                return replay(path, market, partition, root, preludes=preludes, following=following)
            except SourceError:
                written.append(len(tuple(root.rglob('*.rowbinary'))))
                raise

        _fault_columnar(monkeypatch, missing_tail)
        monkeypatch.setattr(vendor, 'replay_hour', observed)
        with pytest.raises(SourceError) as tail:
            adapter.fetch(partition)
        assert tail.value.code == 'BOOK_VENDOR_TAIL_MISSING'
        assert written and all(count == 0 for count in written)
        assert not tuple((tmp_path / 'locks').rglob('hour-*'))
        assert adapter.rejected_revision(partition, adapter.discover(partition))


@pytest.fixture(scope='session')
def original_missing_prelude_archives(vendor_session_root: Path) -> tuple[OriginalArchive, ...]:
    result: list[OriginalArchive] = []
    for hour in range(6, 17):
        partition = vendor.hour_partition(f'2025-06-28T{hour:02}Z')
        _, body, headers = _original_archive(vendor_session_root, 'spot', partition)
        result.append(OriginalArchive(partition, body, headers))
    return tuple(result)


def _missing_prelude_transport(
    archives: tuple[OriginalArchive, ...], monkeypatch: pytest.MonkeyPatch,
    *, error_code: str = 'PROVIDER_HTTP_404', failed_hour: int = 5,
) -> list[tuple[str, bool]]:
    originals = {vendor._file('spot', item.partition): item for item in archives}
    missing = vendor._file('spot', vendor.hour_partition(f'2025-06-28T{failed_hour:02}Z'))
    calls: list[tuple[str, bool]] = []
    monkeypatch.delenv('CRYPTOHFTDATA_API_KEY', raising=False)

    def recorded(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> Response:
        assert url == vendor.CRYPTOHFT_URL and weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        assert isinstance(params, dict)
        file = str(cast(dict[str, object], params)['file'])
        calls.append((file, bool(headers)))
        if file == missing:
            raise SourceError(error_code, f'Observed unavailable archive: {file}')
        original = originals[file]
        return Response(original.body[:1] if headers else original.body,
                        original.headers, 206 if headers else 200)

    monkeypatch.setattr(vendor, 'get_response', recorded)
    return calls


@pytest.mark.parametrize('hour', (7, 8, 13, 15))
def test_missing_prelude_retains_original_replay_rejection(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, hour: int,
) -> None:
    calls = _missing_prelude_transport(original_missing_prelude_archives, monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    adapter = vendor.CryptoHFTBookHourly('spot')
    partition = adapter.partition(f'2025-06-28T{hour:02}Z')
    with pytest.raises(SourceError) as failure:
        adapter.fetch(partition)
    assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    assert 'binance_spot/2025-06-28/05/BTCUSDT_orderbook.parquet (HTTP 404)' in str(failure.value)
    count = len(calls)
    revision = adapter.discover(partition)
    assert adapter.rejected_revision(partition, revision)
    assert calls[count:] and all(metadata for _, metadata in calls[count:])
    assert not tuple(tmp_path.glob('book_vendor_spot/hour-*'))


def test_missing_prelude_rejection_rechecks_absence(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _missing_prelude_transport(original_missing_prelude_archives, monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    adapter = vendor.CryptoHFTBookHourly('spot')
    partition = adapter.partition('2025-06-28T07Z')
    with pytest.raises(SourceError, match='Top depth exceeds the proven region'):
        adapter.fetch(partition)
    rejected = adapter.discover(partition)
    assert adapter.rejected_revision(partition, rejected)
    recorded = vendor.get_response

    for hour in (6, 7, 8):
        def changed_metadata(url: str, *, params: object = None, headers: object = None,
                             weight: int = 0) -> Response:
            response = recorded(url, params=params, headers=headers, weight=weight)
            assert isinstance(params, dict)
            if f'/{hour:02}/' in str(cast(dict[str, object], params)['file']):
                return Response(response.body, {**response.headers, 'ETag': 'deliberate-metadata-change'}, response.status)
            return response

        with monkeypatch.context() as changed:
            changed.setattr(vendor, 'get_response', changed_metadata)
            assert not adapter.rejected_revision(partition, adapter.discover(partition))

    def newly_missing(url: str, *, params: object = None, headers: object = None,
                      weight: int = 0) -> Response:
        assert isinstance(params, dict)
        if '/06/' in str(cast(dict[str, object], params)['file']):
            raise SourceError('PROVIDER_HTTP_404', 'Deliberately removed a previously present object.')
        return recorded(url, params=params, headers=headers, weight=weight)

    with monkeypatch.context() as changed:
        changed.setattr(vendor, 'get_response', newly_missing)
        with pytest.raises(SourceError) as failure:
            adapter.discover(partition)
        assert failure.value.code == 'PROVIDER_HTTP_404' and '/06/' in str(failure.value)

    def available_metadata(url: str, *, params: object = None, headers: object = None,
                           weight: int = 0) -> Response:
        assert isinstance(params, dict)
        if '/05/' in str(cast(dict[str, object], params)['file']):
            assert headers == {'Range': 'bytes=0-0'}
            # Availability/ETag is the controlled transport change; no market rows are invented.
            return Response(b'', original_missing_prelude_archives[0].headers, 206)
        return recorded(url, params=cast(dict[str, str], params),
                        headers=cast(dict[str, str] | None, headers), weight=weight)

    monkeypatch.setattr(vendor, 'get_response', available_metadata)
    changed = adapter.discover(partition)
    assert changed != rejected
    assert not adapter.rejected_revision(partition, changed)


@pytest.mark.parametrize('code', ('PROVIDER_HTTP_401', 'PROVIDER_HTTP_429', 'PROVIDER_TRANSPORT_FAILED'))
def test_missing_prelude_preserves_other_provider_errors(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, code: str,
) -> None:
    _missing_prelude_transport(original_missing_prelude_archives, monkeypatch, error_code=code)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    with pytest.raises(SourceError) as failure:
        vendor.CryptoHFTBookHourly('spot').fetch(vendor.hour_partition('2025-06-28T07Z'))
    assert failure.value.code == code
    assert not vendor._dependency_path('spot', vendor.hour_partition('2025-06-28T07Z')).exists()
    assert not tuple(tmp_path.glob('book_vendor_spot/hour-*'))


@pytest.mark.parametrize('hour', (7, 8))
def test_missing_prelude_does_not_relabel_current_or_following_404(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, hour: int,
) -> None:
    _missing_prelude_transport(original_missing_prelude_archives, monkeypatch, failed_hour=hour)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    with pytest.raises(SourceError) as failure:
        vendor.CryptoHFTBookHourly('spot').fetch(vendor.hour_partition('2025-06-28T07Z'))
    assert failure.value.code == 'PROVIDER_HTTP_404'
    assert not vendor._dependency_path('spot', vendor.hour_partition('2025-06-28T07Z')).exists()
    assert not tuple(tmp_path.glob('book_vendor_spot/hour-*'))


@pytest.mark.parametrize('case', ('rejected', 'recovered', 'uncacheable'))
def test_missing_prelude_attempts_the_last_available_untried_prefix(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    if case == 'rejected':
        _missing_prelude_transport(original_missing_prelude_archives, monkeypatch)
        # Deliberately defer intermediate checkpoints; original market rows stay unchanged.
        monkeypatch.setattr(vendor, '_has_snapshot', lambda path, start: False)
        partition = vendor.hour_partition('2025-06-28T15Z')
        complete_prefix = 9
    else:
        import polars as pl

        archives = list(_recorded_inputs(original_reconstruction_archives['update-only']))
        path = tmp_path / 'original-18.bin'
        path.write_bytes(archives[0].body)
        frame = pl.read_parquet(vendor._prepare_archive(path, tmp_path))
        boundary = int((archives[1].partition.start - timedelta(minutes=1)).timestamp() * 1000)
        # Protocol faults only: move complete genuine messages across an object boundary,
        # then make the older object unavailable. Values, clocks and IDs stay exact.
        parts = []
        for key, selected in (
            ('2025-07-15T17Z', frame.filter(pl.col('event_time') < boundary)),
            ('2025-07-15T18Z', frame.filter(pl.col('event_time') >= boundary)),
        ):
            assert selected.height
            path = tmp_path / f'{key}.parquet'
            selected.write_parquet(path)
            parts.append(OriginalArchive(vendor.hour_partition(key), path.read_bytes(), archives[0].headers))
        _transport(tuple(parts) + tuple(archives[1:]), 'spot', monkeypatch, max_prelude_hours=24)
        recorded = vendor.get_response

        def missing(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> Response:
            assert isinstance(params, dict)
            if '/16/' in str(cast(dict[str, object], params)['file']):
                raise SourceError('PROVIDER_HTTP_404', 'Deliberate unavailable older object.')
            return recorded(url, params=params, headers=headers, weight=weight)

        monkeypatch.setattr(vendor, 'get_response', missing)
        partition = vendor.hour_partition('2025-07-15T20Z')
        complete_prefix = 3
    replay = vendor.replay_hour
    prefixes: list[int] = []

    def observed(path: Path, market: Market, partition: Partition, root: Path, *,
                 preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> vendor.Revision:
        prefixes.append(len(preludes))
        if case == 'uncacheable' and len(preludes) == complete_prefix:
            raise SourceError('BOOK_VENDOR_SEED_MISSING', 'Deliberate noncacheable final verdict.')
        return replay(path, market, partition, root, preludes=preludes, following=following)

    monkeypatch.setattr(vendor, 'replay_hour', observed)
    adapter = vendor.CryptoHFTBookHourly('spot')
    if case == 'recovered':
        revision = adapter.fetch(partition)
        _assert_reconstructed_hour(revision, partition, 'local-book-binary-v1:feb1c48b8a290bc7797075469d77c51bd1715e1710a57196afd9fef69f73d1c4')
        memo = json.loads(vendor._dependency_path('spot', partition).read_text())
        assert 'rejection' not in memo
        assert all(key != '2025-07-15T16Z' for key, _ in memo['dependencies'])
    else:
        with pytest.raises(SourceError) as failure:
            adapter.fetch(partition)
        assert failure.value.code == ('BOOK_KNOWN_DEPTH_EXHAUSTED' if case == 'rejected' else 'BOOK_VENDOR_SEED_MISSING')
        if case == 'uncacheable':
            assert not vendor._dependency_path('spot', partition).exists()
    assert prefixes[-1] == complete_prefix and prefixes.count(complete_prefix) == 1


@pytest.mark.parametrize('operation', ('fetch', 'discover'))
def test_missing_prelude_token_404_stays_an_authentication_failure(
    original_missing_prelude_archives: tuple[OriginalArchive, ...], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    _missing_prelude_transport(original_missing_prelude_archives, monkeypatch)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    adapter = vendor.CryptoHFTBookHourly('spot')
    partition = adapter.partition('2025-06-28T07Z')
    if operation == 'discover':
        with pytest.raises(SourceError) as original_failure:
            adapter.fetch(partition)
        assert original_failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    evidence = vendor._dependency_path('spot', partition)
    before = evidence.read_bytes() if evidence.exists() else None
    originals = {vendor._file('spot', item.partition): item for item in original_missing_prelude_archives}
    from contextvars import ContextVar
    active = ContextVar('archive_partition', default='')
    response = vendor._response
    monkeypatch.setenv('CRYPTOHFTDATA_API_KEY', 'deliberate-test-credential')

    def selected(market: Market, partition: Partition, *, metadata: bool) -> Response:
        marker = active.set(partition.key)
        try:
            return response(market, partition, metadata=metadata)
        finally:
            active.reset(marker)

    def token(key: str, *, rejected: str | None = None) -> str:
        assert key == 'deliberate-test-credential' and rejected is None
        if active.get() == '2025-06-28T05Z':
            raise SourceError('PROVIDER_HTTP_404', 'Token endpoint returned HTTP404.')
        return 'deliberate-test-token'

    def archive(url: str, *, params: object = None, headers: object = None, weight: int = 0) -> Response:
        assert url == vendor.CRYPTOHFT_URL
        assert weight == vendor.CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT
        assert isinstance(params, dict) and isinstance(headers, dict)
        assert headers.get('Authorization') == 'Bearer deliberate-test-token'
        file = str(cast(dict[str, object], params)['file'])
        assert '/05/' not in file  # Token failure occurs before this archive is checked.
        original = originals[file]
        metadata = headers.get('Range') == 'bytes=0-0'
        return Response(b'' if metadata else original.body, original.headers, 206 if metadata else 200)

    monkeypatch.setattr(vendor, '_token_for', token)
    monkeypatch.setattr(vendor, '_response', selected)
    monkeypatch.setattr(vendor, 'get_response', archive)
    with pytest.raises(SourceError) as failure:
        if operation == 'fetch':
            adapter.fetch(partition)
        else:
            adapter.discover(partition)
    assert failure.value.code == 'PROVIDER_HTTP_404'
    assert str(failure.value) == 'Token endpoint returned HTTP404.'
    assert (evidence.read_bytes() if evidence.exists() else None) == before
