from __future__ import annotations

import hashlib
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


@pytest.fixture(scope='session', params=['spot', 'perp'])
def original_vendor_archive(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> tuple[Market, bytes, dict[str, str]]:
    """Real private inputs fetched from the vendor, never committed or published.

    This integration input is deliberately unavailable rather than substituted
    when the actual provider cannot serve it. No Binance request is made.
    """
    market = cast(Market, request.param)
    # Versioned test preparation, not operator configuration. Only this vendor
    # host is contacted, once per market for the complete test session.
    import requests

    partition = vendor.hour_partition(REAL_HOUR)
    response = requests.get(
        vendor.CRYPTOHFT_URL, params={'file': vendor._file(market, partition)}, timeout=(5, 60)
    )
    response.raise_for_status()
    path = tmp_path_factory.mktemp('original-vendor-hour') / 'input.parquet'
    path.write_bytes(response.content)
    return market, path.read_bytes(), dict(response.headers)


@pytest.fixture()
def vendor_transport(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Market, bytes, dict[str, str]]]:
    market, body, headers = original_vendor_archive
    expected_file = vendor._file(market, vendor.hour_partition(REAL_HOUR))

    def captured(
        url: str, *, params: object = None, headers: object = None, weight: int = 0
    ) -> Response:
        assert url == vendor.CRYPTOHFT_URL and params == {'file': expected_file}
        assert weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        return Response(
            body[:1] if headers else body, original_vendor_archive[2], 206 if headers else 200
        )

    monkeypatch.setattr(vendor, 'get_response', captured)
    yield market, body, headers


def test_original_vendor_hour_builds_all_book_grids_without_binance(
    original_vendor_revision: tuple[Market, vendor.Revision],
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
) -> None:
    market, revision = original_vendor_revision
    assert market == original_vendor_archive[0]
    partition = vendor.hour_partition(REAL_HOUR)
    assert revision.content_hash == hashlib.sha256(original_vendor_archive[1]).hexdigest()
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


@pytest.fixture(scope='session')
def original_vendor_revision(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Market, vendor.Revision]:
    market, body, headers = original_vendor_archive

    def captured(
        url: str, *, params: object = None, headers: object = None, weight: int = 0
    ) -> Response:
        assert url == vendor.CRYPTOHFT_URL
        assert params == {'file': vendor._file(market, vendor.hour_partition(REAL_HOUR))}
        assert weight == vendor.CRYPTOHFT_REQUEST_WEIGHT
        return Response(
            body[:1] if headers else body, original_vendor_archive[2], 206 if headers else 200
        )

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path_factory.mktemp('vendor-scratch-locks')))
        patch.setattr(vendor, 'get_response', captured)
        revision = vendor.CryptoHFTBookHourly(market).fetch(vendor.hour_partition(REAL_HOUR))
        assert revision.key == vendor.CryptoHFTBookHourly(market).discover(
            vendor.hour_partition(REAL_HOUR)
        )
    return market, revision


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
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    import json
    from decimal import Decimal
    from origo.workers.book_capture import (
        BOOK_SEED_DEPTH, BookSampler, DepthEvent, DiffBook, parse_book_integer, utc_millisecond,
    )

    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'
    path.write_bytes(body)
    frames = iter(vendor._frames((path,)))
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
        assert first['event_type'] == 'update'
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
        assert sampler.book.top(200) == full.top(200)
        bids, asks = sampler.book.top(200)
        assert sampler.book.top(20) == (bids[:20], asks[:20])
        beyond += bids[-1][0] < original_floor or asks[-1][0] > original_ceiling
        applied += 1
    sampler.finish(int(partition.end.timestamp() * 1000))
    revision = vendor.fetch_sealed_partition(sampler.root, market, partition)
    assert revision.row_count == 36000
    assert applied > 10000
    if market == 'perp':
        assert beyond > 10000
