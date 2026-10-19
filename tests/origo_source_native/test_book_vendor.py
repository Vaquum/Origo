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
from origo.workers.book_capture import BookSampler, DepthEvent
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
    ('spot', '2025-07-15T18Z'): '229093fe0ed893b621c6d5c342ba4675d9cf565d09e0ba55418b1eb6c4ac2a67',
    ('spot', '2025-07-15T19Z'): '4a08e3432e0bd014fc6ab267123c46da8c78ade77653ad21d47cf8c344423dec',
    ('spot', '2025-07-15T20Z'): '3c8d66d7afdae5dce221fbddcd5120e730e200e1770905575a0baac3111c6e06',
    ('spot', '2025-07-15T21Z'): '892e01cee38c3f8dd838b15047c541759410043dd3b8d41f9a47c6f22e9dfbdc',
    ('spot', '2026-07-30T22Z'): 'a42000a537dc3508e7ad39d27868a9b364959f303e94ed450608f02efc2bd501',
    ('spot', '2026-07-31T13Z'): 'dad8c8387a3b23b8c6148c6cb2338567af269b5e7a9b5999b99068987355a721',
    ('spot', '2026-07-31T14Z'): 'a31a9c556fea9344c4efaaf1b41f915224e226816cf611fb35027ad041381bde',
    ('spot', '2026-07-31T15Z'): '9cb366aaee1c32bb4af015fb5ae113f075dbef196287269cb11fb5ee24d9e9f1',
    ('spot', '2026-07-31T16Z'): 'e11927b8d106175e6cfe8f2d4550f6d0667e19b6e7bb86e49938f8e40b6a28e7',
    ('spot', '2026-07-31T17Z'): '1dc5ba57cd0c68b9a699c9e10d2d80ec7e51adc38a2365f443a2ccd1328f5e47',
    ('spot', '2026-07-31T18Z'): '26494276425eb2f929d1a3eae77f34c8683d64b180c61fdba5e189f0f2d7cecd',
    ('spot', '2025-06-28T06Z'): '8f7525c4cbdd1d7e0c8da6b5f5704caf4ce08aeeaf65b55779d397bed2574fc4',
    ('spot', '2025-06-28T07Z'): '3fb37a5e3084b09e56f2a459e5819bb59a562e6b558698d77b4d95f74f69d99d',
    ('spot', '2025-06-28T08Z'): '800bd09c8d96b56123a81c07da39a64c38c55be612901d35c33e94b7c5b45df4',
    ('perp', '2025-06-28T05Z'): '1404ec4c80d8ad7400131d6a5e3fe686226c743a6051fe90d553ac2a035bb789',
    ('perp', '2025-06-28T06Z'): '8404548e0f8e2954a1653ed6c00401d414321a7dbbfcd0ff89439fc1d852eb05',
    ('perp', '2025-06-28T07Z'): 'a69620ed9f26aa848984c25d1589f546ab6c34b279a9cfd298f6aa1a82e5d0ee',
}
_MINUTE_HASHES = {
    'spot': 'd8d884160b44755251b3208e67c91c7e37102cbcfcb73fc7633832e27bbc93ac',
    'perp': '194362e76263a438c157b8b546cc6689c7968f97148f2d725e498e883335b7ef',
}


OriginalRecording = tuple[str, bytes, dict[str, str]]


def _reference_replay_hour(
    path: Path,
    market: Market,
    partition: Partition,
    root: Path,
    *,
    preludes: tuple[Path, ...] = (),
    following: tuple[Path, ...] = (),
) -> vendor.Revision:
    from origo.workers.book_capture import (
        BookSampler,
        DepthEvent,
        DiffBook,
        parse_book_integer,
        utc_millisecond,
    )

    start_ms, end_ms = (
        int(partition.start.timestamp() * 1000),
        int(partition.end.timestamp() * 1000),
    )
    sampler = vendor._ArchiveGrid(root, market, start_ms)

    def anchor(event: DepthEvent, received: int) -> None:
        sampler.invalidate()
        sampler.book = DiffBook(market, event, archive_start_ms=start_ms)
        sampler.next_grid = start_ms
        sampler.last_received = utc_millisecond(received // 1000000)

    previous_received = 0
    boundary_update: list[dict[str, object]] = []
    legacy = partition.start < datetime(2026, 8, 19, tzinfo=UTC)
    paths = (*preludes, path, *following)
    nominal_snapshots: frozenset[tuple[int, int]] = (
        frozenset() if legacy else vendor._nominal_snapshots(paths)
    )
    for frame in vendor._frames(paths, legacy=legacy):
        first = frame[0]
        received = parse_book_integer(first['received_time'])
        event_ms = parse_book_integer(first['event_time'])
        if (received < previous_received and not legacy) or any(
            row['symbol'] != 'BTCUSDT' for row in frame
        ):
            raise SourceError(
                'BOOK_VENDOR_ORDERING', 'Hourly archive has invalid receive ordering or symbol.'
            )
        # Legacy files merge redundant collectors by native update ID. Their
        # receive clocks can interleave; accepted exchange clocks and ID links
        # still have to pass the sampler's continuity and freshness checks.
        if not legacy:
            previous_received = received
        sides: dict[str, list[list[str]]] = {'bid': [], 'ask': []}
        for row in frame:
            side = str(row['side'])
            price, quantity = row['price'], row['quantity']
            if side not in sides or not isinstance(price, str) or not isinstance(quantity, str):
                raise SourceError('BOOK_VENDOR_ROW', 'Hourly archive has invalid level fields.')
            sides[side].append([price, quantity])
        if first['event_type'] == 'snapshot':
            from decimal import Decimal

            seed = {
                'lastUpdateId': parse_book_integer(first['last_update_id']),
                'bids': sorted(sides['bid'], key=lambda v: Decimal(v[0]), reverse=True),
                'asks': sorted(sides['ask'], key=lambda v: Decimal(v[0])),
            }
            if legacy:
                if sampler.book is None:
                    if boundary_update:
                        vendor._bind_boundary_seed(
                            sampler, json.dumps(seed).encode(), boundary_update, start_ms, market
                        )
                        boundary_update.clear()
                    else:
                        sampler.seed(json.dumps(seed).encode())
                        sampler.next_grid = start_ms
                elif sampler.book.verified and sampler.book.last == seed['lastUpdateId']:
                    # The matching update supplies the real exchange clock for this
                    # REST snapshot. Its legacy event_time is a collector clock.
                    assert sampler.book.event_ms is not None
                    sampler.checkpoint(json.dumps(seed).encode(), sampler.book.event_ms, start_ms)
            else:
                if (
                    sampler.book is not None
                    and sampler.book.verified
                    and sampler.book.last != seed['lastUpdateId']
                    and event_ms >= start_ms
                    and not (
                        event_ms == start_ms
                        and sampler.book.event_ms is not None
                        and sampler.book.event_ms < start_ms
                    )
                ):
                    raise SourceError(
                        'BOOK_SEQUENCE_GAP',
                        'Checkpoint skips native updates inside the requested hour.',
                    )
                nominal = (event_ms, seed['lastUpdateId']) in nominal_snapshots
                if event_ms % 3600000 == 0 and not nominal:
                    raise SourceError(
                        'BOOK_VENDOR_CLOCK_UNPROVEN',
                        'Hour marker has no witnessed nominal identity.',
                    )
                vendor._archive_checkpoint(
                    sampler, json.dumps(seed).encode(), event_ms, start_ms, nominal=nominal
                )
                if nominal:
                    continue
            if legacy:
                # A legacy REST snapshot's collector clock is not an exchange
                # watermark and cannot terminate the requested hour.
                continue
        elif first['event_type'] == 'update':
            wire = {
                'e': 'depthUpdate',
                's': 'BTCUSDT',
                'E': event_ms,
                'U': parse_book_integer(first['first_update_id']),
                'u': parse_book_integer(first['final_update_id']),
                'b': sides['bid'],
                'a': sides['ask'],
            }
            if market == 'perp':
                wire['pu'] = parse_book_integer(first['prev_final_update_id'])
            event = DepthEvent.parse(wire, market)
            if event_ms < start_ms and received // 1000000 < event_ms:
                sampler.invalidate()
                continue
            if sampler.book is None:
                if event_ms < start_ms:
                    anchor(event, received)
                    continue
                if (
                    legacy
                    and event_ms == start_ms
                    and (
                        not boundary_update
                        or first['final_update_id'] == boundary_update[0]['final_update_id']
                    )
                ):
                    if len(boundary_update) + len(frame) > 100000:
                        raise SourceError(
                            'BOOK_VENDOR_FRAME_BOUND',
                            'Boundary seed messages exceed their frame bound.',
                        )
                    boundary_update.extend(frame)
                    continue
                raise SourceError(
                    'BOOK_VENDOR_SEED_MISSING', 'Hourly archive has no initial snapshot.'
                )
            clockless = not legacy and sampler.book.verified and sampler.book.event_ms is None
            try:
                applied = sampler.accept(
                    event,
                    received_at=utc_millisecond(received // 1000000),
                )
                if clockless and applied and event_ms > start_ms:
                    raise SourceError(
                        'BOOK_VENDOR_CLOCK_UNPROVEN',
                        'First exchange witness follows the initial grid.',
                    )
            except SourceError as error:
                if event_ms >= start_ms or error.code != 'BOOK_SEQUENCE_GAP':
                    raise
                anchor(event, received)
        else:
            raise SourceError('BOOK_VENDOR_EVENT', 'Hourly archive has an unknown event type.')
        if event_ms >= end_ms:
            break
    if sampler.book is None or not sampler.book.verified:
        raise SourceError('BOOK_VENDOR_SEED_MISSING', 'Hourly archive has no verified book.')
    if not legacy and sampler.book.event_ms is None:
        raise SourceError(
            'BOOK_VENDOR_CLOCK_UNPROVEN', 'Hourly archive has no real initial exchange witness.'
        )
    if sampler.book.event_ms is None or sampler.book.event_ms < end_ms - 100:
        raise SourceError(
            'BOOK_VENDOR_TAIL_MISSING', 'No exchange update witnesses the final 100-ms sample.'
        )
    sampler.finish(end_ms)
    result = sampler.revision(partition)
    return replace(
        result,
        evidence_json=json.dumps(
            {
                **json.loads(result.evidence_json),
                'terminal_exchange_event_ms': sampler.book.event_ms,
                'terminal_update_id': sampler.book.last,
                'final_grid_ms': end_ms - 100,
            },
            separators=(',', ':'),
        ),
    )


def _fault_columnar(monkeypatch, change):
    from origo.sources.adapters import book_columnar

    original = book_columnar._Inputs.ordered

    def ordered(self, paths, *, native_order):
        return change(*original(self, paths, native_order=native_order))

    monkeypatch.setattr(book_columnar._Inputs, 'ordered', ordered)


def _json_reference(path: Path, market: Market, partition: Partition, root: Path, *,
                    following: tuple[Path, ...] = ()) -> vendor.Revision:
    class ReferenceGrid(BookSampler):
        def __init__(self, root: Path, market: Market, start_ms: int) -> None:
            super().__init__(root, market, archive_start_ms=start_ms)

        def revision(self, partition: Partition) -> vendor.Revision:
            return vendor.fetch_sealed_partition(self.root, self.market, partition)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(vendor, '_ArchiveGrid', ReferenceGrid)

        return _reference_replay_hour(path, market, partition, root, following=following)


@pytest.fixture(scope='session')
def original_reconstruction_archives(vendor_session_root: Path) -> dict[str, tuple[OriginalRecording, ...]]:
    cases = {
        'update-only': tuple(f'2025-07-15T{hour:02}Z' for hour in range(18, 22)),
        'shallow': ('2026-07-30T22Z', *(f'2026-07-31T{hour:02}Z' for hour in range(13, 19))),
    }
    result: dict[str, tuple[OriginalRecording, ...]] = {}
    for case, keys in cases.items():
        recordings: list[OriginalRecording] = []
        for key in keys:
            _, body, headers = _original_archive(vendor_session_root, 'spot', vendor.hour_partition(key))
            recordings.append((key, body, headers))
        result[case] = tuple(recordings)
    return result


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
    original_vendor_spool: Path,
    original_vendor_preparation: tuple[Market, vendor.Revision, Path],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
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
    reference = vendor.fetch_sealed_partition(original_vendor_spool, market, partition)
    minutes = json.loads(reference.evidence_json)['sealed_minutes']
    actual_minutes = evidence['grid_evidence']['sealed_minutes']
    assert [(item['first_update_id'], item['last_update_id']) for item in actual_minutes] == [
        (item['first_update_id'], item['last_update_id']) for item in minutes
    ]
    assert len(minutes) == 60
    expected = {
        'spot': 'd5f899d30aec852c58f2612a887d4911fdf090313c456eff0001b6f8fb133461',
        'perp': 'a283738467478da33f96cf9a360c2bf4063c771d31d3fb900dc0ffaafe49e58c',
    }
    assert hashlib.sha256(json.dumps(
        [minute['payload_sha256'] for minute in minutes], separators=(',', ':'),
    ).encode()).hexdigest() == expected[market]
    assert hashlib.sha256(json.dumps(
        [(minute['minute_start'], minute['payload_sha256']) for minute in minutes],
        separators=(',', ':'),
    ).encode()).hexdigest() == _MINUTE_HASHES[market]
    counts = {20: 0, 200: 0}
    first: dict[int, datetime] = {}
    last: dict[int, datetime] = {}
    for row, expected_row in zip(revision.rows(), reference.rows(), strict=True):
        assert row == expected_row
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
    _assert_archive_transport(revision, original_vendor_preparation, tmp_path, monkeypatch)


def _assert_archive_transport(
    revision: vendor.Revision, preparation: tuple[Market, vendor.Revision, Path],
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager
    from origo.sources import columnar

    sizes: list[int] = []

    class BinarySink:
        def raw_insert(self, table: str, *, insert_block: Iterator[bytes], fmt: str) -> None:
            assert fmt == 'RowBinary'
            sizes.append(sum(len(block) for block in insert_block))

    @contextmanager
    def sink() -> Iterator[BinarySink]:
        yield BinarySink()

    def no_rows(self: vendor._ArchiveGrid, depth: int) -> Iterator[vendor.Row]:
        raise AssertionError('Archive insertion decoded rows.')

    with monkeypatch.context() as patch:
        patch.setattr(columnar, 'arrow_client', sink)
        patch.setattr(vendor._ArchiveGrid, '_rows', no_rows)
        assert revision.insert_bulk is not None
        for depth in (20, 200):
            revision.insert_bulk(f'stage.depth{depth}')
    assert sizes == [vendor._GRID_RECORDS[20].size * 36000,
                     vendor._GRID_RECORDS[200].size * 3600]
    for depth in (20, 200):
        _assert_archive_scratch_rejects_corruption(preparation, tmp_path, depth)


def test_hourly_book_partitions_reuse_native_source_jobs() -> None:
    from itertools import islice
    from dagster import Definitions, HourlyPartitionsDefinition
    from dagster._utils.schedules import schedule_execution_time_iterator
    from origo.sources.bundle import build_source_bundle
    from origo.sources.registry import SOURCE_REGISTRY

    for spec in SOURCE_REGISTRY:
        assert spec.partitions.interval == ('hour' if spec.key.endswith('_book') else 'day')
        if not spec.key.endswith('_book'):
            assert spec.orchestration.canonical_cron == (
                '0 4 * * *' if 'spot' in spec.key else '5 8 * * *'
            )
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
        canonical = next(s for s in bundle.schedules if s.name.endswith('_canonical_schedule'))
        assert canonical.cron_schedule == '1-59/5 * * * *'
        assert canonical.execution_timezone == 'UTC'
        ticks = list(islice(schedule_execution_time_iterator(
            datetime(2026, 10, 4, 11, tzinfo=UTC).timestamp(),
            canonical.cron_schedule, canonical.execution_timezone,
        ), 13))
        assert [tick.minute for tick in ticks] == [*range(1, 60, 5), 1]
        assert [spec.canonical.candidate(tick).key for tick in ticks] == (
            [REAL_HOUR] * 12 + ['2026-10-04T10Z']
        )
        assert spec.orchestration.audit_cron == '5,20,35,50 * * * *'
        assert spec.orchestration.provisional_cron == '* * * * *'


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
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path
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
    following = tmp_path / 'untouched-following.parquet'
    following.write_bytes(original_vendor_following_archive[1])
    with pytest.raises(SourceError) as failure:
        vendor.replay_hour(path, market, vendor.hour_partition(REAL_HOUR), tmp_path / 'grid',
                           following=(following,))
    # Updates can now anchor unknown archive depth, but cannot certify this first grid.
    assert failure.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
    assert not tuple((tmp_path / 'grid').rglob('*.rowbinary'))


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
        partition = vendor.hour_partition(REAL_HOUR)
        binary = vendor._ArchiveGrid(grid, market, int(partition.start.timestamp() * 1000))
        binary.minutes = json.loads(str(values[2]))['grid_evidence']['sealed_minutes']
        for depth in (20, 200):
            size = vendor._GRID_RECORDS[depth].size * (600 if depth == 20 else 60)
            with (grid / f'depth{depth}.rowbinary').open('rb') as stream:
                binary.digests[depth] = [hashlib.sha256(block).digest()
                                         for block in iter(lambda: stream.read(size), b'')]
        local = binary.revision(partition)
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


def _assert_archive_scratch_rejects_corruption(
    original_vendor_preparation: tuple[Market, vendor.Revision, Path],
    tmp_path: Path, depth: int,
) -> None:
    """A deliberate length-prefix fault in genuine output must never reach insertion."""
    market, _, original = original_vendor_preparation
    grid = vendor._ArchiveGrid(tmp_path, market, 0)
    path = tmp_path / f'depth{depth}.rowbinary'
    shutil.copyfile(original / path.name, path)
    size = vendor._GRID_RECORDS[depth].size * (600 if depth == 20 else 60)
    with path.open('rb') as stream:
        grid.digests[depth] = [hashlib.sha256(block).digest()
                              for block in iter(lambda: stream.read(size), b'')]
    with path.open('r+b') as stream:
        stream.seek(24)
        value = stream.read(1)
        stream.seek(24)
        stream.write(bytes([value[0] ^ 1]))
    for output in (grid._chunks(depth), grid._rows(depth)):
        with pytest.raises(SourceError, match='Archive grid scratch failed its hash') as failure:
            next(output)
        assert failure.value.code == 'BOOK_PAYLOAD_CHANGED'


@pytest.fixture(scope='session')
def original_vendor_spool(
    original_vendor_preparation: tuple[Market, vendor.Revision, Path],
    vendor_session_root: Path,
) -> Path:
    market = original_vendor_preparation[0]
    root = vendor_session_root / f'live-reference-{market}'
    _json_reference(vendor_session_root / f'vendor-{market}-{REAL_HOUR}.input', market,
                    vendor.hour_partition(REAL_HOUR), root,
                    following=(vendor_session_root / f'vendor-{market}-2026-10-04T10Z.input',))
    return root


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


def test_following_pruning_preserves_preboundary_sequence_fault(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]],
    tmp_path: Path,
) -> None:
    import polars as pl
    from origo.sources.adapters.book_columnar import _Inputs, _KEYS

    market, body, _ = original_vendor_archive
    current = tmp_path / 'current.parquet'
    current.write_bytes(body)
    following = tmp_path / 'following.parquet'
    following.write_bytes(original_vendor_following_archive[1])
    current_scan = pl.scan_parquet(current).filter(pl.col('event_type') == 'update')
    cursor, clock = current_scan.select(
        pl.col('final_update_id').max(), pl.col('event_time').max()
    ).collect().row(0)
    scan = pl.scan_parquet(following).filter(pl.col('event_type') == 'update')
    headers = (
        scan.select(_KEYS).unique(maintain_order=True)
        .filter(pl.col('final_update_id') > cursor)
        .unique(subset='final_update_id', maintain_order=True).collect()
    )
    end = int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
    low = headers.filter(pl.col('event_time') >= end).row(0, named=True)
    high = headers.filter(pl.col('final_update_id') > low['final_update_id']).row(0, named=True)
    prefix = headers.filter(pl.col('received_time') < low['received_time'])
    if prefix.height:
        cursor = max(cursor, prefix['final_update_id'].max())
        clock = max(clock, prefix['event_time'].max())
    assert clock < int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
    assert low['event_time'] >= int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
    assert high['final_update_id'] > low['final_update_id']
    if market == 'perp':
        assert low['prev_final_update_id'] == cursor
        assert high['prev_final_update_id'] != cursor
    else:
        assert low['first_update_id'] <= cursor + 1 <= low['final_update_id']
        assert high['first_update_id'] > cursor + 1
    schema = scan.collect_schema()

    def message(header):
        return scan.filter(
            (pl.col('received_time') == header['received_time'])
            & (pl.col('final_update_id') == header['final_update_id'])
        )

    # Reassociate only genuine clocks/IDs to inject the gap; all original levels remain.
    before = message(high).with_columns(
        pl.lit(low['received_time']).cast(schema['received_time']).alias('received_time'),
        pl.lit(clock).cast(schema['event_time']).alias('event_time'),
    )
    witness = message(low).with_columns(
        pl.lit(high['received_time']).cast(schema['received_time']).alias('received_time')
    )
    damaged = tmp_path / 'gap-following.parquet'
    pl.concat([
        scan.filter(pl.col('received_time') < low['received_time']).collect(),
        before.collect(), witness.collect(),
    ]).write_parquet(damaged)
    with pytest.raises(SourceError) as error:
        vendor.replay_hour(current, market, vendor.hour_partition(REAL_HOUR),
                           tmp_path / 'grid', following=(damaged,))
    assert error.value.code == 'BOOK_SEQUENCE_GAP'
    assert not tuple((tmp_path / 'grid').rglob('*.rowbinary'))

    # Older archives sort by native ID: a valid earlier event can arrive later.
    late = message(low).with_columns(
        pl.lit(high['received_time']).cast(schema['received_time']).alias('received_time'),
        pl.lit(clock).cast(schema['event_time']).alias('event_time'),
    )
    closing = message(high).with_columns(
        pl.lit(low['received_time']).cast(schema['received_time']).alias('received_time')
    )
    late_file = tmp_path / 'late-preboundary.parquet'
    pl.concat([closing.collect(), late.collect()]).write_parquet(late_file)
    retained, _ = _Inputs(market, end).add(late_file, following=True, native_order=True)
    assert set(retained['final_update_id']) == {low['final_update_id'], high['final_update_id']}
    closing_late = message(low).with_columns(
        pl.lit(high['received_time']).cast(schema['received_time']).alias('received_time')
    )
    closing_file = tmp_path / 'late-closing.parquet'
    pl.concat([closing.collect(), closing_late.collect()]).write_parquet(closing_file)
    retained, _ = _Inputs(market, end).add(closing_file, following=True, native_order=True)
    assert set(retained['final_update_id']) == {low['final_update_id']}


def test_snapshot_presence_reads_only_original_required_columns(
    vendor_session_root, tmp_path, monkeypatch,
):
    import pyarrow.parquet as pq

    partition = vendor.hour_partition('2025-06-28T05Z')
    _, body, _ = _original_archive(vendor_session_root, 'perp', partition)
    raw = tmp_path / 'original.parquet'
    raw.write_bytes(body)
    path = vendor._prepare_archive(raw, tmp_path / 'unwrapped')

    import polars as pl

    table = pq.read_table(path)
    observed: list[str] = []
    collect = pl.LazyFrame.collect

    def projected(frame: pl.LazyFrame, *args, **kwargs):
        observed.append(frame.explain(optimized=True))
        return collect(frame, *args, **kwargs)

    for start in (0, int(partition.end.timestamp() * 1000)):

        snapshots = pl.from_arrow(table).filter(pl.col('event_type') == 'snapshot')

        expected = bool((snapshots['event_time'] <= start).any())
        with monkeypatch.context() as patch:
            patch.setattr(pl.LazyFrame, 'collect', projected)
            assert vendor._has_snapshot(path, start) == expected
    assert len(observed) == 2
    assert all(f'PROJECT 2/{table.num_columns} COLUMNS' in plan for plan in observed)
    damaged = tmp_path / 'missing-column.parquet'
    pq.write_table(table.drop(['quantity']), damaged)
    with pytest.raises(SourceError) as failure:
        vendor._has_snapshot(damaged, 0)
    assert failure.value.code == 'BOOK_VENDOR_SCHEMA'

    missing_clock = tmp_path / 'missing-clock.parquet'
    pl.from_arrow(table).with_columns(
        pl.when(pl.col('event_type') == 'snapshot')
        .then(None)
        .otherwise(pl.col('event_time'))
        .alias('event_time')
    ).write_parquet(missing_clock)

    with pytest.raises(ValueError):

        vendor._has_snapshot(missing_clock, 123)


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
    reference = _json_reference(current, market, partition, tmp_path / 'json', following=(following,))
    assert revision.row_count == 36000
    evidence = json.loads(revision.evidence_json)
    assert len(evidence['sealed_minutes']) == 60
    seals = json.loads(reference.evidence_json)['sealed_minutes']
    digest = hashlib.sha256(json.dumps([seal['payload_sha256'] for seal in seals]).encode()).hexdigest()
    assert digest == {
        'spot': 'd84a112d98d4c36287129ce30b4cfad280765b45a7e12e1f7324e5884b354949',
        'perp': '78a1bc9c390297723b6e6d75cf6fa46813de131f8fb12e6750696415f236e626',
    }[market]
    rows = revision.rows()
    first = next(rows)
    assert first[1] == partition.start
    assert (first[2], first[3]) == {
        'spot': (1790287199914, 100634608796), 'perp': (1790287199988, 11650503406048),
    }[market]
    for row, expected in zip(revision.rows(), reference.rows(), strict=True):
        assert row == expected
    assert evidence['terminal_exchange_event_ms'] >= 1790290799900
    assert evidence['final_grid_ms'] == 1790290799900
    if market == 'spot':
        assert (evidence['terminal_exchange_event_ms'], evidence['terminal_update_id']) == (1790290800014, 100636094069)
        assert seals[0]['first_update_id'] == 100634608796


def test_nominal_archive_frame_inventory(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    # Deliberate protocol fault: replace the nominal marker's clock with a real update's receipt.

    def marker_clock(events, levels, prices):
        import polars as pl

        real = events.filter(pl.col('event_type') == 'update')['received_time'][0]
        return (
            events.with_columns(
                pl.when(pl.col('event_type') == 'snapshot')
                .then(pl.lit(real))
                .otherwise(pl.col('received_time'))
                .alias('received_time')
            ),
            levels,
            prices,
        )

    _fault_columnar(monkeypatch, marker_clock)

    with pytest.raises(SourceError) as unproven:
        vendor.replay_hour(current, market, vendor.hour_partition('2026-09-24T22Z'), tmp_path)
    assert unproven.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'


def test_nominal_archive_requires_initial_bridge(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, current, following = nominal_archives
    partition = vendor.hour_partition('2026-09-24T22Z')
    # Withhold the entire current input; the following file's untouched bridge is valid but late.
    with pytest.raises(SourceError, match='initial grid') as late:
        vendor.replay_hour(following, market, partition, tmp_path / 'late')
    assert late.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'

    def prefix(events, levels, prices):
        import polars as pl

        return events.filter(pl.col('event_type') == 'snapshot'), levels, prices

    _fault_columnar(monkeypatch, prefix)

    with pytest.raises(SourceError) as absent:
        vendor.replay_hour(current, market, partition, tmp_path / 'absent')
    assert absent.value.code == 'BOOK_VENDOR_CLOCK_UNPROVEN'


def test_nominal_archive_requires_real_tail(
    nominal_archives: tuple[Market, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, current, following = nominal_archives
    limit = [0]

    def subset(events, levels, prices):
        import polars as pl

        future = events.filter(pl.col('file') == 1).head(1 + limit[0])['order']
        return (
            events.filter((pl.col('file') == 0) | pl.col('order').is_in(future.implode())),
            levels,
            prices,
        )

    _fault_columnar(monkeypatch, subset)

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
    partition = vendor.hour_partition('2026-09-24T22Z')
    first_second = int(partition.start.timestamp() * 1000) + 1000

    def missing_bridge(events, levels, prices):
        import polars as pl

        removed = events.filter(
            (pl.col('event_type') == 'update') & (pl.col('event_time') >= first_second)
        )['order'][0]
        return events.filter(pl.col('order') != removed), levels, prices

    _fault_columnar(monkeypatch, missing_bridge)

    with pytest.raises(SourceError) as gap:
        vendor.replay_hour(current, market, partition, tmp_path, following=(following,))
    assert gap.value.code == 'BOOK_SEQUENCE_GAP'
    assert list(tmp_path.rglob('*.rowbinary')) == []


def test_nominal_archive_real_checkpoint_after_clockless_state(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]],
    original_vendor_following_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
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
    following = tmp_path / 'following.parquet'
    following.write_bytes(original_vendor_following_archive[1])
    next_frame = next(vendor._frames((following,)))
    next_event = parse_book_integer(next_frame[0]['event_time'])
    future_start = start_ms + 7200000
    prelude = BookSampler(tmp_path / 'prelude', market)
    vendor._archive_checkpoint(prelude, payload, event_ms, future_start, nominal=True)
    vendor._archive_checkpoint(prelude, payload, event_ms, future_start, nominal=False)
    assert prelude.book is not None and prelude.book.event_ms == event_ms
    vendor._archive_checkpoint(prelude, _checkpoint_payload(next_frame), next_event, future_start, nominal=True)
    assert prelude.book is not None and prelude.book.verified and prelude.book.event_ms is None
    assert prelude.book.last == parse_book_integer(next_frame[0]['last_update_id'])
    assert prelude.next_grid == future_start and prelude.lines == []


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
    old = 'cryptohftdata-v2:' + hashlib.sha256(json.dumps([2, current, dependencies], separators=(',', ':')).encode()).hexdigest()
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    path = vendor._dependency_path(market, partition)
    vendor.atomic_write(path, json.dumps({'current': current, 'dependencies': dependencies,
        'rejection': {'policy': 2, 'code': 'BOOK_EVENT_STALE', 'revision': old}}).encode())
    adapter = vendor.CryptoHFTBookHourly(market)
    discovered = adapter.discover(partition)
    assert discovered == 'cryptohftdata-v2:' + hashlib.sha256(json.dumps(
        [vendor._REPLAY_POLICY_VERSION, current, dependencies], separators=(',', ':')).encode()).hexdigest()
    assert discovered != old and not adapter.rejected_revision(partition, discovered)
    vendor.atomic_write(path, json.dumps({'current': current, 'dependencies': dependencies,
        'rejection': {'policy': 3, 'code': 'BOOK_EVENT_STALE', 'revision': discovered}}).encode())
    assert adapter.discover(partition) == discovered
    assert not adapter.rejected_revision(partition, discovered)
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
    revision = vendor.replay_hour(paths[0], 'perp', vendor.hour_partition('2026-09-24T00Z'), tmp_path, following=(paths[1],))
    assert revision.row_count == 36000
    assert len(tuple(revision.rows())) == 39600
    assert len(json.loads(revision.evidence_json)['sealed_minutes']) == 60


def test_real_clock_archive_preserves_original_sequence_gap(vendor_session_root: Path, tmp_path: Path) -> None:
    paths = []
    for key in ('2026-09-29T09Z', '2026-09-29T10Z'):
        _original_archive(vendor_session_root, 'spot', vendor.hour_partition(key))
        paths.append(vendor_session_root / f'vendor-spot-{key}.input')
    with pytest.raises(SourceError) as gap:
        vendor.replay_hour(paths[0], 'spot', vendor.hour_partition('2026-09-29T09Z'), tmp_path, following=(paths[1],))
    assert gap.value.code == 'BOOK_SEQUENCE_GAP'


def _recorded_event(frame: list[dict[str, object]], market: Market) -> DepthEvent:
    from origo.workers.book_capture import DepthEvent
    first = frame[0]
    return DepthEvent.parse(json.dumps({
        'e': 'depthUpdate', 's': 'BTCUSDT', 'E': first['event_time'],
        'U': first['first_update_id'], 'u': first['final_update_id'],
        'pu': first['prev_final_update_id'],
        'b': [[row['price'], row['quantity']] for row in frame if row['side'] == 'bid'],
        'a': [[row['price'], row['quantity']] for row in frame if row['side'] == 'ask'],
    }).encode(), market)


def test_quote_witness_matches_original_reference(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    from origo.workers.book_capture import DiffBook
    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'; path.write_bytes(body)
    frames = iter(vendor._frames((path,)))
    payload = _checkpoint_payload(next(frames))
    reference = DiffBook(market, payload); reference.verified = True
    seed = json.loads(payload); seed['bids'] = seed['bids'][:100]; seed['asks'] = seed['asks'][:100]
    end_ms = int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
    partial = DiffBook(market, json.dumps(seed).encode(), archive_start_ms=end_ms)
    partial.verified = True  # Original archive checkpoint, not an asynchronous REST seed.
    candidates = [partial, None]
    compared = [0, 0]; unknown_with_positive_depth = [False, False]; frame_count = 0
    for frame in frames:
        if frame[0]['event_type'] != 'update':
            continue
        event = _recorded_event(frame, market)
        reference.apply(event); frame_count += 1
        for index, candidate in enumerate(candidates):
            if candidate is None:
                candidate = DiffBook(market, event, archive_start_ms=end_ms)
                candidates[index] = candidate
            else:
                candidate.apply(event)
            try:
                actual = candidate.top(200)
            except SourceError as error:
                assert error.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
                unknown_with_positive_depth[index] |= len(candidate.bids) >= 200 and len(candidate.asks) >= 200
            else:
                assert candidate.last == reference.last
                assert actual == reference.top(200)
                compared[index] += 1
    expected_frames, expected_states = {'spot': (35810, 11530), 'perp': (136338, 131758)}[market]
    assert frame_count == expected_frames
    assert compared == [expected_states, expected_states]
    assert unknown_with_positive_depth == [True, True]


def test_quote_witness_rejects_unknown_or_crossed_state(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    from copy import deepcopy
    from origo.workers.book_capture import DiffBook
    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'; path.write_bytes(body)
    frames = iter(vendor._frames((path,)))
    seed = json.loads(_checkpoint_payload(next(frames)))
    seed['bids'] = seed['bids'][:100]; seed['asks'] = seed['asks'][:100]
    end_ms = int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
    initial = DiffBook(market, json.dumps(seed).encode(), archive_start_ms=end_ms)
    initial.verified = True
    book = deepcopy(initial); deletion_checked = False; cross_checked = False
    for frame in frames:
        if frame[0]['event_type'] != 'update':
            continue
        event = _recorded_event(frame, market)
        if not deletion_checked:
            bids = tuple((p, q) for p, q in event.bids if not q and p < initial.bid_floor - initial.tick)
            asks = tuple((p, q) for p, q in event.asks if not q and p > initial.ask_ceiling + initial.tick)
            if bids or asks:
                prior = (initial.bid_floor, initial.ask_ceiling, dict(initial.bids), dict(initial.asks))
                initial.observe_levels(bids, asks)
                assert prior == (initial.bid_floor, initial.ask_ceiling, initial.bids, initial.asks)
                with pytest.raises(SourceError) as unknown:
                    initial.top(200)
                assert unknown.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
                deletion_checked = True
        # Withhold a genuine bid deletion only when this very native message also
        # introduces a genuine ask crossing that old positive bid. No values/IDs
        # or clocks are invented; the complete original message must still pass.
        bid = book.bid_prices[-1]
        if any(p == bid and not q for p, q in event.bids) and any(q and p <= bid for p, q in event.asks):
            corrupted = replace(event, bids=tuple((p, q) for p, q in event.bids if p != bid))
            fault = deepcopy(book); quantity = fault.bids[bid]
            book.apply(event)
            with pytest.raises(SourceError) as crossed:
                fault.apply(corrupted)
            assert crossed.value.code == 'BOOK_CROSSED'
            assert fault.bids[bid] == quantity
            # The same withheld real deletion also creates a crossed replacement
            # snapshot. Reject it in both preperiod checkpoint paths, before a
            # later complete update could remove the visible cross.
            from origo.workers.book_capture import BookSampler
            payload = json.dumps({'lastUpdateId': fault.last,
                'bids': [[str(p), str(q)] for p, q in sorted(fault.bids.items(), reverse=True)],
                'asks': [[str(p), str(q)] for p, q in sorted(fault.asks.items())]}).encode()
            for nominal in (False, True):
                sampler = BookSampler(tmp_path / str(nominal), market, archive_start_ms=end_ms)
                with pytest.raises(SourceError) as snapshot_cross:
                    vendor._archive_checkpoint(sampler, payload, event.event_ms, end_ms, nominal=nominal)
                assert snapshot_cross.value.code == 'BOOK_CROSSED'
                assert sampler.book is None
            cross_checked = True
        else:
            book.apply(event)
        if deletion_checked and cross_checked:
            break
    assert deletion_checked and cross_checked


def test_archive_reconstruction_keeps_live_seed_strict(
    original_vendor_archive: tuple[Market, bytes, dict[str, str]], tmp_path: Path,
) -> None:
    from origo.workers.book_capture import BookSampler, DiffBook, utc_millisecond
    market, body, _ = original_vendor_archive
    path = tmp_path / 'original.parquet'; path.write_bytes(body)
    frames = iter(vendor._frames((path,)))
    checkpoint = next(frames); seed = json.loads(_checkpoint_payload(checkpoint))
    shallow = {**seed, 'bids': seed['bids'][:100], 'asks': seed['asks'][:100]}
    sampler = BookSampler(tmp_path / 'live', market)
    with pytest.raises(SourceError) as shallow_error:
        sampler.seed(json.dumps(shallow).encode())
    assert shallow_error.value.code == 'BOOK_SEED_TOO_SHALLOW' and sampler.book is None
    # A genuine minimum-depth REST snapshot must still invalidate when native
    # changes leave its proven200-level region; archive inference is never used.
    seed['bids'] = seed['bids'][:200]; seed['asks'] = seed['asks'][:200]
    partition = vendor.hour_partition(REAL_HOUR)
    sampler.checkpoint(json.dumps(seed).encode(), int(str(checkpoint[0]['event_time'])),
                       int(partition.start.timestamp() * 1000))
    assert sampler.archive_start_ms is None
    for frame in frames:
        if frame[0]['event_type'] != 'update':
            continue
        event = _recorded_event(frame, market)
        with pytest.raises(SourceError) as unseeded:
            DiffBook(market, event)
        assert unseeded.value.code == 'BOOK_UNSEEDED'
        try:
            sampler.accept(event, received_at=utc_millisecond(int(str(frame[0]['received_time'])) // 1000000))
        except SourceError as error:
            assert error.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
            assert sampler.book is None
            break
    else:
        pytest.fail('Original native changes did not exhaust the minimum live seed.')


@pytest.mark.parametrize('case', [
    'preperiod-reset', 'requested-gap', 'first-grid-late',
    'first-event-stale', 'event-stale', 'price-grid',
])
def test_archive_warmup_preserves_requested_validation(
    case: str, original_reconstruction_archives: dict[str, tuple[OriginalRecording, ...]],
    vendor_session_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.workers.book_capture import BOOK_PRICE_TICK, DiffBook
    if case == 'price-grid':
        _, body, _ = _original_archive(vendor_session_root, 'spot', vendor.hour_partition(REAL_HOUR))
        path = tmp_path / 'spot.parquet'; path.write_bytes(body)
        end = int(vendor.hour_partition(REAL_HOUR).end.timestamp() * 1000)
        for frame in vendor._frames((path,)):
            if frame[0]['event_type'] == 'update':
                event = _recorded_event(frame, 'spot')
                if any(p % BOOK_PRICE_TICK['perp'] for p, _ in (*event.bids, *event.asks)):
                    with pytest.raises(SourceError) as off_grid:
                        DiffBook('perp', event, archive_start_ms=end)
                    assert off_grid.value.code == 'BOOK_PRICE_GRID'
                    break
        else:
            pytest.fail('Original spot input has no off-perp-grid price.')
        return
    if case in ('first-event-stale', 'event-stale'):
        keys = ('2026-09-24T00Z', '2026-09-24T01Z') if case == 'event-stale' else (
            '2026-09-24T00Z', '2026-09-24T22Z', '2026-09-24T23Z')
        paths = []
        for key in keys:
            _original_archive(vendor_session_root, 'perp', vendor.hour_partition(key))
            paths.append(vendor_session_root / f'vendor-perp-{key}.input')
        if case == 'event-stale':
            revision = vendor.replay_hour(paths[0], 'perp', vendor.hour_partition(keys[0]), tmp_path / 'grid', following=(paths[1],))
            assert revision.row_count == 36000
        else:
            partition = vendor.hour_partition(keys[1])
            baseline = vendor.replay_hour(paths[1], 'perp', partition, tmp_path / 'baseline', following=(paths[2],))
            stale_seen: list[int] = []

            def stale_before_seed(events, levels, prices):
                import polars as pl

                updates = events.filter((pl.col('file') == 0) & (pl.col('event_type') == 'update'))
                age = pl.col('received_time') // 1000000 - pl.col('event_time')
                stale = updates.filter((age < 0) | (age > 5000)).head(1)
                stale_seen.extend(stale['final_update_id'].to_list())
                last = stale['order'][0]
                return (
                    events.filter(
                        (pl.col('file') != 0)
                        | ((pl.col('event_type') == 'update') & (pl.col('order') <= last))
                    ),
                    levels,
                    prices,
                )

            _fault_columnar(monkeypatch, stale_before_seed)

            revision = vendor.replay_hour(paths[1], 'perp', partition, tmp_path / 'grid',
                                          preludes=(paths[0],), following=(paths[2],))
            assert stale_seen and revision.key == baseline.key
            assert len(json.loads(revision.evidence_json)['sealed_minutes']) == 60
        return
    if case == 'first-grid-late':
        _, body, _ = _original_archive(vendor_session_root, 'spot', vendor.hour_partition(REAL_HOUR))
        path = tmp_path / 'original.parquet'; path.write_bytes(body)
        partition = vendor.hour_partition(REAL_HOUR)
        start_ms, end_ms = int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000)
        frames = iter(vendor._frames((path,)))
        initial = next(frames)
        seed = json.loads(_checkpoint_payload(initial))
        seed['bids'] = seed['bids'][:100]; seed['asks'] = seed['asks'][:100]
        initial_prices = {'bid': {level[0] for level in seed['bids']},
                          'ask': {level[0] for level in seed['asks']}}
        candidate = DiffBook('spot', json.dumps(seed).encode(), archive_start_ms=end_ms)
        candidate.verified = True
        for frame in frames:
            if frame[0]['event_type'] != 'update':
                continue
            event = _recorded_event(frame, 'spot'); candidate.apply(event)
            try:
                candidate.top(200)
            except SourceError as unknown:
                assert unknown.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
            else:
                assert event.event_ms > start_ms
                break
        else:
            pytest.fail('Original partial seed never became certifiable after the initial grid.')

        def shallow_initial(events, levels, prices):
            import polars as pl
            from decimal import Decimal

            ranks = {price: index for index, price in enumerate(prices)}
            allowed = {
                side: [ranks[Decimal(p)] for p in values] for side, values in initial_prices.items()
            }
            initial_orders = events.filter(
                (pl.col('event_type') == 'snapshot')
                & (pl.col('native') == initial[0]['last_update_id'])
            )['order']
            filtered = levels.filter(
                ~pl.col('order').is_in(initial_orders.implode())
                | ((pl.col('side') == 0) & pl.col('price').is_in(allowed['bid']))
                | ((pl.col('side') == 1) & pl.col('price').is_in(allowed['ask']))
            )
            return events, filtered, prices

        _fault_columnar(monkeypatch, shallow_initial)

        _, following_body, _ = _original_archive(
            vendor_session_root, 'spot', vendor.hour_partition('2026-10-04T10Z')
        )
        following = tmp_path / 'following.parquet'
        following.write_bytes(following_body)
        with pytest.raises(SourceError) as late:
            vendor.replay_hour(path, 'spot', partition, tmp_path / 'grid', following=(following,))
        assert late.value.code == 'BOOK_KNOWN_DEPTH_EXHAUSTED'
        assert not tuple((tmp_path / 'grid').rglob('*.rowbinary'))
        return
    paths = []
    for key, body, _ in original_reconstruction_archives['update-only']:
        path = tmp_path / f'{key}.bin'; path.write_bytes(body)
        paths.append(vendor._prepare_archive(path, tmp_path))
    partition = vendor.hour_partition('2025-07-15T20Z')
    anchor = vendor.hour_partition('2025-07-15T18Z').start if case == 'preperiod-reset' else partition.start
    gap_start = int(anchor.timestamp() * 1000) + 200
    omitted = []

    def missing_message(events, levels, prices):
        import polars as pl

        missing = (pl.col('event_type') == 'update') & pl.col('event_time').is_between(
            gap_start, gap_start + 1000, closed='left'
        )
        omitted.extend(events.filter(missing)['final_update_id'].to_list())
        return events.filter(~missing), levels, prices

    _fault_columnar(monkeypatch, missing_message)

    if case == 'requested-gap':
        with pytest.raises(SourceError) as gap:
            vendor.replay_hour(paths[2], 'spot', partition, tmp_path / 'grid', preludes=tuple(paths[:2]), following=(paths[3],))
        assert gap.value.code == 'BOOK_SEQUENCE_GAP'
    else:
        revision = vendor.replay_hour(paths[2], 'spot', partition, tmp_path / 'grid', preludes=tuple(paths[:2]), following=(paths[3],))
        assert revision.key == 'local-book-binary-v1:feb1c48b8a290bc7797075469d77c51bd1715e1710a57196afd9fef69f73d1c4'
        assert len(json.loads(revision.evidence_json)['sealed_minutes']) == 60
    assert omitted
