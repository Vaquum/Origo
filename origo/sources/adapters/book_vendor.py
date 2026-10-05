from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from ..arrow_types import ArrowCompression, ArrowParquet
from ..contracts import ArchiveNotPublishedYet, Partition, Revision, Row, SourceError
from .binance_daily import Response, get_response
from .book_local import fetch_sealed_partition
from .book_spool import Market, atomic_write, object_mapping

CRYPTOHFT_URL = 'https://api.cryptohftdata.com/v1/download'
CRYPTOHFT_MAX_FILE_BYTES = 256 * 1024**2
CRYPTOHFT_BATCH_ROWS = 8192
BOOK_MAX_PRELUDE_HOURS = 24
BOOK_MAX_PRELUDE_BYTES = 1024**3
BOOK_DEPENDENCY_MAX_BYTES = 16 * 1024
# The existing unknown-host transport allows 20 units/second. Charge 40 to
# keep both sources together below the vendor's anonymous 60 requests/minute.
CRYPTOHFT_REQUEST_WEIGHT = 40
# The :15 schedule has five minutes for admission, replay and activation.
BOOK_HOURLY_DELIVERY_GRACE_SECONDS = 20 * 60
BOOK_CANONICAL_ACTIVATION_GRACE_SECONDS = 5 * 60
BOOK_AVAILABILITY_MAX_AGE_SECONDS = 20 * 60
HOUR_KEY_FORMAT = '%Y-%m-%dT%HZ'
_COLUMNS = (
    'received_time',
    'event_time',
    'event_type',
    'first_update_id',
    'final_update_id',
    'prev_final_update_id',
    'last_update_id',
    'symbol',
    'side',
    'price',
    'quantity',
)


def hour_partition(key: str) -> Partition:
    start = datetime.strptime(key, HOUR_KEY_FORMAT).replace(tzinfo=UTC)
    if start.strftime(HOUR_KEY_FORMAT) != key or start.minute or start.second:
        raise ValueError('Canonical book keys require an exact UTC hour.')
    return Partition(key, start, start + timedelta(hours=1))


def _file(market: Market, partition: Partition) -> str:
    exchange = 'binance_spot' if market == 'spot' else 'binance_futures'
    return f'{exchange}/{partition.start:%Y-%m-%d/%H}/BTCUSDT_orderbook.parquet'


def _response(market: Market, partition: Partition, *, metadata: bool) -> Response:
    try:
        return get_response(
            CRYPTOHFT_URL,
            params={'file': _file(market, partition)},
            headers={'Range': 'bytes=0-0'} if metadata else None,
            weight=CRYPTOHFT_REQUEST_WEIGHT,
        )
    except SourceError as error:
        latest = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        if error.code == 'PROVIDER_HTTP_404' and partition.start >= latest:
            raise ArchiveNotPublishedYet(_file(market, partition)) from error
        raise


def _identity(market: Market, partition: Partition, response: Response) -> str:
    etag = response.headers.get('ETag') or response.headers.get('etag')
    if not etag:
        raise SourceError('BOOK_VENDOR_IDENTITY_MISSING', 'Hourly archive has no object ETag.')
    payload = json.dumps([_file(market, partition), etag], separators=(',', ':')).encode()
    return 'cryptohftdata-v1:' + hashlib.sha256(payload).hexdigest()


def _archive_rows(path: Path) -> Iterator[dict[str, object]]:
    parquet = cast(ArrowParquet, importlib.import_module('pyarrow.parquet'))
    archive = parquet.ParquetFile(str(path))
    if not set(_COLUMNS) <= set(archive.schema_arrow.names):
        raise SourceError('BOOK_VENDOR_SCHEMA', 'Hourly archive lacks required order-book columns.')
    for batch in archive.iter_batches(batch_size=CRYPTOHFT_BATCH_ROWS, columns=list(_COLUMNS)):
        for raw in batch.to_pylist():
            yield dict(object_mapping(raw))


def _frame_key(row: dict[str, object]) -> tuple[object, ...]:
    return tuple(row[name] for name in _COLUMNS[:7])


def _prepare_archive(path: Path, root: Path) -> Path:
    with path.open('rb') as stream:
        magic = stream.read(4)
    if magic != b'\x28\xb5\x2f\xfd':
        return path
    arrow = cast(ArrowCompression, importlib.import_module('pyarrow'))
    root.mkdir(parents=True, exist_ok=True)
    target = root / (path.stem + '-unwrapped.parquet')
    total = 0
    with arrow.input_stream(str(path), compression='zstd') as compressed, target.open('wb') as output:
        while chunk := compressed.read(1024**2):
            total += len(chunk)
            if total > CRYPTOHFT_MAX_FILE_BYTES:
                raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Expanded vendor archive exceeds its byte bound.')
            output.write(chunk)
    return target


def _frames(paths: tuple[Path, ...], *, legacy: bool = False) -> Iterator[list[dict[str, object]]]:
    for path in paths:
        # Older archives interleave snapshot and update rows at the same native ID.
        # Reassemble those existing messages without sorting clocks or inventing rows.
        for _, grouped in itertools.groupby(_archive_rows(path), key=(
                (lambda row: row['final_update_id'] or row['last_update_id']) if legacy else _frame_key)):
            rows = list(itertools.islice(grouped, 100001))
            if len(rows) > 100000:
                raise SourceError('BOOK_VENDOR_FRAME_BOUND', 'Hourly archive frame exceeds its bound.')
            if legacy:
                messages: dict[tuple[object, ...], list[dict[str, object]]] = {}
                for row in rows:
                    messages.setdefault(_frame_key(row), []).append(row)
                yield from sorted(messages.values(), key=lambda frame: int(str(frame[0]['received_time'])))
            else:
                yield rows


def _has_snapshot(path: Path, start_ms: int) -> bool:
    return any(row['event_type'] == 'snapshot' and int(str(row['event_time'])) <= start_ms
               for row in _archive_rows(path))


def _dependency_path(market: Market, partition: Partition) -> Path:
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute():
        raise ValueError('Vendor dependency evidence requires the shared absolute lock mount.')
    return root / f'book_vendor_{market}' / (partition.key + '.dependencies.json')


def _dependencies(market: Market, partition: Partition, current: str) -> tuple[tuple[str, str], ...]:
    path = _dependency_path(market, partition)
    if not path.exists():
        return ()
    with path.open('rb') as stream:
        raw = stream.read(BOOK_DEPENDENCY_MAX_BYTES + 1)
    if len(raw) > BOOK_DEPENDENCY_MAX_BYTES:
        raise SourceError('BOOK_VENDOR_DEPENDENCY_BOUND', 'Vendor dependency evidence exceeds its bound.')
    value = object_mapping(json.loads(raw))
    if value.get('current') != current:
        return ()
    items = value.get('dependencies')
    if not isinstance(items, list) or len(cast(list[object], items)) > BOOK_MAX_PRELUDE_HOURS:
        raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency list is invalid.')
    dependencies: list[tuple[str, str]] = []
    for item in cast(list[object], items):
        if not isinstance(item, list) or len(cast(list[object], item)) != 2:
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency entry is invalid.')
        key, identity = cast(list[object], item)
        if not isinstance(key, str) or not isinstance(identity, str):
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency identity is invalid.')
        hour = hour_partition(key)
        if not timedelta(hours=1) <= partition.start - hour.start <= timedelta(hours=BOOK_MAX_PRELUDE_HOURS):
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency is outside the preceding-hour bound.')
        dependencies.append((key, identity))
    if [key for key, _ in dependencies] != sorted({key for key, _ in dependencies}):
        raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependencies require distinct ordered hours.')
    return tuple(dependencies)


def _revision_key(current: str, dependencies: tuple[tuple[str, str], ...]) -> str:
    if not dependencies:
        return current
    return 'cryptohftdata-v1:' + hashlib.sha256(
        json.dumps([current, dependencies], separators=(',', ':')).encode()).hexdigest()


def replay_hour(path: Path, market: Market, partition: Partition, root: Path, *,
                preludes: tuple[Path, ...] = ()) -> Revision:
    from origo.workers.book_capture import (
        BookSampler,
        DepthEvent,
        parse_book_integer,
        utc_millisecond,
    )

    sampler = BookSampler(root, market)
    start_ms, end_ms = (
        int(partition.start.timestamp() * 1000),
        int(partition.end.timestamp() * 1000),
    )
    previous_received = 0
    legacy = partition.start < datetime(2026, 8, 19, tzinfo=UTC)
    for frame in _frames((*preludes, path), legacy=legacy):
        first = frame[0]
        received = parse_book_integer(first['received_time'])
        event_ms = parse_book_integer(first['event_time'])
        if (received < previous_received and not legacy) or any(row['symbol'] != 'BTCUSDT' for row in frame):
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

            # A vendor snapshot carries the exchange clock of its already reconstructed
            # state. A REST seed has no such clock and retains its existing bridge rule.
            seed = {
                'lastUpdateId': parse_book_integer(first['last_update_id']),
                'bids': sorted(sides['bid'], key=lambda v: Decimal(v[0]), reverse=True),
                'asks': sorted(sides['ask'], key=lambda v: Decimal(v[0])),
            }
            if legacy:
                if sampler.book is None:
                    sampler.seed(json.dumps(seed).encode())
                    sampler.next_grid = start_ms
                elif sampler.book.verified and sampler.book.last == seed['lastUpdateId']:
                    # The matching update supplies the real exchange clock for this
                    # REST snapshot. Its legacy event_time is a collector clock.
                    assert sampler.book.event_ms is not None
                    sampler.checkpoint(json.dumps(seed).encode(), sampler.book.event_ms, start_ms)
            else:
                sampler.checkpoint(json.dumps(seed).encode(), event_ms, start_ms)
        elif first['event_type'] == 'update':
            if sampler.book is None:
                if event_ms < start_ms:
                    continue
                raise SourceError(
                    'BOOK_VENDOR_SEED_MISSING', 'Hourly archive has no initial snapshot.'
                )
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
            try:
                sampler.accept(
                    DepthEvent.parse(json.dumps(wire).encode(), market),
                    received_at=utc_millisecond(received // 1000000),
                )
            except SourceError as error:
                if event_ms >= start_ms or error.code != 'BOOK_KNOWN_DEPTH_EXHAUSTED':
                    raise
                # A pre-period book is not output. Resume only from an actual later
                # vendor snapshot; every requested minute must still be complete.
                sampler.invalidate()
        else:
            raise SourceError('BOOK_VENDOR_EVENT', 'Hourly archive has an unknown event type.')
        if event_ms >= end_ms:
            break
    if sampler.book is None or not sampler.book.verified:
        raise SourceError('BOOK_VENDOR_SEED_MISSING', 'Hourly archive has no verified book.')
    sampler.finish(end_ms)
    return fetch_sealed_partition(root, market, partition)


@dataclass(frozen=True)
class CryptoHFTBookHourly:
    market: Market

    def candidate(self, now: datetime) -> Partition:
        start = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        return self.partition(start.strftime(HOUR_KEY_FORMAT))

    def partition(self, key: str) -> Partition:
        return hour_partition(key)

    def discover(self, partition: Partition) -> str:
        current = _identity(self.market, partition, _response(self.market, partition, metadata=True))
        dependencies = _dependencies(self.market, partition, current)
        checked = tuple((key, _identity(self.market, hour_partition(key),
                        _response(self.market, hour_partition(key), metadata=True)))
                        for key, _ in dependencies)
        return _revision_key(current, checked)

    def fetch(self, partition: Partition) -> Revision:
        response = _response(self.market, partition, metadata=False)
        current = _identity(self.market, partition, response)
        if len(response.body) > CRYPTOHFT_MAX_FILE_BYTES:
            raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Hourly archive exceeds its byte bound.')
        scratch = _dependency_path(self.market, partition).parent
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = TemporaryDirectory(prefix='hour-', dir=scratch)
        root = Path(temporary.name)
        archive = root / 'input.parquet'
        archive.write_bytes(response.body)
        dependencies: list[tuple[str, str]] = []
        input_hashes: list[tuple[str, str]] = []
        preludes: list[Path] = []
        try:
            archive = _prepare_archive(archive, root)
            first = next(_frames((archive,)))
            boundary = int(partition.start.timestamp() * 1000)
            if first[0]['event_type'] != 'snapshot' or int(str(first[0]['event_time'])) > boundary:
                used = archive.stat().st_size
                for offset in range(1, BOOK_MAX_PRELUDE_HOURS + 1):
                    before = hour_partition((partition.start - timedelta(hours=offset)).strftime(HOUR_KEY_FORMAT))
                    original = _response(self.market, before, metadata=False)
                    if len(original.body) > CRYPTOHFT_MAX_FILE_BYTES:
                        raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Preceding vendor archive exceeds its byte bound.')
                    prior = root / f'prelude-{offset:02}.parquet'
                    prior.write_bytes(original.body)
                    prior = _prepare_archive(prior, root)
                    used += prior.stat().st_size
                    if used > BOOK_MAX_PRELUDE_BYTES:
                        raise SourceError('BOOK_VENDOR_PRELUDE_BOUND', 'Preceding archives exceed the replay byte bound.')
                    dependencies.insert(0, (before.key, _identity(self.market, before, original)))
                    input_hashes.insert(0, (before.key, hashlib.sha256(original.body).hexdigest()))
                    preludes.insert(0, prior)
                    if _has_snapshot(prior, boundary):
                        break
                else:
                    raise SourceError('BOOK_VENDOR_SEED_MISSING', 'No preceding checkpoint within the 24-hour replay bound.')
            local = replay_hour(archive, self.market, partition, root / 'grid', preludes=tuple(preludes))
            key = _revision_key(current, tuple(dependencies))
            if dependencies:
                payload = json.dumps({'current': current, 'dependencies': dependencies}, separators=(',', ':')).encode()
                if len(payload) > BOOK_DEPENDENCY_MAX_BYTES:
                    raise SourceError('BOOK_VENDOR_DEPENDENCY_BOUND', 'Vendor dependency evidence exceeds its bound.')
                atomic_write(_dependency_path(self.market, partition), payload)
        except BaseException:
            temporary.cleanup()
            raise

        def rows(retained: TemporaryDirectory[str] = temporary) -> Iterator[Row]:
            # The revision retains scratch input for every component/hash pass. It is
            # released with the revision, never mounted as a second authoritative store.
            if not Path(retained.name).exists():
                raise SourceError(
                    'BOOK_VENDOR_SCRATCH_MISSING', 'Hourly replay scratch was removed.'
                )
            yield from local.rows()

        evidence = json.dumps(
            {
                'provider': 'cryptohftdata',
                'file': _file(self.market, partition),
                'etag': response.headers.get('ETag') or response.headers.get('etag'),
                'input_sha256': hashlib.sha256(response.body).hexdigest(),
                'prelude_identities': dependencies,
                'prelude_input_sha256': input_hashes,
                'grid_evidence': json.loads(local.evidence_json),
            },
            separators=(',', ':'),
        )
        return Revision(
            key, (hashlib.sha256(json.dumps([hashlib.sha256(response.body).hexdigest(), input_hashes], separators=(',', ':')).encode()).hexdigest()
                  if input_hashes else hashlib.sha256(response.body).hexdigest()), evidence, local.row_count, rows
        )

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Hourly vendor archive changed during the build.'
            )
