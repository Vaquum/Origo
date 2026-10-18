from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import math
import os
import shutil
import struct
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Protocol, cast

from origo.workers.book_capture import BookSampler, Level, utc_millisecond

from ..arrow_types import ArrowCompression, ArrowParquet
from ..contracts import ArchiveNotPublishedYet, Partition, Revision, Row, SourceError
from .binance_daily import Response, get_response
from .book_local import fetch_sealed_partition as fetch_sealed_partition
from .book_spool import Market, atomic_write, object_mapping

CRYPTOHFT_URL = 'https://api.cryptohftdata.com/v1/download'
CRYPTOHFT_MAX_FILE_BYTES = 256 * 1024**2
CRYPTOHFT_BATCH_ROWS = 8192
BOOK_MAX_PRELUDE_HOURS = 24
BOOK_MAX_FOLLOWING_HOURS = 1
BOOK_MAX_PRELUDE_BYTES = 1024**3
BOOK_DEPENDENCY_MAX_BYTES = 16 * 1024
# Only changes to accepted logical output invalidate retained identities.
_REPLAY_POLICY_VERSION = 3
_REJECTION_POLICY_VERSION = 4
_REJECTION_CODES = frozenset({'BOOK_SEQUENCE_GAP', 'BOOK_EVENT_STALE', 'BOOK_KNOWN_DEPTH_EXHAUSTED',
                              'BOOK_VENDOR_CLOCK_UNPROVEN', 'BOOK_VENDOR_TAIL_MISSING'})
# The existing unknown-host transport allows 20 units/second. Charge 40 to
# keep both sources together below the vendor's anonymous 60 requests/minute.
CRYPTOHFT_REQUEST_WEIGHT = 40
CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT = 2
CRYPTOHFT_TOKEN_URL = 'https://api.cryptohftdata.com/v1/jwt-token'
_TOKEN_LOCK = Lock()
_token: tuple[str, str, float] | None = None
# Before availability is observed, the hourly delivery fallback expires at :20.
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

_GRID_RECORDS = {depth: struct.Struct(f'<qQQ{width}{2 * depth}d{width}{2 * depth}d')
                 for depth, width in ((20, 'B'), (200, '2s'))}
_GRID_LENGTHS = {20: 20, 200: b'\xc8\x01'}
_GridSample = tuple[int, int, int, int, tuple[Level, ...], tuple[Level, ...]]


class _BinaryInsert(Protocol):
    def raw_insert(self, table: str, *, insert_block: Iterator[bytes], fmt: str) -> object: ...


class _ArchiveGrid(BookSampler):
    """One minute of typed samples, then per-depth ClickHouse RowBinary scratch."""

    def __init__(self, root: Path, market: Market, start_ms: int) -> None:
        super().__init__(root, market, archive_start_ms=start_ms)
        root.mkdir(parents=True, exist_ok=True)
        self.samples: list[_GridSample] = []
        self.minutes: list[dict[str, object]] = []
        self.digests = {depth: hashlib.sha256() for depth in (20, 200)}

    def invalidate(self) -> None:
        super().invalidate()
        self.samples.clear()

    def _emit(self, depth: int, grid: int, bids: tuple[Level, ...], asks: tuple[Level, ...]) -> None:
        book = self.book
        assert book is not None and book.event_ms is not None
        self.samples.append((depth, grid, book.event_ms, book.last, bids, asks))

    def _seal(self) -> None:
        if self.counts == {20: 600, 200: 60}:
            assert self.book is not None and self.minute is not None and self.first_id is not None
            buffers = {depth: bytearray() for depth in (20, 200)}
            minute_hash = hashlib.sha256()
            for depth, grid, event, last, bids, asks in self.samples:
                bid = tuple(float(value) for level in bids for value in level)
                ask = tuple(float(value) for level in asks for value in level)
                if (any(not math.isfinite(value) or value <= 0 for value in (*bid, *ask))
                        or any(a <= b for a, b in zip(bid[::2], bid[2::2]))
                        or any(a >= b for a, b in zip(ask[::2], ask[2::2])) or bid[0] >= ask[0]):
                    raise SourceError('BOOK_FLOAT64_INVALID', 'Book levels lose validity as Float64.')
                length = _GRID_LENGTHS[depth]
                encoded = _GRID_RECORDS[depth].pack(grid, event, last, length, *bid, length, *ask)
                buffers[depth].extend(encoded)
                minute_hash.update(encoded)
            for depth, buffer in buffers.items():
                self.digests[depth].update(buffer)
                with (self.root / f'depth{depth}.rowbinary').open('ab') as output:
                    output.write(buffer)
            self.minutes.append({
                'market': self.market, 'minute_start': utc_millisecond(self.minute).isoformat(),
                'samples_depth20': 600, 'samples_depth200': 60, 'first_update_id': self.first_id,
                'last_update_id': self.book.last, 'payload_sha256': minute_hash.hexdigest(),
            })
            self.last_seal = utc_millisecond(self.minute + 60000)
        self.samples.clear()
        self.counts = {20: 0, 200: 0}
        self.first_id = None

    def _chunks(self, depth: int) -> Iterator[bytes]:
        digest = hashlib.sha256()
        with (self.root / f'depth{depth}.rowbinary').open('rb') as stream:
            while block := stream.read(_GRID_RECORDS[depth].size * 128):
                digest.update(block)
                yield block
        if digest.digest() != self.digests[depth].digest():
            raise SourceError('BOOK_PAYLOAD_CHANGED', 'Archive grid scratch failed its hash.')

    def _rows(self, depth: int) -> Iterator[Row]:
        for block in self._chunks(depth):
            for raw in _GRID_RECORDS[depth].iter_unpack(block):
                values = cast(tuple[object, ...], raw)
                bid_end = 4 + depth * 2
                bids = [(cast(float, values[i]), cast(float, values[i + 1])) for i in range(4, bid_end, 2)]
                asks = [(cast(float, values[i]), cast(float, values[i + 1]))
                        for i in range(bid_end + 1, len(values), 2)]
                yield (depth, utc_millisecond(cast(int, values[0])), values[1], values[2], bids, asks)

    def revision(self, partition: Partition) -> Revision:
        from heapq import merge

        if ([item['minute_start'] for item in self.minutes]
                != [(partition.start + timedelta(minutes=i)).isoformat() for i in range(60)]):
            raise SourceError('BOOK_MINUTES_MISSING', 'Archive lacks a complete sealed grid.')
        evidence = json.dumps({'market': self.market, 'sealed_minutes': self.minutes}, separators=(',', ':'))
        key = 'local-book-binary-v1:' + hashlib.sha256(evidence.encode()).hexdigest()

        def rows() -> Iterator[Row]:
            yield from merge(self._rows(20), self._rows(200), key=lambda row: cast(datetime, row[1]))

        def insert(table: str) -> None:
            from ..columnar import arrow_client

            depth = {'depth20': 20, 'depth200': 200}[table.rsplit('.', 1)[-1]]
            with arrow_client() as client:
                cast(_BinaryInsert, client).raw_insert(table, insert_block=self._chunks(depth), fmt='RowBinary')

        return Revision(key, key.removeprefix('local-book-binary-v1:'), evidence, 36000, rows, insert_bulk=insert)


def hour_partition(key: str) -> Partition:
    start = datetime.strptime(key, HOUR_KEY_FORMAT).replace(tzinfo=UTC)
    if start.strftime(HOUR_KEY_FORMAT) != key or start.minute or start.second:
        raise ValueError('Canonical book keys require an exact UTC hour.')
    return Partition(key, start, start + timedelta(hours=1))


def _file(market: Market, partition: Partition) -> str:
    exchange = 'binance_spot' if market == 'spot' else 'binance_futures'
    return f'{exchange}/{partition.start:%Y-%m-%d/%H}/BTCUSDT_orderbook.parquet'


def _token_for(key: str, *, rejected: str | None = None) -> str:
    global _token
    with _TOKEN_LOCK:
        if _token is not None and _token[0] == key and time.time() < _token[2] and _token[1] != rejected:
            return _token[1]
        response = get_response(
            CRYPTOHFT_TOKEN_URL, headers={'X-API-Key': key},
            weight=CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT, method='POST',
        )
        try:
            payload = object_mapping(json.loads(response.body))
        except (ValueError, TypeError):
            raise SourceError('BOOK_VENDOR_AUTH_INVALID', 'Provider token response is invalid.') from None
        token = payload.get('jwt_token')
        lifetime = payload.get('expires_in', 14400)
        if (not isinstance(token, str) or not token.strip()
                or not isinstance(lifetime, (int, float)) or isinstance(lifetime, bool)
                or not 0 < lifetime < float('inf')
                or str(payload.get('token_type', 'Bearer')).lower() != 'bearer'):
            raise SourceError('BOOK_VENDOR_AUTH_INVALID', 'Provider token response is invalid.')
        _token = (key, token, time.time() + lifetime - min(300, lifetime / 10))
        return token


def _archive_response(market: Market, partition: Partition, *, metadata: bool,
                      token: str | None) -> Response:
    headers = {'Authorization': f'Bearer {token}'} if token is not None else {}
    if metadata:
        headers['Range'] = 'bytes=0-0'
    try:
        return get_response(
            CRYPTOHFT_URL, params={'file': _file(market, partition)},
            headers=headers or None,
            weight=CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT if token is not None else CRYPTOHFT_REQUEST_WEIGHT,
        )
    except SourceError as error:
        latest = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1 + BOOK_MAX_FOLLOWING_HOURS)
        if error.code == 'PROVIDER_HTTP_404' and partition.start >= latest:
            raise ArchiveNotPublishedYet(_file(market, partition)) from error
        if error.code == 'PROVIDER_HTTP_404':
            raise SourceError(error.code, f'Missing provider archive: {_file(market, partition)}') from error
        raise


def _response(market: Market, partition: Partition, *, metadata: bool) -> Response:
    key = os.environ.get('CRYPTOHFTDATA_API_KEY', '')
    if not key:
        return _archive_response(market, partition, metadata=metadata, token=None)
    token = _token_for(key)
    for attempt in range(2):
        try:
            return _archive_response(market, partition, metadata=metadata, token=token)
        except SourceError as error:
            if error.code != 'PROVIDER_HTTP_401' or attempt:
                raise
        token = _token_for(key, rejected=token)
    raise RuntimeError('Provider authentication attempts exhausted.')


def _identity(market: Market, partition: Partition, response: Response) -> str:
    etag = response.headers.get('ETag') or response.headers.get('etag')
    if not etag:
        raise SourceError('BOOK_VENDOR_IDENTITY_MISSING', 'Hourly archive has no object ETag.')
    payload = json.dumps([_file(market, partition), etag], separators=(',', ':')).encode()
    return 'cryptohftdata-v1:' + hashlib.sha256(payload).hexdigest()


def _archive_rows(path: Path, *, columns: tuple[str, ...] = _COLUMNS) -> Iterator[dict[str, object]]:
    parquet = cast(ArrowParquet, importlib.import_module('pyarrow.parquet'))
    archive = parquet.ParquetFile(str(path))
    if not set(_COLUMNS) <= set(archive.schema_arrow.names):
        raise SourceError('BOOK_VENDOR_SCHEMA', 'Hourly archive lacks required order-book columns.')
    for batch in archive.iter_batches(batch_size=CRYPTOHFT_BATCH_ROWS, columns=list(columns)):
        for raw in batch.to_pylist():
            yield cast(dict[str, object], object_mapping(raw))


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
                # Apply the native update before its same-ID snapshot, even when
                # another collector received the snapshot first.
                yield from sorted(messages.values(), key=lambda frame: (
                    frame[0]['event_type'] == 'snapshot', int(str(frame[0]['received_time']))))
            else:
                yield rows


def _has_snapshot(path: Path, start_ms: int) -> bool:
    return any(row['event_type'] == 'snapshot' and int(str(row['event_time'])) <= start_ms
               for row in _archive_rows(path, columns=('event_type', 'event_time')))


def _nominal_snapshots(paths: tuple[Path, ...]) -> frozenset[tuple[int, int]]:
    from origo.workers.book_capture import parse_book_integer

    markers: set[tuple[int, int]] = set()
    for path in paths:
        for row in _archive_rows(path, columns=('event_type', 'event_time', 'received_time', 'last_update_id')):
            if row['event_type'] != 'snapshot':
                break
            event_ms = parse_book_integer(row['event_time'])
            if event_ms % 3600000 == 0 and parse_book_integer(row['received_time']) == event_ms * 1000000:
                markers.add((event_ms, parse_book_integer(row['last_update_id'])))
    return frozenset(markers)


def _archive_checkpoint(sampler: BookSampler, payload: bytes, event_ms: int, start_ms: int,
                        *, nominal: bool) -> None:
    from origo.workers.book_capture import DiffBook

    book = sampler.book
    if nominal:
        replacement = DiffBook(sampler.market, payload, archive_start_ms=sampler.archive_start_ms)
        if book is not None and book.verified and book.last == replacement.last:
            book.refresh(replacement)
        else:
            replacement.verified = True
            sampler.book = replacement
        if sampler.archive_start_ms is None or event_ms >= start_ms:
            assert sampler.book is not None
            sampler.book.top(200)
        sampler.next_grid = max(sampler.next_grid or start_ms, start_ms)
        return
    if book is not None and book.verified and book.event_ms is None:
        if event_ms > start_ms:
            raise SourceError('BOOK_VENDOR_CLOCK_UNPROVEN', 'Checkpoint has no exchange clock at the initial grid.')
        book.event_ms = event_ms
    sampler.checkpoint(payload, event_ms, start_ms)


def _dependency_path(market: Market, partition: Partition) -> Path:
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute():
        raise ValueError('Vendor dependency evidence requires the shared absolute lock mount.')
    return root / f'book_vendor_{market}' / (partition.key + '.dependencies.json')


def _dependency_evidence(market: Market, partition: Partition) -> dict[str, object]:
    path = _dependency_path(market, partition)
    if not path.exists():
        return {}
    with path.open('rb') as stream:
        raw = stream.read(BOOK_DEPENDENCY_MAX_BYTES + 1)
    if len(raw) > BOOK_DEPENDENCY_MAX_BYTES:
        raise SourceError('BOOK_VENDOR_DEPENDENCY_BOUND', 'Vendor dependency evidence exceeds its bound.')
    return dict(object_mapping(json.loads(raw)))


def _dependencies(market: Market, partition: Partition, current: str) -> tuple[tuple[str, str], ...]:
    return _dependency_entries(partition, current, _dependency_evidence(market, partition))


def _dependency_entries(partition: Partition, current: str, value: dict[str, object]) -> tuple[tuple[str, str], ...]:
    if value.get('current') != current:
        return ()
    items = value.get('dependencies')
    if not isinstance(items, list) or len(cast(list[object], items)) > BOOK_MAX_PRELUDE_HOURS + BOOK_MAX_FOLLOWING_HOURS:
        raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency list is invalid.')
    dependencies: list[tuple[str, str]] = []
    for item in cast(list[object], items):
        if not isinstance(item, list) or len(cast(list[object], item)) != 2:
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency entry is invalid.')
        key, identity = cast(list[object], item)
        if not isinstance(key, str) or not isinstance(identity, str):
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency identity is invalid.')
        hour = hour_partition(key)
        distance = hour.start - partition.start
        if distance == timedelta(0) or not -timedelta(hours=BOOK_MAX_PRELUDE_HOURS) <= distance <= timedelta(hours=BOOK_MAX_FOLLOWING_HOURS):
            raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependency is outside the adjacent-hour bounds.')
        dependencies.append((key, identity))
    if [key for key, _ in dependencies] != sorted({key for key, _ in dependencies}):
        raise SourceError('BOOK_VENDOR_DEPENDENCIES', 'Vendor dependencies require distinct ordered hours.')
    return tuple(dependencies)


def _write_dependencies(market: Market, partition: Partition, current: str,
                        dependencies: list[tuple[str, str]], *, rejected: str | None = None) -> None:
    value: dict[str, object] = {'current': current, 'dependencies': dependencies}
    if rejected is not None:
        value['rejection'] = {'policy': _REJECTION_POLICY_VERSION, 'code': rejected,
                              'revision': _revision_key(current, tuple(dependencies))}
    payload = json.dumps(value, separators=(',', ':')).encode()
    if len(payload) > BOOK_DEPENDENCY_MAX_BYTES:
        raise SourceError('BOOK_VENDOR_DEPENDENCY_BOUND', 'Vendor dependency evidence exceeds its bound.')
    atomic_write(_dependency_path(market, partition), payload)


def _revision_key(current: str, dependencies: tuple[tuple[str, str], ...]) -> str:
    # A policy change must revalidate retained generations as well as new builds.
    return 'cryptohftdata-v2:' + hashlib.sha256(
        json.dumps([_REPLAY_POLICY_VERSION, current, dependencies], separators=(',', ':')).encode()).hexdigest()


def _bind_boundary_seed(sampler: BookSampler, seed: bytes, update: list[dict[str, object]], start_ms: int, market: Market) -> None:
    from decimal import Decimal
    from origo.workers.book_capture import DepthEvent, DiffBook, parse_book_integer

    first = update[0]
    sides: dict[str, dict[str, str]] = {'bid': {}, 'ask': {}}
    for row in update:
        side, price, quantity = str(row['side']), str(row['price']), str(row['quantity'])
        previous = sides[side].setdefault(price, quantity)
        if Decimal(previous) != Decimal(quantity):
            raise SourceError('BOOK_SNAPSHOT_MISMATCH', 'Boundary collectors disagree about an absolute quantity.')
    event = DepthEvent.parse({
        'e': 'depthUpdate', 's': 'BTCUSDT', 'E': first['event_time'],
        'U': first['first_update_id'], 'u': first['final_update_id'], 'pu': first['prev_final_update_id'],
        'b': list(map(list, sides['bid'].items())), 'a': list(map(list, sides['ask'].items())),
    }, market)
    snapshot = DiffBook(market, seed)
    if snapshot.last != event.last or event.event_ms != start_ms:
        raise SourceError('BOOK_VENDOR_SEED_MISSING', 'Initial snapshot does not match the boundary exchange update.')
    if parse_book_integer(first['received_time']) // 1000000 < event.event_ms:
        raise SourceError('BOOK_EVENT_STALE', 'Boundary exchange update is ahead of receipt.')
    for levels, changes, bound, bid in (
            (snapshot.bids, event.bids, snapshot.bid_floor, True),
            (snapshot.asks, event.asks, snapshot.ask_ceiling, False)):
        if any(levels.get(price, Decimal(0)) != quantity for price, quantity in changes
               if (price >= bound if bid else price <= bound)):
            raise SourceError('BOOK_SNAPSHOT_MISMATCH', 'Initial snapshot contradicts its native boundary update.')
    sampler.checkpoint(seed, event.event_ms, start_ms)
    assert sampler.book is not None
    sampler.book.observe_levels(event.bids, event.asks)


def replay_hour(path: Path, market: Market, partition: Partition, root: Path, *,
                preludes: tuple[Path, ...] = (), following: tuple[Path, ...] = ()) -> Revision:
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
    sampler = _ArchiveGrid(root, market, start_ms)

    def anchor(event: DepthEvent, received: int) -> None:
        sampler.invalidate()
        sampler.book = DiffBook(market, event, archive_start_ms=start_ms)
        sampler.next_grid = start_ms
        sampler.last_received = utc_millisecond(received // 1000000)

    previous_received = 0
    boundary_update: list[dict[str, object]] = []
    legacy = partition.start < datetime(2026, 8, 19, tzinfo=UTC)
    paths = (*preludes, path, *following)
    nominal_snapshots: frozenset[tuple[int, int]] = frozenset() if legacy else _nominal_snapshots(paths)
    for frame in _frames(paths, legacy=legacy):
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

            seed = {
                'lastUpdateId': parse_book_integer(first['last_update_id']),
                'bids': sorted(sides['bid'], key=lambda v: Decimal(v[0]), reverse=True),
                'asks': sorted(sides['ask'], key=lambda v: Decimal(v[0])),
            }
            if legacy:
                if sampler.book is None:
                    if boundary_update:
                        _bind_boundary_seed(sampler, json.dumps(seed).encode(), boundary_update, start_ms, market)
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
                if (sampler.book is not None and sampler.book.verified
                        and sampler.book.last != seed['lastUpdateId'] and event_ms >= start_ms):
                    raise SourceError('BOOK_SEQUENCE_GAP', 'Checkpoint skips native updates inside the requested hour.')
                nominal = (event_ms, seed['lastUpdateId']) in nominal_snapshots
                if event_ms % 3600000 == 0 and not nominal:
                    raise SourceError('BOOK_VENDOR_CLOCK_UNPROVEN', 'Hour marker has no witnessed nominal identity.')
                _archive_checkpoint(sampler, json.dumps(seed).encode(), event_ms, start_ms, nominal=nominal)
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
                if legacy and event_ms == start_ms and (
                        not boundary_update or first['final_update_id'] == boundary_update[0]['final_update_id']):
                    if len(boundary_update) + len(frame) > 100000:
                        raise SourceError('BOOK_VENDOR_FRAME_BOUND', 'Boundary seed messages exceed their frame bound.')
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
                    raise SourceError('BOOK_VENDOR_CLOCK_UNPROVEN', 'First exchange witness follows the initial grid.')
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
        raise SourceError('BOOK_VENDOR_CLOCK_UNPROVEN', 'Hourly archive has no real initial exchange witness.')
    if sampler.book.event_ms is None or sampler.book.event_ms < end_ms - 100:
        raise SourceError('BOOK_VENDOR_TAIL_MISSING', 'No exchange update witnesses the final 100-ms sample.')
    sampler.finish(end_ms)
    result = sampler.revision(partition)
    return replace(result, evidence_json=json.dumps({**json.loads(result.evidence_json),
                   'terminal_exchange_event_ms': sampler.book.event_ms,
                   'terminal_update_id': sampler.book.last, 'final_grid_ms': end_ms - 100}, separators=(',', ':')))


@dataclass(frozen=True)
class CryptoHFTBookHourly:
    market: Market

    def candidate(self, now: datetime) -> Partition:
        start = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1 + BOOK_MAX_FOLLOWING_HOURS)
        return self.partition(start.strftime(HOUR_KEY_FORMAT))

    def partition(self, key: str) -> Partition:
        return hour_partition(key)

    def discover(self, partition: Partition) -> str:
        current = _identity(self.market, partition, _response(self.market, partition, metadata=True))
        dependencies = _dependencies(self.market, partition, current)
        after = hour_partition(partition.end.strftime(HOUR_KEY_FORMAT))
        keys = tuple(key for key, _ in dependencies)
        if after.key not in keys:
            keys += (after.key,)
        checked = tuple((key, _identity(self.market, hour_partition(key),
                        _response(self.market, hour_partition(key), metadata=True)))
                        for key in keys)
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
        following_hashes: list[tuple[str, str]] = []
        preludes: list[Path] = []
        try:
            archive = _prepare_archive(archive, root)
            used = archive.stat().st_size
            first = next(_frames((archive,)))
            boundary = int(partition.start.timestamp() * 1000)
            needs_prelude = first[0]['event_type'] != 'snapshot' or int(str(first[0]['event_time'])) > boundary
            after = hour_partition(partition.end.strftime(HOUR_KEY_FORMAT))
            original = _response(self.market, after, metadata=False)
            if len(original.body) > CRYPTOHFT_MAX_FILE_BYTES:
                raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Following vendor archive exceeds its byte bound.')
            tail = root / 'following.parquet'
            tail.write_bytes(original.body)
            tail = _prepare_archive(tail, root)
            used += tail.stat().st_size
            if used > BOOK_MAX_PRELUDE_BYTES:
                raise SourceError('BOOK_VENDOR_PRELUDE_BOUND', 'Adjacent archives exceed the replay byte bound.')
            dependencies.append((after.key, _identity(self.market, after, original)))
            following_hashes.append((after.key, hashlib.sha256(original.body).hexdigest()))
            grid = root / 'grid'
            for offset in range(BOOK_MAX_PRELUDE_HOURS + 1):
                if offset:
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
                    if (offset not in (1, 2, 4, 8, 16, BOOK_MAX_PRELUDE_HOURS)
                            and not _has_snapshot(prior, boundary)):
                        continue
                elif needs_prelude:
                    continue
                try:
                    local = replay_hour(archive, self.market, partition, grid,
                                        preludes=tuple(preludes), following=(tail,))
                except SourceError as error:
                    if grid.exists():
                        shutil.rmtree(grid)
                    if (offset < BOOK_MAX_PRELUDE_HOURS and error.code in {
                            'BOOK_VENDOR_SEED_MISSING', 'BOOK_SEED_TOO_SHALLOW', 'BOOK_KNOWN_DEPTH_EXHAUSTED'}):
                        continue
                    if error.code in _REJECTION_CODES:
                        _write_dependencies(self.market, partition, current, dependencies, rejected=error.code)
                    raise
                else:
                    break
            else:
                raise SourceError('BOOK_VENDOR_SEED_MISSING', 'No complete hour within the bounded replay search.')
            key = _revision_key(current, tuple(dependencies))
            _write_dependencies(self.market, partition, current, dependencies)
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
                'prelude_identities': [(key, identity) for key, identity in dependencies if key < partition.key],
                'following_identities': [(key, identity) for key, identity in dependencies if key > partition.key],
                'prelude_input_sha256': input_hashes,
                'following_input_sha256': following_hashes,
                'grid_evidence': json.loads(local.evidence_json),
            },
            separators=(',', ':'),
        )
        return Revision(
            key, (hashlib.sha256(json.dumps([hashlib.sha256(response.body).hexdigest(), input_hashes, following_hashes], separators=(',', ':')).encode()).hexdigest()
                  if input_hashes or following_hashes else hashlib.sha256(response.body).hexdigest()),
            evidence, local.row_count, rows, insert_bulk=local.insert_bulk,
        )

    def rejected_revision(self, partition: Partition, revision: str) -> bool:
        value = _dependency_evidence(self.market, partition)
        rejection = value.get('rejection')
        if not isinstance(rejection, dict):
            return False
        verdict = object_mapping(rejection)
        if verdict.get('policy') != _REJECTION_POLICY_VERSION or verdict.get('code') not in _REJECTION_CODES:
            return False
        if verdict.get('revision') != revision:
            return False
        current = value.get('current')
        if not isinstance(current, str):
            return False
        dependencies = _dependency_entries(partition, current, value)
        if not dependencies or dependencies[-1][0] != partition.end.strftime(HOUR_KEY_FORMAT):
            return False
        return revision == _revision_key(current, dependencies)

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Hourly vendor archive changed during the build.'
            )
