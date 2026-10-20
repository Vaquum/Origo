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
from collections import OrderedDict
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Protocol, cast
from urllib.parse import parse_qs, unquote, urlsplit

from dagster import get_dagster_logger

from origo.workers.book_capture import BookSampler, Level, utc_millisecond

from ..arrow_types import ArrowCompression, ArrowParquet
from ..archive import archive_references, archive_session
from ..cleanup import preserve_primary_failure
from ..contracts import ArchiveNotPublishedYet, Partition, Revision, Row, SourceError
from .binance_daily import Response, get_response, provider_connections
from .book_local import fetch_sealed_partition as fetch_sealed_partition
from .book_spool import Market, atomic_write, object_mapping

CRYPTOHFT_URL = 'https://api.cryptohftdata.com/v1/download'
CRYPTOHFT_MAX_FILE_BYTES = 256 * 1024**2
CRYPTOHFT_BATCH_ROWS = 8192
BOOK_MAX_PRELUDE_HOURS = 24
BOOK_MAX_FOLLOWING_HOURS = 1
BOOK_MAX_PRELUDE_BYTES = 1024**3
BOOK_DEPENDENCY_MAX_BYTES = 16 * 1024
BOOK_BACKFILL_BATCH_SIZE = 24
BOOK_BATCH_FILE_COUNT = BOOK_BACKFILL_BATCH_SIZE + BOOK_MAX_PRELUDE_HOURS + 1
BOOK_BATCH_BODY_BYTES = 512 * 1024**2
BOOK_BATCH_PREPARED_BYTES = 2 * BOOK_MAX_PRELUDE_BYTES
# Only changes to accepted logical output invalidate retained identities.
_REPLAY_POLICY_VERSION = 4
# Versions changes to rejection verdict semantics across Python/native replay.
_REJECTION_POLICY_VERSION = 5
_MISSING_DEPENDENCY = 'provider-http-404'
_REJECTION_CODES = frozenset({'BOOK_SEQUENCE_GAP', 'BOOK_EVENT_STALE', 'BOOK_KNOWN_DEPTH_EXHAUSTED',
                              'BOOK_VENDOR_CLOCK_UNPROVEN', 'BOOK_VENDOR_TAIL_MISSING'})
# The existing unknown-host transport allows 20 units/second. Charge 40 to
# keep both sources together below the vendor's anonymous 60 requests/minute.
CRYPTOHFT_REQUEST_WEIGHT = 40
CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT = 2
CRYPTOHFT_TOKEN_URL = 'https://api.cryptohftdata.com/v1/jwt-token'
_TOKEN_LOCK = Lock()
_REFERENCE_LOCK = Lock()
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


class _MissingArchive(SourceError):
    """An archive lookup returned404; authentication failures are separate."""


class _BookBatch:
    def __init__(self, market: Market, root: Path) -> None:
        self.market, self.root = market, root
        self.responses: OrderedDict[tuple[str, str], Response] = OrderedDict()
        self.body_bytes = 0
        self.prepared: OrderedDict[str, tuple[Path, int]] = OrderedDict()
        self.prepared_bytes = 0
        self.active: set[Path] = set()
        self.lock = Lock()

    def response(self, key: tuple[str, str]) -> Response | None:
        with self.lock:
            response = self.responses.get(key)
            if response is not None:
                self.responses.move_to_end(key)
            return response

    def remember(self, key: tuple[str, str], response: Response) -> None:
        etag = response.headers.get('ETag') or response.headers.get('etag') or ''
        if (len(response.body) > CRYPTOHFT_MAX_FILE_BYTES
                or not etag.startswith('"') or not etag.endswith('"')):
            return
        with self.lock:
            previous = self.responses.pop(key, None)
            if previous is not None:
                self.body_bytes -= len(previous.body)
            self.responses[key] = replace(response, headers=dict(response.headers))
            self.body_bytes += len(response.body)
            while (len(self.responses) > BOOK_BATCH_FILE_COUNT
                   or self.body_bytes > BOOK_BATCH_BODY_BYTES):
                _, removed = self.responses.popitem(last=False)
                self.body_bytes -= len(removed.body)

    def prepare(self, body: bytes) -> Path:
        digest = hashlib.sha256(body).hexdigest()
        retained = self.prepared.get(digest)
        if retained is not None:
            self.prepared.move_to_end(digest)
            self.active.add(retained[0])
            return retained[0]
        raw = self.root / (digest + '.input')
        raw.write_bytes(body)
        prepared = _prepare_archive_uncached(raw, self.root)
        if prepared != raw:
            raw.unlink()
        size = prepared.stat().st_size
        self.prepared[digest] = prepared, size
        self.prepared_bytes += size
        self.active.add(prepared)
        self.trim()
        return prepared

    def trim(self) -> None:
        from .book_columnar import _inputs

        for previous in tuple(self.prepared):
            if (self.prepared_bytes <= BOOK_BATCH_PREPARED_BYTES
                    and len(self.prepared) <= BOOK_BATCH_FILE_COUNT):
                break
            candidate, count = self.prepared[previous]
            if candidate not in self.active:
                candidate.unlink()
                del self.prepared[previous]
                self.prepared_bytes -= count
                decoder = _inputs.get()
                if decoder is not None:
                    decoder.forget(candidate)


_batch: ContextVar[_BookBatch | None] = ContextVar('book_backfill_batch', default=None)


class _BinaryInsert(Protocol):
    def raw_insert(self, table: str, *, insert_block: Iterator[bytes], fmt: str) -> object: ...


class _ArchiveGrid(BookSampler):
    """One minute of typed samples, then per-depth ClickHouse RowBinary scratch."""

    def __init__(self, root: Path, market: Market, start_ms: int) -> None:
        super().__init__(root, market, archive_start_ms=start_ms)
        root.mkdir(parents=True, exist_ok=True)
        self.samples: list[_GridSample] = []
        self.minutes: list[dict[str, object]] = []
        self.digests: dict[int, list[bytes]] = {20: [], 200: []}

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
                self.digests[depth].append(hashlib.sha256(buffer).digest())
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
        size = _GRID_RECORDS[depth].size * (600 if depth == 20 else 60)
        with (self.root / f'depth{depth}.rowbinary').open('rb') as stream:
            for digest in self.digests[depth]:
                block = stream.read(size)
                if len(block) != size or hashlib.sha256(block).digest() != digest:
                    raise SourceError('BOOK_PAYLOAD_CHANGED', 'Archive grid scratch failed its hash.')
                yield block
            if stream.read(1):
                raise SourceError('BOOK_PAYLOAD_CHANGED', 'Archive grid scratch has trailing bytes.')

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


def _remember_reference(file: str, response: Response) -> None:
    references = archive_references()
    if references is None:
        return
    key = (CRYPTOHFT_URL, file)
    with _REFERENCE_LOCK:
        references.pop(key, None)
    if response.resource_url is None:
        return
    try:
        target = urlsplit(response.resource_url)
        query = parse_qs(target.query)
        if (target.scheme != 'https' or target.hostname is None
                or not target.hostname.endswith('.r2.cloudflarestorage.com')
                or target.username is not None or target.port not in (None, 443)
                or not unquote(target.path).endswith(('/' + file, '/' + file + '.zst'))
                or any(name.lower() == 'versionid' for name in query)
                or query.get('X-Amz-SignedHeaders') != ['host']
                or len(query.get('X-Amz-Signature', [])) != 1
                or len(query.get('X-Amz-Date', [])) != 1
                or len(query.get('X-Amz-Expires', [])) != 1):
            return
        issued = datetime.strptime(query['X-Amz-Date'][0], '%Y%m%dT%H%M%SZ').replace(tzinfo=UTC)
        deadline = issued.timestamp() + int(query['X-Amz-Expires'][0])
    except (ValueError, OverflowError):
        deadline = 0.0
    if not math.isfinite(deadline) or deadline <= time.time() + 35:
        return
    with _REFERENCE_LOCK:
        if len(references) >= BOOK_MAX_PRELUDE_HOURS + 2:
            references.pop(next(iter(references)))
        references[key] = (response.resource_url, deadline)


def _archive_response(market: Market, partition: Partition, *, metadata: bool,
                      token: str | None) -> Response:
    headers = {'Authorization': f'Bearer {token}'} if token is not None else {}
    if metadata:
        headers['Range'] = 'bytes=0-0'
    file = _file(market, partition)
    references = archive_references()
    resource_url = None
    if metadata and references is not None:
        with _REFERENCE_LOCK:
            reference = references.get((CRYPTOHFT_URL, file))
            # Leave the configured 5+30-second request budget before signature expiry.
            if reference is not None and reference[1] > time.time() + 35:
                resource_url = reference[0]

    def request(target: str | None) -> Response:
        weight = CRYPTOHFT_AUTHENTICATED_REQUEST_WEIGHT if token is not None else CRYPTOHFT_REQUEST_WEIGHT
        if target is None:
            return get_response(CRYPTOHFT_URL, params={'file': file}, headers=headers or None,
                                weight=weight)
        return get_response(CRYPTOHFT_URL, params={'file': file}, headers=headers or None,
                            weight=weight, resource_url=target)

    try:
        try:
            response = request(resource_url)
        except SourceError as error:
            if resource_url is None or error.code not in {
                'PROVIDER_HTTP_401', 'PROVIDER_HTTP_403', 'PROVIDER_HTTP_404',
            }:
                raise
            assert references is not None
            with _REFERENCE_LOCK:
                references.pop((CRYPTOHFT_URL, file), None)
            # A stale storage reference cannot establish official archive absence.
            response = request(None)
        _remember_reference(file, response)
        return response
    except SourceError as error:
        latest = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1 + BOOK_MAX_FOLLOWING_HOURS)
        if error.code == 'PROVIDER_HTTP_404' and partition.start >= latest:
            raise ArchiveNotPublishedYet(_file(market, partition)) from error
        if error.code == 'PROVIDER_HTTP_404':
            raise _MissingArchive(error.code, f'Missing provider archive: {_file(market, partition)}') from error
        raise


def _provider_response(market: Market, partition: Partition, *, metadata: bool) -> Response:
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


def _response(market: Market, partition: Partition, *, metadata: bool) -> Response:
    batch = _batch.get()
    if batch is None or batch.market != market:
        return _provider_response(market, partition, metadata=metadata)
    cache_key = (partition.key, hashlib.sha256(
        os.environ.get('CRYPTOHFTDATA_API_KEY', '').encode()
    ).hexdigest())
    if metadata:
        response = _provider_response(market, partition, metadata=True)
        with batch.lock:
            retained = batch.responses.get(cache_key)
            if retained is not None and _identity(market, partition, retained) != _identity(market, partition, response):
                batch.body_bytes -= len(batch.responses.pop(cache_key).body)
        return response
    retained = batch.response(cache_key)
    if retained is not None:
        return retained
    response = _provider_response(market, partition, metadata=metadata)
    batch.remember(cache_key, response)
    return response


def _identity(market: Market, partition: Partition, response: Response) -> str:
    etag = response.headers.get('ETag') or response.headers.get('etag')
    if not etag:
        raise SourceError('BOOK_VENDOR_IDENTITY_MISSING', 'Hourly archive has no object ETag.')
    payload = json.dumps([_file(market, partition), etag], separators=(',', ':')).encode()
    return 'cryptohftdata-v1:' + hashlib.sha256(payload).hexdigest()


@contextmanager
def _paired_responses(
    market: Market, partition: Partition, following: Partition, *, metadata: bool
) -> Iterator[tuple[Response, Future[Response]]]:
    with ThreadPoolExecutor(max_workers=2) as pool:
        current = pool.submit(copy_context().run, _response, market, partition, metadata=metadata)
        closing = pool.submit(copy_context().run, _response, market, following, metadata=metadata)
        yield current.result(), closing


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
    batch = _batch.get()
    return batch.prepare(path.read_bytes()) if batch is not None else _prepare_archive_uncached(path, root)


def _store_archive(path: Path, root: Path, body: bytes) -> Path:
    batch = _batch.get()
    if batch is not None:
        return batch.prepare(body)
    path.write_bytes(body)
    return _prepare_archive(path, root)


def _prepare_archive_uncached(path: Path, root: Path) -> Path:
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

    import polars as pl

    scan = pl.scan_parquet(path)
    if not set(_COLUMNS) <= set(scan.collect_schema().names()):
        raise SourceError('BOOK_VENDOR_SCHEMA', 'Archive lacks required order-book columns.')
    clocks = (
        scan.filter(pl.col('event_type') == 'snapshot').select('event_time').collect()['event_time']
    )
    if clocks.null_count():
        raise ValueError('Snapshot clock is missing.')
    return bool((clocks <= start_ms).any())


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

    from .book_columnar import replay

    result = replay((*preludes, path, *following), market, partition)

    grid = _ArchiveGrid(root, market, int(partition.start.timestamp() * 1000))
    for depth, payload in zip((20, 200), result.payloads, strict=True):
        (root / f'depth{depth}.rowbinary').write_bytes(payload)
        size = _GRID_RECORDS[depth].size * (600 if depth == 20 else 60)
        grid.digests[depth] = [
            hashlib.sha256(memoryview(payload)[start : start + size]).digest()
            for start in range(0, len(payload), size)
        ]
    grid.minutes = result.minutes
    revision = grid.revision(partition)
    return replace(
        revision,
        evidence_json=json.dumps(
            {
                **json.loads(revision.evidence_json),
                'terminal_exchange_event_ms': result.terminal_ms,
                'terminal_update_id': result.terminal_id,
                'final_grid_ms': int(partition.end.timestamp() * 1000) - 100,
            },
            separators=(',', ':'),
        ),
    )


@dataclass(frozen=True)
class CryptoHFTBookHourly:
    market: Market

    @property
    def backfill_batch_size(self) -> int:
        return BOOK_BACKFILL_BATCH_SIZE

    @contextmanager
    def backfill_batch(self) -> Iterator[None]:
        from .book_columnar import replay_session

        root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
        if not root.is_absolute():
            raise ValueError('Book input reuse requires the shared absolute lock mount.')
        root = root / f'book_vendor_{self.market}'
        root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix='backfill-', dir=root) as directory:
            batch = _BookBatch(self.market, Path(directory))
            token = _batch.set(batch)
            try:
                with archive_session(), provider_connections(), replay_session(self.market):
                    yield
            finally:
                _batch.reset(token)
                batch.responses.clear()
                batch.prepared.clear()
                batch.active.clear()

    def backfill_rejection(self, error: BaseException | None) -> bool:
        return isinstance(error, _MissingArchive) or (
            isinstance(error, SourceError) and error.code in (
                _REJECTION_CODES | {'BOOK_VENDOR_SEED_MISSING', 'BOOK_SEED_TOO_SHALLOW'}
            )
        )

    def candidate(self, now: datetime) -> Partition:
        start = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1 + BOOK_MAX_FOLLOWING_HOURS)
        return self.partition(start.strftime(HOUR_KEY_FORMAT))

    def partition(self, key: str) -> Partition:
        return hour_partition(key)

    def discover(self, partition: Partition) -> str:
        after = hour_partition(partition.end.strftime(HOUR_KEY_FORMAT))
        with _paired_responses(self.market, partition, after, metadata=True) as (opening, closing):
            current = _identity(self.market, partition, opening)
            dependencies = _dependencies(self.market, partition, current)
            keys = tuple(key for key, _ in dependencies)
            if after.key not in keys:
                keys += (after.key,)
            previous = dict(dependencies)
            checked: list[tuple[str, str]] = []
            for key in keys:
                dependency = hour_partition(key)
                try:
                    response = (closing.result() if key == after.key else
                                _response(self.market, dependency, metadata=True))
                except _MissingArchive:
                    if key >= partition.key or previous.get(key) != _MISSING_DEPENDENCY:
                        raise
                    checked.append((key, _MISSING_DEPENDENCY))
                else:
                    checked.append((key, _identity(self.market, dependency, response)))
            return _revision_key(current, tuple(checked))

    def fetch(self, partition: Partition) -> Revision:
        def finish_inputs() -> None:
            batch = _batch.get()
            if batch is not None:
                batch.active.clear()
                batch.trim()
                from .book_columnar import _inputs

                decoder = _inputs.get()
                if decoder is not None:
                    decoder.active_paths.clear()
                    decoder.trim()

        with preserve_primary_failure('book input cache cleanup', finish_inputs):
            return self._fetch(partition)

    def _fetch(self, partition: Partition) -> Revision:
        try:
            value = _dependency_evidence(self.market, partition)
            rejection = value.get('rejection')
            if isinstance(rejection, dict):
                verdict = object_mapping(rejection)
                if self._rejected_revision(partition, self.discover(partition), value):
                    code = str(verdict['code'])
                    get_dagster_logger('origo.sources').info(
                        'market=%s partition=%s phase=fetch_rejected_reused code=%s revision=%s',
                        self.market, partition.key, code, verdict['revision'],
                    )
                    raise SourceError(code, 'Unchanged provider revision retains its rejection.')
        except SourceError as error:
            if error.code not in {'BOOK_VENDOR_DEPENDENCIES', 'BOOK_VENDOR_DEPENDENCY_BOUND'}:
                raise
            get_dagster_logger('origo.sources').warning(
                'market=%s partition=%s phase=fetch_dependency_rebuild code=%s',
                self.market, partition.key, error.code,
            )
        except (ValueError, TypeError) as error:
            get_dagster_logger('origo.sources').warning(
                'market=%s partition=%s phase=fetch_dependency_rebuild code=%s',
                self.market, partition.key, type(error).__name__,
            )
        after = hour_partition(partition.end.strftime(HOUR_KEY_FORMAT))
        with _paired_responses(self.market, partition, after, metadata=False) as (response, closing):
            current = _identity(self.market, partition, response)
            if len(response.body) > CRYPTOHFT_MAX_FILE_BYTES:
                raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Hourly archive exceeds its byte bound.')
            scratch = _dependency_path(self.market, partition).parent
            scratch.mkdir(parents=True, exist_ok=True)
            temporary = TemporaryDirectory(prefix='hour-', dir=scratch)
            root = Path(temporary.name)
            archive = root / 'input.parquet'
            try:
                archive = _store_archive(archive, root, response.body)
                original = closing.result()
            except BaseException:
                temporary.cleanup()
                raise
        dependencies: list[tuple[str, str]] = []
        input_hashes: list[tuple[str, str]] = []
        following_hashes: list[tuple[str, str]] = []
        preludes: list[Path] = []
        try:
            used = archive.stat().st_size
            boundary = int(partition.start.timestamp() * 1000)
            if len(original.body) > CRYPTOHFT_MAX_FILE_BYTES:
                raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Following vendor archive exceeds its byte bound.')
            tail = root / 'following.parquet'
            tail = _store_archive(tail, root, original.body)
            used += tail.stat().st_size
            if used > BOOK_MAX_PRELUDE_BYTES:
                raise SourceError('BOOK_VENDOR_PRELUDE_BOUND', 'Adjacent archives exceed the replay byte bound.')
            dependencies.append((after.key, _identity(self.market, after, original)))
            following_hashes.append((after.key, hashlib.sha256(original.body).hexdigest()))
            grid = root / 'grid'

            from .book_columnar import replay_session

            last_rejection: tuple[str, str] | None = None
            last_prefix = -1
            with replay_session(self.market):
                for offset in range(BOOK_MAX_PRELUDE_HOURS + 1):
                    unavailable: Partition | None = None
                    if offset:
                        before = hour_partition(
                            (partition.start - timedelta(hours=offset)).strftime(HOUR_KEY_FORMAT)
                        )
                        try:
                            original = _response(self.market, before, metadata=False)
                        except _MissingArchive:
                            unavailable = before
                        if unavailable is None and len(original.body) > CRYPTOHFT_MAX_FILE_BYTES:
                            raise SourceError(
                                'BOOK_VENDOR_FILE_BOUND',
                                'Preceding vendor archive exceeds its byte bound.',
                            )
                        if unavailable is None:
                            prior = root / f'prelude-{offset:02}.parquet'
                            prior = _store_archive(prior, root, original.body)
                            used += prior.stat().st_size
                            if used > BOOK_MAX_PRELUDE_BYTES:
                                raise SourceError(
                                    'BOOK_VENDOR_PRELUDE_BOUND',
                                    'Preceding archives exceed the replay byte bound.',
                                )
                            dependencies.insert(
                                0, (before.key, _identity(self.market, before, original))
                            )
                            input_hashes.insert(
                                0, (before.key, hashlib.sha256(original.body).hexdigest())
                            )
                            preludes.insert(0, prior)
                            if offset not in (
                                1, 2, 4, 8, 16, BOOK_MAX_PRELUDE_HOURS,
                            ) and not _has_snapshot(prior, boundary):
                                continue
                    try:
                        if (unavailable is not None and last_rejection is not None
                                and last_prefix == len(preludes)):
                            raise SourceError(*last_rejection)
                        local = replay_hour(
                            archive,
                            self.market,
                            partition,
                            grid,
                            preludes=tuple(preludes),
                            following=(tail,),
                        )
                    except SourceError as error:
                        last_rejection = (error.code, str(error))
                        last_prefix = len(preludes)
                        if grid.exists():
                            shutil.rmtree(grid)
                        if unavailable is not None:
                            if error.code in _REJECTION_CODES:
                                dependencies.insert(0, (unavailable.key, _MISSING_DEPENDENCY))
                                _write_dependencies(
                                    self.market, partition, current, dependencies, rejected=error.code
                                )
                            raise SourceError(
                                error.code,
                                f'{error} Preceding archive is unavailable: '
                                f'{_file(self.market, unavailable)} (HTTP 404).',
                            ) from error
                        if offset < BOOK_MAX_PRELUDE_HOURS and error.code in {
                            'BOOK_VENDOR_SEED_MISSING',
                            'BOOK_SEED_TOO_SHALLOW',
                            'BOOK_KNOWN_DEPTH_EXHAUSTED',
                            'BOOK_VENDOR_CLOCK_UNPROVEN',
                        }:
                            continue
                        if error.code in _REJECTION_CODES:
                            _write_dependencies(
                                self.market, partition, current, dependencies, rejected=error.code
                            )
                        raise
                    else:
                        break
                else:
                    raise SourceError(
                        'BOOK_VENDOR_SEED_MISSING',
                        'No complete hour within the bounded replay search.',
                    )

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
        return self._rejected_revision(partition, revision, _dependency_evidence(self.market, partition))

    def _rejected_revision(self, partition: Partition, revision: str, value: dict[str, object]) -> bool:
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
        if verdict['code'] in {'BOOK_KNOWN_DEPTH_EXHAUSTED', 'BOOK_VENDOR_CLOCK_UNPROVEN'}:
            preceding = [(key, identity) for key, identity in dependencies if key < partition.key]
            if len(preceding) != BOOK_MAX_PRELUDE_HOURS and not (
                    preceding and preceding[0][1] == _MISSING_DEPENDENCY):
                return False
        return revision == _revision_key(current, dependencies)

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Hourly vendor archive changed during the build.'
            )
