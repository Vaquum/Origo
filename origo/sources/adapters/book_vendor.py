from __future__ import annotations

import hashlib
import importlib
import itertools
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from ..arrow_types import ArrowParquet
from ..contracts import ArchiveNotPublishedYet, Partition, Revision, Row, SourceError
from .binance_daily import Response, get_response
from .book_local import fetch_sealed_partition
from .book_spool import Market, object_mapping

CRYPTOHFT_URL = 'https://api.cryptohftdata.com/v1/download'
CRYPTOHFT_MAX_FILE_BYTES = 256 * 1024**2
CRYPTOHFT_BATCH_ROWS = 8192
# The existing unknown-host transport allows 20 units/second. Charge 40 to
# keep both sources together below the vendor's anonymous 60 requests/minute.
CRYPTOHFT_REQUEST_WEIGHT = 40
BOOK_HOURLY_DELIVERY_GRACE_SECONDS = 15 * 60
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


def replay_hour(path: Path, market: Market, partition: Partition, root: Path) -> Revision:
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
    for _, grouped in itertools.groupby(_archive_rows(path), key=_frame_key):
        frame = list(itertools.islice(grouped, 100001))
        if len(frame) > 100000:
            raise SourceError('BOOK_VENDOR_FRAME_BOUND', 'Hourly archive frame exceeds its bound.')
        first = frame[0]
        received = parse_book_integer(first['received_time'])
        event_ms = parse_book_integer(first['event_time'])
        if received < previous_received or any(row['symbol'] != 'BTCUSDT' for row in frame):
            raise SourceError(
                'BOOK_VENDOR_ORDERING', 'Hourly archive has invalid receive ordering or symbol.'
            )
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
            sampler.checkpoint(json.dumps(seed).encode(), event_ms, start_ms)
        elif first['event_type'] == 'update':
            if sampler.book is None:
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
            sampler.accept(
                DepthEvent.parse(json.dumps(wire).encode(), market),
                received_at=utc_millisecond(received // 1000000),
            )
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
        return _identity(self.market, partition, _response(self.market, partition, metadata=True))

    def fetch(self, partition: Partition) -> Revision:
        response = _response(self.market, partition, metadata=False)
        key = _identity(self.market, partition, response)
        if len(response.body) > CRYPTOHFT_MAX_FILE_BYTES:
            raise SourceError('BOOK_VENDOR_FILE_BOUND', 'Hourly archive exceeds its byte bound.')
        temporary = TemporaryDirectory(prefix='origo-book-hour-')
        root = Path(temporary.name)
        archive = root / 'input.parquet'
        archive.write_bytes(response.body)
        try:
            local = replay_hour(archive, self.market, partition, root / 'grid')
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
                'grid_evidence': json.loads(local.evidence_json),
            },
            separators=(',', ':'),
        )
        return Revision(
            key, hashlib.sha256(response.body).hexdigest(), evidence, local.row_count, rows
        )

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Hourly vendor archive changed during the build.'
            )
