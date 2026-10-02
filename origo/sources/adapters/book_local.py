from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ..contracts import Partition, Revision, Row, SourceError
from ..locking import source_lock
from .book_spool import (
    Market,
    SealedMinute,
    atomic_write,
    object_mapping,
    payload_rows,
    read_payload,
    sealed_minutes,
)


def book_root() -> Path:
    root = Path(os.environ.get('ORIGO_BOOK_SPOOL_ROOT', '/var/lib/origo-book-spool'))
    if not root.is_absolute():
        raise ValueError('Book source requires an absolute local spool mount.')
    return root


def _minutes(root: Path, market: Market, partition: Partition) -> tuple[SealedMinute, ...]:
    minutes = sealed_minutes(root, market, partition.start, partition.end)
    expected = int((partition.end - partition.start).total_seconds()) // 60
    starts = tuple(datetime.fromisoformat(minute['minute_start']) for minute in minutes)
    if starts != tuple(partition.start + timedelta(minutes=i) for i in range(expected)):
        raise SourceError(
            'BOOK_MINUTES_MISSING', f'{market} {partition.key} lacks a complete sealed grid.'
        )
    return minutes


def _revision_key(minutes: tuple[SealedMinute, ...]) -> str:
    payload = json.dumps(minutes, sort_keys=True, separators=(',', ':')).encode()
    return 'local-book-v1:' + hashlib.sha256(payload).hexdigest()


def _fetch(root: Path, market: Market, partition: Partition) -> Revision:
    minutes = _minutes(root, market, partition)
    key = _revision_key(minutes)

    def rows() -> Iterator[Row]:
        for minute in minutes:
            with source_lock(root, f'book_spool_{market}', 'sealed', shared=True, wait=True):
                payload = read_payload(root, minute)
            # Database insertion and validation cannot hold up the capture's next seal.
            yield from payload_rows(payload, minute)

    return Revision(
        key,
        key.removeprefix('local-book-v1:'),
        json.dumps({'market': market, 'sealed_minutes': minutes}, separators=(',', ':')),
        len(minutes) * 600,
        rows,
    )


def _acknowledge(root: Path, market: Market, covered: tuple[Partition, ...], now: datetime) -> None:
    for day in covered:
        if day.provisional or day.end > now - timedelta(days=2):
            continue
        directory = root / market / day.start.strftime('%Y-%m-%d')
        acknowledgment = directory / 'canonical.ack.json'
        if acknowledgment.exists():
            with acknowledgment.open('rb') as stream:
                raw = stream.read(2049)
            if len(raw) > 2048:
                raise ValueError('Canonical acknowledgment exceeds its bound.')
            prior = object_mapping(json.loads(raw))
            if prior.get('cleanup_complete') is True:
                continue
        minutes = _minutes(root, market, day)
        with source_lock(root, f'book_spool_{market}', 'sealed', wait=True):
            # Only accepted canonical coverage permits payload deletion. Keep immutable seals
            # so retries can identify and revalidate the retained native generation.
            evidence = {
                'revision': _revision_key(minutes),
                'start': day.start.isoformat(),
                'end': day.end.isoformat(),
                'cleanup_complete': False,
            }
            atomic_write(acknowledgment, json.dumps(evidence, separators=(',', ':')).encode())
            for path in directory.glob('*.seal.gz'):
                path.unlink()
            evidence['cleanup_complete'] = True
            atomic_write(acknowledgment, json.dumps(evidence, separators=(',', ':')).encode())


@dataclass(frozen=True)
class LocalBookCanonical:
    market: Market

    def candidate(self, now: datetime) -> Partition:
        return self.partition((now.astimezone(UTC).date() - timedelta(days=1)).isoformat())

    def partition(self, key: str) -> Partition:
        start = datetime.strptime(key, '%Y-%m-%d').replace(tzinfo=UTC)
        if start.strftime('%Y-%m-%d') != key:
            raise ValueError('Canonical book keys require an exact UTC date.')
        return Partition(key, start, start + timedelta(days=1))

    def discover(self, partition: Partition) -> str:
        return _revision_key(_minutes(book_root(), self.market, partition))

    def fetch(self, partition: Partition) -> Revision:
        return _fetch(book_root(), self.market, partition)

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError('BOOK_SEAL_CHANGED', 'Local book evidence changed during the build.')
        if revision.row_count:
            count = sum(row[0] == 20 for row in revision.rows())
            if count != revision.row_count:
                raise SourceError(
                    'BOOK_PAYLOAD_CHANGED', 'Local book payload count changed during the build.'
                )


BOOK_CATCHUP_LOOKBACK_HOURS = 36


@dataclass(frozen=True)
class LocalBookProvisional:
    market: Market

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        root = book_root()
        closed = now.astimezone(UTC).replace(second=0, microsecond=0)
        _acknowledge(root, self.market, covered, now)
        starts = {
            closed - timedelta(minutes=i)
            for i in range(1, BOOK_CATCHUP_LOOKBACK_HOURS * 60 + 1)
            if closed - timedelta(minutes=i) >= anchor
        }
        market_root = root / self.market
        if market_root.exists():
            for directory in sorted(market_root.iterdir()):
                if not directory.is_dir():
                    raise ValueError('Book market spool contains an unexpected file.')
                day = datetime.strptime(directory.name, '%Y-%m-%d').replace(tzinfo=UTC)
                if (
                    day >= closed
                    or day + timedelta(days=1) <= anchor
                    or any(
                        not interval.provisional and interval.start <= day < interval.end
                        for interval in covered
                    )
                ):
                    continue
                starts.update(
                    datetime.fromisoformat(minute['minute_start'])
                    for minute in sealed_minutes(
                        root, self.market, max(day, anchor), min(day + timedelta(days=1), closed)
                    )
                    if day + timedelta(days=1) > anchor
                )
        return tuple(
            self.partition(start.strftime('%Y-%m-%dT%H:%MZ'))
            for start in sorted(starts)
            if not any(interval.start <= start < interval.end for interval in covered)
        )

    def partition(self, key: str) -> Partition:
        start = datetime.strptime(key, '%Y-%m-%dT%H:%MZ').replace(tzinfo=UTC)
        if start.strftime('%Y-%m-%dT%H:%MZ') != key:
            raise ValueError('Provisional book keys require an exact UTC minute.')
        return Partition(key, start, start + timedelta(minutes=1), provisional=True)

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        return _fetch(book_root(), self.market, partition)
