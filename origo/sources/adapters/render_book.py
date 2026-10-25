from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import aiohttp

from ..contracts import Partition, Revision, Row, SourceError
from ..locking import source_lock
from .book_spool import Market, atomic_write, object_mapping

MAX_SEGMENT_BYTES = 64 * 1024**2
REPLICAS = ('A', 'B')


@dataclass(frozen=True)
class RenderBookEndpoint:
    url: str
    token: str

    def read(self, path: str, limit: int) -> bytes:
        return asyncio.run(self._read(path, limit))

    async def _read(self, path: str, limit: int) -> bytes:
        timeout = aiohttp.ClientTimeout(total=300, connect=5, sock_read=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                self.url.rstrip('/') + path,
                headers={'Authorization': 'Bearer ' + self.token},
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    code = 'RENDER_EXPIRED' if response.status == 410 else 'RENDER_HTTP'
                    raise SourceError(code, f'Render returned HTTP {response.status}.')
                payload = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    payload.extend(chunk)
                    if len(payload) > limit:
                        raise SourceError(
                            'RENDER_TRANSFER_BOUND', 'Render transfer exceeded its byte bound.'
                        )
        return bytes(payload)


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise SourceError('RENDER_FORMAT', 'Expected a nonnegative integer.')
    return value


def _text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise SourceError('RENDER_FORMAT', 'Expected a nonempty string.')
    return value


def _objects(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise SourceError('RENDER_FORMAT', 'Expected a list of objects.')
    return tuple(object_mapping(item) for item in cast(list[object], value))


def _micros(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000 + value.microsecond


def _at(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)


def _levels(value: object, bid: bool) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise SourceError('RENDER_DEPTH', 'Each book side requires twenty levels.')
    items = cast(list[object], value)
    if len(items) != 20:
        raise SourceError('RENDER_DEPTH', 'Each book side requires twenty levels.')
    levels: list[tuple[str, str]] = []
    previous: Decimal | None = None
    for item in items:
        if not isinstance(item, list):
            raise SourceError('RENDER_DEPTH', 'Invalid price/quantity pair.')
        pair = cast(list[object], item)
        if len(pair) != 2:
            raise SourceError('RENDER_DEPTH', 'Invalid price/quantity pair.')
        price, quantity = _text(pair[0]), _text(pair[1])
        p, q = Decimal(price), Decimal(quantity)
        if not p.is_finite() or not q.is_finite() or p <= 0 or q <= 0:
            raise SourceError('RENDER_DEPTH', 'Book levels must be finite and positive.')
        if previous is not None and (p >= previous if bid else p <= previous):
            raise SourceError('RENDER_DEPTH', 'Book levels are unordered or duplicated.')
        levels.append((price, quantity))
        previous = p
    return tuple(levels)


@dataclass(frozen=True)
class _Observation:
    raw: bytes
    exchange: int
    observed: int
    update: int
    replica: str
    session: str
    sequence: int
    transaction: int
    first: int
    previous: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]

    @property
    def identity(self) -> tuple[str, str, int]:
        return self.replica, self.session, self.sequence

    @property
    def rank(self) -> tuple[int, int, bool, str, int]:
        return self.exchange, self.update, self.replica == 'A', self.session, self.sequence

    def row(self) -> Row:
        return (
            _at(self.exchange // 1_000_000 * 1_000_000),
            self.exchange,
            self.observed,
            self.update,
            self.replica,
            self.session,
            self.sequence,
            self.transaction,
            self.first,
            self.previous,
            [(float(p), float(q)) for p, q in self.bids],
            [(float(p), float(q)) for p, q in self.asks],
            self.raw.decode(),
        )


def _observation(raw: bytes, market: Market, replica: str) -> _Observation:
    value = object_mapping(json.loads(raw))
    if (
        value.get('market') != market
        or value.get('collector') != replica
        or value.get('symbol') != 'BTCUSDT'
    ):
        raise SourceError('RENDER_IDENTITY', 'Unexpected market, collector or symbol.')
    bids, asks = _levels(value.get('bids'), True), _levels(value.get('asks'), False)
    if Decimal(bids[0][0]) >= Decimal(asks[0][0]):
        raise SourceError('RENDER_DEPTH', 'Book is crossed.')
    integers = tuple(
        _integer(value.get(key))
        for key in ('exchange_time', 'observed_at', 'update_id', 'sequence')
    )
    if not all(integers):
        raise SourceError('RENDER_FORMAT', 'Observation clocks and identities must be positive.')
    transaction, first, previous = (
        _integer(value.get(key, 0))
        for key in ('transaction_time', 'first_update_id', 'previous_update_id')
    )
    return _Observation(
        _json(value),
        integers[0],
        integers[1],
        integers[2],
        replica,
        _text(value.get('session')),
        integers[3],
        transaction,
        first,
        previous,
        bids,
        asks,
    )


@dataclass(frozen=True)
class RenderBookCache:
    root: Path
    market: Market

    def _directory(self, replica: str, metadata: Mapping[str, object]) -> Path:
        if replica not in REPLICAS:
            raise SourceError('RENDER_IDENTITY', 'Expected replica A or B.')
        closed = _at(_integer(metadata.get('closed_at')))
        return self.root / self.market / replica / closed.strftime('%Y-%m-%dT%H')

    def _validate(
        self, replica: str, metadata: Mapping[str, object], payload: bytes
    ) -> tuple[_Observation, ...]:
        if (
            replica not in REPLICAS
            or metadata.get('collector') != replica
            or metadata.get('market') != self.market
            or metadata.get('schema') != 1
            or metadata.get('timestamp_unit') != 'microseconds'
            or metadata.get('exchange_precision_us') != (1 if self.market == 'spot' else 1000)
        ):
            raise SourceError(
                'RENDER_IDENTITY', 'Unsupported segment identity, schema or clock unit.'
            )
        if (
            len(payload) > MAX_SEGMENT_BYTES
            or len(payload) != _integer(metadata.get('bytes'))
            or hashlib.sha256(payload).hexdigest() != metadata.get('sha256')
            or not payload.endswith(b'\n')
        ):
            raise SourceError('RENDER_INTEGRITY', 'Segment bytes or SHA256 do not match.')
        rows = tuple(_observation(line, self.market, replica) for line in payload.splitlines())
        if not rows or len(rows) != _integer(metadata.get('count')):
            raise SourceError('RENDER_INTEGRITY', 'Segment count does not match.')
        actual = (
            rows[0].observed,
            rows[-1].observed,
            min(r.exchange for r in rows),
            max(r.exchange for r in rows),
        )
        declared = tuple(
            _integer(metadata.get(key))
            for key in (
                'first_observed_at',
                'last_observed_at',
                'min_exchange_time',
                'max_exchange_time',
            )
        )
        if actual != declared:
            raise SourceError('RENDER_INTEGRITY', 'Segment clock bounds do not match.')
        return rows

    def cursor(self, replica: str) -> int:
        path = self.root / self.market / replica / 'cursor.json'
        return _integer(json.loads(path.read_bytes())) if path.exists() else 0

    def sync(self, replica: str, read: Callable[[str, int], bytes]) -> int:
        if replica not in REPLICAS:
            raise ValueError('Expected replica A or B.')
        count = 0
        with source_lock(self.root, f'render_{self.market}_{replica.lower()}', 'import', wait=True):
            after = self.cursor(replica)
            more = True
            while more:
                page = object_mapping(
                    json.loads(read(f'/segments/{self.market}?after={after}', 1024**2))
                )
                if page.get('collector') != replica or page.get('market') != self.market:
                    raise SourceError('RENDER_IDENTITY', 'Inventory belongs to another feed.')
                if after and _integer(page.get('pruned_through')) > after:
                    raise SourceError('RENDER_EXPIRED', 'Cursor precedes retained source history.')
                entries = _objects(page.get('segments'))
                if len(entries) > 100 or type(page.get('has_more')) is not bool:
                    raise SourceError('RENDER_FORMAT', 'Invalid bounded inventory page.')
                for metadata in entries:
                    identifier = _integer(metadata.get('id'))
                    if identifier <= after:
                        raise SourceError('RENDER_CURSOR', 'Segment cursor does not advance.')
                    directory = self._directory(replica, metadata)
                    path = directory / f'{identifier:020}.ndjson'
                    payload = (
                        path.read_bytes()
                        if path.exists()
                        else read(f'/segments/{self.market}/{identifier:020}', MAX_SEGMENT_BYTES)
                    )
                    self._validate(replica, metadata, payload)
                    sidecar = path.with_suffix('.json')
                    encoded = _json(metadata)
                    if sidecar.exists() and sidecar.read_bytes() != encoded:
                        raise SourceError('RENDER_INTEGRITY', 'Immutable segment metadata changed.')
                    if not path.exists():
                        atomic_write(path, payload)
                    atomic_write(sidecar, encoded)
                    atomic_write(
                        self.root / self.market / replica / 'cursor.json', _json(identifier)
                    )
                    after = identifier
                    count += 1
                if _integer(page.get('next_cursor')) != after:
                    raise SourceError(
                        'RENDER_CURSOR', 'Inventory next cursor does not match its segments.'
                    )
                more = page.get('has_more') is True
                if more and not entries:
                    raise SourceError(
                        'RENDER_CURSOR', 'Empty page cannot advance retained history.'
                    )
        return count

    def entries(self, partition: Partition) -> tuple[Mapping[str, object], ...]:
        cutoff = partition.end + timedelta(minutes=5)
        first = (partition.start - timedelta(minutes=5)).replace(minute=0, second=0, microsecond=0)
        result: list[Mapping[str, object]] = []
        while first <= cutoff:
            for replica in REPLICAS:
                directory = self.root / self.market / replica / first.strftime('%Y-%m-%dT%H')
                for path in sorted(directory.glob('*.json')):
                    entry = object_mapping(json.loads(path.read_bytes()))
                    if (
                        _integer(entry.get('closed_at')) <= _micros(cutoff)
                        and _integer(entry.get('max_exchange_time')) >= _micros(partition.start)
                        and _integer(entry.get('min_exchange_time')) < _micros(partition.end)
                    ):
                        result.append(entry)
            first += timedelta(hours=1)
        return tuple(
            sorted(result, key=lambda m: (_text(m.get('collector')), _integer(m.get('id'))))
        )

    def revision(self, partition: Partition, entries: tuple[Mapping[str, object], ...]) -> Revision:
        selected: dict[int, _Observation] = {}
        identities: dict[tuple[str, str, int], bytes] = {}
        states: dict[int, tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]] = {}
        coverage: dict[str, set[int]] = {replica: set() for replica in REPLICAS}
        for metadata in entries:
            replica = _text(metadata.get('collector'))
            path = self._directory(replica, metadata) / f'{_integer(metadata.get("id")):020}.ndjson'
            rows = self._validate(replica, metadata, path.read_bytes())
            for row in rows:
                if _micros(partition.start) <= row.exchange < _micros(partition.end):
                    second = row.exchange // 1_000_000
                    prior = identities.setdefault(row.identity, row.raw)
                    state = (row.bids, row.asks)
                    if prior != row.raw or states.setdefault(row.update, state) != state:
                        raise SourceError(
                            'RENDER_CONFLICT',
                            'An identical observation or book update has conflicting content.',
                        )
                    coverage[replica].add(second)
                    previous = selected.get(second)
                    rank = (partition.provisional and row.replica == 'A', row.rank)
                    old_rank = (
                        (partition.provisional and previous.replica == 'A', previous.rank)
                        if previous is not None
                        else None
                    )
                    if old_rank is None or rank > old_rank:
                        selected[second] = row
        expected = int((partition.end - partition.start).total_seconds())
        if len(selected) != expected:
            raise SourceError(
                'RENDER_SECONDS_MISSING',
                f'{partition.key} has {len(selected)}/{expected} actual seconds.',
            )
        chosen = tuple(selected[second] for second in sorted(selected))
        digest = hashlib.sha256(b'\n'.join(row.raw for row in chosen)).hexdigest()
        evidence = {
            'format': 1,
            'market': self.market,
            'partition': partition.key,
            'selection': 'A-then-B-v1'
            if partition.provisional
            else 'max-exchange-update-A-identity-v1',
            'cutoff_us': _micros(partition.end + timedelta(minutes=5)),
            'segments': entries,
            'selected_sha256': digest,
            'replica_seconds': {r: len(coverage[r]) for r in REPLICAS},
            'redundant_seconds': len(coverage['A'] & coverage['B']),
        }
        encoded = _json(evidence)
        revision = 'render-top20-v1:' + hashlib.sha256(encoded).hexdigest()
        return Revision(
            revision, digest, encoded.decode(), len(chosen), lambda: (row.row() for row in chosen)
        )


@dataclass(frozen=True)
class RenderBookCanonical:
    cache: RenderBookCache

    def candidate(self, now: datetime) -> Partition:
        start = (now.astimezone(UTC) - timedelta(minutes=5)).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
        return self.partition(start.strftime('%Y-%m-%dT%HZ'))

    def partition(self, key: str) -> Partition:
        start = datetime.strptime(key, '%Y-%m-%dT%HZ').replace(tzinfo=UTC)
        if start.strftime('%Y-%m-%dT%HZ') != key:
            raise ValueError('Canonical top20 keys require an exact UTC hour.')
        return Partition(key, start, start + timedelta(hours=1))

    def discover(self, partition: Partition) -> str:
        return self.fetch(partition).key

    def fetch(self, partition: Partition) -> Revision:
        if datetime.now(UTC) < partition.end + timedelta(minutes=5):
            raise SourceError('RENDER_NOT_DUE', 'The hour is not due until end plus five minutes.')
        path = self.cache.root / self.cache.market / 'hours' / f'{partition.key}.json'
        lock = 'hour_' + hashlib.sha256(partition.key.encode()).hexdigest()
        with source_lock(self.cache.root, f'render_{self.cache.market}', lock, wait=True):
            if path.exists():
                pinned = object_mapping(json.loads(path.read_bytes()))
                entries = _objects(pinned.get('segments'))
                revision = self.cache.revision(partition, entries)
                if revision.evidence_json.encode() != path.read_bytes():
                    raise SourceError('RENDER_PIN_CHANGED', 'Pinned hour evidence changed.')
            else:
                revision = self.cache.revision(partition, self.cache.entries(partition))
                atomic_write(path, revision.evidence_json.encode())
        return revision

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.fetch(partition).key != revision.key:
            raise SourceError('RENDER_PIN_CHANGED', 'Pinned input changed during the build.')


@dataclass(frozen=True)
class RenderBookProvisional:
    cache: RenderBookCache

    def partition(self, key: str) -> Partition:
        start = datetime.strptime(key, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)
        if start.strftime('%Y-%m-%dT%H:%M:%SZ') != key or start.second:
            raise ValueError('Provisional top20 keys require an exact UTC minute.')
        return Partition(key, start, start + timedelta(minutes=1), provisional=True)

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        closed = now.astimezone(UTC).replace(second=0, microsecond=0)
        start = max(anchor, closed - timedelta(hours=48))
        return tuple(
            self.partition((start + timedelta(minutes=offset)).strftime('%Y-%m-%dT%H:%M:%SZ'))
            for offset in range(int((closed - start).total_seconds()) // 60)
            if not any(p.start <= start + timedelta(minutes=offset) < p.end for p in covered)
        )

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        return self.cache.revision(partition, self.cache.entries(partition))
