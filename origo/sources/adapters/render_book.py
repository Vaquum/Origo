from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import uuid4

import aiohttp

from ..contracts import (
    Partition,
    Revision,
    RevisionedSourceSpec,
    Row,
    SourceError,
    StateRecord,
    beat_worker,
    failure_code,
    failure_message,
)
from ..locking import source_lock

Market = Literal['spot', 'perp']
MAX_SEGMENT_BYTES = 64 * 1024**2
MAX_REPLICA_CACHE_BYTES = 8 * 1024**3
MAX_REPLICA_CACHE_FILES = 20_000
REPLICAS = ('A', 'B')

class _RetentionStore(Protocol):
    def table(self, name: str) -> str: ...

    def execute(self, query: str, params: object | None = None) -> list[Row]: ...

    def record(self, partition: Partition) -> StateRecord | None: ...


class _RetentionFailures(Protocol):
    def record(
        self, *, operation: str, scope: str, partition: str,
        error_code: str, message: str,
    ) -> str: ...

    def recover(self, *, operation: str, partition: str) -> int: ...


class _RetentionRuntime(Protocol):
    @property
    def spec(self) -> RevisionedSourceSpec: ...

    @property
    def store(self) -> _RetentionStore: ...

    @property
    def lock_root(self) -> Path: ...

    @property
    def failures(self) -> _RetentionFailures: ...

    def validate_retained(self, record: StateRecord) -> None: ...


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{uuid4().hex}.tmp')
    with temporary.open('xb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def object_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise SourceError('RENDER_FORMAT', 'Render evidence must be an object.')
    return cast(dict[str, object], value)


@dataclass(frozen=True)
class RenderBookEndpoint:
    url: str
    token: str
    deadline: float | None = None

    @classmethod
    def configured(cls, replica: str) -> RenderBookEndpoint:
        if replica not in REPLICAS:
            raise ValueError('Expected replica A or B.')
        token = os.environ.get(f'ORIGO_RENDER_BOOK_{replica}_TOKEN', '')
        if not token:
            raise SourceError('RENDER_CREDENTIAL_MISSING', f'Replica {replica} token is absent.')
        return cls(os.environ.get(
            f'ORIGO_RENDER_BOOK_{replica}_URL',
            f'https://binance-book-collector-{replica.lower()}.onrender.com',
        ), token)

    def read(self, path: str, limit: int) -> bytes:
        try:
            return asyncio.run(self._read(path, limit))
        except TimeoutError as error:
            if self.deadline is not None and time.monotonic() >= self.deadline:
                raise SourceError('RENDER_IMPORT_BUDGET', 'This tick exhausted its import time budget.') from error
            raise

    async def _read(self, path: str, limit: int) -> bytes:
        remaining = 15.0 if self.deadline is None else min(15.0, self.deadline - time.monotonic())
        if remaining <= 0:
            raise SourceError('RENDER_IMPORT_BUDGET', 'This tick exhausted its import time budget.')
        timeout = aiohttp.ClientTimeout(total=remaining, connect=5, sock_read=10)
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

    def directory(self, replica: str, metadata: Mapping[str, object]) -> Path:
        if replica not in REPLICAS:
            raise SourceError('RENDER_IDENTITY', 'Expected replica A or B.')
        closed = _at(_integer(metadata.get('closed_at')))
        return self.root / self.market / replica / closed.strftime('%Y-%m-%dT%H')

    def validate_segment(
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

    def resume_retained(self, replica: str, previous: int, pruned_through: int) -> None:
        if replica not in REPLICAS or previous <= 0 or pruned_through <= previous:
            raise ValueError('Retention recovery requires an expired positive cursor.')
        with source_lock(self.root, f'render_{self.market}_{replica.lower()}', 'import', wait=True):
            if self.cursor(replica) != previous:
                raise SourceError('RENDER_CURSOR', 'Retention cursor changed before recovery.')
            atomic_write(self.root / self.market / replica / 'cursor.json', _json(pruned_through))

    def _usage(self, replica: str) -> int:
        root = self.root / self.market / replica
        if not root.exists():
            return 0
        used, count, hours = 0, 0, 0
        for directory in root.iterdir():
            if directory.is_symlink():
                raise SourceError('RENDER_CACHE_PATH', 'Cache directories cannot be links.')
            if not directory.is_dir():
                continue
            datetime.strptime(directory.name, '%Y-%m-%dT%H')
            hours += 1
            if hours > 96:
                raise SourceError('RENDER_CACHE_FULL', 'Unreclaimed cache exceeds 96 arrival hours.')
            for path in directory.iterdir():
                count += 1
                if count > MAX_REPLICA_CACHE_FILES:
                    raise SourceError('RENDER_CACHE_FULL', 'Unreclaimed cache exceeds its file bound.')
                if path.is_symlink() or not path.is_file():
                    raise SourceError('RENDER_CACHE_PATH', 'Cache entries must be regular files.')
                if path.name.startswith('.') and path.name.endswith('.tmp'):
                    path.unlink()
                else:
                    used += path.stat().st_size
        return used

    def _save(
        self, replica: str, metadata: Mapping[str, object], read: Callable[[str, int], bytes], used: int
    ) -> int:
        identifier = _integer(metadata.get('id'))
        directory = self.directory(replica, metadata)
        path = directory / f'{identifier:020}.ndjson'
        sidecar = path.with_suffix('.json')
        encoded = _json(metadata)
        added = (0 if path.exists() else _integer(metadata.get('bytes'))) + (
            0 if sidecar.exists() else len(encoded)
        )
        if used + added > MAX_REPLICA_CACHE_BYTES:
            raise SourceError('RENDER_CACHE_FULL', 'Replica cache exceeds its byte bound.')
        if sidecar.exists() and sidecar.read_bytes() != encoded:
            raise SourceError('RENDER_INTEGRITY', 'Immutable segment metadata changed.')
        payload = (
            path.read_bytes() if path.exists()
            else read(f'/segments/{self.market}/{identifier:020}', MAX_SEGMENT_BYTES)
        )
        self.validate_segment(replica, metadata, payload)
        if not path.exists():
            atomic_write(path, payload)
        if not sidecar.exists():
            atomic_write(sidecar, encoded)
        beat_worker()
        return used + added

    def sync_tail(self, replica: str, read: Callable[[str, int], bytes]) -> tuple[int, int]:
        if replica not in REPLICAS:
            raise ValueError('Expected replica A or B.')
        with source_lock(self.root, f'render_{self.market}_{replica.lower()}', 'import', wait=True):
            page = object_mapping(json.loads(read(f'/segments/{self.market}?tail=2', 1024**2)))
            entries = _objects(page.get('segments'))
            if (page.get('collector') != replica or page.get('market') != self.market
                    or page.get('tail') is not True or page.get('has_more') is not False
                    or len(entries) > 2):
                raise SourceError('RENDER_IDENTITY', 'Expected a bounded recent-segment inventory.')
            identifiers = tuple(_integer(entry.get('id')) for entry in entries)
            if (any(identifier == 0 for identifier in identifiers)
                    or tuple(sorted(set(identifiers))) != identifiers
                    or _integer(page.get('next_cursor')) != (identifiers[-1] if identifiers else 0)):
                raise SourceError('RENDER_CURSOR', 'Recent inventory IDs or next cursor are invalid.')
            used = self._usage(replica)
            for metadata in entries:
                used = self._save(replica, metadata, read, used)
            return len(entries), _integer(page.get('pruned_through'))

    def sync(
        self, replica: str, read: Callable[[str, int], bytes], *, max_segments: int = 100
    ) -> int:
        if replica not in REPLICAS:
            raise ValueError('Expected replica A or B.')
        if not 1 <= max_segments <= 100:
            raise ValueError('An import admits between one and one hundred segments.')
        count = 0
        with source_lock(self.root, f'render_{self.market}_{replica.lower()}', 'import', wait=True):
            used = self._usage(replica)
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
                if _integer(page.get('next_cursor')) != (
                    _integer(entries[-1].get('id')) if entries else after
                ):
                    raise SourceError('RENDER_CURSOR', 'Inventory next cursor does not match its segments.')
                for metadata in entries:
                    identifier = _integer(metadata.get('id'))
                    if identifier <= after:
                        raise SourceError('RENDER_CURSOR', 'Segment cursor does not advance.')
                    used = self._save(replica, metadata, read, used)
                    atomic_write(
                        self.root / self.market / replica / 'cursor.json', _json(identifier)
                    )
                    after = identifier
                    count += 1
                    if count == max_segments:
                        return count
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
            path = self.directory(replica, metadata) / f'{_integer(metadata.get("id")):020}.ndjson'
            rows = self.validate_segment(replica, metadata, path.read_bytes())
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

    @property
    def recovery_window(self) -> timedelta:
        return timedelta(hours=48)

    def recovery_identity(self, partition: Partition) -> str:
        return hashlib.sha256(_json(self.cache.entries(partition))).hexdigest()

    @property
    def audit_maintenance(
        self,
    ) -> Callable[[_RetentionRuntime, datetime, Callable[[Partition], str]], int]:
        return _reclaim_render_payloads

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
        path = self._pin(partition)
        if not path.exists():
            return self.fetch(partition).key
        encoded = path.read_bytes()
        evidence = object_mapping(json.loads(encoded))
        if (
            evidence.get('format') != 1
            or evidence.get('market') != self.cache.market
            or evidence.get('partition') != partition.key
            or evidence.get('selection') != 'max-exchange-update-A-identity-v1'
            or evidence.get('cutoff_us') != _micros(partition.end + timedelta(minutes=5))
        ):
            raise SourceError('RENDER_PIN_CHANGED', 'Pinned hour identity is invalid.')
        return 'render-top20-v1:' + hashlib.sha256(encoded).hexdigest()

    def _pin(self, partition: Partition) -> Path:
        return self.cache.root / self.cache.market / 'hours' / f'{partition.key}.json'

    def fetch(self, partition: Partition) -> Revision:
        if datetime.now(UTC) < partition.end + timedelta(minutes=5):
            raise SourceError('RENDER_NOT_DUE', 'The hour is not due until end plus five minutes.')
        path = self._pin(partition)
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
        key = self.fetch(partition).key if revision.row_count else self.discover(partition)
        if key != revision.key:
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
        missing = {
            start + timedelta(minutes=offset)
            for offset in range(int((closed - start).total_seconds()) // 60)
        }
        for interval in covered:
            minute = max(start, interval.start)
            end = min(closed, interval.end)
            while minute < end:
                missing.discard(minute)
                minute += timedelta(minutes=1)
        ordered = sorted(missing)
        if ordered:
            ordered.insert(0, ordered.pop())
        return tuple(self.partition(minute.strftime('%Y-%m-%dT%H:%M:%SZ')) for minute in ordered[:8])

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        return self.cache.revision(partition, self.cache.entries(partition))


def _reclaim_render_payloads(
    runtime: _RetentionRuntime, now: datetime, partition_lock: Callable[[Partition], str],
) -> int:
    adapter = runtime.spec.canonical
    if not isinstance(adapter, RenderBookCanonical):
        raise ValueError('Render retention requires the Render canonical adapter.')
    cache = adapter.cache
    root = cache.root / cache.market
    cutoff = (now.astimezone(UTC) - timedelta(hours=48)).replace(minute=0, second=0, microsecond=0)
    cursor = root / 'cleanup-cursor.json'
    reclaimed, charged = 0, 0
    verified: set[str] = set()
    with source_lock(runtime.lock_root, runtime.spec.key, 'heavy'):
        after = str(json.loads(cursor.read_bytes())) if cursor.exists() else ''
        candidates = sorted(
            path for replica in REPLICAS
            for directory in (root / replica).glob('*')
            if directory.is_dir() and datetime.strptime(directory.name, '%Y-%m-%dT%H').replace(tzinfo=UTC) < cutoff
            for path in directory.glob('*.json')
        )
        ordered = [p for p in candidates if str(p.relative_to(root)) > after] + [
            p for p in candidates if str(p.relative_to(root)) <= after
        ]
        for path in ordered[:128]:
            relative = path.relative_to(root)
            replica = relative.parts[0]
            try:
                with source_lock(cache.root, f'render_{cache.market}_{replica.lower()}', 'import', wait=True):
                    metadata = object_mapping(json.loads(path.read_bytes()))
                    expected = cache.directory(replica, metadata) / f'{_integer(metadata.get("id")):020}.json'
                    if path != expected or path.is_symlink():
                        raise SourceError('RENDER_RETENTION_IDENTITY', 'Cache path contradicts its manifest.')
                    size = _integer(metadata.get('bytes'))
                    if charged + size > 256 * 1024**2:
                        break
                    charged += size
                    start = _at(_integer(metadata.get('min_exchange_time'))).replace(minute=0, second=0, microsecond=0)
                    end = _at(_integer(metadata.get('max_exchange_time')))
                    if end - start > timedelta(hours=96):
                        raise SourceError('RENDER_RETENTION_SPAN', 'Segment clock span exceeds cleanup bounds.')
                    complete = True
                    while start <= end:
                        if start >= runtime.spec.partitions.start:
                            key = start.strftime('%Y-%m-%dT%HZ')
                            if key not in verified:
                                partition = adapter.partition(key)
                                record = runtime.store.record(partition)
                                if record is None:
                                    complete = False
                                    break
                                if adapter.discover(partition) != record.revision:
                                    raise SourceError('RENDER_PIN_CHANGED', 'Retention requires the accepted input pin.')
                                runtime.validate_retained(record)
                                verified.add(key)
                        start += timedelta(hours=1)
                    if not complete:
                        continue
                    payload = path.with_suffix('.ndjson')
                    if payload.exists():
                        cache.validate_segment(replica, metadata, payload.read_bytes())
                    evidence = {'operation': 'render_payload_reclaimed', 'manifest': dict(metadata),
                                'verified_hours': sorted(verified), 'removed_bytes': size if payload.exists() else 0}
                    runtime.store.execute(
                        f'INSERT INTO {runtime.store.table("source_observation_log")} VALUES',
                        [(runtime.spec.key, str(relative), json.dumps({**evidence, 'phase': 'prepared'}), 0, now)],
                    )
                    payload.unlink(missing_ok=True)
                    path.unlink()
                    directory = path.parent
                    if not any(directory.iterdir()):
                        directory.rmdir()
                    runtime.store.execute(
                        f'INSERT INTO {runtime.store.table("source_observation_log")} VALUES',
                        [(runtime.spec.key, str(relative), json.dumps({**evidence, 'phase': 'completed'}), 1, now)],
                    )
                    runtime.failures.recover(operation='payload_cleanup', partition=str(relative))
                    reclaimed += 1
            except (OSError, ValueError, RuntimeError) as error:
                runtime.failures.record(
                    operation='payload_cleanup', scope='NONE', partition=str(relative),
                    error_code=failure_code(error), message=failure_message(error),
                )
            finally:
                atomic_write(cursor, json.dumps(str(relative)).encode())
    return reclaimed
