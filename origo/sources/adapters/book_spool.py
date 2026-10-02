from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, TypedDict, cast
from uuid import uuid4

from ..contracts import Row, SourceError
from ..locking import source_lock

Market = Literal['spot', 'perp']
BOOK_SPOOL_MAX_BYTES = 16 * 1024**3
MINUTE_METADATA_MAX_BYTES = 2048
MINUTE_PAYLOAD_MAX_BYTES = 8 * 1024**2


class SealedMinute(TypedDict):
    market: Market
    minute_start: str
    samples_depth20: int
    samples_depth200: int
    first_update_id: int
    last_update_id: int
    payload_sha256: str


def utc_millisecond(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)


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
        raise ValueError('Book evidence must be an object.')
    return cast(dict[str, object], value)


def _minute_start(value: str) -> datetime:
    start = datetime.fromisoformat(value)
    if start.utcoffset() != timedelta(0) or start.second or start.microsecond:
        raise ValueError('Book minute requires an aligned UTC start.')
    return start


def _path(root: Path, market: Market, start: datetime) -> Path:
    if not root.is_absolute():
        raise ValueError('Book spool requires an absolute persistent mount.')
    return root / market / start.strftime('%Y-%m-%d') / start.strftime('%H-%M.seal.json')


def _metadata(path: Path) -> SealedMinute:
    with path.open('rb') as stream:
        raw = stream.read(MINUTE_METADATA_MAX_BYTES + 1)
    if len(raw) > MINUTE_METADATA_MAX_BYTES:
        raise ValueError('Book minute evidence exceeds its bound.')
    value = object_mapping(json.loads(raw))
    if value.get('market') not in ('spot', 'perp'):
        raise ValueError('Book minute has an unknown market.')
    if not isinstance(value.get('minute_start'), str):
        raise ValueError('Book minute has no start.')
    _minute_start(cast(str, value['minute_start']))
    for key in ('samples_depth20', 'samples_depth200', 'first_update_id', 'last_update_id'):
        if type(value.get(key)) is not int or cast(int, value[key]) < 0:
            raise ValueError('Book minute has invalid counts or sequence evidence.')
    digest = value.get('payload_sha256')
    if not isinstance(digest, str) or len(digest) != 64:
        raise ValueError('Book minute lacks a payload hash.')
    if value['samples_depth20'] != 600 or value['samples_depth200'] != 60:
        raise ValueError('A book minute must contain all 600/60 grid samples.')
    if cast(int, value['first_update_id']) > cast(int, value['last_update_id']):
        raise ValueError('Book minute update IDs regress.')
    return cast(SealedMinute, dict(value))


def _spool_bytes(root: Path, market: Market) -> int:
    return sum(path.stat().st_size for path in (root / market).rglob('*') if path.is_file())


def spool_bytes(root: Path, market: Market) -> int:
    with source_lock(root, f'book_spool_{market}', 'sealed', shared=True, wait=True):
        return _spool_bytes(root, market)


def seal_minute(root: Path, minute: SealedMinute, payload: bytes) -> None:
    start = _minute_start(minute['minute_start'])
    market = minute['market']
    if len(payload) > MINUTE_PAYLOAD_MAX_BYTES:
        raise SourceError('BOOK_MINUTE_TOO_LARGE', 'Book minute exceeds the payload bound.')
    if hashlib.sha256(payload).hexdigest() != minute['payload_sha256']:
        raise ValueError('Book minute payload contradicts its seal.')
    metadata = json.dumps(minute, separators=(',', ':'), allow_nan=False).encode()
    path = _path(root, market, start)
    packed = gzip.compress(payload, mtime=0)
    with source_lock(root, f'book_spool_{market}', 'sealed', wait=True):
        if path.exists():
            if _metadata(path) != minute or read_payload(root, minute) != payload:
                raise SourceError('BOOK_SEAL_CHANGED', 'An immutable book minute changed.')
            return
        if _spool_bytes(root, market) + len(packed) + len(metadata) > BOOK_SPOOL_MAX_BYTES:
            raise SourceError('BOOK_SPOOL_FULL', 'Unacknowledged book input cannot be discarded.')
        atomic_write(path.with_suffix('.gz'), packed)
        atomic_write(path, metadata)
        _metadata(path)


def sealed_minutes(
    root: Path, market: Market, start: datetime, end: datetime
) -> tuple[SealedMinute, ...]:
    if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0) or start >= end:
        raise ValueError('Book minute selection requires increasing UTC bounds.')
    minutes: list[SealedMinute] = []
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    with source_lock(root, f'book_spool_{market}', 'sealed', shared=True, wait=True):
        while day < end:
            for path in sorted((root / market / day.strftime('%Y-%m-%d')).glob('*.seal.json')):
                minute = _metadata(path)
                instant = _minute_start(minute['minute_start'])
                if minute['market'] != market or path != _path(root, market, instant):
                    raise ValueError('Book seal path contradicts its identity.')
                if start <= instant < end:
                    minutes.append(minute)
            day += timedelta(days=1)
    return tuple(minutes)


def read_payload(root: Path, minute: SealedMinute) -> bytes:
    path = _path(root, minute['market'], _minute_start(minute['minute_start']))
    with gzip.open(path.with_suffix('.gz'), 'rb') as stream:
        payload = stream.read(MINUTE_PAYLOAD_MAX_BYTES + 1)
    if len(payload) > MINUTE_PAYLOAD_MAX_BYTES:
        raise ValueError('Book minute payload exceeds its bound.')
    if hashlib.sha256(payload).hexdigest() != minute['payload_sha256']:
        raise SourceError('BOOK_PAYLOAD_CHANGED', 'Sealed book payload failed its hash.')
    return payload


def payload_rows(payload: bytes, minute: SealedMinute) -> Iterator[Row]:
    start_ms = int(_minute_start(minute['minute_start']).timestamp() * 1000)
    counts = {20: 0, 200: 0}
    previous = (-1, -1, -1)
    top20: tuple[list[tuple[float, float]], list[tuple[float, float]]] | None = None
    for line in payload.splitlines():
        value = object_mapping(json.loads(line))
        depth = value.get('depth')
        if type(depth) is not int or depth not in counts:
            raise ValueError('Book sample has an invalid depth.')
        stamp, event, update = (
            value.get(name) for name in ('datetime_ms', 'source_timestamp_ms', 'last_update_id')
        )
        if any(type(item) is not int for item in (stamp, event, update)):
            raise ValueError('Book sample lacks integer clocks and update ID.')
        stamp, event, update = cast(int, stamp), cast(int, event), cast(int, update)
        interval = 100 if depth == 20 else 1000
        if stamp != start_ms + counts[depth] * interval or event > stamp or event < 0:
            raise ValueError('Book sample violates its exchange-time grid.')
        if not minute['first_update_id'] <= update <= minute['last_update_id']:
            raise ValueError('Book sample update ID contradicts the sealed interval.')
        if stamp < previous[0] or event < previous[1] or update < previous[2]:
            raise ValueError('Sealed sample clocks or update IDs regress.')
        previous = (stamp, event, update)
        counts[depth] += 1
        sides: list[list[tuple[float, float]]] = []
        for side in ('bids', 'asks'):
            levels = value.get(side)
            if not isinstance(levels, list) or len(cast(list[object], levels)) != depth:
                raise ValueError('Book sample does not contain the declared depth.')
            parsed: list[tuple[float, float]] = []
            for level in cast(list[object], levels):
                if not isinstance(level, list) or len(cast(list[object], level)) != 2:
                    raise ValueError('Book level must contain price and quantity.')
                price, quantity = cast(list[object], level)
                if not isinstance(price, str) or not isinstance(quantity, str):
                    raise ValueError('Book levels require decimal strings.')
                p, q = Decimal(price), Decimal(quantity)
                if not p.is_finite() or not q.is_finite() or p <= 0 or q <= 0:
                    raise ValueError('Book levels require finite positive values.')
                converted = (float(p), float(q))
                if not all(math.isfinite(value) and value > 0 for value in converted):
                    raise ValueError(
                        'Book values cannot be represented as finite positive Float64.'
                    )
                parsed.append(converted)
            ordered = sorted(parsed, reverse=side == 'bids')
            if parsed != ordered or len({price for price, _ in parsed}) != depth:
                raise ValueError('Book levels are not strictly ordered.')
            sides.append(parsed)
        if sides[0][0][0] >= sides[1][0][0]:
            raise ValueError('Book sample is crossed.')
        if depth == 20:
            top20 = (sides[0], sides[1])
        elif top20 != (sides[0][:20], sides[1][:20]):
            raise ValueError('The two depths do not share one reconstructed book.')
        yield (depth, utc_millisecond(stamp), event, update, sides[0], sides[1])
    if counts != {20: 600, 200: 60}:
        raise ValueError('Sealed book minute is incomplete.')
