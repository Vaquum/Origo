"""Durable bounded spool of recent raw perpetual trades with overlap proof (S439, SS-05).

The capture worker polls the keyless ``/fapi/v1/trades`` page (the newest 1000 trades,
weight 5) and hands every response to the spool. Two successive responses *chain* when
their native id ranges intersect and every stored row in the intersection is byte-for-byte
the same row in the new page; a chain of responses is a *segment*. A closed minute is
*sealed* from a segment only when the segment holds a row strictly before the minute
start and a row at or after the minute end: the chain then proves every trade the
provider served for the minute is present, provider-side id holes included. No overlap,
a conflicting row, or a shorter page than the previous tail closes the segment with its
reason; the next page opens a new segment, and the minutes between are explicitly
incomplete rather than padded. Empty minutes are never sealed here: the provisional
adapter's independent two-observation evidence stays the only proof of emptiness.

Storage is one SQLite file (WAL, synchronous FULL) so every poll is one fsynced atomic
commit. Rows keep their raw text; the content hash of a sealed minute is computed
through the same row mapping as the authenticated historical path, so a spool minute and
a paged minute of the same trades hash identically. Rows are released only after the
acknowledgement main wires from a durable accepted generation; unresolved inputs stay
until the byte cap, at which point the spool refuses new pages (explicit backpressure).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from origo.sources.contracts import Row
from origo.sources.hashing import content_hash

SCHEMA_VERSION = 1
SPOOL_DIR_ENV = 'ORIGO_TRADE_SPOOL_DIR'
SPOOL_DIR_DEFAULT = '/opt/origo/spool'
SPOOL_MAX_BYTES_ENV = 'ORIGO_TRADE_SPOOL_MAX_BYTES'
SPOOL_MAX_BYTES_DEFAULT = 4 * 1024**3
RELEASED_MINUTE_RETENTION = timedelta(days=14)
FAULT_RETENTION_ROWS = 1000
PRUNE_INTERVAL_MS = 60_000
_SEGMENT_HEAD = 'segment_head'
_NOT_BRACKETED = 'not_bracketed'

Mapper = Callable[[Mapping[str, object]], Row]

_SCHEMA = (
    'CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    """CREATE TABLE IF NOT EXISTS segments(
        segment INTEGER PRIMARY KEY AUTOINCREMENT,
        opened_seq INTEGER NOT NULL,
        closed_seq INTEGER,
        closed_reason TEXT,
        min_id INTEGER, max_id INTEGER, min_time INTEGER, max_time INTEGER,
        sealed_through INTEGER NOT NULL DEFAULT -1)""",
    """CREATE TABLE IF NOT EXISTS responses(
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        segment INTEGER NOT NULL,
        captured_at INTEGER NOT NULL,
        completed_at INTEGER NOT NULL,
        status INTEGER NOT NULL,
        body_sha256 TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        first_id INTEGER, last_id INTEGER, first_time INTEGER, last_time INTEGER,
        overlap_rows INTEGER,
        new_rows INTEGER NOT NULL,
        weight INTEGER NOT NULL,
        lock_wait_ms INTEGER NOT NULL,
        pace_wait_ms INTEGER NOT NULL,
        latency_ms INTEGER NOT NULL,
        used_weight_1m INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS trades(
        segment INTEGER NOT NULL,
        id INTEGER NOT NULL,
        price TEXT NOT NULL,
        qty TEXT NOT NULL,
        quote_qty TEXT NOT NULL,
        time INTEGER NOT NULL,
        is_buyer_maker INTEGER NOT NULL,
        is_rpi INTEGER NOT NULL,
        PRIMARY KEY(segment, id)) WITHOUT ROWID""",
    'CREATE INDEX IF NOT EXISTS trades_time ON trades(time)',
    """CREATE TABLE IF NOT EXISTS minutes(
        minute_start INTEGER PRIMARY KEY,
        segment INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        first_id INTEGER NOT NULL,
        last_id INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        sealed_at INTEGER NOT NULL,
        first_seq INTEGER NOT NULL,
        sealed_by_seq INTEGER NOT NULL,
        segment_min_time INTEGER NOT NULL,
        segment_max_time INTEGER NOT NULL,
        conflict TEXT)""",
    """CREATE TABLE IF NOT EXISTS acknowledgements(
        minute_start INTEGER PRIMARY KEY,
        generation TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        acknowledged_at INTEGER NOT NULL,
        released_at INTEGER)""",
    """CREATE TABLE IF NOT EXISTS faults(
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        at INTEGER NOT NULL,
        code TEXT NOT NULL,
        detail TEXT NOT NULL)""",
)


@dataclass(frozen=True)
class RawTrade:
    """One ``/fapi/v1/trades`` row with its decimal fields kept as the provider's text."""

    id: int
    price: str
    qty: str
    quote_qty: str
    time: int
    is_buyer_maker: bool
    is_rpi: bool

    def provider_row(self) -> dict[str, object]:
        return {
            'id': self.id,
            'price': self.price,
            'qty': self.qty,
            'quoteQty': self.quote_qty,
            'time': self.time,
            'isBuyerMaker': self.is_buyer_maker,
            'isRPITrade': self.is_rpi,
        }


@dataclass(frozen=True)
class PageCost:
    weight: int = 0
    lock_wait_ms: int = 0
    pace_wait_ms: int = 0
    latency_ms: int = 0
    used_weight_1m: int = 0


@dataclass(frozen=True)
class CaptureOutcome:
    seq: int
    segment: int
    chained: bool
    closed_reason: str | None
    overlap_rows: int
    new_rows: int
    sealed: tuple[datetime, ...]


@dataclass(frozen=True)
class SealedMinute:
    minute_start: datetime
    rows: tuple[Row, ...]
    content_hash: str
    evidence: dict[str, object]


@dataclass(frozen=True)
class CaptureMiss:
    reason: str


@dataclass(frozen=True)
class MinuteCoverage:
    minute_start: datetime
    sealed: bool
    reason: str
    rows: int


@dataclass(frozen=True)
class AckOutcome:
    minute_start: datetime
    sealed: bool
    hash_matched: bool
    rows_released: int


@dataclass(frozen=True)
class UsefulWork:
    """Distinct newly sealed minutes and the raw inputs and request costs behind them.
    Overlap rows verified but not stored, re-polls and duplicate seals earn nothing."""

    sealed_minutes: int
    sealed_rows: int
    requests: int
    request_weight: int
    stored_rows: int
    overlap_rows: int
    latency_ms_total: int
    pace_wait_ms_total: int
    lock_wait_ms_total: int


def _int(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'Binance {what} must be an unsigned integer.')
    return value


def _text(value: object, what: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'Binance {what} must be decimal text.')
    return value


def _bool(value: object, what: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f'Binance {what} must be a boolean.')
    return value


def parse_recent_trades(body: bytes) -> tuple[RawTrade, ...]:
    """The exact ``/fapi/v1/trades`` shape: ids strictly increasing, times never decreasing."""
    payload: object = json.loads(body)
    if not isinstance(payload, list):
        raise ValueError('Binance recent trades must be a list.')
    trades: list[RawTrade] = []
    for item in cast(list[object], payload):
        if not isinstance(item, dict):
            raise ValueError('Binance recent trades must be objects.')
        row = cast(dict[object, object], item)
        if not {'id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isRPITrade'} <= set(row):
            raise ValueError('Binance recent trade lacks a documented field.')
        timestamp = _int(row['time'], 'time')
        if len(str(timestamp)) != 13:
            raise ValueError('Binance recent trade time must be milliseconds.')
        trade = RawTrade(
            _int(row['id'], 'id'),
            _text(row['price'], 'price'),
            _text(row['qty'], 'qty'),
            _text(row['quoteQty'], 'quoteQty'),
            timestamp,
            _bool(row['isBuyerMaker'], 'isBuyerMaker'),
            _bool(row['isRPITrade'], 'isRPITrade'),
        )
        if trades and (trade.id <= trades[-1].id or trade.time < trades[-1].time):
            raise ValueError('Binance recent trades are unordered or duplicated.')
        trades.append(trade)
    return tuple(trades)


def _ms(instant: datetime) -> int:
    return int(instant.astimezone(UTC).timestamp() * 1000)


def _minute(minute_start_ms: int) -> datetime:
    return datetime.fromtimestamp(minute_start_ms / 1000, UTC)


def _cell(row: Sequence[object], index: int) -> int:
    value = row[index]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError('Spool integer column is corrupt.')
    return value


def _optional(row: Sequence[object], index: int) -> int | None:
    value = row[index]
    return None if value is None else _cell(row, index)


def _row_tuple(row: object) -> tuple[object, ...]:
    if not isinstance(row, tuple):
        raise ValueError('Spool row is not a tuple.')
    return cast(tuple[object, ...], row)


def spool_path(directory: Path, source_key: str, symbol: str) -> Path:
    return directory / f'{source_key}.{symbol}.recent_trades.sqlite3'


class TradeSpool:
    def __init__(self, path: Path, mapper: Mapper, *, max_bytes: int, create: bool) -> None:
        if not path.is_absolute():
            raise ValueError('The trade spool needs an absolute shared mount path.')
        if max_bytes <= 0:
            raise ValueError('The trade spool byte cap must be positive.')
        if create:
            path.parent.mkdir(parents=True, exist_ok=True)
        elif not path.is_file():
            raise FileNotFoundError(path)
        self.path = path
        self.mapper = mapper
        self.max_bytes = max_bytes
        mode = 'rwc' if create else 'rw'
        self.connection = sqlite3.connect(
            f'file:{path}?mode={mode}', uri=True, timeout=10.0, isolation_level=None
        )
        self.connection.execute('PRAGMA busy_timeout = 10000')
        if create:
            self.connection.execute('PRAGMA journal_mode = WAL')
            self.connection.execute('PRAGMA synchronous = FULL')
            self.connection.execute('PRAGMA auto_vacuum = INCREMENTAL')
            with self._transaction():
                for statement in _SCHEMA:
                    self.connection.execute(statement)
                self.connection.execute(
                    'INSERT OR IGNORE INTO meta VALUES (?, ?)',
                    ('schema_version', str(SCHEMA_VERSION)),
                )
        version = self.connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        if version is None or _row_tuple(version)[0] != str(SCHEMA_VERSION):
            raise ValueError('The trade spool schema version is not the one this code owns.')

    @classmethod
    def create(
        cls, path: Path, mapper: Mapper, *, max_bytes: int = SPOOL_MAX_BYTES_DEFAULT
    ) -> TradeSpool:
        return cls(path, mapper, max_bytes=max_bytes, create=True)

    @classmethod
    def attach(
        cls, path: Path, mapper: Mapper, *, max_bytes: int = SPOOL_MAX_BYTES_DEFAULT
    ) -> TradeSpool:
        """Open an existing spool; never creates one (readers and acknowledgers)."""
        return cls(path, mapper, max_bytes=max_bytes, create=False)

    def close(self) -> None:
        self.connection.close()

    def _transaction(self) -> _Transaction:
        return _Transaction(self.connection)

    def _one(self, query: str, params: tuple[object, ...] = ()) -> tuple[object, ...] | None:
        row = self.connection.execute(query, params).fetchone()
        return None if row is None else _row_tuple(row)

    def _all(self, query: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        return [_row_tuple(row) for row in self.connection.execute(query, params).fetchall()]

    # -- capture -----------------------------------------------------------------

    def used_bytes(self) -> int:
        page_size = _cell(self._one('PRAGMA page_size') or (0,), 0)
        page_count = _cell(self._one('PRAGMA page_count') or (0,), 0)
        freelist = _cell(self._one('PRAGMA freelist_count') or (0,), 0)
        return (page_count - freelist) * page_size

    def backpressure(self) -> bool:
        return self.used_bytes() >= self.max_bytes

    def _open_segment(self) -> tuple[object, ...] | None:
        return self._one(
            'SELECT segment, min_id, max_id, min_time, max_time, sealed_through '
            'FROM segments WHERE closed_seq IS NULL ORDER BY segment DESC LIMIT 1'
        )

    def _latest_response(self, segment: int) -> tuple[object, ...] | None:
        return self._one(
            'SELECT seq, first_id, last_id, row_count FROM responses '
            'WHERE segment = ? AND row_count > 0 ORDER BY seq DESC LIMIT 1',
            (segment,),
        )

    def record(
        self,
        trades: Sequence[RawTrade],
        *,
        captured_at: datetime,
        completed_at: datetime,
        status: int,
        body_sha256: str,
        cost: PageCost,
    ) -> CaptureOutcome:
        """Store one page and seal every minute the page's segment now brackets."""
        if self.backpressure():
            raise RuntimeError(
                f'Trade spool holds {self.used_bytes()} unacknowledged bytes; cap {self.max_bytes}.'
            )
        with self._transaction():
            open_segment = self._open_segment()
            chained = False
            closed_reason: str | None = None
            overlap = 0
            if open_segment is None:
                segment = self._open_new_segment(None, None)
            else:
                segment = _cell(open_segment, 0)
                latest = self._latest_response(segment)
                if trades and latest is not None:
                    previous_last = _cell(latest, 2)
                    verdict = self._overlap_verdict(segment, trades, previous_last)
                    if verdict is None:
                        chained = True
                        overlap = sum(1 for trade in trades if trade.id <= previous_last)
                    else:
                        closed_reason = verdict
            seq = self._next_seq()
            if closed_reason is not None:
                self.connection.execute(
                    'UPDATE segments SET closed_seq = ?, closed_reason = ? WHERE segment = ?',
                    (seq, closed_reason, segment),
                )
                segment = self._open_new_segment(seq, closed_reason)
            new_rows = 0
            if trades:
                previous_last = -1
                if chained:
                    latest = self._latest_response(segment)
                    previous_last = _cell(latest, 2) if latest is not None else -1
                fresh = [trade for trade in trades if trade.id > previous_last]
                new_rows = len(fresh)
                self.connection.executemany(
                    'INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    [
                        (
                            segment,
                            trade.id,
                            trade.price,
                            trade.qty,
                            trade.quote_qty,
                            trade.time,
                            int(trade.is_buyer_maker),
                            int(trade.is_rpi),
                        )
                        for trade in fresh
                    ],
                )
                self.connection.execute(
                    'UPDATE segments SET min_id = COALESCE(min_id, ?), max_id = ?, '
                    'min_time = COALESCE(min_time, ?), max_time = ? WHERE segment = ?',
                    (trades[0].id, trades[-1].id, trades[0].time, trades[-1].time, segment),
                )
            self.connection.execute(
                'INSERT INTO responses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    seq,
                    segment,
                    _ms(captured_at),
                    _ms(completed_at),
                    status,
                    body_sha256,
                    len(trades),
                    trades[0].id if trades else None,
                    trades[-1].id if trades else None,
                    trades[0].time if trades else None,
                    trades[-1].time if trades else None,
                    overlap if chained else None,
                    new_rows,
                    cost.weight,
                    cost.lock_wait_ms,
                    cost.pace_wait_ms,
                    cost.latency_ms,
                    cost.used_weight_1m,
                ),
            )
            sealed = self._seal(segment, seq, completed_at) if trades else ()
            self._release_acknowledged(completed_at)
        return CaptureOutcome(seq, segment, chained, closed_reason, overlap, new_rows, sealed)

    def _next_seq(self) -> int:
        row = self._one('SELECT COALESCE(MAX(seq), 0) + 1 FROM responses')
        return _cell(row or (1,), 0)

    def _open_new_segment(self, seq: int | None, reason: str | None) -> int:
        opened = seq if seq is not None else self._next_seq()
        cursor = self.connection.execute('INSERT INTO segments(opened_seq) VALUES (?)', (opened,))
        if cursor.lastrowid is None:
            raise RuntimeError('SQLite did not assign a segment id.')
        return cursor.lastrowid

    def _overlap_verdict(
        self, segment: int, trades: Sequence[RawTrade], previous_last: int
    ) -> str | None:
        """``None`` when the page chains to the segment, else the closing reason."""
        first = trades[0].id
        if first > previous_last:
            return 'overlap_missing'
        stored = {
            _cell(row, 0): row[1:]
            for row in self._all(
                'SELECT id, price, qty, quote_qty, time, is_buyer_maker, is_rpi FROM trades '
                'WHERE segment = ? AND id >= ? AND id <= ?',
                (segment, first, previous_last),
            )
        }
        if not stored:
            return 'overlap_released'
        seen: set[int] = set()
        for trade in trades:
            if trade.id > previous_last:
                break
            expected = stored.get(trade.id)
            if expected is None:
                return 'conflict'
            if expected != (
                trade.price,
                trade.qty,
                trade.quote_qty,
                trade.time,
                int(trade.is_buyer_maker),
                int(trade.is_rpi),
            ):
                return 'conflict'
            seen.add(trade.id)
        if seen != set(stored):
            return 'conflict'
        return None

    def _seal(self, segment: int, seq: int, completed_at: datetime) -> tuple[datetime, ...]:
        row = self._one(
            'SELECT min_time, max_time, sealed_through FROM segments WHERE segment = ?',
            (segment,),
        )
        if row is None:
            raise RuntimeError('Segment vanished during capture.')
        min_time, max_time, sealed_through = _cell(row, 0), _cell(row, 1), _cell(row, 2)
        first_minute = min_time // 60000 + 1
        last_minute = max_time // 60000 - 1
        sealed: list[datetime] = []
        minute = max(first_minute, sealed_through + 1)
        bracket = (min_time, max_time)
        while minute <= last_minute:
            start_ms = minute * 60000
            if self._seal_minute(segment, seq, start_ms, completed_at, bracket):
                sealed.append(_minute(start_ms))
            minute += 1
        if last_minute > sealed_through:
            self.connection.execute(
                'UPDATE segments SET sealed_through = ? WHERE segment = ?', (last_minute, segment)
            )
        return tuple(sealed)

    def _minute_rows(self, segment: int, start_ms: int) -> list[tuple[object, ...]]:
        return self._all(
            'SELECT id, price, qty, quote_qty, time, is_buyer_maker, is_rpi FROM trades '
            'WHERE segment = ? AND time >= ? AND time < ? ORDER BY id',
            (segment, start_ms, start_ms + 60000),
        )

    def _mapped(self, rows: Sequence[tuple[object, ...]]) -> tuple[Row, ...]:
        return tuple(
            self.mapper(
                RawTrade(
                    _cell(row, 0),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    _cell(row, 4),
                    bool(_cell(row, 5)),
                    bool(_cell(row, 6)),
                ).provider_row()
            )
            for row in rows
        )

    def _seal_minute(
        self,
        segment: int,
        seq: int,
        start_ms: int,
        completed_at: datetime,
        bracket: tuple[int, int],
    ) -> bool:
        rows = self._minute_rows(segment, start_ms)
        if not rows:
            return False
        digest = content_hash(self._mapped(rows), schema_version=1)
        existing = self._one(
            'SELECT content_hash, conflict FROM minutes WHERE minute_start = ?', (start_ms,)
        )
        if existing is not None:
            if existing[0] != digest and existing[1] is None:
                self.connection.execute(
                    'UPDATE minutes SET conflict = ? WHERE minute_start = ?',
                    (f'segment {segment} sealed a different hash {digest}', start_ms),
                )
            return False
        opened = self._one('SELECT opened_seq FROM segments WHERE segment = ?', (segment,))
        if opened is None:
            raise RuntimeError('Segment vanished during sealing.')
        self.connection.execute(
            'INSERT INTO minutes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)',
            (
                start_ms,
                segment,
                len(rows),
                _cell(rows[0], 0),
                _cell(rows[-1], 0),
                digest,
                _ms(completed_at),
                _cell(opened, 0),
                seq,
                bracket[0],
                bracket[1],
            ),
        )
        return True

    def record_fault(self, at: datetime, code: str, detail: str) -> None:
        with self._transaction():
            self.connection.execute(
                'INSERT INTO faults(at, code, detail) VALUES (?, ?, ?)',
                (_ms(at), code[:200], detail[:1000]),
            )
            self.connection.execute(
                'DELETE FROM faults WHERE seq <= (SELECT MAX(seq) FROM faults) - ?',
                (FAULT_RETENTION_ROWS,),
            )

    # -- reading -----------------------------------------------------------------

    def sealed_minute(self, minute_start: datetime) -> SealedMinute | CaptureMiss:
        """The sealed minute with its hash re-verified from the stored rows, or the miss reason."""
        start_ms = _ms(minute_start)
        if start_ms % 60000:
            raise ValueError('Sealed minutes start on minute boundaries.')
        row = self._one(
            'SELECT segment, row_count, first_id, last_id, content_hash, sealed_at, first_seq, '
            'sealed_by_seq, segment_min_time, segment_max_time, conflict FROM minutes '
            'WHERE minute_start = ?',
            (start_ms,),
        )
        if row is None:
            return CaptureMiss(self._miss_reason(start_ms))
        if row[10] is not None:
            return CaptureMiss(f'conflict: {row[10]}')
        segment = _cell(row, 0)
        rows = self._minute_rows(segment, start_ms)
        if len(rows) != _cell(row, 1):
            return CaptureMiss('rows_released' if not rows else 'row_count_mismatch')
        mapped = self._mapped(rows)
        digest = content_hash(mapped, schema_version=1)
        if digest != row[4]:
            return CaptureMiss('checksum_mismatch')
        evidence: dict[str, object] = {
            'spool': str(self.path),
            'segment': segment,
            'responses': [_cell(row, 6), _cell(row, 7)],
            'segment_first_time': _cell(row, 8),
            'segment_last_time': _cell(row, 9),
            'first_id': _cell(row, 2),
            'last_id': _cell(row, 3),
            'row_count': len(rows),
            'sealed_at': _minute(_cell(row, 5)).isoformat(),
            'content_hash': digest,
        }
        return SealedMinute(minute_start, mapped, digest, evidence)

    def _miss_reason(self, start_ms: int) -> str:
        acknowledged = self._one(
            'SELECT generation FROM acknowledgements WHERE minute_start = ?', (start_ms,)
        )
        if acknowledged is not None:
            return 'acknowledged_without_seal'
        segments = self._all(
            'SELECT segment, min_time, max_time, closed_reason FROM segments '
            'WHERE min_time IS NOT NULL AND min_time < ? AND max_time >= ?',
            (start_ms + 60000, start_ms),
        )
        if not segments:
            return 'not_captured'
        reasons: list[str] = []
        for entry in segments:
            if _cell(entry, 1) >= start_ms:
                reasons.append(f'segment {_cell(entry, 0)}: {_SEGMENT_HEAD}')
            elif _cell(entry, 2) < start_ms + 60000:
                closed = entry[3]
                reasons.append(
                    f'segment {_cell(entry, 0)}: {_NOT_BRACKETED}'
                    + (f' ({closed})' if isinstance(closed, str) else '')
                )
            else:
                reasons.append(f'segment {_cell(entry, 0)}: no rows inside the minute')
        return '; '.join(reasons)

    def coverage(self, start: datetime, end: datetime) -> tuple[MinuteCoverage, ...]:
        """Every minute of ``[start, end)`` with its sealed state and, otherwise, why not."""
        result: list[MinuteCoverage] = []
        cursor = _ms(start) - _ms(start) % 60000
        while cursor < _ms(end):
            row = self._one(
                'SELECT row_count, conflict FROM minutes WHERE minute_start = ?', (cursor,)
            )
            if row is not None and row[1] is None:
                result.append(MinuteCoverage(_minute(cursor), True, 'sealed', _cell(row, 0)))
            else:
                reason = f'conflict: {row[1]}' if row is not None else self._miss_reason(cursor)
                count = self._one(
                    'SELECT COUNT(DISTINCT id) FROM trades WHERE time >= ? AND time < ?',
                    (cursor, cursor + 60000),
                )
                result.append(
                    MinuteCoverage(_minute(cursor), False, reason, _cell(count or (0,), 0))
                )
            cursor += 60000
        return tuple(result)

    def useful_work(self, start: datetime, end: datetime) -> UsefulWork:
        """Credit earned by responses completed inside ``[start, end)``."""
        totals = self._one(
            'SELECT COUNT(*), COALESCE(SUM(weight), 0), COALESCE(SUM(new_rows), 0), '
            'COALESCE(SUM(overlap_rows), 0), COALESCE(SUM(latency_ms), 0), '
            'COALESCE(SUM(pace_wait_ms), 0), COALESCE(SUM(lock_wait_ms), 0) '
            'FROM responses WHERE completed_at >= ? AND completed_at < ?',
            (_ms(start), _ms(end)),
        )
        sealed = self._one(
            'SELECT COUNT(*), COALESCE(SUM(row_count), 0) FROM minutes '
            'WHERE conflict IS NULL AND sealed_at >= ? AND sealed_at < ?',
            (_ms(start), _ms(end)),
        )
        if totals is None or sealed is None:
            raise RuntimeError('SQLite aggregate returned no row.')
        return UsefulWork(
            sealed_minutes=_cell(sealed, 0),
            sealed_rows=_cell(sealed, 1),
            requests=_cell(totals, 0),
            request_weight=_cell(totals, 1),
            stored_rows=_cell(totals, 2),
            overlap_rows=_cell(totals, 3),
            latency_ms_total=_cell(totals, 4),
            pace_wait_ms_total=_cell(totals, 5),
            lock_wait_ms_total=_cell(totals, 6),
        )

    def health(self, now: datetime) -> dict[str, object]:
        latest = self._one(
            'SELECT completed_at, status, row_count, overlap_rows, segment FROM responses '
            'ORDER BY seq DESC LIMIT 1'
        )
        newest_sealed = self._one('SELECT MAX(minute_start) FROM minutes WHERE conflict IS NULL')
        unacknowledged = self._one(
            'SELECT COUNT(*) FROM minutes WHERE conflict IS NULL AND minute_start NOT IN '
            '(SELECT minute_start FROM acknowledgements)'
        )
        stored = self._one('SELECT COUNT(*) FROM trades')
        open_segment = self._open_segment()
        fault = self._one('SELECT at, code FROM faults ORDER BY seq DESC LIMIT 1')
        newest = None if newest_sealed is None else _optional(newest_sealed, 0)
        latest_at = None if latest is None else _cell(latest, 0)
        return {
            'spool': str(self.path),
            'used_bytes': self.used_bytes(),
            'max_bytes': self.max_bytes,
            'backpressure': self.backpressure(),
            'last_response_at': None if latest_at is None else _minute(latest_at).isoformat(),
            'last_response_age_seconds': (
                None if latest_at is None else round((_ms(now) - latest_at) / 1000, 3)
            ),
            'last_response_rows': None if latest is None else _cell(latest, 2),
            'last_response_overlap': None if latest is None else _optional(latest, 3),
            'open_segment': None if open_segment is None else _cell(open_segment, 0),
            'newest_sealed_minute': None if newest is None else _minute(newest).isoformat(),
            'newest_sealed_age_seconds': (
                None if newest is None else round((_ms(now) - newest - 60000) / 1000, 3)
            ),
            'unacknowledged_sealed_minutes': _cell(unacknowledged or (0,), 0),
            'stored_rows': _cell(stored or (0,), 0),
            'last_fault': None
            if fault is None
            else {
                'at': _minute(_cell(fault, 0)).isoformat(),
                'code': str(fault[1]),
            },
        }

    # -- acknowledgement and retention ---------------------------------------------

    def acknowledge(
        self, minute_start: datetime, *, content_hash: str, generation: str, now: datetime
    ) -> AckOutcome:
        """Record that a durable accepted generation covers the minute, then release its rows.

        A sealed hash that differs from the accepted one is recorded as a conflict and
        logged by the caller; disagreement preserves the captured copy for resolution.
        Only an agreeing accepted generation authorizes releasing a sealed minute.
        """
        start_ms = _ms(minute_start)
        if start_ms % 60000:
            raise ValueError('Acknowledged minutes start on minute boundaries.')
        if not generation or not content_hash:
            raise ValueError('An acknowledgement names its generation and content hash.')
        with self._transaction():
            sealed = self._one(
                'SELECT content_hash FROM minutes WHERE minute_start = ?', (start_ms,)
            )
            matched = sealed is not None and sealed[0] == content_hash
            if sealed is not None and not matched:
                self.connection.execute(
                    'UPDATE minutes SET conflict = COALESCE(conflict, ?) WHERE minute_start = ?',
                    (f'acknowledged generation {generation} hashes {content_hash}', start_ms),
                )
            if sealed is not None and not matched:
                # A divergent accepted input must not discard the sole captured copy.
                return AckOutcome(minute_start, True, False, 0)
            self.connection.execute(
                'INSERT OR REPLACE INTO acknowledgements VALUES (?, ?, ?, ?, NULL)',
                (start_ms, generation, content_hash, _ms(now)),
            )
            released = self._release_acknowledged(now)
        return AckOutcome(minute_start, sealed is not None, matched, released.get(start_ms, 0))

    def _release_acknowledged(self, now: datetime) -> dict[int, int]:
        """Delete rows of acknowledged minutes once no future page can overlap them."""
        open_segment = self._open_segment()
        safe_time = 0
        open_id = -1
        if open_segment is not None:
            open_id = _cell(open_segment, 0)
            latest = self._one(
                'SELECT first_time FROM responses WHERE segment = ? AND row_count > 0 '
                'ORDER BY seq DESC LIMIT 1',
                (open_id,),
            )
            safe_time = _cell(latest, 0) if latest is not None else 0
        released: dict[int, int] = {}
        for row in self._all(
            'SELECT minute_start FROM acknowledgements WHERE released_at IS NULL ORDER BY minute_start'
        ):
            start_ms = _cell(row, 0)
            end_ms = start_ms + 60000
            cursor = self.connection.execute(
                'DELETE FROM trades WHERE time >= ? AND time < ? AND (segment <> ? OR ? <= ?)',
                (start_ms, end_ms, open_id, end_ms, safe_time),
            )
            remaining = self._one(
                'SELECT COUNT(*) FROM trades WHERE time >= ? AND time < ?', (start_ms, end_ms)
            )
            released[start_ms] = cursor.rowcount
            if _cell(remaining or (0,), 0) == 0:
                self.connection.execute(
                    'UPDATE acknowledgements SET released_at = ? WHERE minute_start = ?',
                    (_ms(now), start_ms),
                )
        self._prune(now)
        return released

    def _prune(self, now: datetime) -> None:
        last = self._one("SELECT value FROM meta WHERE key='last_prune_ms'")
        instant = _ms(now)
        if last is not None and instant < int(str(last[0])) + PRUNE_INTERVAL_MS:
            return
        # Keep receipts that still describe retained input, not every response
        # newer than the oldest partial head. One unresolved head must not pin
        # an otherwise acknowledged response history forever.
        self.connection.execute(
            'DELETE FROM responses WHERE seq < (SELECT MAX(seq) FROM responses) '
            'AND NOT EXISTS (SELECT 1 FROM trades WHERE trades.segment=responses.segment '
            'AND trades.id >= responses.first_id AND trades.id <= responses.last_id)'
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO meta VALUES ('last_prune_ms', ?)", (str(instant),)
        )
        self.connection.execute(
            'DELETE FROM segments WHERE closed_seq IS NOT NULL AND segment NOT IN '
            '(SELECT DISTINCT segment FROM responses) AND segment NOT IN '
            '(SELECT DISTINCT segment FROM trades) AND segment NOT IN '
            '(SELECT DISTINCT segment FROM minutes)'
        )
        horizon = _ms(now - RELEASED_MINUTE_RETENTION)
        self.connection.execute(
            'DELETE FROM minutes WHERE minute_start < ? AND minute_start IN '
            '(SELECT minute_start FROM acknowledgements WHERE released_at IS NOT NULL)',
            (horizon,),
        )
        self.connection.execute(
            'DELETE FROM acknowledgements WHERE minute_start < ? AND released_at IS NOT NULL',
            (horizon,),
        )
        self.connection.execute('PRAGMA incremental_vacuum(256)')

    def segments(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                'segment': _cell(row, 0),
                'opened_seq': _cell(row, 1),
                'closed_seq': _optional(row, 2),
                'closed_reason': row[3],
                'min_id': _optional(row, 4),
                'max_id': _optional(row, 5),
                'min_time': _optional(row, 6),
                'max_time': _optional(row, 7),
            }
            for row in self._all(
                'SELECT segment, opened_seq, closed_seq, closed_reason, min_id, max_id, '
                'min_time, max_time FROM segments ORDER BY segment'
            )
        )


class _Transaction:
    """``BEGIN IMMEDIATE`` .. ``COMMIT``, rolled back on any exception."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def __enter__(self) -> None:
        self.connection.execute('BEGIN IMMEDIATE')

    def __exit__(self, kind: object, value: object, traceback: object) -> None:
        if kind is None:
            self.connection.execute('COMMIT')
        else:
            self.connection.execute('ROLLBACK')


def spool_directory(environ: Mapping[str, str]) -> Path:
    return Path(environ.get(SPOOL_DIR_ENV, SPOOL_DIR_DEFAULT))


def spool_max_bytes(environ: Mapping[str, str]) -> int:
    value = environ.get(SPOOL_MAX_BYTES_ENV)
    return int(value) if value else SPOOL_MAX_BYTES_DEFAULT


def iter_minutes(start: datetime, end: datetime) -> Iterator[datetime]:
    cursor = start.astimezone(UTC).replace(second=0, microsecond=0)
    while cursor < end:
        yield cursor
        cursor += timedelta(minutes=1)
