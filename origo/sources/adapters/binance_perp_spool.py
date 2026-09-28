from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Literal, TypedDict, cast

from ..contracts import Partition, Revision, Row, SourceError, failure_code
from ..hashing import content_hash
from .binance_daily import get_response
from .binance_perp_daily import parse_decimal, timestamp_datetime
from .binance_provisional import _bool, _int, _text

SPOOL_LIMIT_BYTES = 16 * 1024**3
MINUTE_LIMIT_BYTES = 32 * 1024**2
REPAIR_PAGE_BUDGET = 7
HISTORICAL_LAG_SECONDS = 60
CAPTURE_PROGRESS_SECONDS = 120
_LOG = logging.getLogger(__name__)
_SCHEMA_VERSION = 1
_SOURCE = 'binance_perp_trades'
_SYMBOL = 'BTCUSDT'


class MinuteProof(TypedDict):
    schema_version: Literal[1]
    source_key: Literal['binance_perp_trades']
    partition_key: str
    symbol: Literal['BTCUSDT']
    chain_ids: list[str]
    row_count: int
    content_hash: str
    first_trade_id: int
    last_trade_id: int
    before_start_trade_id: int
    at_or_after_end_trade_id: int
    evidence_json: str


@dataclass(frozen=True)
class CaptureCommit:
    segment_id: str
    last_durable_capture_at: datetime
    complete_through: datetime | None
    spool_bytes: int
    advanced: bool
    overlap: bool
    pending_gaps: int = 0


@dataclass(frozen=True)
class _Segment:
    identity: str
    first_id: int
    last_id: int
    first_ms: int
    last_ms: int
    quarantined: bool


@dataclass(frozen=True)
class _Gap:
    identity: str
    left: _Segment
    right: _Segment


def historical_row(row: Mapping[str, object]) -> Row:
    timestamp = _int(row, 'time')
    if len(str(timestamp)) != 13:
        raise ValueError('The frozen provisional perp timestamp contract is milliseconds.')
    price = parse_decimal(_text(row, 'price'))
    quantity = parse_decimal(_text(row, 'qty'))
    parse_decimal(_text(row, 'quoteQty'))
    return (
        _int(row, 'id'), price, quantity, (price * quantity).normalize(),
        timestamp, _bool(row, 'isBuyerMaker'), timestamp_datetime(timestamp),
    )


def _wire(row: Mapping[str, object]) -> str:
    normalized = historical_row(row)
    return json.dumps({
        'id': normalized[0], 'price': format(cast(Decimal, normalized[1]).normalize(), 'f'),
        'qty': format(cast(Decimal, normalized[2]).normalize(), 'f'),
        'quoteQty': str(normalized[3]), 'time': normalized[4],
        'isBuyerMaker': bool(normalized[5]),
    }, sort_keys=True, separators=(',', ':'))


def _rows(rows: Sequence[Mapping[str, object]]) -> tuple[tuple[int, int, str], ...]:
    result = tuple((_int(row, 'id'), _int(row, 'time'), _wire(row)) for row in rows)
    if any(right[0] <= left[0] or right[1] < left[1]
           for left, right in pairwise(result)):
        raise SourceError('CAPTURE_UNORDERED', 'Trade IDs or timestamps are unordered.')
    return result


def _unwire(payload: str) -> Row:
    return historical_row(cast(dict[str, object], json.loads(payload)))


def _sync_directory(root: Path) -> None:
    descriptor = os.open(root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def spool_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob('*') if path.is_file())


def _capacity(root: Path, extra: int = 0) -> None:
    # Reserve journal/index/checkpoint headroom as well as payload bytes.
    if spool_bytes(root) + extra + 8 * 1024**2 > SPOOL_LIMIT_BYTES:
        raise SourceError('CAPTURE_CAPACITY', 'The durable perp spool is at capacity.')


@contextmanager
def _database(root: Path, owner: Literal['capture', 'repair'], *, create: bool = False
              ) -> Iterator[sqlite3.Connection]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{owner}.sqlite3'
    if not create and not path.exists():
        raise SourceError('CAPTURE_MISSING', 'The durable capture store is absent.')
    initialization = (root / f'{owner}.init.lock').open('a+')
    fcntl.flock(initialization, fcntl.LOCK_EX)
    connection = sqlite3.connect(path, timeout=1)
    try:
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        if version not in (0, _SCHEMA_VERSION):
            raise SourceError('CAPTURE_SCHEMA', 'Unsupported durable perp spool schema.')
        if version == 0 and not create:
            tables = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone()
            if tables is None:
                raise SourceError('CAPTURE_MISSING', 'The durable capture store has not been initialized.')
            raise SourceError('CAPTURE_SCHEMA', 'Unsupported durable perp spool schema.')
        if version == 0:
            connection.execute('PRAGMA auto_vacuum=INCREMENTAL')
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA synchronous=FULL')
        connection.execute('PRAGMA wal_autocheckpoint=128')
        if version == 0:
            if owner == 'capture':
                connection.executescript('''
                    BEGIN IMMEDIATE;
                    CREATE TABLE identity(source TEXT NOT NULL, symbol TEXT NOT NULL);
                    INSERT INTO identity VALUES ('binance_perp_trades', 'BTCUSDT');
                    CREATE TABLE segments(seq INTEGER PRIMARY KEY, identity TEXT UNIQUE,
                        first_id INTEGER, last_id INTEGER, first_ms INTEGER, last_ms INTEGER,
                        quarantined INTEGER NOT NULL DEFAULT 0);
                    CREATE TABLE trades(id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                        segment TEXT NOT NULL, payload TEXT NOT NULL);
                    CREATE INDEX trades_time ON trades(time);
                    CREATE INDEX trades_segment ON trades(segment);
                    CREATE TABLE pages(identity TEXT PRIMARY KEY, segment TEXT NOT NULL,
                        first_id INTEGER, last_id INTEGER, first_ms INTEGER, last_ms INTEGER,
                        previous_id INTEGER, evidence TEXT NOT NULL);
                    CREATE INDEX pages_segment ON pages(segment, first_id);
                    CREATE TABLE polls(seq INTEGER PRIMARY KEY, received_at TEXT NOT NULL,
                        first_id INTEGER, last_id INTEGER, evidence TEXT NOT NULL);
                    CREATE TABLE state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        segment TEXT, durable_at TEXT, response_at TEXT, overlap INTEGER);
                ''')
            else:
                connection.executescript('''
                    BEGIN IMMEDIATE;
                    CREATE TABLE identity(source TEXT NOT NULL, symbol TEXT NOT NULL);
                    INSERT INTO identity VALUES ('binance_perp_trades', 'BTCUSDT');
                    CREATE TABLE gaps(identity TEXT PRIMARY KEY, left_id INTEGER,
                        right_id INTEGER, left_payload TEXT, right_payload TEXT,
                        next_id INTEGER, complete INTEGER NOT NULL DEFAULT 0,
                        error TEXT, created_at TEXT NOT NULL, retired INTEGER NOT NULL DEFAULT 0);
                    CREATE TABLE pages(gap TEXT, first_id INTEGER, last_id INTEGER,
                        body_sha256 TEXT, evidence TEXT, normalized_hash TEXT, PRIMARY KEY(gap, first_id));
                    CREATE TABLE trades(gap TEXT, id INTEGER, time INTEGER,
                        payload TEXT NOT NULL, PRIMARY KEY(gap, id));
                    CREATE INDEX trades_time ON trades(time);
                    CREATE TABLE attempts(seq INTEGER PRIMARY KEY, gap TEXT NOT NULL,
                        started_at TEXT NOT NULL, params TEXT NOT NULL, outcome TEXT);
                    CREATE TABLE acknowledgments(start_ms INTEGER, end_ms INTEGER,
                        PRIMARY KEY(start_ms, end_ms));
                ''')
            connection.execute(f'PRAGMA user_version={_SCHEMA_VERSION}')
            connection.commit()
            _sync_directory(root)
        identity = connection.execute('SELECT source,symbol FROM identity').fetchall()
        if identity != [(_SOURCE, _SYMBOL)]:
            raise SourceError('CAPTURE_IDENTITY', 'The perp spool source or symbol differs.')
        fcntl.flock(initialization, fcntl.LOCK_UN)
        initialization.close()
        yield connection
    except sqlite3.DatabaseError as error:
        raise SourceError('CAPTURE_STORAGE', 'The durable perp spool cannot be read or written.') from error
    finally:
        initialization.close()
        connection.close()


def _segments(connection: sqlite3.Connection, start_ms: int | None = None,
              end_ms: int | None = None) -> tuple[_Segment, ...]:
    if start_ms is None:
        records = connection.execute('SELECT identity,first_id,last_id,first_ms,last_ms,quarantined '
                                     'FROM segments ORDER BY seq DESC LIMIT 1').fetchall()
    else:
        records = connection.execute('SELECT identity,first_id,last_id,first_ms,last_ms,quarantined '
            'FROM segments WHERE seq >= COALESCE((SELECT seq FROM segments WHERE first_ms < ? '
            'ORDER BY seq DESC LIMIT 1), 0) AND seq <= COALESCE((SELECT seq FROM segments WHERE last_ms >= ? '
            'ORDER BY seq LIMIT 1), (SELECT max(seq) FROM segments)) ORDER BY seq LIMIT 10001',
            (start_ms, end_ms)).fetchall()
        if len(records) > 10000:
            raise SourceError('CAPTURE_FRAGMENTED', 'The minute exceeds the segment read bound.')
    return tuple(_Segment(str(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]),
                          bool(row[5])) for row in records)


def _commit(root: Path, connection: sqlite3.Connection, *, advanced: bool) -> CaptureCommit | None:
    state = connection.execute('SELECT segment,durable_at,overlap FROM state WHERE singleton=1').fetchone()
    if state is None:
        return None
    segment = _segments(connection)[0]
    first = datetime.fromtimestamp(segment.first_ms / 1000, UTC)
    last = datetime.fromtimestamp(segment.last_ms / 1000, UTC).replace(second=0, microsecond=0)
    pending_gaps = _pending_gaps(root, connection)
    complete = last if first < last and not segment.quarantined and pending_gaps == 0 else None
    return CaptureCommit(str(state[0]), datetime.fromisoformat(str(state[1])), complete,
                         spool_bytes(root), advanced, bool(state[2]), pending_gaps)


def _pending_gaps(root: Path, capture: sqlite3.Connection) -> int:
    count = int(capture.execute('SELECT max(count(*) - 1, 0) FROM segments').fetchone()[0])
    if (root / 'repair.sqlite3').exists():
        with _database(root, 'repair') as repair:
            count -= int(repair.execute('SELECT count(*) FROM gaps WHERE complete=1 OR retired=1').fetchone()[0])
    return max(0, count)


def capture_state(root: Path) -> CaptureCommit | None:
    if not (root / 'capture.sqlite3').exists():
        return None
    with _database(root, 'capture', create=True) as connection:
        return _commit(root, connection, advanced=False)


def begin_capture_attempt(root: Path, evidence: Mapping[str, object]) -> int:
    if (root / 'quarantine.json').exists():
        raise SourceError('CAPTURE_QUARANTINED', 'The durable spool requires explicit historical repair.')
    root.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(dict(evidence), sort_keys=True)
    if len(serialized.encode()) > 16384:
        raise SourceError('CAPTURE_EVIDENCE_CAPACITY', 'Poll intent exceeds 16 KiB.')
    _capacity(root, len(serialized.encode()) * 4 + 65536)
    with _database(root, 'capture', create=True) as connection:
        attempt = connection.execute('INSERT INTO polls(received_at,evidence) VALUES (?,?)',
            (now_utc().isoformat(), serialized)).lastrowid
        connection.commit()
    if attempt is None:
        raise SourceError('CAPTURE_ATTEMPT_MISSING', 'Poll intent has no durable identity.')
    return attempt


def finish_capture_attempt(root: Path, attempt_id: int, evidence: Mapping[str, object]) -> None:
    with _database(root, 'capture') as connection:
        existing = connection.execute('SELECT evidence FROM polls WHERE seq=?', (attempt_id,)).fetchone()
        if existing is None:
            raise SourceError('CAPTURE_ATTEMPT_MISSING', 'The durable poll intent is absent.')
        merged = cast(dict[str, object], json.loads(str(existing[0])))
        merged.update(evidence)
        serialized = json.dumps(merged, sort_keys=True)
        if len(serialized.encode()) > 16384:
            raise SourceError('CAPTURE_EVIDENCE_CAPACITY', 'Poll outcome exceeds 16 KiB.')
        _capacity(root, len(serialized.encode()) * 4 + 65536)
        connection.execute('UPDATE polls SET evidence=? WHERE seq=?', (serialized, attempt_id))
        connection.commit()


def append_capture(root: Path, rows: Sequence[Mapping[str, object]], *,
                   received_at: datetime, evidence: Mapping[str, object] | None = None,
                   attempt_id: int | None = None) -> CaptureCommit:
    if received_at.tzinfo != UTC:
        raise ValueError('Capture receipt time must be UTC.')
    if not 1 <= len(rows) <= 1000:
        raise SourceError('CAPTURE_RESPONSE_SIZE', 'Recent capture requires 1 to 1000 actual trades.')
    records = _rows(rows)
    if (root / 'quarantine.json').exists():
        raise SourceError('CAPTURE_QUARANTINED', 'The durable spool is quarantined; historical repair is explicit.')
    root.mkdir(parents=True, exist_ok=True)
    _capacity(root, sum(len(row[2].encode()) for row in records) * 4 + 65536)
    serialized_evidence = json.dumps(dict(evidence or {}), sort_keys=True)
    if len(serialized_evidence.encode()) > 16384:
        raise SourceError('CAPTURE_EVIDENCE_CAPACITY', 'Capture response evidence exceeds 16 KiB.')
    conflicts: set[str] = set()
    result: CaptureCommit | None = None
    with _database(root, 'capture', create=True) as connection:
        connection.execute('BEGIN IMMEDIATE')
        if attempt_id is None:
            connection.execute('INSERT INTO polls(received_at,first_id,last_id,evidence) VALUES (?,?,?,?)',
                               (received_at.isoformat(), records[0][0], records[-1][0], serialized_evidence))
        else:
            changed = connection.execute('UPDATE polls SET received_at=?,first_id=?,last_id=?,evidence=? WHERE seq=?',
                               (received_at.isoformat(), records[0][0], records[-1][0], serialized_evidence, attempt_id)).rowcount
            if changed != 1:
                raise SourceError('CAPTURE_ATTEMPT_MISSING', 'The durable poll intent is absent.')
        current = _segments(connection)
        previous = current[0] if current else None
        if previous is not None and previous.quarantined:
            raise SourceError('CAPTURE_QUARANTINED', 'Capture is quarantined pending explicit historical repair.')
        for trade_id, _, payload in records:
            saved = connection.execute('SELECT payload,segment FROM trades WHERE id=?', (trade_id,)).fetchone()
            if saved is not None and str(saved[0]) != payload:
                conflicts.add(str(saved[1]))
        overlap = previous is not None and any(row[0] == previous.last_id for row in records)
        if previous is not None and records[0][0] <= previous.last_id < records[-1][0] and not overlap:
            conflicts.add(previous.identity)
        if previous is not None and not overlap and records[-1][0] > previous.last_id and records[0][1] < previous.last_ms:
            conflicts.add(previous.identity)
        if conflicts:
            connection.executemany('UPDATE segments SET quarantined=1 WHERE identity=?', ((identity,) for identity in conflicts))
            connection.commit()
        else:
            advanced = previous is None or records[-1][0] > previous.last_id
            if advanced:
                unseen = records if previous is None or not overlap else tuple(row for row in records if row[0] > previous.last_id)
                if overlap and previous is not None:
                    identity = previous.identity
                    connection.execute('UPDATE segments SET last_id=?,last_ms=? WHERE identity=?',
                                       (records[-1][0], records[-1][1], identity))
                else:
                    identity = hashlib.sha256(records[0][2].encode()).hexdigest()
                    connection.execute('INSERT INTO segments(identity,first_id,last_id,first_ms,last_ms) VALUES (?,?,?,?,?)',
                                       (identity, records[0][0], records[-1][0], records[0][1], records[-1][1]))
                connection.executemany('INSERT INTO trades(id,time,segment,payload) VALUES (?,?,?,?)',
                                       ((row[0], row[1], identity, row[2]) for row in unseen))
                page_identity = hashlib.sha256(''.join(row[2] for row in unseen).encode()).hexdigest()
                connection.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?)',
                    (page_identity, identity, unseen[0][0], unseen[-1][0], unseen[0][1], unseen[-1][1],
                     previous.last_id if overlap and previous is not None else None,
                     serialized_evidence))
                connection.execute('INSERT OR REPLACE INTO state VALUES (1,?,?,?,?)',
                                   (identity, received_at.isoformat(), received_at.isoformat(), int(overlap or previous is None)))
            else:
                connection.execute('UPDATE state SET response_at=? WHERE singleton=1', (received_at.isoformat(),))
            connection.commit()
            result = _commit(root, connection, advanced=advanced)
            if result is None:
                raise SourceError('CAPTURE_STORAGE', 'A committed capture lacks its checkpoint.')
    if conflicts:
        raise SourceError('CAPTURE_CONFLICT', 'An actual trade conflicts with durable capture; segment quarantined.')
    if result is None:
        raise SourceError('CAPTURE_STORAGE', 'The capture transaction did not return a checkpoint.')
    return result


def now_utc() -> datetime:
    return datetime.now(UTC)


def _quarantine(root: Path, error: SourceError) -> None:
    path = root / 'quarantine.json'
    if not path.exists():
        with path.open('w') as output:
            json.dump({'code': error.code, 'message': error.safe_message,
                       'recorded_at': now_utc().isoformat()}, output)
            output.flush()
            os.fsync(output.fileno())
        _sync_directory(root)
        _LOG.error('perp_spool_quarantined code=%s', error.code)


def _partition(partition: Partition) -> tuple[int, int]:
    if (not partition.provisional or partition.end > now_utc()
        or partition.end - partition.start != timedelta(minutes=1)
        or partition.start.second != 0 or partition.start.microsecond != 0
        or partition.key != partition.start.strftime('%Y-%m-%dT%H:%M:%SZ')):
        raise ValueError('Spool reads require an exact closed UTC provisional minute.')
    if os.environ.get('BINANCE_PERP_LATEST_SYMBOL', 'BTCUSDT') != _SYMBOL:
        raise ValueError('This source declares BTCUSDT only.')
    return int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000)


def _gap(left: _Segment, right: _Segment) -> _Gap:
    identity = hashlib.sha256(f'{left.identity}:{left.last_id}:{right.identity}:{right.first_id}'.encode()).hexdigest()
    return _Gap(identity, left, right)


def _coverage(root: Path, partition: Partition) -> tuple[Literal['ready', 'pending', 'fallback'], tuple[_Segment, ...], tuple[_Gap, ...]]:
    start_ms, end_ms = _partition(partition)
    if not (root / 'capture.sqlite3').exists() or (root / 'quarantine.json').exists():
        return 'fallback', (), ()
    try:
        with _database(root, 'capture') as connection:
            segments = _segments(connection, start_ms, end_ms)
            checkpoint = connection.execute('SELECT durable_at FROM state WHERE singleton=1').fetchone()
    except SourceError as error:
        if error.code == 'CAPTURE_MISSING':
            return 'fallback', (), ()
        if error.code not in {'CAPTURE_SCHEMA', 'CAPTURE_STORAGE'}:
            raise
        _quarantine(root, error)
        return 'fallback', (), ()
    if not segments or segments[0].first_ms >= start_ms or any(segment.quarantined for segment in segments):
        return 'fallback', segments, ()
    gaps = tuple(_gap(left, right) for left, right in pairwise(segments))
    pending = bool(gaps)
    if gaps and (root / 'repair.sqlite3').exists():
        pending = False
        with _database(root, 'repair') as connection:
            for gap in gaps:
                complete = connection.execute('SELECT complete,retired,error FROM gaps WHERE identity=?', (gap.identity,)).fetchone()
                if complete is not None and (complete[1] or complete[2] is not None):
                    return 'fallback', segments, gaps
                if complete != (1, 0, None):
                    if gap.left.last_ms < start_ms and _covered_until(connection, gap.left.last_ms // 60000 * 60000) >= start_ms:
                        return 'fallback', segments, gaps
                    pending = True
    if segments[-1].last_ms < end_ms:
        healthy = checkpoint is not None and (now_utc() - datetime.fromisoformat(str(checkpoint[0]))).total_seconds() <= CAPTURE_PROGRESS_SECONDS
        return ('pending' if healthy else 'fallback'), segments, gaps
    return ('pending' if pending else 'ready'), segments, gaps


def classify_spooled_partition(root: Path, partition: Partition) -> Literal['ready', 'pending', 'fallback']:
    return _coverage(root, partition)[0]


def _minute_rows(connection: sqlite3.Connection, start_ms: int, end_ms: int,
                 *, gap: str | None = None) -> list[tuple[int, int, str]]:
    clause = '' if gap is None else ' AND gap=?'
    params: tuple[object, ...] = () if gap is None else (gap,)
    size = connection.execute('SELECT COALESCE(sum(length(payload)),0) FROM trades WHERE time>=? AND time<?' + clause,
                              (start_ms, end_ms, *params)).fetchone()[0]
    if int(size) > MINUTE_LIMIT_BYTES:
        raise SourceError('CAPTURE_MINUTE_CAPACITY', 'The captured minute exceeds its payload bound.')
    records = connection.execute('SELECT id,time,payload FROM trades WHERE time>=? AND time<?' + clause,
                                 (start_ms, end_ms, *params)).fetchall()
    for predicate, order, instant in [('<', 'DESC', start_ms), ('>=', 'ASC', end_ms)]:
        boundary = connection.execute('SELECT id,time,payload FROM trades WHERE time' + predicate + '?' + clause + ' ORDER BY id ' + order + ' LIMIT 1',
                                      (instant, *params)).fetchone()
        if boundary is not None:
            records.append(boundary)
    return [(int(row[0]), int(row[1]), str(row[2])) for row in records]


def _validate_bridge_chain(connection: sqlite3.Connection, gap: _Gap) -> None:
    checkpoint = connection.execute('SELECT left_id,right_id,left_payload,right_payload,complete FROM gaps WHERE identity=?', (gap.identity,)).fetchone()
    if checkpoint is None or checkpoint[4] != 1:
        raise SourceError('CAPTURE_CHAIN', 'The bridge lacks its durable completion proof.')
    expected = int(checkpoint[0])
    last_id: int | None = None
    for first, last, evidence in connection.execute('SELECT first_id,last_id,evidence FROM pages WHERE gap=? ORDER BY first_id', (gap.identity,)):
        request = cast(dict[str, object], json.loads(str(evidence)))
        params = cast(dict[str, object], request['params'])
        if params['fromId'] != expected or int(first) < expected:
            raise SourceError('CAPTURE_CHAIN', 'The committed bridge page chain has a missing request range.')
        expected = int(last) + 1
        last_id = int(last)
    if last_id != checkpoint[1]:
        raise SourceError('CAPTURE_CHAIN', 'The committed bridge never reaches its exact right witness.')
    for trade_id, payload in ((checkpoint[0], checkpoint[2]), (checkpoint[1], checkpoint[3])):
        witness = connection.execute('SELECT payload FROM trades WHERE gap=? AND id=?', (gap.identity, trade_id)).fetchone()
        if witness is None or str(witness[0]) != str(payload):
            raise SourceError('CAPTURE_CHAIN', 'The committed bridge endpoint differs from its pinned witness.')


def _validate_capture_chain(connection: sqlite3.Connection, segments: tuple[_Segment, ...],
                            start_ms: int, end_ms: int) -> None:
    for segment in segments:
        first, last = segment.first_id, segment.last_id
        if segment.first_ms < start_ms:
            before = connection.execute('SELECT id FROM trades WHERE segment=? AND time<? ORDER BY id DESC LIMIT 1', (segment.identity, start_ms)).fetchone()
            if before is None:
                raise SourceError('CAPTURE_CHAIN', 'A capture segment has lost its before-start witness.')
            first = int(before[0])
        if segment.last_ms >= end_ms:
            after = connection.execute('SELECT id FROM trades WHERE segment=? AND time>=? ORDER BY id LIMIT 1', (segment.identity, end_ms)).fetchone()
            if after is None:
                raise SourceError('CAPTURE_CHAIN', 'A capture segment has lost its end witness.')
            last = int(after[0])
        pages = connection.execute('SELECT first_id,last_id,previous_id FROM pages WHERE segment=? AND first_id<=? AND last_id>=? ORDER BY first_id', (segment.identity, last, first)).fetchall()
        if not pages or not pages[0][0] <= first <= pages[0][1] or not pages[-1][0] <= last <= pages[-1][1]:
            raise SourceError('CAPTURE_CHAIN', 'A capture segment has lost its committed page chain.')
        if any(right[2] != left[1] for left, right in pairwise(pages)):
            raise SourceError('CAPTURE_CHAIN', 'Committed capture pages do not form the recorded overlap chain.')


def _validate_pages(connection: sqlite3.Connection, records: list[tuple[int, int, str]], *,
                    gap: str | None = None) -> tuple[list[str], list[object]]:
    if not records:
        return [], []
    first, last = min(row[0] for row in records), max(row[0] for row in records)
    if gap is None:
        pages = connection.execute('SELECT identity,segment,first_id,last_id,previous_id,evidence FROM pages WHERE first_id<=? AND last_id>=? ORDER BY first_id', (last, first)).fetchall()
    else:
        pages = connection.execute('SELECT normalized_hash,gap,first_id,last_id,NULL,evidence FROM pages WHERE gap=? AND first_id<=? AND last_id>=? ORDER BY first_id', (gap, last, first)).fetchall()
    identities: list[str] = []
    requests: list[object] = []
    previous: tuple[str, int] | None = None
    verified: set[int] = set()
    for digest, owner, first_id, last_id, previous_id, evidence in pages:
        owner_column = 'segment' if gap is None else 'gap'
        payloads = connection.execute('SELECT id,payload FROM trades WHERE ' + owner_column + '=? AND id>=? AND id<=? ORDER BY id', (owner, first_id, last_id)).fetchall()
        measured = hashlib.sha256(''.join(str(row[1]) for row in payloads).encode()).hexdigest()
        if measured != str(digest) or not payloads or payloads[0][0] != first_id or payloads[-1][0] != last_id:
            raise SourceError('CAPTURE_PAYLOAD_HASH', 'A committed capture or bridge page hash differs.')
        if gap is None and previous is not None and previous[0] == str(owner) and previous_id != previous[1]:
            raise SourceError('CAPTURE_CHAIN', 'Committed capture pages do not form the recorded overlap chain.')
        previous = (str(owner), int(last_id))
        identities.append(str(digest))
        requests.append(json.loads(str(evidence)))
        verified.update(int(row[0]) for row in payloads)
    if any(row[0] not in verified for row in records):
        raise SourceError('CAPTURE_CHAIN', 'A minute row lacks its immutable committed page.')
    return identities, requests


def read_spooled_revision(root: Path, partition: Partition) -> Revision | None:
    _partition(partition)
    if not root.exists():
        return None
    with (root / 'read.lock').open('a+') as readers:
        fcntl.flock(readers, fcntl.LOCK_SH)
        try:
            return _read_spooled_revision(root, partition)
        except SourceError as error:
            if error.code in {'CAPTURE_PAYLOAD_HASH', 'CAPTURE_CHAIN'}:
                _quarantine(root, error)
            raise
        finally:
            fcntl.flock(readers, fcntl.LOCK_UN)


def _read_spooled_revision(root: Path, partition: Partition) -> Revision | None:
    state, segments, gaps = _coverage(root, partition)
    if state != 'ready':
        return None
    start_ms, end_ms = _partition(partition)
    with _database(root, 'capture') as connection:
        connection.execute('BEGIN')
        _validate_capture_chain(connection, segments, start_ms, end_ms)
        records = _minute_rows(connection, start_ms, end_ms)
        page_ids, requests = _validate_pages(connection, records)
    if gaps:
        with _database(root, 'repair') as connection:
            for gap in gaps:
                _validate_bridge_chain(connection, gap)
                bridged = _minute_rows(connection, start_ms, end_ms, gap=gap.identity)
                bridge_ids, bridge_requests = _validate_pages(connection, bridged, gap=gap.identity)
                page_ids.extend(bridge_ids)
                requests.extend(bridge_requests)
                records.extend(bridged)
                if sum(len(row[2].encode()) for row in records) > MINUTE_LIMIT_BYTES + 2 * 1024**2:
                    raise SourceError('CAPTURE_MINUTE_CAPACITY', 'Minute plus boundary witnesses exceeds its read bound.')
    unique: dict[int, tuple[int, str]] = {}
    for trade_id, timestamp, payload in records:
        if trade_id in unique and unique[trade_id] != (timestamp, payload):
            raise SourceError('CAPTURE_CONFLICT', 'Capture and verified bridge rows disagree.')
        unique[trade_id] = (timestamp, payload)
    ordered = sorted(unique.items())
    before = [trade_id for trade_id, (timestamp, _) in ordered if timestamp < start_ms]
    after = [trade_id for trade_id, (timestamp, _) in ordered if timestamp >= end_ms]
    minute = tuple(_unwire(payload) for _, (timestamp, payload) in ordered if start_ms <= timestamp < end_ms)
    if not before or not after or not minute:
        return None
    if sum(len(payload.encode()) for _, (timestamp, payload) in ordered if start_ms <= timestamp < end_ms) > MINUTE_LIMIT_BYTES:
        raise SourceError('CAPTURE_MINUTE_CAPACITY', 'The assembled minute exceeds its payload bound.')
    digest = content_hash(minute, schema_version=1)
    chain: list[str] = []
    for index, segment in enumerate(segments):
        chain.append(segment.identity)
        if index < len(gaps):
            chain.append(gaps[index].identity)
    evidence = json.dumps({'source': _SOURCE, 'symbol': _SYMBOL, 'start': partition.start.isoformat(),
                           'end': partition.end.isoformat(), 'chain_ids': chain, 'page_ids': page_ids,
                           'requests': requests}, sort_keys=True)
    proof: MinuteProof = {'schema_version': 1, 'source_key': 'binance_perp_trades',
        'partition_key': partition.key, 'symbol': 'BTCUSDT', 'chain_ids': chain,
        'row_count': len(minute), 'content_hash': digest, 'first_trade_id': cast(int, minute[0][0]),
        'last_trade_id': cast(int, minute[-1][0]), 'before_start_trade_id': max(before),
        'at_or_after_end_trade_id': min(after), 'evidence_json': evidence}
    return Revision(digest, digest, json.dumps({'capture_proof': proof}, sort_keys=True),
                    len(minute), lambda: iter(minute))


def _witness(connection: sqlite3.Connection, trade_id: int) -> str:
    record = connection.execute('SELECT payload FROM trades WHERE id=?', (trade_id,)).fetchone()
    if record is None:
        raise SourceError('CAPTURE_WITNESS_MISSING', 'A bridge endpoint is not durable.')
    return str(record[0])


def repair_spooled_gaps(root: Path, partition: Partition, *, egress_ip: str) -> None:
    state, _, gaps = _coverage(root, partition)
    if state != 'pending' or not gaps:
        return
    if egress_ip != '37.27.112.144':
        raise ValueError('Captured-gap repair requires the dedicated .144 role.')
    credential = os.environ.get('BINANCE_API_KEY')
    if not credential:
        raise SourceError('PROVIDER_CREDENTIAL_MISSING', 'BINANCE_API_KEY is required for perp gap repair.')
    with (root / 'repair.lock').open('a+') as lock:
        acquired = True
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            acquired = False
        if not acquired:
            return
        try:
            _repair(root, gaps, credential, egress_ip)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _repair(root: Path, gaps: tuple[_Gap, ...], credential: str, egress_ip: str) -> None:
    requests = 0
    for gap in gaps:
        if gap.right.first_ms > (now_utc().timestamp() - HISTORICAL_LAG_SECONDS) * 1000:
            return
        with _database(root, 'capture') as capture:
            left = _witness(capture, gap.left.last_id)
            right = _witness(capture, gap.right.first_id)
        _capacity(root, 65536)
        with _database(root, 'repair', create=True) as repair:
            repair.execute('INSERT OR IGNORE INTO gaps(identity,left_id,right_id,left_payload,right_payload,next_id,created_at) VALUES (?,?,?,?,?,?,?)',
                           (gap.identity, gap.left.last_id, gap.right.first_id, left, right, gap.left.last_id, now_utc().isoformat()))
            repair.commit()
        while requests < REPAIR_PAGE_BUDGET:
            with _database(root, 'repair') as repair:
                checkpoint = repair.execute('SELECT next_id,complete,error,left_payload,right_payload,retired FROM gaps WHERE identity=?', (gap.identity,)).fetchone()
            if checkpoint is None:
                raise SourceError('CAPTURE_STORAGE', 'The durable bridge checkpoint is absent.')
            if int(checkpoint[1]) == 1 or int(checkpoint[5]) == 1:
                break
            if checkpoint[2] is not None:
                raise SourceError('CAPTURE_BRIDGE_CONFLICT', f'Historical bridge rejected: {checkpoint[2]}.')
            next_id = int(checkpoint[0])
            requests += 1
            params: dict[str, str | int] = {'symbol': _SYMBOL, 'limit': 500, 'fromId': next_id}
            _capacity(root, 65536)
            with _database(root, 'repair') as repair:
                attempt = repair.execute('INSERT INTO attempts(gap,started_at,params) VALUES (?,?,?)',
                    (gap.identity, now_utc().isoformat(), json.dumps({'params': params, 'weight': 200, 'egress_ip': egress_ip}, sort_keys=True))).lastrowid
                repair.commit()
            try:
                response = get_response(os.environ.get('BINANCE_PERP_REST_BASE_URL', 'https://fapi.binance.com').rstrip('/') + '/fapi/v1/historicalTrades',
                    params=params, headers={'X-MBX-APIKEY': credential}, weight=200, egress_ip=egress_ip)
            except Exception as transport_error:
                with _database(root, 'repair') as repair:
                    repair.execute('UPDATE attempts SET outcome=? WHERE seq=?',
                        (json.dumps({'error_code': failure_code(transport_error), 'completed_at': now_utc().isoformat()}, sort_keys=True), attempt))
                    repair.commit()
                raise
            evidence = json.dumps({'params': params, 'status': response.status,
                'body_sha256': hashlib.sha256(response.body).hexdigest(), 'headers': {name: value for name, value in response.headers.items() if name.lower() in {'x-mbx-used-weight-1m', 'retry-after', 'date'}},
                'completed_at': now_utc().isoformat(), 'egress_ip': egress_ip}, sort_keys=True)
            with _database(root, 'repair') as repair:
                repair.execute('UPDATE attempts SET outcome=? WHERE seq=?', (evidence, attempt))
                repair.commit()
            if response.status != 200:
                raise SourceError('PROVIDER_HTTP_ERROR', f'Perp bridge provider HTTP {response.status}.')
            payload: object = json.loads(response.body)
            if not isinstance(payload, list):
                raise SourceError('CAPTURE_BRIDGE_SCHEMA', 'Historical bridge response is not a trade list.')
            untyped_rows = cast(list[object], payload)
            if any(not isinstance(row, dict) for row in untyped_rows):
                raise SourceError('CAPTURE_BRIDGE_SCHEMA', 'Historical bridge response is not a trade list.')
            page = _rows(cast(list[dict[str, object]], untyped_rows))
            if not page:
                return
            error: str | None = None
            with _database(root, 'repair') as repair:
                prior_time = repair.execute('SELECT time FROM trades WHERE gap=? ORDER BY id DESC LIMIT 1', (gap.identity,)).fetchone()
            if prior_time is not None and page[0][1] < int(prior_time[0]):
                error = 'decreasing_time'
            if page[0][0] < next_id or page[-1][0] < next_id:
                error = 'nonprogress'
            if next_id == gap.left.last_id and (page[0][0] != gap.left.last_id or page[0][2] != left):
                error = 'left_witness'
            right_rows = [row for row in page if row[0] == gap.right.first_id]
            complete = bool(right_rows)
            if complete and right_rows[0][2] != right:
                error = 'right_witness'
            if page[-1][0] > gap.right.first_id and not complete:
                error = 'skipped_right_witness'
            if error is not None:
                with _database(root, 'repair') as repair:
                    repair.execute('UPDATE gaps SET error=? WHERE identity=?', (error, gap.identity))
                    repair.commit()
                raise SourceError('CAPTURE_BRIDGE_CONFLICT', f'Historical bridge rejected: {error}.')
            retained = tuple(row for row in page if row[0] <= gap.right.first_id)
            _capacity(root, sum(len(row[2].encode()) for row in retained) * 4 + 65536)
            with _database(root, 'repair') as repair:
                # Payload/page rows are immutable. The checkpoint commits in the same FULL-sync transaction.
                repair.executemany('INSERT INTO trades(gap,id,time,payload) VALUES (?,?,?,?)',
                    ((gap.identity, row[0], row[1], row[2]) for row in retained))
                repair.execute('INSERT INTO pages VALUES (?,?,?,?,?,?)', (gap.identity, retained[0][0], retained[-1][0], hashlib.sha256(response.body).hexdigest(), evidence, hashlib.sha256(''.join(row[2] for row in retained).encode()).hexdigest()))
                repair.execute('UPDATE gaps SET next_id=?,complete=? WHERE identity=?',
                    (page[-1][0] + 1, int(complete), gap.identity))
                repair.commit()
        if requests >= REPAIR_PAGE_BUDGET:
            return


def acknowledge_spooled_revision(root: Path, partition: Partition) -> None:
    if not (root / 'capture.sqlite3').exists() or (root / 'quarantine.json').exists():
        return
    start_ms = int(partition.start.timestamp() * 1000)
    end_ms = int(partition.end.timestamp() * 1000)
    with _database(root, 'repair', create=True) as repair:
        existing = repair.execute('SELECT 1 FROM acknowledgments WHERE start_ms<=? AND end_ms>=? LIMIT 1', (start_ms, end_ms)).fetchone()
        if existing is not None:
            return
        overlapping = repair.execute('SELECT start_ms,end_ms FROM acknowledgments WHERE start_ms<=? AND end_ms>=?', (end_ms, start_ms)).fetchall()
        for left, right in overlapping:
            start_ms, end_ms = min(start_ms, int(left)), max(end_ms, int(right))
        repair.execute('DELETE FROM acknowledgments WHERE start_ms>=? AND end_ms<=?', (start_ms, end_ms))
        repair.execute('INSERT INTO acknowledgments VALUES (?,?)', (start_ms, end_ms))
        repair.commit()
    cleanup_spool(root)


def _covered_until(repair: sqlite3.Connection, start_ms: int) -> int:
    row = repair.execute('SELECT max(end_ms) FROM acknowledgments WHERE start_ms<=? AND end_ms>?', (start_ms, start_ms)).fetchone()
    return start_ms if row is None or row[0] is None else int(row[0])


def _reclaim(connection: sqlite3.Connection) -> None:
    connection.commit()
    free = int(connection.execute('PRAGMA freelist_count').fetchone()[0])
    connection.execute('BEGIN IMMEDIATE')
    for _ in range(min(free, 256)):
        connection.execute('PRAGMA incremental_vacuum(1)')
    connection.commit()
    connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()


def cleanup_spool(root: Path) -> None:
    """Reclaim at most 8 MiB/512 pages, committing each page to bound writer stalls/WAL."""
    if not (root / 'repair.sqlite3').exists() or (root / 'quarantine.json').exists():
        return
    cutoff = int((now_utc() - timedelta(hours=1)).timestamp() * 1000)
    remaining, remaining_pages = 8 * 1024**2, 512
    with (root / 'repair.lock').open('a+') as lock, (root / 'read.lock').open('a+') as readers:
        fcntl.flock(lock, fcntl.LOCK_EX)
        fcntl.flock(readers, fcntl.LOCK_EX)
        try:
            with _database(root, 'capture') as capture, _database(root, 'repair') as repair:
                segments = tuple(_Segment(str(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), bool(row[5]))
                    for row in capture.execute('SELECT identity,first_id,last_id,first_ms,last_ms,quarantined FROM segments ORDER BY seq'))
                for left, right in pairwise(segments):
                    first_minute, end_minute = left.last_ms // 60000 * 60000, (right.first_ms // 60000 + 1) * 60000
                    if _covered_until(repair, first_minute) >= end_minute and repair.execute('SELECT 1 FROM gaps WHERE left_id=?', (left.last_id,)).fetchone() is None:
                        gap = _gap(left, right)
                        repair.execute('INSERT INTO gaps(identity,left_id,right_id,left_payload,right_payload,next_id,created_at,retired) VALUES (?,?,?,?,?,?,?,1)',
                            (gap.identity, left.last_id, right.first_id, _witness(capture, left.last_id), _witness(capture, right.first_id), left.last_id, now_utc().isoformat()))
                repair.commit()
                gaps = repair.execute('SELECT identity,left_payload,right_payload FROM gaps').fetchall()
                for gap_id, left_payload, right_payload in gaps:
                    left_row, right_row = _unwire(str(left_payload)), _unwire(str(right_payload))
                    first_ms, last_ms = cast(int, left_row[4]), cast(int, right_row[4])
                    first_minute, end_minute = first_ms // 60000 * 60000, (last_ms // 60000 + 1) * 60000
                    if _covered_until(repair, first_minute) < end_minute:
                        continue
                    # Active coverage supersedes both finished and unfinished bridges.
                    repair.execute('UPDATE gaps SET retired=1 WHERE identity=?', (gap_id,))
                    repair.commit()
                    if last_ms >= cutoff:
                        continue
                    pages = repair.execute('SELECT first_id,last_id FROM pages WHERE gap=? ORDER BY first_id LIMIT ?', (gap_id, remaining_pages)).fetchall()
                    for first_id, last_id in pages:
                        size = int(repair.execute('SELECT coalesce(sum(length(payload)),0) FROM trades WHERE gap=? AND id BETWEEN ? AND ?', (gap_id, first_id, last_id)).fetchone()[0])
                        if size > remaining or remaining_pages == 0:
                            return
                        repair.execute('DELETE FROM trades WHERE gap=? AND id BETWEEN ? AND ?', (gap_id, first_id, last_id))
                        repair.execute('DELETE FROM pages WHERE gap=? AND first_id=?', (gap_id, first_id))
                        _reclaim(repair)
                        remaining -= size
                        remaining_pages -= 1
                candidates = capture.execute('SELECT identity,first_ms,last_ms,first_id,last_id FROM segments ORDER BY seq').fetchall()
                for identity, first_ms, last_ms, first_id, last_id in candidates:
                    boundary = min(cutoff, _covered_until(repair, int(first_ms) // 60000 * 60000))
                    pinned = repair.execute('SELECT 1 FROM gaps WHERE retired=0 AND (left_id=? OR right_id=?) LIMIT 1', (last_id, first_id)).fetchone()
                    if pinned is not None:
                        continue
                    retained = capture.execute('SELECT first_id FROM pages WHERE segment=? AND last_ms<? ORDER BY first_id DESC LIMIT 1', (identity, boundary)).fetchone()
                    if retained is None:
                        continue
                    pages = capture.execute('SELECT first_id,last_id FROM pages WHERE segment=? AND last_id<? ORDER BY first_id LIMIT ?', (identity, int(retained[0]), remaining_pages)).fetchall()
                    for first_page_id, last_page_id in pages:
                        size = int(capture.execute('SELECT coalesce(sum(length(payload)),0) FROM trades WHERE segment=? AND id BETWEEN ? AND ?', (identity, first_page_id, last_page_id)).fetchone()[0])
                        if size > remaining or remaining_pages == 0:
                            return
                        capture.execute('DELETE FROM trades WHERE segment=? AND id BETWEEN ? AND ?', (identity, first_page_id, last_page_id))
                        capture.execute('DELETE FROM pages WHERE segment=? AND first_id=?', (identity, first_page_id))
                        first = capture.execute('SELECT id,time FROM trades WHERE segment=? ORDER BY id LIMIT 1', (identity,)).fetchone()
                        if first is None:
                            raise SourceError('CAPTURE_STORAGE', 'Cleanup removed a required overlap witness.')
                        capture.execute('UPDATE segments SET first_id=?,first_ms=? WHERE identity=?', (first[0], first[1], identity))
                        _reclaim(capture)
                        remaining -= size
                        remaining_pages -= 1
                # Attempts have their own lifecycle: failed requests own no payload page.
                while remaining_pages:
                    attempts = repair.execute("SELECT seq,length(params)+coalesce(length(outcome),0) FROM attempts WHERE gap IN (SELECT identity FROM gaps WHERE retired=1 AND json_extract(right_payload,'$.time')<?) ORDER BY seq LIMIT ?", (cutoff, min(100, remaining_pages))).fetchall()
                    if not attempts:
                        break
                    size = sum(int(row[1]) for row in attempts)
                    if size > remaining:
                        return
                    repair.executemany('DELETE FROM attempts WHERE seq=?', ((row[0],) for row in attempts))
                    _reclaim(repair)
                    remaining -= size
                    remaining_pages -= len(attempts)
                for start_ms, end_ms in repair.execute('SELECT start_ms,end_ms FROM acknowledgments'):
                    start = datetime.fromtimestamp(int(start_ms) / 1000, UTC).isoformat()
                    end = datetime.fromtimestamp(min(int(end_ms), cutoff) / 1000, UTC).isoformat()
                    while remaining_pages:
                        polls = capture.execute("SELECT seq,length(evidence) FROM polls WHERE received_at>=? AND received_at<? AND (last_id IS NOT NULL OR json_extract(evidence,'$.completed_at') IS NOT NULL) ORDER BY seq LIMIT ?", (start, end, min(100, remaining_pages))).fetchall()
                        if not polls:
                            break
                        size = sum(int(row[1]) for row in polls)
                        if size > remaining:
                            return
                        capture.executemany('DELETE FROM polls WHERE seq=?', ((row[0],) for row in polls))
                        _reclaim(capture)
                        remaining -= size
                        remaining_pages -= len(polls)
                _reclaim(capture)
                _reclaim(repair)
        finally:
            fcntl.flock(readers, fcntl.LOCK_UN)
            fcntl.flock(lock, fcntl.LOCK_UN)
