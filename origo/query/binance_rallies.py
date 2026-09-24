"""Binance spot rallies with their trades and book, selected by ID or time range (PRD-0006).

A rally is a UTC minute anchor whose first trade at or above the last pre-anchor trade
price x 1.003 comes within 240 minutes. An export writes three Arrow IPC files:
``rallies.arrow`` holds one row per rally with the bounds that select its rows from
``trades.arrow`` and ``book.arrow``, which hold each trade and snapshot once. A rally's rows
start ``minutes_before`` ahead of its anchor, at the first record at or after that instant
(``boundary='after'``) or the last record before it (``boundary='before'``), and end at its hit.

ClickHouse is reachable only on the production host, so exports run inside an Origo
container there, for example::

    docker exec tdw-control-plane-dagster-1 python -c "
    from datetime import UTC, datetime
    from pathlib import Path
    from origo.query.binance_rallies import export_binance_rallies
    export_binance_rallies(
        output_dir=Path('/tmp/rallies'),
        start=datetime(2026, 6, 27, 11, 39, tzinfo=UTC),
        end=datetime(2026, 6, 27, 11, 55, tzinfo=UTC),
    )"
"""

from __future__ import annotations

import io
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Literal, Protocol, cast

import numpy as np
import polars as pl

DEFINITION_VERSION = 'r30v1'
TARGET_BPS = 30
TARGET = 1.003
MAX_TIME_TO_HIT = timedelta(minutes=240)
HORIZON_GRID_MINUTES = (1, 3, 5, 15, 30, 60, 120, 240)
# The reference trade and the before-start records are looked up this far back.
LOOKBACK = timedelta(hours=24)
# Bounds the extra history one request reads from production ClickHouse.
MAX_MINUTES_BEFORE = 1440

RALLY_FIELDS = (
    'rally_id', 'anchor_time', 'reference_trade_id', 'reference_time', 'reference_price',
    'hit_trade_id', 'hit_time', 'hit_price', 'time_to_hit', 'first_trade_id',
    'first_snapshot_time', 'last_snapshot_time',
)
TRADE_FIELDS = (
    'trade_id', 'timestamp', 'price', 'quantity', 'quote_quantity',
    'is_buyer_maker', 'is_best_match',
)
BOOK_FIELDS = ('observed_at', 'last_update_id', 'bids', 'asks')
OUTPUT_FILENAMES = ('rallies.arrow', 'trades.arrow', 'book.arrow')
METADATA_KEY = 'origo.rallies'

TRADES_SOURCE = 'binance_spot_trades'
BOOK_TABLE = 'binance_spot_depth200_snapshots'

Boundary = Literal['after', 'before']

_US = timedelta(microseconds=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MINUTE = 60_000_000
_MAX = MAX_TIME_TO_HIT // _US
_LOOKBACK = LOOKBACK // _US
_ID = re.compile(rf'binance:spot:BTCUSDT:{DEFINITION_VERSION}:t(\d+)')

_LEVELS = pl.List(pl.Struct({'price': pl.Float64, 'quantity': pl.Float64}))
_TRADES = {
    'trade_id': pl.UInt64,
    'timestamp': pl.Datetime('us', 'UTC'),
    'price': pl.Float64,
    'quantity': pl.Float64,
    'quote_quantity': pl.Float64,
    'is_buyer_maker': pl.Boolean,
    'is_best_match': pl.Boolean,
}
_BOOK = {
    'observed_at': pl.Datetime('ms', 'UTC'),
    'last_update_id': pl.UInt64,
    'bids': _LEVELS,
    'asks': _LEVELS,
}


class _Client(Protocol):
    def raw_query(self, query: str, parameters: Mapping[str, object], *, fmt: str) -> bytes: ...

    def close(self) -> None: ...


class _Table(Protocol):
    def cast(self, target_schema: object) -> _Table: ...


class _Writer(Protocol):
    def write_table(self, table: _Table) -> None: ...


class _PyArrow(Protocol):
    def field(self, name: str, type: object, nullable: bool = True) -> object: ...
    def schema(self, fields: Sequence[object], metadata: Mapping[str, str]) -> object: ...
    def string(self) -> object: ...
    def uint64(self) -> object: ...
    def float64(self) -> object: ...
    def bool_(self) -> object: ...
    def timestamp(self, unit: str, tz: str) -> object: ...
    def duration(self, unit: str) -> object: ...
    def list_(self, value_type: object) -> object: ...
    def struct(self, fields: Sequence[object]) -> object: ...


class _IPC(Protocol):
    def IpcWriteOptions(self, *, compression: str) -> object: ...
    def new_file(
        self, sink: str, schema: object, *, options: object
    ) -> AbstractContextManager[_Writer]: ...


pa = cast(_PyArrow, import_module('pyarrow'))
ipc = cast(_IPC, import_module('pyarrow.ipc'))


@dataclass(frozen=True)
class _Request:
    anchors: tuple[int, ...]
    end: int | None
    boundary: Boundary
    lead: int
    description: dict[str, object]


@dataclass(frozen=True)
class _Partition:
    """A trade-source partition pinned for the whole export; start and end in microseconds."""

    key: str
    provisional: bool
    revision: str
    build_id: str
    start: int
    end: int


@dataclass(frozen=True)
class _Rally:
    anchor: int
    start: int
    reference_id: int
    reference_time: int
    reference_price: float
    hit_id: int
    hit_time: int
    hit_price: float
    first_trade_id: int | None


def export_binance_rallies(
    *,
    output_dir: Path,
    rally_ids: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    boundary: Literal['after', 'before'] = 'after',
    minutes_before: int = 0,
) -> tuple[Path, Path, Path]:
    request = _request(rally_ids, start, end, boundary, minutes_before)
    if output_dir.exists():
        raise FileExistsError(f'{output_dir} already exists.')
    reader = _Reader(_connect(), _database())
    try:
        rallies, trades, partitions = _detect_all(reader, request)
        book, bounds, book_reads = _book_rows(reader, rallies, request.boundary)
    finally:
        reader.client.close()
    metadata = {
        'definition': {
            'version': DEFINITION_VERSION,
            'market': 'spot',
            'symbol': 'BTCUSDT',
            'target_bps': TARGET_BPS,
            'max_time_to_hit_minutes': MAX_TIME_TO_HIT // timedelta(minutes=1),
            'horizon_grid_minutes': list(HORIZON_GRID_MINUTES),
            'lookback_hours': LOOKBACK // timedelta(hours=1),
        },
        'request': request.description,
        'sources': {
            'trades': {
                'source': TRADES_SOURCE,
                'partitions': [
                    {'partition_key': part.key, 'provisional': part.provisional,
                     'revision': part.revision, 'build_id': part.build_id}
                    for part in partitions
                ],
            },
            'book': {'table': f'{reader.database}.{BOOK_TABLE}', 'read': 'FINAL', **book_reads},
        },
    }
    frames = (_rally_frame(rallies, bounds), _trade_rows(trades, rallies), book)
    return _publish(output_dir, frames, _schemas(json.dumps(metadata, sort_keys=True)))


def _request(
    rally_ids: Sequence[str] | None,
    start: datetime | None,
    end: datetime | None,
    boundary: Boundary,
    minutes_before: int,
) -> _Request:
    if cast(str, boundary) not in ('after', 'before'):
        raise ValueError(f'Unknown boundary {boundary!r}; use "after" or "before".')
    if (
        not isinstance(cast(object, minutes_before), int)
        or not 0 <= minutes_before <= MAX_MINUTES_BEFORE
    ):
        raise ValueError(
            f'minutes_before must be an integer from 0 to {MAX_MINUTES_BEFORE}, '
            f'not {minutes_before!r}.'
        )
    lead = minutes_before * _MINUTE
    description: dict[str, object] = {'boundary': boundary, 'minutes_before': minutes_before}
    if rally_ids is not None and start is None and end is None:
        anchors = tuple(sorted({_parse_id(value) for value in rally_ids}))
        description['rally_ids'] = [_rally_id(anchor) for anchor in anchors]
        return _Request(anchors, None, boundary, lead, description)
    if rally_ids is None and start is not None and end is not None:
        low, high = _utc_us(start), _utc_us(end)
        if low >= high:
            raise ValueError('start must be before end.')
        description |= {'start': _iso(low), 'end': _iso(high)}
        first = -(-low // _MINUTE) * _MINUTE
        return _Request(tuple(range(first, high, _MINUTE)), high, boundary, lead, description)
    raise ValueError('Select rallies with either rally_ids, or both start and end.')


def _utc_us(value: datetime) -> int:
    if value.utcoffset() is None:
        raise ValueError('start and end must be timezone-aware.')
    return (value - _EPOCH) // _US


def _iso(value: int) -> str:
    return (_EPOCH + value * _US).isoformat()


def _parse_id(value: str) -> int:
    match = _ID.fullmatch(value)
    if match is None or int(match.group(1)) % 60:
        raise ValueError(f'Unsupported rally ID: {value!r}')
    return int(match.group(1)) * 1_000_000


def _rally_id(anchor: int) -> str:
    return f'binance:spot:BTCUSDT:{DEFINITION_VERSION}:t{anchor // 1_000_000}'


def _connect() -> _Client:
    factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _Client,
        factory(
            host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            port=int(os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')),
            username=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ['CLICKHOUSE_PASSWORD'],
        ),
    )


def _database() -> str:
    database = os.environ.get('CLICKHOUSE_DATABASE', 'origo')
    if not re.fullmatch(r'[a-z][a-z0-9_]*', database):
        raise ValueError(f'Invalid ClickHouse database: {database!r}')
    return database


@dataclass(frozen=True)
class _Reader:
    client: _Client
    database: str

    def frame(self, query: str, parameters: Mapping[str, object]) -> pl.DataFrame:
        body = self.client.raw_query(query, parameters, fmt='ArrowStream')
        return pl.read_ipc_stream(io.BytesIO(body))

    def partitions(self, low: int, high: int) -> list[_Partition]:
        """The trade partitions current now that overlap [low, high)."""
        frame = self.frame(
            f"""SELECT partition_key, toBool(provisional) AS provisional, revision,
                toString(build_id) AS build_id,
                toUnixTimestamp64Micro(toDateTime64(partition_start, 6, 'UTC')) AS start,
                toUnixTimestamp64Micro(toDateTime64(partition_end, 6, 'UTC')) AS end
            FROM {self.database}.source_current_partitions
            WHERE source_key = {{source:String}}
              AND partition_start < fromUnixTimestamp64Micro({{high:Int64}}, 'UTC')
              AND partition_end > fromUnixTimestamp64Micro({{low:Int64}}, 'UTC')
            ORDER BY partition_start, provisional""",
            {'source': TRADES_SOURCE, 'low': low, 'high': high},
        )
        return [
            _Partition(str(key), bool(provisional), str(revision), str(build), int(start), int(end))
            for key, provisional, revision, build, start, end in frame.iter_rows()
        ]

    def trades(
        self, pinned: Sequence[_Partition], low: int, high: int, *, last: bool = False
    ) -> pl.DataFrame:
        """Trades of the pinned partitions in [low, high), or only the last of them."""
        parts = [part for part in pinned if part.start < high and part.end > low]
        selects = ' UNION ALL '.join(
            f"""SELECT trade_id, toDateTime64(datetime, 6, 'UTC') AS timestamp, price, quantity,
                quote_quantity, toBool(is_buyer_maker) AS is_buyer_maker,
                toBool(is_best_match) AS is_best_match
            FROM {self.database}.{TRADES_SOURCE}_{component}_revisions
            WHERE has({{{name}:Array(Tuple(String, String, String))}},
                      (partition_key, revision, toString(build_id)))
              AND datetime >= fromUnixTimestamp64Micro({{low:Int64}}, 'UTC')
              AND datetime < fromUnixTimestamp64Micro({{high:Int64}}, 'UTC')"""
            for component, name in (('raw', 'canonical'), ('raw_latest', 'provisional'))
        )
        order = 'trade_id DESC LIMIT 1' if last else 'trade_id'
        return self.frame(
            f'SELECT * FROM ({selects}) ORDER BY {order}',
            {
                'canonical': [
                    (part.key, part.revision, part.build_id)
                    for part in parts
                    if not part.provisional
                ],
                'provisional': [
                    (part.key, part.revision, part.build_id) for part in parts if part.provisional
                ],
                'low': low,
                'high': high,
            },
        )

    def book(self, low: int, high: int, *, last: bool = False) -> pl.DataFrame:
        """Snapshots observed in [low, high], or only the last one before high."""
        until = '<' if last else '<='
        order = 'datetime DESC LIMIT 1' if last else 'datetime'
        return self.frame(
            f"""SELECT toDateTime64(datetime, 3, 'UTC') AS observed_at, last_update_id,
                CAST(bids, 'Array(Tuple(price Float64, quantity Float64))') AS bids,
                CAST(asks, 'Array(Tuple(price Float64, quantity Float64))') AS asks
            FROM {self.database}.{BOOK_TABLE} FINAL
            WHERE datetime >= fromUnixTimestamp64Micro({{low:Int64}}, 'UTC')
              AND datetime {until} fromUnixTimestamp64Micro({{high:Int64}}, 'UTC')
            ORDER BY {order}""",
            {'low': low, 'high': high},
        )


def _detect_all(
    reader: _Reader, request: _Request
) -> tuple[list[_Rally], pl.DataFrame, list[_Partition]]:
    windows = _windows(request)
    reads = [
        (anchors[0] - request.lead - _LOOKBACK, _limit(anchors[-1], end))
        for anchors, end in windows
    ]
    # One pin for every window, so no two windows read different revisions of a partition.
    pinned = (
        reader.partitions(min(low for low, _ in reads), max(high for _, high in reads))
        if reads
        else []
    )
    rallies: list[_Rally] = []
    frames = [pl.DataFrame(schema=_TRADES)]
    for anchors, end in windows:
        found, frame = _detect(reader, pinned, anchors, end, request.boundary, request.lead)
        rallies += found
        frames.append(frame)
    if request.end is None:
        missing = sorted(set(request.anchors) - {rally.anchor for rally in rallies})
        if missing:
            ids = ', '.join(_rally_id(anchor) for anchor in missing)
            raise ValueError(f'Not rallies in the available data: {ids}')
    # A window's predecessor is the previous window's last pinned trade when none lies between.
    trades = pl.concat(frames).unique('trade_id', keep='first', maintain_order=True)
    read = [
        part for part in pinned if any(part.start < high and part.end > low for low, high in reads)
    ]
    return rallies, trades.sort('trade_id'), read


def _windows(request: _Request) -> list[tuple[tuple[int, ...], int | None]]:
    """Anchors that share one trade read: the whole range, or IDs whose horizons overlap."""
    if request.end is not None:
        return [(request.anchors, request.end)] if request.anchors else []
    groups: list[list[int]] = []
    for anchor in request.anchors:
        if groups and anchor < groups[-1][-1] + _MAX:
            groups[-1].append(anchor)
        else:
            groups.append([anchor])
    return [(tuple(group), None) for group in groups]


def _limit(anchor: int, end: int | None) -> int:
    """Exclusive end of an anchor's hit search: 240 minutes on, or the range end if sooner."""
    return anchor + _MAX if end is None else min(anchor + _MAX, end)


def _detect(
    reader: _Reader,
    pinned: Sequence[_Partition],
    anchors: tuple[int, ...],
    end: int | None,
    boundary: Boundary,
    lead: int,
) -> tuple[list[_Rally], pl.DataFrame]:
    low, high = anchors[0] - lead, _limit(anchors[-1], end)
    trades = pl.concat(
        [
            reader.trades(pinned, low - _LOOKBACK, low, last=True),
            reader.trades(pinned, low, high),
        ]
    )
    # Trade-ID order is time order: the source adapters enforce strictly increasing IDs with
    # non-decreasing timestamps, so `times` is sorted for every search below.
    ids = trades['trade_id'].to_numpy()
    times = trades['timestamp'].dt.epoch('us').to_numpy()
    prices = trades['price'].to_numpy()
    rallies: list[_Rally] = []
    for anchor in anchors:
        first = int(np.searchsorted(times, anchor))
        if first == 0 or times[first - 1] < anchor - _LOOKBACK:
            continue
        stop = int(np.searchsorted(times, _limit(anchor, end)))
        hits = np.flatnonzero(prices[first:stop] >= prices[first - 1] * TARGET)
        if hits.size == 0:
            continue
        reference, hit = first - 1, first + int(hits[0])
        # The rows start `lead` before the anchor, at the record the boundary names there.
        start = anchor - lead
        opening = int(np.searchsorted(times, start))
        if boundary == 'before':
            has_prior = opening > 0 and times[opening - 1] >= start - _LOOKBACK
            opening = opening - 1 if has_prior else -1
        rallies.append(
            _Rally(
                anchor,
                start,
                int(ids[reference]),
                int(times[reference]),
                float(prices[reference]),
                int(ids[hit]),
                int(times[hit]),
                float(prices[hit]),
                None if opening < 0 else int(ids[opening]),
            )
        )
    return rallies, trades


def _trade_rows(trades: pl.DataFrame, rallies: Sequence[_Rally]) -> pl.DataFrame:
    ids = trades['trade_id'].to_numpy()
    keep = np.zeros(len(ids), dtype=bool)
    for rally in rallies:
        if rally.first_trade_id is None:
            continue
        low = int(np.searchsorted(ids, rally.first_trade_id))
        keep[low : int(np.searchsorted(ids, rally.hit_id, side='right'))] = True
    return trades.filter(pl.Series(keep))


def _book_rows(
    reader: _Reader, rallies: Sequence[_Rally], boundary: Boundary
) -> tuple[pl.DataFrame, list[tuple[int, int] | None], dict[str, list[list[str]]]]:
    """Snapshots of the rallies, each once, each rally's first and last snapshot time, and
    the book reads: inclusive `spans` and the `[from, to)` windows searched for `last_before`."""
    frames = [pl.DataFrame(schema=_BOOK)]
    reads: dict[str, list[list[str]]] = {'spans': [], 'last_before': []}
    for low, high in _spans([(rally.start, rally.hit_time) for rally in rallies]):
        if boundary == 'before':
            reads['last_before'].append([_iso(low - _LOOKBACK), _iso(low)])
            carry_in = reader.book(low - _LOOKBACK, low, last=True)
            if carry_in.height:
                low = int(carry_in['observed_at'].dt.epoch('us')[0])
        reads['spans'].append([_iso(low), _iso(high)])
        frames.append(reader.book(low, high))
    # A span's before-anchor snapshot repeats the previous span's last one across a book gap.
    book = pl.concat(frames).unique('observed_at', keep='first', maintain_order=True)
    book = book.sort('observed_at')
    times = book['observed_at'].dt.epoch('us').to_numpy()
    keep = np.zeros(len(times), dtype=bool)
    bounds: list[tuple[int, int] | None] = []
    for rally in rallies:
        first = int(np.searchsorted(times, rally.start))
        if boundary == 'before':
            # Without a snapshot before the start the rally cannot start there: no book rows.
            if first == 0 or times[first - 1] < rally.start - _LOOKBACK:
                bounds.append(None)
                continue
            first -= 1
        last = int(np.searchsorted(times, rally.hit_time, side='right')) - 1
        if first > last:
            bounds.append(None)
            continue
        keep[first : last + 1] = True
        bounds.append((int(times[first]), int(times[last])))
    return book.filter(pl.Series(keep)), bounds, reads


def _spans(intervals: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping [start, end] intervals, sorted by start."""
    merged: list[tuple[int, int]] = []
    for low, high in sorted(intervals):
        if merged and low <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], high))
        else:
            merged.append((low, high))
    return merged


def _rally_frame(
    rallies: Sequence[_Rally], bounds: Sequence[tuple[int, int] | None]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            'rally_id': [_rally_id(rally.anchor) for rally in rallies],
            'anchor_time': [rally.anchor for rally in rallies],
            'reference_trade_id': [rally.reference_id for rally in rallies],
            'reference_time': [rally.reference_time for rally in rallies],
            'reference_price': [rally.reference_price for rally in rallies],
            'hit_trade_id': [rally.hit_id for rally in rallies],
            'hit_time': [rally.hit_time for rally in rallies],
            'hit_price': [rally.hit_price for rally in rallies],
            'time_to_hit': [rally.hit_time - rally.anchor for rally in rallies],
            'first_trade_id': [rally.first_trade_id for rally in rallies],
            'first_snapshot_time': [None if span is None else span[0] // 1000 for span in bounds],
            'last_snapshot_time': [None if span is None else span[1] // 1000 for span in bounds],
        },
        schema={
            'rally_id': pl.String,
            'anchor_time': pl.Int64,
            'reference_trade_id': pl.UInt64,
            'reference_time': pl.Int64,
            'reference_price': pl.Float64,
            'hit_trade_id': pl.UInt64,
            'hit_time': pl.Int64,
            'hit_price': pl.Float64,
            'time_to_hit': pl.Int64,
            'first_trade_id': pl.UInt64,
            'first_snapshot_time': pl.Int64,
            'last_snapshot_time': pl.Int64,
        },
    ).with_columns(
        pl.col('anchor_time', 'reference_time', 'hit_time')
        .cast(pl.Datetime('us'))
        .dt.replace_time_zone('UTC'),
        pl.col('time_to_hit').cast(pl.Duration('us')),
        pl.col('first_snapshot_time', 'last_snapshot_time')
        .cast(pl.Datetime('ms'))
        .dt.replace_time_zone('UTC'),
    )


def _schemas(metadata: str) -> tuple[object, object, object]:
    micros, millis = pa.timestamp('us', tz='UTC'), pa.timestamp('ms', tz='UTC')
    uint64, float64, boolean = pa.uint64(), pa.float64(), pa.bool_()
    levels = pa.list_(pa.struct([pa.field('price', float64), pa.field('quantity', float64)]))
    types = {
        'rally_id': pa.string(),
        'anchor_time': micros,
        'reference_trade_id': uint64,
        'reference_time': micros,
        'reference_price': float64,
        'hit_trade_id': uint64,
        'hit_time': micros,
        'hit_price': float64,
        'time_to_hit': pa.duration('us'),
        'first_trade_id': uint64,
        'first_snapshot_time': millis,
        'last_snapshot_time': millis,
        'trade_id': uint64,
        'timestamp': micros,
        'price': float64,
        'quantity': float64,
        'quote_quantity': float64,
        'is_buyer_maker': boolean,
        'is_best_match': boolean,
        'observed_at': millis,
        'last_update_id': uint64,
        'bids': levels,
        'asks': levels,
    }
    nullable = {'first_trade_id', 'first_snapshot_time', 'last_snapshot_time'}
    meta = {METADATA_KEY: metadata}
    rallies, trades, book = (
        pa.schema([pa.field(name, types[name], name in nullable) for name in fields], meta)
        for fields in (RALLY_FIELDS, TRADE_FIELDS, BOOK_FIELDS)
    )
    return rallies, trades, book


def _publish(
    output_dir: Path, frames: Sequence[pl.DataFrame], schemas: Sequence[object]
) -> tuple[Path, Path, Path]:
    """Write the three files into a staging directory and rename it into place."""
    staging = Path(tempfile.mkdtemp(prefix=f'.{output_dir.name}-', dir=output_dir.parent))
    try:
        for name, frame, schema in zip(OUTPUT_FILENAMES, frames, schemas, strict=True):
            _write(staging / name, frame, schema)
        staging.rename(output_dir)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    rallies, trades, book = (output_dir / name for name in OUTPUT_FILENAMES)
    return rallies, trades, book


def _write(path: Path, frame: pl.DataFrame, schema: object) -> None:
    table = cast(_Table, frame.to_arrow(compat_level=pl.CompatLevel.oldest())).cast(schema)
    with ipc.new_file(str(path), schema, options=ipc.IpcWriteOptions(compression='zstd')) as out:
        out.write_table(table)
