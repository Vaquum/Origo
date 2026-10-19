"""Exact archive dictionaries and bounded native order-book replay."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

import numpy as np
import polars as pl

from origo.workers.book_capture import BOOK_PRICE_TICK, parse_book_integer

from ..contracts import Partition, SourceError
from .book_spool import Market, utc_millisecond

_KEYS = (
    'received_time',
    'event_time',
    'event_type',
    'first_update_id',
    'final_update_id',
    'prev_final_update_id',
    'last_update_id',
)
_COLUMNS = (*_KEYS, 'symbol', 'side', 'price', 'quantity')


class _NativeReplay(Protocol):
    def replay(self, buffers: tuple[object, ...], start: int, end: int) -> tuple[bytes, bytes]: ...


@dataclass(frozen=True)
class ArchiveReplay:
    payloads: tuple[bytes, bytes]
    minutes: list[dict[str, object]]
    terminal_ms: int
    terminal_id: int


class _Inputs:
    def __init__(self, market: Market, end_ms: int) -> None:
        self.market, self.end_ms = market, end_ms
        self.files: dict[Path, tuple[pl.DataFrame, pl.DataFrame]] = {}
        self.offset = 0
        self.prices: list[Decimal] = []
        self.quantities = [Decimal(0)]
        self.price_codes: dict[Decimal, int] = {}
        self.quantity_codes = {Decimal(0): 0}
        self.literals: dict[str, dict[str, int]] = {'price': {}, 'quantity': {}}

    def add(self, path: Path, *, following: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
        if path in self.files:
            return self.files[path]
        scan = pl.scan_parquet(path)
        if not set(_COLUMNS) <= set(scan.collect_schema().names()):
            raise SourceError('BOOK_VENDOR_SCHEMA', 'Archive lacks required order-book columns.')
        scan = scan.select(_COLUMNS)
        if following:
            terminal = (
                scan.filter(
                    (pl.col('event_type') == 'update') & (pl.col('event_time') >= self.end_ms)
                )
                .select(pl.col('received_time').min())
                .collect()
                .item()
            )
            if terminal is not None:
                scan = scan.filter(
                    (pl.col('received_time') <= terminal) | (pl.col('event_time') < self.end_ms)
                )
        frame = scan.collect()
        if frame.schema['price'] != pl.String or frame.schema['quantity'] != pl.String:
            raise SourceError('BOOK_VENDOR_ROW', 'Archive values require decimal strings.')
        required = (
            'received_time',
            'event_time',
            'event_type',
            'symbol',
            'side',
            'price',
            'quantity',
        )
        if any(frame[name].null_count() for name in required):
            raise SourceError('BOOK_VENDOR_ROW', 'Archive contains a null required field.')
        if frame.height and (
            frame['symbol'].unique().to_list() != ['BTCUSDT']
            or not frame['side'].is_in(['bid', 'ask']).all()
        ):
            raise SourceError('BOOK_VENDOR_ROW', 'Archive has an invalid symbol or side.')
        if not frame['event_type'].is_in(['snapshot', 'update']).all():
            raise SourceError('BOOK_VENDOR_EVENT', 'Unknown archive event type.')
        frame = frame.with_columns(pl.struct(_KEYS).rle_id().alias('block'))
        headers = frame.filter(
            pl.col('block') != pl.col('block').shift(1).fill_null(2**32 - 1)
        ).select('block', *_KEYS)
        events = (
            headers.select(_KEYS)
            .unique(maintain_order=True)
            .with_row_index('event', offset=self.offset)
        )
        block_events = (
            headers.join(events, on=_KEYS, how='left', nulls_equal=True)
            .select('block', 'event')
            .sort('block')
        )
        event_for_row = block_events['event'].to_numpy()[frame['block'].to_numpy()]
        frame = frame.select('side', 'price', 'quantity')
        numeric: list[pl.Series] = []
        for name, mapping, values in (
            ('price', self.price_codes, self.prices),
            ('quantity', self.quantity_codes, self.quantities),
        ):
            dictionary = frame[name].to_arrow().dictionary_encode()
            strings = pl.from_arrow(dictionary.dictionary).to_list()
            codes: list[int] = []
            for raw in strings:
                string = str(raw)
                code = self.literals[name].get(string)
                if code is None:
                    value = Decimal(string)
                    if not value.is_finite() or value < 0 or (name == 'price' and not value):
                        raise ValueError('Invalid archive decimal.')
                    if name == 'price' and value % BOOK_PRICE_TICK[self.market]:
                        raise SourceError('BOOK_PRICE_GRID', 'Archive price is off the venue grid.')
                    code = mapping.get(value)
                    if code is None:
                        code = len(values)
                        mapping[value] = code
                        values.append(value)
                    self.literals[name][string] = code
                codes.append(code)
            numeric.append(
                pl.Series(name, np.asarray(codes, dtype=np.uint32)[dictionary.indices.to_numpy()])
            )
            del dictionary
        levels = frame.select(
            pl.Series('event', event_for_row),
            (pl.col('side') == 'ask').cast(pl.UInt8).alias('side'),
            *numeric,
        )
        del frame
        duplicate = levels.group_by('event', 'side', 'price').len().filter(pl.col('len') > 1)
        if duplicate.height:
            repeated = levels.join(
                duplicate.select('event', 'side', 'price'), on=('event', 'side', 'price')
            )
            if (
                repeated.group_by('event', 'side', 'price')
                .agg(pl.col('quantity').n_unique())
                .filter(pl.col('quantity') > 1)
                .height
            ):
                raise SourceError(
                    'BOOK_VENDOR_ROW', 'One native message gives conflicting absolute quantities.'
                )
            levels = levels.unique(maintain_order=True)
        self.offset += events.height
        self.files[path] = events, levels
        return events, levels

    def ordered(
        self, paths: tuple[Path, ...], *, native_order: bool
    ) -> tuple[pl.DataFrame, pl.DataFrame, tuple[Decimal, ...]]:
        messages: list[pl.DataFrame] = []
        pieces: list[pl.DataFrame] = []
        for index, path in enumerate(paths):
            events, levels = self.add(path, following=index == len(paths) - 1)
            messages.append(events.with_columns(pl.lit(index, dtype=pl.UInt8).alias('file')))
            pieces.append(levels)
        events = pl.concat(messages).with_columns(
            pl.when(pl.col('event_type') == 'update')
            .then(pl.col('final_update_id'))
            .otherwise(pl.col('last_update_id'))
            .alias('native')
        )
        if native_order:
            events = (
                events.with_columns((pl.col('event_type') == 'snapshot').alias('snapshot'))
                .sort('native', 'snapshot', 'received_time', 'file')
                .drop('snapshot')
            )
        events = events.with_row_index('order')
        order_for_event = np.empty(self.offset, dtype=np.uint32)
        order_for_event[events['event'].to_numpy()] = np.arange(events.height, dtype=np.uint32)
        raw = pl.concat(pieces)
        prices = tuple(sorted(self.prices))
        ranks = {price: index for index, price in enumerate(prices)}
        rank_for_price = np.array([ranks[p] for p in self.prices], dtype=np.uint32)
        levels = pl.DataFrame(
            {
                'order': order_for_event[raw['event'].to_numpy()],
                'side': raw['side'],
                'price': rank_for_price[raw['price'].to_numpy()],
                'quantity': raw['quantity'],
            }
        ).sort('order')
        return events, levels, prices


_inputs: ContextVar[_Inputs | None] = ContextVar('book_archive_inputs', default=None)


@contextmanager
def replay_session(market: Market, end_ms: int) -> Iterator[None]:
    token = _inputs.set(_Inputs(market, end_ms))
    try:
        yield
    finally:
        _inputs.reset(token)


def _plan(
    events: pl.DataFrame, market: Market, start: int, end: int, native_order: bool
) -> pl.DataFrame:
    rows: list[tuple[int, int, int, str]] = []
    pending: list[tuple[int, int, int, str]] = []
    cursor: int | None = None
    clock: int | None = None
    verified = False
    previous_receive = 0
    nominal: set[tuple[int, int]] = set()
    leading = True
    previous_file: int | None = None
    for values in events.select('order', *_KEYS, 'file', 'native').iter_rows():
        row = cast(tuple[object, ...], values)
        order, received, event = (parse_book_integer(row[i]) for i in (0, 1, 2))
        kind = row[3]
        file, last_native = parse_book_integer(row[8]), parse_book_integer(row[9])
        if file != previous_file:
            leading, previous_file = True, file
        if kind != 'snapshot':
            leading = False
        if not native_order:
            if received < previous_receive:
                raise SourceError('BOOK_VENDOR_ORDERING', 'Archive receive order regressed.')
            previous_receive = received
            if (
                leading
                and kind == 'snapshot'
                and event % 3600000 == 0
                and received == event * 1000000
            ):
                nominal.add((event, last_native))
        reset = False
        if kind == 'snapshot':
            last = parse_book_integer(row[7])
            if native_order:
                if cursor is None:
                    cursor, clock, verified, reset = last, None, False, True
                    if pending:
                        if any(item[2] != last for item in pending):
                            raise SourceError(
                                'BOOK_VENDOR_SEED_MISSING',
                                'Boundary snapshot has a different native cursor.',
                            )
                        rows, pending = pending, []
                        clock, verified, reset = start, True, False
                elif not verified or cursor != last:
                    continue
            else:
                is_nominal = (event, last) in nominal
                if event % 3600000 == 0 and not is_nominal:
                    raise SourceError(
                        'BOOK_VENDOR_CLOCK_UNPROVEN', 'Hour marker lacks a nominal identity.'
                    )
                if (
                    cursor is not None
                    and verified
                    and cursor != last
                    and event >= start
                    and not (event == start and clock is not None and clock < start)
                ):
                    raise SourceError(
                        'BOOK_SEQUENCE_GAP', 'Checkpoint skips native updates inside the hour.'
                    )
                if not is_nominal and clock is not None and event < clock:
                    raise SourceError(
                        'BOOK_VENDOR_ORDERING', 'Checkpoint exchange clock regressed.'
                    )
                if cursor != last or not verified:
                    reset, cursor, clock, verified = True, last, None if is_nominal else event, True
                elif not is_nominal:
                    clock = event
        elif kind == 'update':
            first, last = parse_book_integer(row[4]), parse_book_integer(row[5])
            previous = parse_book_integer(row[6]) if market == 'perp' else None
            if first > last:
                raise ValueError('Native update IDs are reversed.')
            if cursor is not None and (
                last <= cursor if verified or market == 'spot' else last < cursor
            ):
                continue
            if received // 1000000 < event:
                if event >= start:
                    raise SourceError('BOOK_EVENT_STALE', 'Exchange event is ahead of receipt.')
                rows, cursor, clock, verified = [], None, None, False
                continue
            if cursor is None:
                if native_order and event == start and (not pending or pending[0][2] == last):
                    pending.append((order, event, last, 'boundary'))
                    continue
                if event >= start:
                    raise SourceError(
                        'BOOK_VENDOR_SEED_MISSING', 'No initial state before the requested hour.'
                    )
                reset = True
            else:
                bridges = (
                    (previous == cursor if verified else first <= cursor <= last)
                    if market == 'perp'
                    else first <= cursor + 1 <= last
                )
                if not bridges or (clock is not None and event < clock):
                    if event >= start:
                        raise SourceError(
                            'BOOK_SEQUENCE_GAP', 'Native update chain does not bridge.'
                        )
                    reset = True
                elif clock is None and event > start:
                    raise SourceError(
                        'BOOK_VENDOR_CLOCK_UNPROVEN',
                        'First real exchange witness follows the initial grid.',
                    )
            cursor, clock, verified = last, event, True
        else:
            raise SourceError('BOOK_VENDOR_EVENT', 'Unknown archive event type.')
        if reset:
            if clock is not None and clock > start:
                raise SourceError(
                    'BOOK_VENDOR_SEED_MISSING', 'Initial state follows the opening grid.'
                )
            rows = []
        assert cursor is not None
        rows.append((order, clock if clock is not None else -1, cursor, str(kind)))
        if clock is not None and clock >= end:
            break
    if cursor is None or not verified:
        raise SourceError('BOOK_VENDOR_SEED_MISSING', 'No verified initial state.')
    if clock is None:
        raise SourceError('BOOK_VENDOR_CLOCK_UNPROVEN', 'No real exchange clock.')
    if clock < end - 100:
        raise SourceError('BOOK_VENDOR_TAIL_MISSING', 'No exchange witness for the final grid.')
    result = pl.DataFrame(
        rows,
        schema={
            'order': pl.UInt32,
            'effective': pl.Int64,
            'native': pl.UInt64,
            'event_type': pl.String,
        },
        orient='row',
    )
    if result.filter(pl.col('effective') >= 0)['effective'][0] > start:
        raise SourceError(
            'BOOK_VENDOR_CLOCK_UNPROVEN', 'First real exchange witness follows the opening grid.'
        )
    return result


def replay(paths: tuple[Path, ...], market: Market, partition: Partition) -> ArchiveReplay:
    start, end = int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000)
    inputs = _inputs.get() or _Inputs(market, end)
    native_order = partition.key < '2026-08-19T00Z'
    events, levels, prices = inputs.ordered(paths, native_order=native_order)
    events = _plan(events, market, start, end, native_order)
    positions = np.full(int(levels['order'].max()) + 1, -1, dtype=np.int32)
    positions[events['order'].to_numpy()] = np.arange(events.height, dtype=np.int32)
    order = positions[levels['order'].to_numpy()]
    kept = order >= 0
    side, price = levels['side'].to_numpy()[kept], levels['price'].to_numpy()[kept]
    quantity = levels['quantity'].to_numpy()[kept].astype(np.int32)
    order = order[kept]
    boundary = events.filter(pl.col('event_type') == 'boundary')['order'].to_list()
    if boundary:
        repeated = levels.filter(pl.col('order').is_in(boundary))
        if (
            repeated.group_by('side', 'price')
            .agg(pl.col('quantity').n_unique())
            .filter(pl.col('quantity') > 1)
            .height
        ):
            raise SourceError(
                'BOOK_SNAPSHOT_MISMATCH', 'Boundary collectors disagree about an absolute quantity.'
            )
    offsets = np.searchsorted(order, np.arange(events.height + 1)).astype(np.int64)
    effective = events['effective'].to_numpy().astype(np.int64)
    native = events['native'].to_numpy().astype(np.uint64)
    update = (events['event_type'] != 'snapshot').to_numpy().astype(np.uint8)
    adjacent = np.array(
        [b - a == BOOK_PRICE_TICK[market] for a, b in zip(prices, prices[1:])], dtype=np.uint8
    )
    price_values = np.fromiter(map(float, prices), dtype=np.float64)
    quantity_values = np.fromiter(map(float, inputs.quantities), dtype=np.float64)
    engine = cast(_NativeReplay, importlib.import_module('_origo_book_replay'))
    try:
        payloads = engine.replay(
            (
                offsets,
                update,
                effective,
                native,
                side,
                price,
                quantity,
                price_values,
                quantity_values,
                adjacent,
            ),
            start,
            end,
        )
    except ValueError as error:
        code, separator, message = str(error).partition('|')
        if separator and code.startswith('BOOK_'):
            raise SourceError(code, message) from error
        raise
    dtype20 = np.dtype([('time', '<i8'), ('event', '<u8'), ('last', '<u8'), ('payload', 'V642')])
    small = np.frombuffer(payloads[0], dtype=dtype20)
    fused_type = np.dtype([('first', 'V666'), ('large', 'V6428'), ('rest', 'V666', (9,))])
    # Seals include accepted events after the minute's final emitted grid.
    minute_last = np.searchsorted(
        effective, np.arange(start + 60000, end + 1, 60000), side='left'
    ) - 1
    minutes: list[dict[str, object]] = []
    for minute in range(60):
        tiny = np.frombuffer(
            payloads[0], dtype='V666', count=600, offset=minute * 600 * 666
        ).reshape(60, 10)
        large = np.frombuffer(payloads[1], dtype='V6428', count=60, offset=minute * 60 * 6428)
        fused = np.empty(60, dtype=fused_type)
        fused['first'], fused['large'], fused['rest'] = tiny[:, 0], large, tiny[:, 1:]
        minutes.append(
            {
                'market': market,
                'minute_start': utc_millisecond(start + minute * 60000).isoformat(),
                'samples_depth20': 600,
                'samples_depth200': 60,
                'first_update_id': int(small['last'][minute * 600]),
                'last_update_id': int(native[minute_last[minute]]),
                'payload_sha256': hashlib.sha256(fused.tobytes()).hexdigest(),
            }
        )
    return ArchiveReplay(payloads, minutes, int(effective[-1]), int(native[-1]))
