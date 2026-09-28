from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from origo.sources.contracts import BuildContext, Client, Partition, Revision, Row, SourceError
from origo.sources.profiles.market_state import (
    BASE_PRICE_USDT,
    BASE_TIME_US,
    CUBE_START,
    MARKET_STATE_COMPONENTS,
    MARKET_STATE_DETAIL_COMPONENTS,
    build_market_state,
    build_market_state_detail,
)
from origo.sources.profiles.spot import SPOT_COMPONENTS

from .test_market_state_projection import _archive, _create_table, _read, cube_client

_T0 = 1_609_459_200_000_000
_ROW = BASE_PRICE_USDT * 100
_MINUTE = 60_000_000
_EPOCH = datetime(1970, 1, 1)
Key = tuple[int, int]
Totals = dict[Key, int]


@dataclass(frozen=True)
class Trade:
    id: int
    cents: int
    sats: int
    price: float
    micros: int

    @property
    def column(self) -> int:
        return (self.micros - _T0) // BASE_TIME_US

    @property
    def row(self) -> int:
        return self.cents // _ROW


@dataclass
class Cell:
    event: int
    trades: list[Trade] = field(default_factory=list[Trade])
    path: int = 0
    dwell: int = 0


def _micros(instant: datetime) -> int:
    return (instant - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _at(micros: int) -> datetime:
    return _EPOCH + timedelta(microseconds=micros)


def _trades(body: bytes, *, milliseconds: bool = False) -> list[Trade]:
    """The archive's trades from the CSV text: Decimal cents and satoshis, integer microseconds."""
    trades: list[Trade] = []
    for fields in csv.reader(body.decode().splitlines()):
        cents, sats = Decimal(fields[1]) * 100, Decimal(fields[2]) * 100_000_000
        assert cents == int(cents) and sats == int(sats)
        assert len(fields[4]) in (13, 16)
        micros = int(fields[4]) * (1000 if len(fields[4]) == 13 else 1)
        if milliseconds:
            micros -= micros % 1000
        trades.append(Trade(int(fields[0]), int(cents), int(sats), float(fields[1]), micros))
    return sorted(trades, key=lambda trade: (trade.micros, trade.id))


def _spread(target: Totals, begin: int, end: int, row: int, sign: int = 1) -> None:
    """Add the time of [begin, end) at one price row, split across the base columns."""
    for column in range((begin - _T0) // BASE_TIME_US, (end - 1 - _T0) // BASE_TIME_US + 1):
        held = min(end, _T0 + (column + 1) * BASE_TIME_US) - max(begin, _T0 + column * BASE_TIME_US)
        if held > 0:
            target[(column, row)] = target.get((column, row), 0) + sign * held


def _move(target: Totals, before: Trade, after: Trade) -> None:
    """Add one move between consecutive trades, in the later trade's column, per half-open row."""
    low, high = sorted((before.cents, after.cents))
    for row in range(low // _ROW, high // _ROW + 1):
        piece = min(high, (row + 1) * _ROW) - max(low, row * _ROW)
        if piece > 0:
            target[(after.column, row)] = target.get((after.column, row), 0) + piece


def _reference(trades: list[Trade], start: int, end: int) -> dict[Key, Cell]:
    """Every cell of one partition built from its own trades, independently of the builder."""
    cells: dict[Key, Cell] = {}

    def cell(key: Key, event: int) -> Cell:
        found = cells.setdefault(key, Cell(event))
        found.event = min(found.event, event)
        return found

    for index, trade in enumerate(trades):
        cell((trade.column, trade.row), trade.micros).trades.append(trade)
        if index:
            path: Totals = {}
            _move(path, trades[index - 1], trade)
            for key, piece in path.items():
                cell(key, trade.micros).path += piece
        begin = start if index == 0 else trade.micros
        held: Totals = {}
        _spread(held, begin, trades[index + 1].micros if index + 1 < len(trades) else end, trade.row)
        for key, time in held.items():
            cell(key, max(begin, _T0 + key[0] * BASE_TIME_US)).dwell += time
    return cells


def _row(key: Key, cell: Cell) -> Row:
    if not cell.trades:
        at = _at(cell.event)
        return (*key, at, 0, 0, cell.path, cell.dwell, 0.0, 0.0, at, 0, 0.0, at, 0, 0.0)
    first = min(cell.trades, key=lambda trade: trade.id)
    last = max(cell.trades, key=lambda trade: trade.id)
    return (
        *key, _at(cell.event), len(cell.trades), sum(trade.sats for trade in cell.trades),
        cell.path, cell.dwell,
        max(trade.price for trade in cell.trades), min(trade.price for trade in cell.trades),
        _at(first.micros), first.id, first.price, _at(last.micros), last.id, last.price,
    )


def _assert_detail(rows: list[Row], cells: dict[Key, Cell]) -> None:
    actual = {(int(str(row[0])), int(str(row[1]))): tuple(row) for row in rows}
    expected = {key: _row(key, cell) for key, cell in cells.items()}
    assert len(actual) == len(rows)
    assert actual.keys() == expected.keys()
    wrong = [key for key in sorted(expected) if actual[key] != expected[key]]
    assert not wrong, [(actual[key], expected[key]) for key in wrong[:3]]


def _raw(client: Client, partition: Partition, rows: tuple[Row, ...]) -> BuildContext:
    """Private raw, PRD-0022 cube and detail tables of one partition, as the lifecycle lays them out."""
    raw = 'raw_latest' if partition.provisional else 'raw'
    for component in SPOT_COMPONENTS:
        if component.key in (raw, 'market_state' + raw[3:], 'market_state_detail' + raw[3:]):
            _create_table(client, component)
    inserted = tuple((partition.start, *row) for row in rows) if partition.provisional else rows
    client.execute(f'INSERT INTO origo.{raw} VALUES', inserted)
    revision = Revision('capture', 'capture', '{}', len(rows), lambda: iter(rows))
    return BuildContext(client, 'origo', partition, revision, uuid4())


def _build(client: Client, partition: Partition, rows: tuple[Row, ...]) -> tuple[list[Row], list[Row]]:
    """Build one partition's PRD-0022 cube and its detail from the same private raw table."""
    context = _raw(client, partition, rows)
    build_market_state(context)
    build_market_state_detail(context)
    suffix = '_latest' if partition.provisional else ''
    base = next(item for item in MARKET_STATE_COMPONENTS if item.key == 'market_state' + suffix)
    detail = next(item for item in MARKET_STATE_DETAIL_COMPONENTS if item.key == 'market_state_detail' + suffix)
    return _read(client, base), _read(client, detail)


def _field(rows: list[Row], index: int) -> Totals:
    """One column of built rows per cell, without zeros."""
    totals: Totals = defaultdict(int)
    for row in rows:
        totals[(int(str(row[0])), int(str(row[1])))] += int(str(row[index]))
    return {key: value for key, value in totals.items() if value}


def _net(*parts: tuple[int, Totals]) -> Totals:
    totals: Totals = defaultdict(int)
    for sign, part in parts:
        for key, value in part.items():
            totals[key] += sign * value
    return {key: value for key, value in totals.items() if value}


def _inside(rows: list[Row], trades: list[Trade]) -> list[Row]:
    """The built cells without trades whose first event is inside the traded span."""
    begin, end = _at(trades[0].micros), _at(trades[-1].micros)
    return [row for row in rows if not row[3] and isinstance(row[2], datetime) and begin <= row[2] <= end]


def test_detail_components_have_their_own_group() -> None:
    assert [
        (item.key, item.activation_group, item.time_column)
        for item in SPOT_COMPONENTS if item.key.startswith('market_state')
    ] == [
        ('market_state', 'market_state', 'first_trade_at'),
        ('market_state_latest', 'market_state', 'first_trade_at'),
        ('market_state_detail', 'market_state_detail', 'first_event_at'),
        ('market_state_detail_latest', 'market_state_detail', 'first_event_at'),
    ]
    for component in MARKET_STATE_COMPONENTS:
        assert [(column.name, column.sql_type) for column in component.columns] == [
            ('time_index', 'UInt64'),
            ('price_index', 'UInt64'),
            ('first_trade_at', 'DateTime64(6)'),
            ('volume', 'Float64'),
            ('trade_count', 'UInt32'),
            ('taker_buy_volume', 'Float64'),
            ('taker_buy_trade_count', 'UInt32'),
        ]
        assert (component.primary_key, component.build, component.start_at) == (
            ('time_index', 'price_index'), build_market_state, CUBE_START
        )
    for component in MARKET_STATE_DETAIL_COMPONENTS:
        assert [(column.name, column.sql_type) for column in component.columns] == [
            ('time_index', 'UInt64'),
            ('price_index', 'UInt64'),
            ('first_event_at', 'DateTime64(6)'),
            ('trade_count', 'UInt32'),
            ('base_volume', 'UInt64'),
            ('path_length', 'UInt64'),
            ('dwell', 'UInt32'),
            ('high', 'Float64'),
            ('low', 'Float64'),
            ('first_trade_at', 'DateTime64(6)'),
            ('first_trade_id', 'UInt64'),
            ('first_price', 'Float64'),
            ('last_trade_at', 'DateTime64(6)'),
            ('last_trade_id', 'UInt64'),
            ('last_price', 'Float64'),
        ]
        assert (component.primary_key, component.build, component.start_at) == (
            ('time_index', 'price_index'), build_market_state_detail, CUBE_START
        )
    # A current view reads its own component and the provisional ones that target it, so
    # PRD-0022's two current views keep exactly their inputs.
    assert {
        (item.key, item.provisional, item.current_target)
        for item in SPOT_COMPONENTS if item.key.startswith('market_state')
    } == {
        ('market_state', False, None),
        ('market_state_latest', True, 'market_state'),
        ('market_state_detail', False, None),
        ('market_state_detail_latest', True, 'market_state_detail'),
    }
    assert [item.key for item in SPOT_COMPONENTS if item.current_target == 'market_state'] == [
        'market_state_latest'
    ]


@pytest.mark.parametrize(
    'day', ['2021-01-01', '2024-12-31', '2025-01-01', '2021-05-19', '2023-03-24', '2025-10-10']
)
def test_detail_matches_authentic_archives(cube_client: Client, day: str) -> None:
    body, partition, rows = _archive(day)
    start, end = _micros(partition.start), _micros(partition.end)
    trades = _trades(body)
    base, detail = _build(cube_client, partition, rows)
    # Each capture is a segment of its day built as the whole day: its head and tail hold a
    # price for most of the day, and the reference covers those cells too.
    _assert_detail(detail, _reference(trades, start, end))
    assert _field(detail, 3) == _field(base, 4)
    assert {(row[0], row[1]): row[9] for row in detail if row[3]} == {
        (row[0], row[1]): row[2] for row in base
    }
    assert sum(_field(detail, 3).values()) == len(rows) == len(trades)
    assert sum(_field(detail, 4).values()) == sum(trade.sats for trade in trades)
    assert sum(_field(detail, 5).values()) == sum(
        abs(after.cents - before.cents) for before, after in zip(trades, trades[1:], strict=False)
    )
    held: dict[int, int] = defaultdict(int)
    for (column, _), dwell in _field(detail, 6).items():
        held[column] += dwell
    assert held == {
        column: min(end, _T0 + (column + 1) * BASE_TIME_US) - max(start, _T0 + column * BASE_TIME_US)
        for column in range((start - _T0) // BASE_TIME_US, (end - 1 - _T0) // BASE_TIME_US + 1)
    }


def _residuals(trades: list[Trade], truncated: list[Trade]) -> tuple[Totals, Totals, Totals, Totals]:
    """How a day's path and dwell over its minutes' span exceed those of its minutes, per cell.

    Four parts, each from the trades alone: the moves into each minute's first trade, the
    minutes without trades, the rows of the minutes' head intervals, and the millisecond
    truncation of the provisional times.
    """
    moves: Totals = {}
    silent: Totals = {}
    heads: Totals = {}
    truncation: Totals = {}
    for index, (trade, cut) in enumerate(zip(trades, truncated, strict=True)):
        minute = cut.micros - cut.micros % _MINUTE
        first = index == 0 or truncated[index - 1].micros < minute
        last = index + 1 == len(trades) or truncated[index + 1].micros >= minute + _MINUTE
        if first:
            # The day holds the price before this trade; the minute's head holds this trade's.
            held = trade if index == 0 else trades[index - 1]
            _spread(truncation, cut.micros, trade.micros, held.row)
            if index:
                _move(moves, trades[index - 1], trade)
                previous = truncated[index - 1].micros
                _spread(silent, previous - previous % _MINUTE + _MINUTE, minute, held.row)
                _spread(heads, minute, cut.micros, held.row)
                _spread(heads, minute, cut.micros, trade.row, -1)
        if last:
            _spread(truncation, cut.micros, trade.micros, trade.row, -1)
        else:
            _spread(truncation, trade.micros, trades[index + 1].micros, trade.row)
            _spread(truncation, cut.micros, truncated[index + 1].micros, trade.row, -1)
    return moves, silent, heads, truncation


@pytest.mark.parametrize('day', ['2025-01-01', '2023-03-24', '2025-10-10'])
def test_detail_minutes_recombine_to_the_day(cube_client: Client, day: str) -> None:
    body, partition, rows = _archive(day)
    _, whole = _build(cube_client, partition, rows)
    trades, truncated = _trades(body), _trades(body, milliseconds=True)
    assert [trade.id for trade in trades] == [trade.id for trade in truncated]
    by_minute: dict[datetime, list[Row]] = defaultdict(list)
    for row in rows:
        assert isinstance(row[-1], datetime)
        by_minute[row[-1].replace(second=0, microsecond=0)].append(row)
    minutes: list[Row] = []
    for minute, selected in sorted(by_minute.items()):
        provisional = Partition(
            minute.strftime('%Y-%m-%dT%H:%M:%SZ'), minute, minute + timedelta(minutes=1), True
        )
        ids = {int(str(row[0])) for row in selected}
        _, built = _build(cube_client, provisional, tuple(selected))
        _assert_detail(built, _reference(
            [trade for trade in truncated if trade.id in ids],
            _micros(provisional.start), _micros(provisional.end),
        ))
        minutes.extend(built)
    span = (_micros(min(by_minute)), _micros(max(by_minute)) + _MINUTE)
    outside: Totals = {}
    _spread(outside, _micros(partition.start), span[0], trades[0].row)
    _spread(outside, span[1], _micros(partition.end), trades[-1].row)
    moves, silent, heads, truncation = _residuals(trades, truncated)
    assert _net((1, _field(whole, 5)), (-1, _field(minutes, 5))) == moves
    assert _net(
        (1, _field(whole, 6)), (-1, outside), (-1, _field(minutes, 6)),
        (-1, silent), (-1, heads), (-1, truncation),
    ) == {}
    for index in (3, 4):
        assert _field(whole, index) == _field(minutes, index)
    assert moves
    if day == '2025-01-01':
        pair = [trade for trade in trades if trade.id in (4359944902, 4359944903)]
        cut = [trade for trade in truncated if trade.id in (4359944902, 4359944903)]
        assert (pair[1].micros - pair[0].micros, cut[1].micros - cut[0].micros) == (2843, 3000)
        assert (pair[0].column, pair[0].row, pair[1].row) == (pair[1].column, 748, 749)
        assert truncation[(pair[0].column, 748)]
    elif day == '2023-03-24':
        assert sum(silent.values()) == 152 * _MINUTE
        assert not _net((1, truncation))
    else:
        # 21:24:00 opens at row 860 after 21:23 closed at 859: the head's rows differ.
        assert _net((1, heads)) and _net((1, truncation))
    assert bool(_net((1, silent))) == (day == '2023-03-24')


def test_detail_moved_through_cells(cube_client: Client) -> None:
    body, partition, rows = _archive('2023-03-24')
    _, detail = _build(cube_client, partition, rows)
    trades = _trades(body)
    halt = max(range(1, len(trades)), key=lambda index: trades[index].micros - trades[index - 1].micros)
    before, after = trades[halt - 1], trades[halt]
    assert (before.micros, after.micros, before.row, after.row) == (
        1_679_657_243_146_000, 1_679_666_400_062_000, 224, 224
    )
    assert [tuple(row[:7]) for row in _inside(detail, trades)] == [
        (column, 224, _at(_T0 + column * BASE_TIME_US), 0, 0, 0, BASE_TIME_US)
        for column in range(before.column + 1, after.column)
    ]
    assert after.column - before.column - 1 == 162

    body, partition, rows = _archive('2021-05-19')
    _, detail = _build(cube_client, partition, rows)
    trades = _trades(body)
    crossings = [
        after for before, after in zip(trades, trades[1:], strict=False)
        if after.column == 212_796
        and min(before.cents, after.cents) < 283 * _ROW and max(before.cents, after.cents) > 282 * _ROW
    ]
    assert len(crossings) == 8
    assert [tuple(row[:7]) for row in _inside(detail, trades)] == [
        (212_796, 282, _at(crossings[0].micros), 0, 0, 100_000, 0)
    ]
    assert _at(crossings[0].micros) == datetime(2021, 5, 19, 12, 56, 16, 909_000)

    body, partition, rows = _archive('2025-10-10')
    _, detail = _build(cube_client, partition, rows)
    trades = _trades(body)
    assert not _inside(detail, trades)
    jump = next(
        index for index in range(1, len(trades))
        if trades[index].cents - trades[index - 1].cents == 300_500
    )
    assert (trades[jump - 1].row, trades[jump].row) == (816, 840)
    burst = [trade for trade in trades if trade.micros == trades[jump].micros]
    assert len(burst) == 24_433
    cells = {(int(str(row[0])), int(str(row[1]))): row for row in detail}
    column = trades[jump].column
    assert all(cells[(column, row)][3] and cells[(column, row)][5] for row in range(816, 841))
    assert not any(cells[(column, row)][6] for row in range(816, 840))

    body, partition, rows = _archive('2024-12-31')
    _, detail = _build(cube_client, partition, rows)
    trades = _trades(body)
    sweep = [trade for trade in trades if trade.id in (4356590637, 4356590638, 4356590639)]
    assert [trade.cents for trade in sweep] == [9_278_000, 9_277_872, 9_278_000]
    assert len({trade.micros for trade in sweep}) == 1
    start, end = _micros(partition.start), _micros(partition.end)
    key = (sweep[0].column, sweep[0].row)
    with_sweep = _reference(trades, start, end)[key]
    without = _reference([trade for trade in trades if trade.id != 4356590638], start, end)[key]
    assert (with_sweep.path - without.path, with_sweep.dwell - without.dwell) == (256, 0)
    assert next((row[5], row[6]) for row in detail if (row[0], row[1]) == key) == (
        with_sweep.path, with_sweep.dwell
    )


def _mutations(rows: list[Row]) -> list[tuple[str, Callable[[], list[Row]]]]:
    moved = next(index for index, row in enumerate(rows) if not row[3])
    traded = next(index for index, row in enumerate(rows) if row[3])

    def changed(index: int, position: int) -> list[Row]:
        row = list(rows[index])
        value = row[position]
        if isinstance(value, datetime):
            row[position] = value + timedelta(microseconds=1)
        elif isinstance(value, int):
            row[position] = value + 1
        else:
            row[position] = float(str(value)) + 0.01
        return [*rows[:index], tuple(row), *rows[index + 1:]]

    def change(index: int, position: int) -> Callable[[], list[Row]]:
        return lambda: changed(index, position)

    extra = (rows[traded][0], 10_000, *rows[traded][2:])
    return [
        ('removed moved-through cell', lambda: [*rows[:moved], *rows[moved + 1:]]),
        ('added cell', lambda: [*rows, extra]),
        *[(f'traded cell, field {position}', change(traded, position)) for position in range(2, 15)],
        *[(f'moved-through cell, field {position}', change(moved, position)) for position in (2, 5, 6)],
    ]


def test_detail_reference_catches_wrong_cells(cube_client: Client) -> None:
    body, partition, rows = _archive('2023-03-24')
    _, detail = _build(cube_client, partition, rows)
    cells = _reference(_trades(body), _micros(partition.start), _micros(partition.end))
    _assert_detail(detail, cells)
    mutations = _mutations(detail)
    assert len(mutations) == 18
    for name, mutate in mutations:
        with pytest.raises(AssertionError):
            _assert_detail(mutate(), cells)
            pytest.fail(name)


def test_detail_build_fails_on_trades_it_cannot_measure(cube_client: Client) -> None:
    _, partition, rows = _archive('2021-01-01')
    at = next(index for index in range(100, len(rows)) if rows[index][4] != rows[index + 1][4])
    first, second = rows[at], rows[at + 1]

    def swapped(row: Row, index: int, value: object) -> Row:
        return (*row[:index], value, *row[index + 1:])

    for changed, message in (
        ((swapped(first, 1, float(str(first[1])) + 0.001), second), 'whole-cent prices'),
        ((swapped(first, 2, float(str(first[2])) + 1e-9), second), 'whole-satoshi quantities'),
        ((swapped(first, 0, second[0]), swapped(second, 0, first[0])), 'increase with trade time'),
    ):
        context = _raw(cube_client, partition, (*rows[:at], *changed, *rows[at + 2:]))
        with pytest.raises(SourceError, match=message):
            build_market_state_detail(context)
