"""Exact rally acceptance against unchanged official and captured provider records."""
from __future__ import annotations

import csv
import hashlib
import inspect
import importlib
import json
import math
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, localcontext
from functools import cache
from pathlib import Path
from typing import Protocol, cast

import numpy as np
import polars as pl
import pytest
from numpy.typing import NDArray

from origo.query.binance_rallies import export_binance_rallies
from origo.query.rally_detection import (
    ANCHORED_DEADLINE_MINUTES,
    ATR_NAME,
    RallyBar,
    RallyDefinition,
    RallyDetection,
    RallyEvent,
    RallyTrades,
    definition_fingerprint,
    detect_rallies,
)

from .test_binance_rallies import rally_data as rally_data

ROOT = Path(__file__).resolve().parents[2]
DAY_ROOT = ROOT / 'tests/fixtures/binance/spot/daily/trades/revisioned'
DAY_FILE = DAY_ROOT / 'BTCUSDT-trades-2017-08-17.csv'
CAPTURE_ROOT = Path(__file__).parent / 'fixtures/binance_rallies'
PROOF_ROOT = Path(__file__).parent / 'fixtures/rally_detection'
DAY_START = datetime(2017, 8, 17, tzinfo=UTC)
DAY_END = DAY_START + timedelta(days=1)
CAPTURE_START = datetime(2026, 6, 27, 11, 38, tzinfo=UTC)
CAPTURE_END = datetime(2026, 6, 27, 11, 55, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUTE_US = 60_000_000
BAR_US = 15 * MINUTE_US
PRESET = RallyDefinition('first_hit', 'bps', Decimal('30'), anchor_minutes=1)
VOLUME_REL_TOLERANCE = 1e-12
VOLUME_ABS_TOLERANCE = 1e-8


class _ArrowSchema(Protocol):
    metadata: dict[bytes, bytes] | None


class _ArrowTable(Protocol):
    schema: _ArrowSchema


class _ArrowReader(Protocol):
    def read_all(self) -> _ArrowTable: ...


class _ArrowIPC(Protocol):
    def open_file(self, source: str) -> _ArrowReader: ...


def _us(moment: datetime) -> int:
    delta = moment - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _at(timestamp_us: int) -> datetime:
    return EPOCH + timedelta(microseconds=int(timestamp_us))


@cache
def _day() -> RallyTrades:
    with DAY_FILE.open(newline='') as handle:
        rows = list(csv.reader(handle))
    return RallyTrades(
        np.array([int(row[0]) for row in rows], dtype=np.uint64),
        np.array([int(row[4]) * 1000 for row in rows], dtype=np.int64),
        np.array([float(row[1]) for row in rows], dtype=np.float64),
        np.array([float(row[3]) for row in rows], dtype=np.float64),
        np.array([row[5] == 'True' for row in rows], dtype=np.bool_),
    )


@cache
def _capture() -> RallyTrades:
    frame = pl.read_parquet(CAPTURE_ROOT / 'BTCUSDT-spot-trades-2026-06-27T1138-1155.parquet')
    return RallyTrades(
        frame['trade_id'].to_numpy(),
        frame['datetime'].dt.epoch('us').to_numpy(),
        frame['price'].to_numpy(),
        frame['quote_quantity'].to_numpy(),
        frame['is_buyer_maker'].cast(pl.Boolean).to_numpy(),
    )


def _slice(trades: RallyTrades, start: int, end: int) -> RallyTrades:
    return RallyTrades(
        trades.trade_id[start:end], trades.timestamp_us[start:end], trades.price[start:end],
        trades.quote_quantity[start:end], trades.is_buyer_maker[start:end],
    )


@cache
def _bars() -> tuple[RallyBar, ...]:
    trades = _day()
    groups: dict[int, list[float]] = {}
    for timestamp, price in zip(trades.timestamp_us, trades.price, strict=True):
        groups.setdefault(int(timestamp) // BAR_US, []).append(float(price))
    return tuple(
        RallyBar(_at(bucket * BAR_US), _at((bucket + 1) * BAR_US), max(prices), min(prices), prices[-1])
        for bucket, prices in sorted(groups.items())
    )


def _atr(freeze_us: int) -> float | None:
    end = freeze_us // BAR_US * BAR_US
    by_start = {_us(bar.start): bar for bar in _bars()}
    required = [by_start.get(end - offset * BAR_US) for offset in range(15, 0, -1)]
    if any(bar is None for bar in required):
        return None
    complete = [bar for bar in required if bar is not None]
    previous_close = complete[0].close
    ranges: list[float] = []
    for bar in complete[1:]:
        ranges.append(max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close)))
        previous_close = bar.close
    return math.fsum(ranges) / 14.0


def _call(
    definition: RallyDefinition = PRESET,
    *,
    trades: RallyTrades | None = None,
    start: datetime = DAY_START,
    end: datetime = DAY_END,
    known_at: datetime | None = None,
    coverage: tuple[tuple[datetime, datetime], ...] | None = ((DAY_START, DAY_END),),
    bars: tuple[RallyBar, ...] = (),
    anchors: NDArray[np.int64] | None = None,
    source: str = 'binance_spot_trades',
    instrument: str = 'BTCUSDT',
) -> RallyDetection:
    return detect_rallies(
        trades=_day() if trades is None else trades, definition=definition,
        source=source, instrument=instrument, analysis_start=start, analysis_end=end,
        known_at=end if known_at is None else known_at, coverage=coverage, bars=bars,
        anchors_us=anchors,
    )


def _anchored_oracle(
    trades: RallyTrades,
    definition: RallyDefinition,
    start: datetime,
    end: datetime,
    anchors: tuple[int, ...] | None = None,
) -> list[tuple[int, int, int, int]]:
    cadence = definition.anchor_minutes
    assert cadence is not None
    step = cadence * MINUTE_US
    first_anchor = ((_us(start) + step - 1) // step) * step
    candidates = tuple(range(first_anchor, _us(end), step)) if anchors is None else anchors
    result: list[tuple[int, int, int, int]] = []
    for anchor in candidates:
        first = int(np.searchsorted(trades.timestamp_us, anchor, side='left'))
        reference = first - 1
        if reference < 0 or int(trades.timestamp_us[reference]) < anchor - 86400_000_000:
            continue
        frozen_atr = _atr(anchor) if definition.scale == 'atr' else None
        if definition.scale == 'atr' and frozen_atr is None:
            continue
        ref_price = float(trades.price[reference])
        if frozen_atr is None:
            threshold = ref_price * (1.0 + float(definition.target) / 10000.0)
            pullback = None if definition.pullback is None else ref_price * float(definition.pullback) / 10000.0
        else:
            threshold = ref_price + frozen_atr * float(definition.target)
            pullback = None if definition.pullback is None else frozen_atr * float(definition.pullback)
        stop = int(np.searchsorted(trades.timestamp_us, min(anchor + 240 * MINUTE_US, _us(end)), side='left'))
        high = ref_price
        for index in range(first, stop):
            price = float(trades.price[index])
            high = max(high, price)
            if pullback is not None and high - price > pullback:
                break
            if price >= threshold:
                result.append((anchor, reference, first, index))
                break
    return result


def _swing_oracle(
    trades: RallyTrades,
    definition: RallyDefinition,
    start: datetime,
    end: datetime,
) -> list[tuple[int, int, int]]:
    """Find alternating directional extrema by independent nested raw scans."""
    assert definition.reversal is not None
    first = int(np.searchsorted(trades.timestamp_us, _us(start), side='left'))
    stop = int(np.searchsorted(trades.timestamp_us, _us(end), side='left'))
    if first == stop:
        return []
    high = first
    retreat: int | None = None
    for index in range(first + 1, stop):
        if trades.price[index] > trades.price[high]:
            high = index
        atr = _atr(int(trades.timestamp_us[high])) if definition.scale == 'atr' else None
        if definition.scale == 'atr' and atr is None:
            continue
        reversed_leg = (
            float(trades.price[index]) <= float(trades.price[high]) * (1.0 - float(definition.reversal) / 10000.0)
            if atr is None else float(trades.price[high]) - float(trades.price[index]) >= atr * float(definition.reversal)
        )
        if reversed_leg:
            retreat = index
            break
    if retreat is None:
        return []
    result: list[tuple[int, int, int]] = []
    cursor = retreat
    while cursor < stop:
        trough = cursor
        qualified: int | None = None
        for index in range(cursor + 1, stop):
            if trades.price[index] < trades.price[trough]:
                trough = index
            atr = _atr(int(trades.timestamp_us[trough])) if definition.scale == 'atr' else None
            if definition.scale == 'atr' and atr is None:
                continue
            threshold = (
                float(trades.price[trough]) * (1.0 + float(definition.target) / 10000.0)
                if atr is None else float(trades.price[trough]) + atr * float(definition.target)
            )
            if trades.price[index] >= threshold:
                qualified = index
                break
        if qualified is None:
            break
        peak = qualified
        confirmation: int | None = None
        frozen_atr = _atr(int(trades.timestamp_us[trough])) if definition.scale == 'atr' else None
        for index in range(qualified + 1, stop):
            if trades.price[index] > trades.price[peak]:
                peak = index
            reversed_leg = (
                float(trades.price[index]) <= float(trades.price[peak]) * (1.0 - float(definition.reversal) / 10000.0)
                if frozen_atr is None else float(trades.price[peak]) - float(trades.price[index]) >= frozen_atr * float(definition.reversal)
            )
            if reversed_leg:
                confirmation = index
                break
        if confirmation is None:
            break
        result.append((trough, peak, confirmation))
        cursor = confirmation
    return result


def _event_members(event: RallyEvent, trades: RallyTrades) -> tuple[int, int]:
    first = int(np.searchsorted(trades.trade_id, event.start_trade_id))
    last = int(np.searchsorted(trades.trade_id, event.end_trade_id))
    assert int(trades.trade_id[first]) == event.start_trade_id
    assert int(trades.trade_id[last]) == event.end_trade_id
    return first, last


def _assert_measures(event: RallyEvent, trades: RallyTrades) -> None:
    first, last = _event_members(event, trades)
    prices = trades.price[first:last + 1]
    quantities = trades.quote_quantity[first:last + 1]
    buys = ~trades.is_buyer_maker[first:last + 1]
    assert event.trade_count == last - first + 1
    assert event.taker_buy_trade_count == int(np.count_nonzero(buys))
    assert math.isclose(event.volume, math.fsum(float(q) for q in quantities), rel_tol=VOLUME_REL_TOLERANCE, abs_tol=VOLUME_ABS_TOLERANCE)
    assert math.isclose(event.taker_buy_volume, math.fsum(float(q) for q in quantities[buys]), rel_tol=VOLUME_REL_TOLERANCE, abs_tol=VOLUME_ABS_TOLERANCE)
    high = event.reference_price
    drawdown = 0.0
    for price in prices:
        high = max(high, float(price))
        drawdown = max(drawdown, high - float(price))
    assert event.max_drawdown == drawdown
    assert event.return_bps == (event.end_price / event.reference_price - 1.0) * 10000.0
    origin = event.anchor_at if event.anchor_at is not None else event.reference_at
    assert event.duration_seconds == (event.end_at - origin).total_seconds()
    assert event.start_at == _at(int(trades.timestamp_us[first]))
    assert event.end_at == _at(int(trades.timestamp_us[last]))
    assert event.start_price == float(trades.price[first])
    assert event.end_price == float(trades.price[last])


def _assert_anchored(
    actual: RallyDetection,
    expected: list[tuple[int, int, int, int]],
    trades: RallyTrades,
) -> None:
    assert [(event.anchor_at, event.reference_trade_id, event.start_trade_id, event.end_trade_id) for event in actual.events] == [
        (_at(anchor), int(trades.trade_id[reference]), int(trades.trade_id[first]), int(trades.trade_id[last]))
        for anchor, reference, first, last in expected
    ]
    for event in actual.events:
        assert event.confirmation_trade_id == event.end_trade_id
        assert event.confirmed_at == event.end_at
        _assert_measures(event, trades)


def _assert_swings(
    actual: RallyDetection,
    expected: list[tuple[int, int, int]],
    trades: RallyTrades,
) -> None:
    assert [(event.start_trade_id, event.end_trade_id, event.confirmation_trade_id) for event in actual.events] == [
        (int(trades.trade_id[trough]), int(trades.trade_id[peak]), int(trades.trade_id[confirmation]))
        for trough, peak, confirmation in expected
    ]
    for event, (trough, _peak, confirmation) in zip(actual.events, expected, strict=True):
        assert event.anchor_at is None
        assert event.reference_trade_id == event.start_trade_id
        assert event.reference_at == _at(int(trades.timestamp_us[trough]))
        assert event.reference_price == float(trades.price[trough])
        assert event.confirmed_at == _at(int(trades.timestamp_us[confirmation]))
        assert event.confirmation_trade_id > event.end_trade_id
        _assert_measures(event, trades)


def test_mode_parameters_and_definition_fingerprint() -> None:
    assert ANCHORED_DEADLINE_MINUTES == 240
    assert ATR_NAME == 'ATR14-SMA15min'
    assert int(_day().trade_id[0]) == 0
    assert definition_fingerprint(PRESET) == 'ae5ca8e31e5a92964556ed189f085f79a0ae69c2def9c97ed7eceb21bfa6960c'
    assert definition_fingerprint(replace(PRESET, target=Decimal('30.000'))) == definition_fingerprint(PRESET)
    with localcontext() as context:
        context.prec = 2
        exact = replace(PRESET, target=Decimal('30.123456789012345678901234567890'))
        payload = b'rally_v1\n{"anchor_minutes":1,"mode":"first_hit","scale":"bps","target":"30.12345678901234567890123456789"}'
        assert definition_fingerprint(exact) == hashlib.sha256(payload).hexdigest()
    valid = (
        PRESET, replace(PRESET, target=Decimal('10000'), anchor_minutes=1440),
        RallyDefinition('controlled_advance', 'bps', Decimal('30'), pullback=Decimal('10000'), anchor_minutes=1),
        RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('9999')),
        RallyDefinition('swing', 'atr', Decimal('100'), reversal=Decimal('100')),
    )
    for definition in valid:
        assert len(definition_fingerprint(definition)) == 64
        _call(definition, bars=_bars())
    invalid = (
        replace(PRESET, mode='unknown'), replace(PRESET, scale='unknown'),
        replace(PRESET, pullback=Decimal('1')), replace(PRESET, reversal=Decimal('1')),
        replace(PRESET, anchor_minutes=None), replace(PRESET, anchor_minutes=True),
        replace(PRESET, anchor_minutes=0), replace(PRESET, anchor_minutes=1441),
        replace(PRESET, target=True), replace(PRESET, target=30.0),
        replace(PRESET, target=Decimal('0')), replace(PRESET, target=Decimal('-1')),
        replace(PRESET, target=Decimal('10000.1')), replace(PRESET, target=Decimal('NaN')),
        replace(PRESET, target=Decimal('Infinity')), replace(PRESET, target=Decimal('1e-9999')),
        replace(PRESET, target=Decimal('1e9999')), replace(PRESET, target=Decimal('1e-20')),
        RallyDefinition('controlled_advance', 'bps', Decimal('30'), anchor_minutes=1),
        RallyDefinition('controlled_advance', 'bps', Decimal('30'), pullback=Decimal('0'), anchor_minutes=1),
        RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('10000')),
        RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('1e-20')),
        RallyDefinition('swing', 'atr', Decimal('100.1'), reversal=Decimal('1')),
        RallyDefinition('swing', 'atr', Decimal('1'), reversal=Decimal('100.1')),
        RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('1'), anchor_minutes=1),
    )
    for definition in invalid:
        with pytest.raises((TypeError, ValueError)):
            _call(definition, bars=_bars())
    trades = _day()
    malformed = (
        replace(trades, trade_id=trades.trade_id.view(np.int64)),
        replace(trades, timestamp_us=trades.timestamp_us.view(np.uint64)),
        replace(trades, price=trades.price.reshape((-1, 1))),
        replace(trades, price=trades.price[:-1]),
        RallyTrades(
            trades.trade_id[::-1], trades.timestamp_us[::-1], trades.price[::-1],
            trades.quote_quantity[::-1], trades.is_buyer_maker[::-1],
        ),
    )
    for rows in malformed:
        with pytest.raises((TypeError, ValueError)):
            _call(trades=rows)
    for source, instrument in (('Binance', 'BTCUSDT'), ('binance_spot_trades', 'btc-usdt')):
        with pytest.raises(ValueError):
            _call(source=source, instrument=instrument)
    for start, end, known in ((DAY_START, DAY_START, DAY_END), (DAY_START, DAY_END, DAY_START), (DAY_START.replace(tzinfo=None), DAY_END, DAY_END)):
        with pytest.raises(ValueError):
            _call(start=start, end=end, known_at=known)
    for anchors in (
        np.array([_us(DAY_START) + 1], dtype=np.int64),
        np.array([_us(DAY_START), _us(DAY_START)], dtype=np.int64),
        np.array([_us(DAY_END)], dtype=np.int64),
    ):
        with pytest.raises(ValueError):
            _call(anchors=anchors)
    with pytest.raises(ValueError):
        _call(RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('20')), anchors=np.array([_us(DAY_START)], dtype=np.int64))
    with pytest.raises(ValueError):
        _call(replace(PRESET, anchor_minutes=2), coverage=None)


def test_first_hit_matches_authentic_trade_reference() -> None:
    trades = _day()
    for cadence in (1, 2, 7, 60, 1440):
        definition = replace(PRESET, anchor_minutes=cadence)
        expected = _anchored_oracle(trades, definition, DAY_START, DAY_END)
        _assert_anchored(_call(definition), expected, trades)
        for _, reference, _, _ in expected:
            assert float(trades.price[reference]) * (1.0 + float(definition.target) / 10000.0) == float(trades.price[reference]) * 1.003
    proof = json.loads((PROOF_ROOT / 'provenance.json').read_text())['parameters']['target_equality']
    definition = replace(PRESET, target=Decimal(proof['parameter']))
    anchor = proof['anchor_us']
    reference = int(np.searchsorted(trades.trade_id, int(proof['reference_trade_id'])))
    hit = int(np.searchsorted(trades.trade_id, int(proof['hit_trade_id'])))
    assert float(trades.price[reference]) * (1.0 + float(definition.target) / 10000.0) == float(trades.price[hit])
    anchors = np.array([anchor], dtype=np.int64)
    _assert_anchored(_call(definition, anchors=anchors), _anchored_oracle(trades, definition, DAY_START, DAY_END, (anchor,)), trades)
    complete = _call()
    assert datetime(2017, 8, 17, 16, 56, tzinfo=UTC) not in {event.anchor_at for event in complete.events}
    near_deadline = next(event for event in complete.events if event.anchor_at == datetime(2017, 8, 17, 17, 16, tzinfo=UTC))
    assert round(near_deadline.duration_seconds / 60.0, 1) == 238.8
    captured = _capture()
    start = CAPTURE_START + timedelta(minutes=1)
    actual = _call(trades=captured, start=start, end=CAPTURE_END, coverage=None)
    _assert_anchored(actual, _anchored_oracle(captured, PRESET, start, CAPTURE_END), captured)
    tied = next(event for event in actual.events if event.anchor_at == datetime(2026, 6, 27, 11, 43, tzinfo=UTC))
    assert tied.end_trade_id == 6453974454
    indices = np.flatnonzero(captured.timestamp_us == _us(tied.end_at))
    assert [int(captured.trade_id[i]) for i in indices] == list(range(6453974448, 6453974461))
    assert tied.trade_count == tied.end_trade_id - tied.start_trade_id + 1
    assert int(captured.trade_id[indices[-1]]) > tied.end_trade_id


def test_controlled_advance_uses_running_maximum() -> None:
    trades = _day()
    definition = RallyDefinition('controlled_advance', 'bps', Decimal('30'), pullback=Decimal('10'), anchor_minutes=1)
    _assert_anchored(_call(definition), _anchored_oracle(trades, definition, DAY_START, DAY_END), trades)
    rejected = {event.anchor_at for event in _call().events} - {event.anchor_at for event in _call(definition).events}
    assert rejected
    proof = json.loads((PROOF_ROOT / 'provenance.json').read_text())['parameters']['pullback_equality']
    anchor = proof['anchor_us']
    equal = replace(definition, pullback=Decimal(proof['parameter']))
    anchor_array = np.array([anchor], dtype=np.int64)
    actual = _call(equal, anchors=anchor_array)
    _assert_anchored(actual, _anchored_oracle(trades, equal, DAY_START, DAY_END, (anchor,)), trades)
    assert len(actual.events) == 1
    event = actual.events[0]
    assert event.end_trade_id == proof['hit_trade_id']
    assert equal.pullback is not None
    assert event.reference_price * float(equal.pullback) / 10000.0 == event.max_drawdown == proof['drawdown']
    first, last = _event_members(event, trades)
    assert float(np.max(trades.price[first:last + 1])) > event.reference_price
    lower = float(equal.pullback)
    while event.reference_price * lower / 10000.0 >= event.max_drawdown:
        lower = float(np.nextafter(lower, -np.inf))
    assert not _call(replace(equal, pullback=Decimal(str(lower))), anchors=anchor_array).events
    assert not _anchored_oracle(trades, replace(equal, pullback=Decimal(str(lower))), DAY_START, DAY_END, (anchor,))


def test_swing_confirmation_membership_and_left_censoring() -> None:
    definition = RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('20'))
    trades = _day()
    expected = _swing_oracle(trades, definition, DAY_START, DAY_END)
    actual = _call(definition)
    assert len(expected) == 134
    _assert_swings(actual, expected, trades)
    assert all(event.start_trade_id > int(trades.trade_id[0]) for event in actual.events)
    assert any(diagnostic.reason == 'initial_swing_leg' and diagnostic.status == 'left_censored' for diagnostic in actual.diagnostics)
    assert all(right.start_trade_id >= left.confirmation_trade_id for left, right in zip(actual.events, actual.events[1:]))
    assert any(event.max_drawdown < event.end_price - float(trades.price[int(np.searchsorted(trades.trade_id, event.confirmation_trade_id))]) for event in actual.events)
    first_trade_end = _at(int(trades.timestamp_us[0]) + 1)
    initial = _call(definition, known_at=first_trade_end)
    assert not initial.events
    assert any(diagnostic.status == 'left_censored' for diagnostic in initial.diagnostics)
    start = datetime(2017, 8, 17, 12, tzinfo=UTC)
    original = _call(definition, start=start)
    cut = int(np.searchsorted(trades.timestamp_us, _us(start)))
    without_preorigin = _call(definition, trades=_slice(trades, cut, len(trades.trade_id)), start=start)
    assert original == without_preorigin
    _assert_swings(original, _swing_oracle(trades, definition, start, DAY_END), trades)
    tied_confirmations = [event for event in actual.events if np.count_nonzero(trades.timestamp_us == _us(event.confirmed_at)) > 1]
    assert len(tied_confirmations) == 9
    for event in tied_confirmations:
        confirmation = int(np.searchsorted(trades.trade_id, event.confirmation_trade_id))
        peak = int(np.searchsorted(trades.trade_id, event.end_trade_id))
        assert definition.reversal is not None
        threshold = event.end_price * (1.0 - float(definition.reversal) / 10000.0)
        assert trades.price[confirmation] <= threshold
        assert all(float(price) > threshold for price in trades.price[peak + 1:confirmation])


def test_atr14_sma_uses_prior_completed_native_bars() -> None:
    start = datetime(2017, 8, 17, 8, tzinfo=UTC)
    end = datetime(2017, 8, 17, 12, tzinfo=UTC)
    bars = _bars()
    atr = _atr(_us(start))
    assert atr is not None and atr > 0.0
    selected = [bar for bar in bars if start - timedelta(minutes=225) <= bar.start < start]
    assert len(selected) == 15
    assert all(left.end == right.start for left, right in zip(selected, selected[1:]))
    ranges = [max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close)) for previous, bar in zip(selected, selected[1:])]
    assert atr == math.fsum(ranges) / 14.0
    for definition in (
        RallyDefinition('first_hit', 'atr', Decimal('0.5'), anchor_minutes=7),
        RallyDefinition('controlled_advance', 'atr', Decimal('0.5'), pullback=Decimal('0.5'), anchor_minutes=7),
    ):
        expected = _anchored_oracle(_day(), definition, start, end)
        assert expected
        actual = _call(definition, start=start, end=end, bars=bars)
        _assert_anchored(actual, expected, _day())
        causal = tuple(bar for bar in bars if bar.end <= end)
        assert actual == _call(definition, start=start, end=end, bars=causal)
    swing = RallyDefinition('swing', 'atr', Decimal('0.1'), reversal=Decimal('0.1'))
    expected_swings = _swing_oracle(_day(), swing, start, end)
    assert expected_swings
    _assert_swings(_call(swing, start=start, end=end, bars=bars), expected_swings, _day())
    early = datetime(2017, 8, 17, 6, tzinfo=UTC)
    one_anchor = np.array([_us(early)], dtype=np.int64)
    definition = RallyDefinition('first_hit', 'atr', Decimal('0.5'), anchor_minutes=1)
    empty = _call(definition, start=early, bars=bars, anchors=one_anchor)
    assert not empty.events
    assert any(diagnostic.reason == 'empty_atr_bar' for diagnostic in empty.diagnostics)
    uncovered = _call(definition, start=early, bars=bars, anchors=one_anchor, coverage=((datetime(2017, 8, 17, 4, tzinfo=UTC), DAY_END),))
    assert not uncovered.events
    assert any(diagnostic.reason == 'uncovered_atr_bar' for diagnostic in uncovered.diagnostics)
    missing = _call(swing, start=datetime(2017, 8, 17, 4, 1, tzinfo=UTC), end=start, bars=bars)
    assert any(diagnostic.status == 'unknown_context' for diagnostic in missing.diagnostics)
    assert missing.events == tuple(event for event in _call(swing, start=datetime(2017, 8, 17, 4, 1, tzinfo=UTC), bars=bars).events if event.confirmed_at < start)


def test_known_at_excludes_future_trades_and_confirmation() -> None:
    for definition in (PRESET, RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('20'))):
        full = _call(definition)
        assert full.events
        for event in (full.events[0], full.events[len(full.events) // 2], full.events[-1]):
            cutoff = event.confirmed_at
            prefix = _call(definition, known_at=cutoff)
            assert prefix.events == tuple(other for other in full.events if other.confirmed_at < cutoff)
            assert event.rally_id not in {other.rally_id for other in prefix.events}
            after = _call(definition, known_at=cutoff + timedelta(microseconds=1))
            assert event in after.events
            stop = int(np.searchsorted(_day().timestamp_us, _us(cutoff), side='left'))
            rows = _slice(_day(), 0, stop)
            assert prefix == _call(definition, trades=rows, known_at=cutoff)
    anchor = datetime(2017, 8, 17, 4, 3, tzinfo=UTC)
    requested = np.array([_us(anchor)], dtype=np.int64)
    censored = _call(anchors=requested, known_at=anchor + timedelta(seconds=1))
    assert not censored.events
    assert any(diagnostic.status == 'right_censored' for diagnostic in censored.diagnostics)
    missed = _call(replace(PRESET, target=Decimal('10000')), anchors=requested, known_at=anchor + timedelta(minutes=240))
    assert not missed.events
    assert not any(diagnostic.status == 'right_censored' for diagnostic in missed.diagnostics)
    before_first = _call(known_at=datetime(2017, 8, 17, 4, tzinfo=UTC), coverage=None)
    assert not before_first.events
    assert any(diagnostic.reason == 'missing_reference' for diagnostic in before_first.diagnostics)
    reference = next(event for event in _call(anchors=requested).events)
    gap_start = reference.reference_at + timedelta(microseconds=1)
    gap_end = anchor
    unknown_reference = _call(anchors=requested, coverage=((DAY_START, gap_start), (gap_end, DAY_END)))
    assert not unknown_reference.events
    assert any(diagnostic.reason == 'uncovered_reference' for diagnostic in unknown_reference.diagnostics)
    member_gap = _call(anchors=requested, coverage=((DAY_START, reference.start_at), (reference.start_at + timedelta(microseconds=1), DAY_END)))
    assert not member_gap.events
    assert any(diagnostic.reason == 'uncovered_trades' for diagnostic in member_gap.diagnostics)
    swing = RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('20'))
    gap_start = datetime(2017, 8, 17, 12, tzinfo=UTC)
    gap_end = gap_start + timedelta(minutes=1)
    gapped = _call(swing, coverage=((DAY_START, gap_start), (gap_end, DAY_END)))
    resumed = _call(swing, start=gap_end)
    assert [(event.start_trade_id, event.end_trade_id, event.confirmation_trade_id) for event in gapped.events if event.start_at >= gap_end] == [(event.start_trade_id, event.end_trade_id, event.confirmation_trade_id) for event in resumed.events]
    assert any(diagnostic.reason == 'uncovered_trades' for diagnostic in gapped.diagnostics)


def test_identity_excludes_grid_filters_and_source_revision() -> None:
    forbidden = {'grid', 'filters', 'source_revision', 'build_id', 'selection', 'result_id'}
    assert forbidden.isdisjoint(inspect.signature(detect_rallies).parameters)
    fingerprint = definition_fingerprint(PRESET)
    day = _call()
    assert all(event.definition_fingerprint == fingerprint and event.definition_version == 'rally_v1' for event in day.events)
    assert all(event.rally_id == f'binance:spot:BTCUSDT:r30v1:t{_us(event.anchor_at) // 1_000_000}' for event in day.events if event.anchor_at is not None)
    general = _call(replace(PRESET, anchor_minutes=2))
    assert all(event.rally_id == f'binance_spot_trades:BTCUSDT:rally_v1:{event.definition_fingerprint}:a{_us(event.anchor_at)}' for event in general.events if event.anchor_at is not None)
    other_source = _call(source='other_spot')
    assert other_source.events
    assert all(event.rally_id.startswith(f'other_spot:BTCUSDT:rally_v1:{fingerprint}:a') for event in other_source.events)
    assert len({event.rally_id for event in day.events}) == len(day.events)
    swing = RallyDefinition('swing', 'bps', Decimal('30'), reversal=Decimal('20'))
    start = datetime(2017, 8, 17, 8, tzinfo=UTC)
    full = _call(swing, start=start)
    assert all(event.rally_id == f'binance_spot_trades:BTCUSDT:rally_v1:{definition_fingerprint(swing)}:o{_us(start)}:t{event.start_trade_id}' for event in full.events)
    cutoff = full.events[len(full.events) // 2].confirmed_at + timedelta(microseconds=1)
    prefix = _call(swing, start=start, known_at=cutoff)
    assert prefix.events == tuple(event for event in full.events if event.confirmed_at < cutoff)
    shifted = _call(swing, start=start + timedelta(microseconds=1))
    assert {event.rally_id for event in full.events}.isdisjoint(event.rally_id for event in shifted.events)
    offset = timezone(timedelta(hours=2))
    assert day == _call(start=DAY_START.astimezone(offset), end=DAY_END.astimezone(offset))


def test_legacy_r30v1_outputs_match_baseline(rally_data: None, tmp_path: Path) -> None:
    expected = json.loads((CAPTURE_ROOT / 'expected.json').read_text())
    for suffix, start, end, key, count in (
        ('day', DAY_START, DAY_END, 'BTCUSDT-trades-2017-08-17', 989),
        ('capture', CAPTURE_START + timedelta(minutes=1), CAPTURE_END, 'BTCUSDT-spot-trades-2026-06-27T1138-1155', 4),
    ):
        paths = export_binance_rallies(output_dir=tmp_path / suffix, start=start, end=end)
        ipc = cast(_ArrowIPC, importlib.import_module('pyarrow.ipc'))
        table = ipc.open_file(str(paths[0])).read_all()
        frame = pl.read_ipc(paths[0])
        labels = frame.select(pl.col('anchor_time').dt.epoch('s'), 'reference_trade_id', 'hit_trade_id', pl.col('time_to_hit').dt.total_microseconds()).rows()
        assert len(labels) == count
        assert [list(row) for row in labels] == expected[key]['rallies']
        assert frame['rally_id'].to_list() == [f'binance:spot:BTCUSDT:r30v1:t{row[0]}' for row in labels]
        metadata = table.schema.metadata
        assert metadata is not None and b'origo.rallies' in metadata
        assert json.loads(metadata[b'origo.rallies'])['definition']['version'] == 'r30v1'


def test_fixture_provenance_has_only_unchanged_provider_records() -> None:
    proof = json.loads((PROOF_ROOT / 'provenance.json').read_text())
    archive_provenance = json.loads((DAY_ROOT / 'BTCUSDT-trades-2017-08-17.provenance.json').read_text())
    assert hashlib.sha256(DAY_FILE.read_bytes()).hexdigest() == proof['archive']['capture_sha256'] == archive_provenance['selected_sha256']
    archive = DAY_ROOT / 'BTCUSDT-trades-2017-08-17.zip'
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == proof['archive']['provider_object_sha256'] == archive_provenance['zip_sha256']
    with zipfile.ZipFile(archive) as handle:
        assert handle.read(archive_provenance['csv_member']) == DAY_FILE.read_bytes()
    assert archive_provenance['official_row_count'] == archive_provenance['selected_row_stop'] == len(_day().trade_id) == 3427
    assert archive_provenance['selected_row_start'] == 0
    assert int(_day().trade_id[0]) == proof['archive']['first_native_id'] == 0
    assert int(_day().trade_id[-1]) == proof['archive']['last_native_id'] == 3426
    capture_provenance = json.loads((CAPTURE_ROOT / 'provenance.json').read_text())['trades']
    capture = CAPTURE_ROOT / capture_provenance['file']
    assert hashlib.sha256(capture.read_bytes()).hexdigest() == proof['production_excerpt']['capture_sha256'] == capture_provenance['sha256']
    assert len(_capture().trade_id) == capture_provenance['rows'] == proof['production_excerpt']['row_count']
    assert proof['archive']['coverage_basis'] == 'Complete official checksum-verified daily archive; every CSV row retained.'
    assert proof['production_excerpt']['coverage'] is None
    assert proof['production_excerpt']['gap_policy'] == 'Legacy unverified reads; SQL excerpts do not establish verified source coverage.'
    assert proof['volume_tolerance'] == {'relative': VOLUME_REL_TOLERANCE, 'absolute': VOLUME_ABS_TOLERANCE}
    assert proof['archive']['native_timestamp_unit'] == 'milliseconds'
    assert proof['production_excerpt']['native_timestamp_unit'] == 'microseconds'
    assert proof['parameter_search_changes_market_records'] is False
