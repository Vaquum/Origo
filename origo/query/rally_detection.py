"""Causal rally detection over native trade columns and declared source coverage."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, cast

import numpy as np
from numpy.typing import NDArray

Mode = Literal['first_hit', 'controlled_advance', 'swing']
Scale = Literal['bps', 'atr']

DEFINITION_VERSION = 'rally_v1'
ANCHORED_DEADLINE_MINUTES = 240
ATR_NAME = 'ATR14-SMA15min'

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)
_MINUTE_US = 60_000_000
_BAR_US = 15 * _MINUTE_US
_DEADLINE_US = ANCHORED_DEADLINE_MINUTES * _MINUTE_US
_LOOKBACK_US = 24 * 60 * _MINUTE_US
_SOURCE = re.compile(r'[a-z][a-z0-9_]*', re.ASCII)
_INSTRUMENT = re.compile(r'[A-Z0-9]+', re.ASCII)


@dataclass(frozen=True)
class RallyDefinition:
    mode: Mode
    scale: Scale
    target: Decimal
    pullback: Decimal | None = None
    reversal: Decimal | None = None
    anchor_minutes: int | None = None


@dataclass(frozen=True)
class RallyTrades:
    trade_id: NDArray[np.uint64]
    timestamp_us: NDArray[np.int64]
    price: NDArray[np.float64]
    quote_quantity: NDArray[np.float64]
    is_buyer_maker: NDArray[np.bool_]


@dataclass(frozen=True)
class RallyBar:
    start: datetime
    end: datetime
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class RallyEvent:
    rally_id: str
    definition_version: str
    definition_fingerprint: str
    anchor_at: datetime | None
    reference_trade_id: int
    reference_at: datetime
    reference_price: float
    start_trade_id: int
    start_at: datetime
    start_price: float
    end_trade_id: int
    end_at: datetime
    end_price: float
    confirmation_trade_id: int
    confirmed_at: datetime
    return_bps: float
    duration_seconds: float
    max_drawdown: float
    volume: float
    trade_count: int
    taker_buy_volume: float
    taker_buy_trade_count: int


@dataclass(frozen=True)
class RallyDiagnostic:
    status: Literal['left_censored', 'right_censored', 'unknown_context']
    at: datetime
    reason: str


@dataclass(frozen=True)
class RallyDetection:
    events: tuple[RallyEvent, ...]
    diagnostics: tuple[RallyDiagnostic, ...]


@dataclass(frozen=True)
class _Parameters:
    target: float
    pullback: float | None
    reversal: float | None
    fields: dict[str, str | int]


@dataclass(frozen=True)
class _Coverage:
    intervals: tuple[tuple[int, int], ...] | None

    def first_gap(self, start: int, end: int) -> int | None:
        if self.intervals is None or start >= end:
            return None
        cursor = start
        for low, high in self.intervals:
            if high > cursor:
                if low > cursor:
                    return cursor
                cursor = high
                if cursor >= end:
                    return None
        return cursor


@dataclass(frozen=True)
class _ATR:
    bars: dict[int, RallyBar]
    coverage: _Coverage

    def freeze(self, at: int) -> tuple[float | None, str | None]:
        end = at // _BAR_US * _BAR_US
        first = end - 15 * _BAR_US
        required: list[RallyBar] = []
        for start in range(first, end, _BAR_US):
            if self.coverage.first_gap(start, start + _BAR_US) is not None:
                return None, 'uncovered_atr_bar'
            bar = self.bars.get(start)
            if bar is None:
                return None, 'empty_atr_bar'
            required.append(bar)
        previous_close = required[0].close
        ranges: list[float] = []
        for bar in required[1:]:
            ranges.append(max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            ))
            previous_close = bar.close
        value = math.fsum(ranges) / 14.0
        _positive(value, 'frozen ATR')
        return value, None


@dataclass(frozen=True)
class _Context:
    trades: RallyTrades
    definition: RallyDefinition
    parameters: _Parameters
    source: str
    instrument: str
    origin: int
    edge: int
    fingerprint: str
    coverage: _Coverage
    atr: _ATR

    def freeze(self, index: int) -> tuple[float | None, str | None]:
        if self.definition.scale == 'atr':
            return self.atr.freeze(int(self.trades.timestamp_us[index]))
        return None, None


def _us(value: datetime, name: str) -> int:
    if not isinstance(cast(object, value), datetime) or value.utcoffset() is None:
        raise ValueError(f'{name} must be a timezone-aware datetime.')
    return (value.astimezone(UTC) - _EPOCH) // _MICROSECOND


def _at(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def _positive(value: float, name: str) -> float:
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(f'{name} must remain finite and positive in Float64 arithmetic.')
    return value


def _number(value: Decimal, name: str, scale: Scale) -> float:
    if not isinstance(cast(object, value), Decimal) or not value.is_finite():
        raise ValueError(f'{name} must be a finite Decimal.')
    maximum = Decimal(10000 if scale == 'bps' else 100)
    if value <= 0 or value > maximum or (scale == 'bps' and name == 'reversal' and value == maximum):
        raise ValueError(f'{name} is outside the {scale} parameter domain.')
    numeric = _positive(float(value), name)
    if scale == 'bps' and (
        1.0 + numeric / 10000.0 == 1.0 or 1.0 - numeric / 10000.0 == 1.0
    ):
        raise ValueError(f'{name} is too small to change a Float64 bps multiplier.')
    return numeric


def _decimal_string(value: Decimal) -> str:
    text = format(value, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def _parameters(definition: RallyDefinition) -> _Parameters:
    if not isinstance(cast(object, definition), RallyDefinition):
        raise ValueError('definition must be a RallyDefinition.')
    if definition.mode not in ('first_hit', 'controlled_advance', 'swing'):
        raise ValueError(f'Unknown rally mode {definition.mode!r}.')
    if definition.scale not in ('bps', 'atr'):
        raise ValueError(f'Unknown rally scale {definition.scale!r}.')
    target = _number(definition.target, 'target', definition.scale)
    fields: dict[str, str | int] = {
        'mode': definition.mode,
        'scale': definition.scale,
        'target': _decimal_string(definition.target),
    }
    pullback: float | None = None
    reversal: float | None = None
    if definition.mode == 'swing':
        if definition.anchor_minutes is not None or definition.pullback is not None:
            raise ValueError('swing cannot specify anchor_minutes or pullback.')
        if definition.reversal is None:
            raise ValueError('swing requires reversal.')
        reversal = _number(definition.reversal, 'reversal', definition.scale)
        fields['reversal'] = _decimal_string(definition.reversal)
    else:
        anchor = definition.anchor_minutes
        if anchor is None or isinstance(anchor, bool) or not isinstance(cast(object, anchor), int):
            raise ValueError('Anchored modes require an integer anchor_minutes.')
        if not 1 <= anchor <= 1440:
            raise ValueError('anchor_minutes must be in 1..1440.')
        if definition.reversal is not None:
            raise ValueError('Anchored modes cannot specify reversal.')
        fields['anchor_minutes'] = anchor
        if definition.mode == 'controlled_advance':
            if definition.pullback is None:
                raise ValueError('controlled_advance requires pullback.')
            pullback = _number(definition.pullback, 'pullback', definition.scale)
            fields['pullback'] = _decimal_string(definition.pullback)
        elif definition.pullback is not None:
            raise ValueError('first_hit cannot specify pullback.')
    return _Parameters(target, pullback, reversal, fields)


def _fingerprint(fields: dict[str, str | int]) -> str:
    encoded = json.dumps(fields, sort_keys=True, separators=(',', ':'), ensure_ascii=True)
    return hashlib.sha256(b'rally_v1\n' + encoded.encode('utf-8')).hexdigest()


def definition_fingerprint(definition: RallyDefinition) -> str:
    return _fingerprint(_parameters(definition).fields)


def _column(column: NDArray[np.generic], dtype: object, name: str) -> None:
    if not isinstance(cast(object, column), np.ndarray) or column.ndim != 1 or column.dtype != dtype:
        raise ValueError(f'{name} must be a one-dimensional NumPy column of dtype {dtype}.')


def _validate_trades(trades: RallyTrades) -> None:
    if not isinstance(cast(object, trades), RallyTrades):
        raise ValueError('trades must be RallyTrades.')
    _column(trades.trade_id, np.uint64, 'trade_id')
    _column(trades.timestamp_us, np.int64, 'timestamp_us')
    _column(trades.price, np.float64, 'price')
    _column(trades.quote_quantity, np.float64, 'quote_quantity')
    _column(trades.is_buyer_maker, np.bool_, 'is_buyer_maker')
    count = len(trades.trade_id)
    if any(len(column) != count for column in (
        trades.timestamp_us, trades.price, trades.quote_quantity, trades.is_buyer_maker,
    )):
        raise ValueError('All five trade columns must have equal length.')
    if np.any(trades.trade_id[1:] <= trades.trade_id[:-1]):
        raise ValueError('Native trade IDs must be strictly increasing.')
    if np.any(trades.timestamp_us[1:] < trades.timestamp_us[:-1]):
        raise ValueError('Native trade timestamps must be nondecreasing.')
    if np.any(~np.isfinite(trades.price)) or np.any(trades.price <= 0.0):
        raise ValueError('Trade prices must be finite and positive.')
    if np.any(~np.isfinite(trades.quote_quantity)) or np.any(trades.quote_quantity < 0.0):
        raise ValueError('Trade quote quantities must be finite and nonnegative.')


def _legacy(parameters: _Parameters, source: str, instrument: str) -> bool:
    return parameters.fields == {
        'mode': 'first_hit', 'scale': 'bps', 'target': '30', 'anchor_minutes': 1,
    } and source == 'binance_spot_trades' and instrument == 'BTCUSDT'


def _coverage(
    coverage: Sequence[tuple[datetime, datetime]] | None, allow_unverified: bool,
) -> _Coverage:
    if coverage is None:
        if not allow_unverified:
            raise ValueError('Only the legacy spot first-hit preset permits coverage=None.')
        return _Coverage(None)
    intervals: list[tuple[int, int]] = []
    for start, end in coverage:
        low, high = _us(start, 'coverage start'), _us(end, 'coverage end')
        if low >= high:
            raise ValueError('Coverage intervals must have start < end.')
        intervals.append((low, high))
    merged: list[tuple[int, int]] = []
    for low, high in sorted(intervals):
        if merged and low <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(high, merged[-1][1]))
        else:
            merged.append((low, high))
    return _Coverage(tuple(merged))


def _bars(bars: Sequence[RallyBar]) -> dict[int, RallyBar]:
    indexed: dict[int, RallyBar] = {}
    for bar in bars:
        if not isinstance(cast(object, bar), RallyBar):
            raise ValueError('bars must contain RallyBar values.')
        start, end = _us(bar.start, 'bar start'), _us(bar.end, 'bar end')
        if start % _BAR_US or end != start + _BAR_US or start in indexed:
            raise ValueError('ATR bars must be unique native UTC 15-minute intervals.')
        for name, value in (('high', bar.high), ('low', bar.low), ('close', bar.close)):
            if isinstance(value, bool) or not isinstance(cast(object, value), (int, float)):
                raise ValueError(f'Bar {name} must be a finite positive Float64 value.')
            _positive(value, f'bar {name}')
        if not bar.low <= bar.close <= bar.high:
            raise ValueError('Bar close must lie within low and high.')
        indexed[start] = bar
    return indexed


def _anchors(
    anchors_us: NDArray[np.int64] | None,
    definition: RallyDefinition,
    origin: int,
    end: int,
    edge: int,
) -> NDArray[np.int64]:
    if definition.mode == 'swing':
        if anchors_us is not None:
            raise ValueError('swing cannot specify anchors_us.')
        return np.empty(0, dtype=np.int64)
    cadence = cast(int, definition.anchor_minutes) * _MINUTE_US
    if anchors_us is not None:
        _column(anchors_us, np.int64, 'anchors_us')
        if np.any(anchors_us[1:] <= anchors_us[:-1]):
            raise ValueError('anchors_us must be strictly increasing.')
        if np.any(anchors_us % cadence != 0):
            raise ValueError('anchors_us must align to the UTC anchor cadence.')
        if np.any(anchors_us < origin) or np.any(anchors_us >= end):
            raise ValueError('anchors_us must lie inside the analysis interval.')
        return anchors_us[:int(np.searchsorted(anchors_us, edge))]
    first = -(-origin // cadence) * cadence
    return np.arange(first, edge, cadence, dtype=np.int64)


def _distance(reference: float, atr: float | None, numeric: float, scale: Scale) -> float:
    if scale == 'bps':
        return _positive(reference * numeric / 10000.0, 'bps distance')
    if atr is None:
        raise ValueError('ATR distance requires complete frozen ATR context.')
    return _positive(atr * numeric, 'ATR distance')


def _target(reference: float, atr: float | None, parameters: _Parameters, scale: Scale) -> float:
    if scale == 'bps':
        return _positive(reference * (1.0 + parameters.target / 10000.0), 'target threshold')
    return _positive(reference + _distance(reference, atr, parameters.target, scale), 'target threshold')


def _reversed(peak: float, current: float, atr: float | None, context: _Context) -> bool:
    reversal = context.parameters.reversal
    if reversal is None:
        raise ValueError('Swing reversal requires reversal.')
    if context.definition.scale == 'bps':
        return current <= _positive(peak * (1.0 - reversal / 10000.0), 'reversal threshold')
    return peak - current >= _distance(peak, atr, reversal, 'atr')


def _first_true(values: NDArray[np.bool_]) -> int | None:
    if not values.size:
        return None
    index = int(np.argmax(values))
    return index if bool(values[index]) else None


def _event(
    context: _Context, reference: int, first: int, last: int, confirmation: int,
    anchor: int | None,
) -> RallyEvent:
    trades = context.trades
    reference_price, end_price = float(trades.price[reference]), float(trades.price[last])
    prices = trades.price[first:last + 1]
    peaks = np.maximum.accumulate(prices)
    if anchor is not None:
        np.maximum(peaks, reference_price, out=peaks)
    max_drawdown = float(np.max(peaks - prices))
    quotes, makers = trades.quote_quantity[first:last + 1], trades.is_buyer_maker[first:last + 1]
    volume = math.fsum(float(value) for value in quotes)
    buy_volume = math.fsum(
        float(value) for value, maker in zip(quotes, makers, strict=True) if not bool(maker)
    )
    buy_count = len(makers) - int(np.count_nonzero(makers))
    prefix = f'{context.source}:{context.instrument}:rally_v1:{context.fingerprint}'
    if anchor is None:
        rally_id = f'{prefix}:o{context.origin}:t{int(trades.trade_id[reference])}'
    elif _legacy(context.parameters, context.source, context.instrument):
        rally_id = f'binance:spot:BTCUSDT:r30v1:t{anchor // 1_000_000}'
    else:
        rally_id = f'{prefix}:a{anchor}'
    duration_start = anchor if anchor is not None else int(trades.timestamp_us[reference])
    return RallyEvent(
        rally_id=rally_id,
        definition_version=DEFINITION_VERSION,
        definition_fingerprint=context.fingerprint,
        anchor_at=None if anchor is None else _at(anchor),
        reference_trade_id=int(trades.trade_id[reference]),
        reference_at=_at(int(trades.timestamp_us[reference])),
        reference_price=reference_price,
        start_trade_id=int(trades.trade_id[first]),
        start_at=_at(int(trades.timestamp_us[first])),
        start_price=float(trades.price[first]),
        end_trade_id=int(trades.trade_id[last]),
        end_at=_at(int(trades.timestamp_us[last])),
        end_price=end_price,
        confirmation_trade_id=int(trades.trade_id[confirmation]),
        confirmed_at=_at(int(trades.timestamp_us[confirmation])),
        return_bps=(end_price / reference_price - 1.0) * 10000.0,
        duration_seconds=(int(trades.timestamp_us[last]) - duration_start) / 1_000_000.0,
        max_drawdown=max_drawdown,
        volume=volume,
        trade_count=last - first + 1,
        taker_buy_volume=buy_volume,
        taker_buy_trade_count=buy_count,
    )


def _diagnostic(
    status: Literal['left_censored', 'right_censored', 'unknown_context'],
    at: int, reason: str,
) -> RallyDiagnostic:
    return RallyDiagnostic(status, _at(at), reason)


def _anchored(context: _Context, anchors: NDArray[np.int64]) -> RallyDetection:
    events: list[RallyEvent] = []
    diagnostics: list[RallyDiagnostic] = []
    trades = context.trades
    for value in anchors:
        anchor = int(value)
        first = int(np.searchsorted(trades.timestamp_us, anchor))
        reference = first - 1
        if reference < 0 or int(trades.timestamp_us[reference]) < anchor - _LOOKBACK_US:
            reason = (
                'uncovered_reference'
                if context.coverage.first_gap(anchor - _LOOKBACK_US, anchor) is not None
                else 'missing_reference'
            )
            diagnostics.append(_diagnostic('unknown_context', anchor, reason))
            continue
        if context.coverage.first_gap(int(trades.timestamp_us[reference]), anchor) is not None:
            diagnostics.append(_diagnostic('unknown_context', anchor, 'uncovered_reference'))
            continue
        atr, reason = context.atr.freeze(anchor) if context.definition.scale == 'atr' else (None, None)
        if reason is not None:
            diagnostics.append(_diagnostic('unknown_context', anchor, reason))
            continue
        reference_price = float(trades.price[reference])
        threshold = _target(reference_price, atr, context.parameters, context.definition.scale)
        deadline = anchor + _DEADLINE_US
        observed_end = min(deadline, context.edge)
        gap = context.coverage.first_gap(anchor, observed_end)
        available_end = observed_end if gap is None else gap
        stop = int(np.searchsorted(trades.timestamp_us, available_end))
        relative_hit = _first_true(trades.price[first:stop] >= threshold)
        hit = None if relative_hit is None else first + relative_hit
        rejected = False
        if context.definition.mode == 'controlled_advance' and stop > first:
            pullback = context.parameters.pullback
            if pullback is None:
                raise ValueError('controlled_advance requires pullback.')
            distance = _distance(reference_price, atr, pullback, context.definition.scale)
            through = stop if hit is None else hit + 1
            prices = trades.price[first:through]
            peaks = np.maximum.accumulate(prices)
            np.maximum(peaks, reference_price, out=peaks)
            rejected = bool(np.any(peaks - prices > distance))
        if hit is not None and not rejected:
            events.append(_event(context, reference, first, hit, hit, anchor))
        elif not rejected:
            if gap is not None:
                diagnostics.append(_diagnostic('unknown_context', gap, 'uncovered_trades'))
            elif observed_end < deadline:
                diagnostics.append(_diagnostic('right_censored', anchor, 'unobserved_deadline'))
    return RallyDetection(tuple(events), tuple(diagnostics))


def _swing_segment(
    context: _Context, low: int, high: int, report_edge: bool,
    events: list[RallyEvent], diagnostics: list[RallyDiagnostic],
) -> None:
    trades = context.trades
    first = int(np.searchsorted(trades.timestamp_us, low))
    stop = int(np.searchsorted(trades.timestamp_us, high))
    if first == stop:
        if report_edge:
            diagnostics.append(_diagnostic('left_censored', low, 'unseeded_swing'))
        return
    state: Literal['unseeded', 'down', 'up'] = 'unseeded'
    extreme = first
    peak = first
    atr, reason = context.freeze(first)
    if reason is not None:
        diagnostics.append(_diagnostic('unknown_context', int(trades.timestamp_us[first]), reason))
    for index in range(first + 1, stop):
        price = float(trades.price[index])
        extreme_price = float(trades.price[extreme])
        if state == 'unseeded':
            if price > extreme_price:
                extreme = index
                atr, reason = context.freeze(index)
                if reason is not None:
                    diagnostics.append(_diagnostic('unknown_context', int(trades.timestamp_us[index]), reason))
            elif (context.definition.scale == 'bps' or atr is not None) and _reversed(
                extreme_price, price, atr, context,
            ):
                diagnostics.append(_diagnostic(
                    'left_censored', int(trades.timestamp_us[extreme]), 'initial_swing_leg',
                ))
                state, extreme = 'down', index
                atr, reason = context.freeze(index)
                if reason is not None:
                    diagnostics.append(_diagnostic('unknown_context', int(trades.timestamp_us[index]), reason))
        elif state == 'down':
            if price < extreme_price:
                extreme = index
                atr, reason = context.freeze(index)
                if reason is not None:
                    diagnostics.append(_diagnostic('unknown_context', int(trades.timestamp_us[index]), reason))
            elif context.definition.scale == 'bps' or atr is not None:
                threshold = _target(extreme_price, atr, context.parameters, context.definition.scale)
                if price >= threshold:
                    state, peak = 'up', index
        elif price > float(trades.price[peak]):
            peak = index
        elif _reversed(float(trades.price[peak]), price, atr, context):
            events.append(_event(context, extreme, extreme, peak, index, None))
            state, extreme = 'down', index
            atr, reason = context.freeze(index)
            if reason is not None:
                diagnostics.append(_diagnostic('unknown_context', int(trades.timestamp_us[index]), reason))
    if report_edge and (context.definition.scale == 'bps' or atr is not None):
        if state == 'unseeded':
            diagnostics.append(_diagnostic('left_censored', int(trades.timestamp_us[extreme]), 'unseeded_swing'))
        else:
            diagnostics.append(_diagnostic('right_censored', int(trades.timestamp_us[extreme]), 'unfinished_swing'))


def _swing(context: _Context) -> RallyDetection:
    intervals = context.coverage.intervals
    if intervals is None:
        raise ValueError('Swing requires verified source coverage.')
    events: list[RallyEvent] = []
    diagnostics: list[RallyDiagnostic] = []
    cursor = context.origin
    for start, end in intervals:
        low, high = max(start, context.origin), min(end, context.edge)
        if low < high:
            if low > cursor:
                diagnostics.append(_diagnostic('unknown_context', cursor, 'uncovered_trades'))
            _swing_segment(context, low, high, high == context.edge, events, diagnostics)
            cursor = high
    if cursor < context.edge:
        diagnostics.append(_diagnostic('unknown_context', cursor, 'uncovered_trades'))
    return RallyDetection(tuple(events), tuple(diagnostics))


def detect_rallies(
    *,
    trades: RallyTrades,
    definition: RallyDefinition,
    source: str,
    instrument: str,
    analysis_start: datetime,
    analysis_end: datetime,
    known_at: datetime,
    coverage: Sequence[tuple[datetime, datetime]] | None,
    bars: Sequence[RallyBar] = (),
    anchors_us: NDArray[np.int64] | None = None,
) -> RallyDetection:
    parameters = _parameters(definition)
    _validate_trades(trades)
    if not isinstance(cast(object, source), str) or _SOURCE.fullmatch(source) is None:
        raise ValueError('source must match [a-z][a-z0-9_]*.')
    if not isinstance(cast(object, instrument), str) or _INSTRUMENT.fullmatch(instrument) is None:
        raise ValueError('instrument must match [A-Z0-9]+.')
    origin = _us(analysis_start, 'analysis_start')
    end = _us(analysis_end, 'analysis_end')
    edge = min(end, _us(known_at, 'known_at'))
    if origin >= edge:
        raise ValueError('analysis_start must be before min(analysis_end, known_at).')
    declared = _coverage(coverage, _legacy(parameters, source, instrument))
    anchors = _anchors(anchors_us, definition, origin, end, edge)
    context = _Context(
        trades, definition, parameters, source, instrument, origin, edge,
        _fingerprint(parameters.fields), declared, _ATR(_bars(bars), declared),
    )
    return _swing(context) if definition.mode == 'swing' else _anchored(context, anchors)
