"""Market state cube queries: one pinned state, rounded bounds, sparse Arrow files (PRD-0022).

A request names a time range, a price range and independent dyadic resolutions, all optional.
Supplied bounds round to the nearest edge of the 56.25 s x 125 USDT base lattice, midpoints
upward; omitted time bounds cover the cube's history and omitted price bounds the occupied
rows inside the selected time window. The base cells inside that rectangle are grouped into
``tR = 56.25 x 2^n`` second columns and ``pR = 125 x 2^m`` USDT rows. ``cells.arrow`` holds
every occupied cell and ``summary.arrow`` the grid totals, both POCs and the pinned state.

A request pins the accepted partitions once, reads only the pinned ``(partition_key,
revision, build_id)`` identities, and afterwards proves that no cleanup reclaimed any of
them: under the source's shared ``heavy`` fence it looks the builds up in the cleanup log. The
fence is never held during a read, so maintenance never waits for a query. The data cutoff
is the end of contiguous cube coverage from 2021-01-01; nothing after the first day or
minute without cube evidence is read, so an uncovered interval can never appear as zero
trades.

Cell volumes are ClickHouse ``sumKahan`` sums of the selected base contributions. Row sums,
grid totals and POCs equal ``math.fsum`` over the emitted cells, so a consumer reproduces them
exactly from ``cells.arrow``; they are accumulated exactly while the cells stream, so memory
does not grow with the result. Counts widen to UInt64 before summing.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast
from uuid import UUID

import numpy as np
import numpy.typing as npt

from origo.sources.contracts import Partition, SourceError, StateRecord
from origo.sources.hashing import state_token
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.profiles.market_state import BASE_PRICE_USDT, BASE_TIME_US, CUBE_START
from origo.sources.storage import SourceStore

SOURCE: Final = 'binance_spot_trades'
METADATA_KEY: Final = 'origo.market_state'
SCHEMA_VERSION: Final = 1
FIELDS: Final = ('t1', 't2', 'p1', 'p2', 'tR', 'pR')
CELLS_FILE: Final = 'cells.arrow'
SUMMARY_FILE: Final = 'summary.arrow'
BATCH_ROWS: Final = 65_536
MAX_BODY_BYTES: Final = 65_536
# Measured on 2026-09-25: the full-history base cells statement needs 0.7-1.3 GiB and takes
# 1.3 s on 4 threads (1.9 s on 2). Two queries use 8 of the host's 48 hardware threads. No
# statement spills to disk: one that outgrows its memory fails, so no temporary data can take
# the disk that result admission keeps for source ingestion.
QUERY_SETTINGS: Final[Mapping[str, object]] = {
    'max_threads': 4,
    'max_memory_usage': 4 * 1024**3,
    'max_execution_time': 60,
    'timeout_overflow_mode': 'throw',
    'max_block_size': BATCH_ROWS,
}
RECEIVE_TIMEOUT_SECONDS: Final = 90
FENCE_WAIT_SECONDS: Final = 60.0

log = logging.getLogger('origo.query.market_state')

_T0_US: Final = int(CUBE_START.timestamp()) * 1_000_000
_TIME_BASE: Final = Fraction(BASE_TIME_US, 1_000_000)
_PRICE_BASE: Final = Fraction(BASE_PRICE_USDT)
# Every returned price bound is an exact Float64: edge x 125 stays at or below 2^53, so a
# price must round to an edge no larger than this, i.e. stay below ``_PRICE_LIMIT``.
_MAX_PRICE_EDGE: Final = 2**53 // BASE_PRICE_USDT
_PRICE_LIMIT: Final = Decimal((_MAX_PRICE_EDGE + 1) * BASE_PRICE_USDT) - Decimal(BASE_PRICE_USDT) / 2
_MAX_TEXT: Final = 64
_FIRST_MIDPOINT: Final = Decimal(BASE_PRICE_USDT) / 2
_FLOAT64_MAX: Final = Fraction(sys.float_info.max)
_CUBE_COMPONENTS: Final = {False: 'market_state', True: 'market_state_latest'}
_PIN_STRUCTURE: Final = 'partition_key String, revision String, build_id UUID'
_CELL_COLUMNS: Final = (
    'time_index, price_index, volume, trade_count, taker_buy_volume, taker_buy_trade_count'
)
_MANTISSA: Final = np.uint64((1 << 52) - 1)
_HIDDEN_BIT: Final = np.uint64(1 << 52)
_LOW_BITS: Final = np.uint64((1 << 26) - 1)
_EXPONENTS: Final = 2048
# A Float64 is ``mantissa * 2**(exponent - 1075)``, with exponent 1 for subnormals.
_SCALE: Final = 1 << 1075


class _Array(Protocol):
    def cast(self, target_type: object) -> _Array: ...
    def to_numpy(self, zero_copy_only: bool = True) -> object: ...


class _Batch(Protocol):
    @property
    def num_rows(self) -> int: ...
    def column(self, i: int) -> _Array: ...


class _Field(Protocol):
    @property
    def type(self) -> object: ...


class _Schema(Protocol):
    def field(self, i: int) -> _Field: ...
    def with_metadata(self, metadata: Mapping[str, str]) -> _Schema: ...


class _BatchClass(Protocol):
    def from_arrays(self, arrays: Sequence[_Array], *, schema: _Schema) -> _Batch: ...
    def from_pylist(self, mapping: Sequence[Mapping[str, object]], *, schema: _Schema) -> _Batch: ...


class _PyArrow(Protocol):
    RecordBatch: _BatchClass

    def field(self, name: str, type: object, nullable: bool = True) -> object: ...
    def schema(self, fields: Sequence[object]) -> _Schema: ...
    def string(self) -> object: ...
    def uint64(self) -> object: ...
    def float64(self) -> object: ...
    def bool_(self) -> object: ...
    def timestamp(self, unit: str, tz: str) -> object: ...


class _Writer(Protocol):
    def write_batch(self, batch: _Batch) -> None: ...


class _IPC(Protocol):
    def open_stream(self, source: object) -> Iterator[_Batch]: ...
    def new_file(self, sink: str, schema: _Schema) -> AbstractContextManager[_Writer]: ...


class _External(Protocol):
    def add_file(self, *, file_name: str, data: bytes, fmt: str, structure: str) -> None: ...


class _HttpClient(Protocol):
    def raw_query(
        self,
        query: str,
        *,
        settings: Mapping[str, object],
        fmt: str,
        external_data: _External | None,
    ) -> bytes: ...

    def raw_stream(
        self,
        query: str,
        *,
        settings: Mapping[str, object],
        fmt: str,
        external_data: _External | None,
    ) -> AbstractContextManager[object]: ...

    def close(self) -> None: ...


pa = cast(_PyArrow, import_module('pyarrow'))
ipc = cast(_IPC, import_module('pyarrow.ipc'))

_UINT64, _FLOAT64, _BOOL = pa.uint64(), pa.float64(), pa.bool_()
_MICROS = pa.timestamp('us', 'UTC')
CELLS_SCHEMA: Final = pa.schema([
    pa.field('time_index', _UINT64, nullable=False),
    pa.field('price_index', _UINT64, nullable=False),
    pa.field('volume', _FLOAT64, nullable=False),
    pa.field('trade_count', _UINT64, nullable=False),
    pa.field('taker_buy_volume', _FLOAT64, nullable=False),
    pa.field('taker_buy_trade_count', _UINT64, nullable=False),
])
SUMMARY_SCHEMA: Final = pa.schema([
    pa.field('result_id', pa.string(), nullable=False),
    pa.field('t1', _MICROS, nullable=False),
    pa.field('t2', _MICROS, nullable=False),
    pa.field('p1', _FLOAT64),
    pa.field('p2', _FLOAT64),
    pa.field('tR', _FLOAT64, nullable=False),
    pa.field('pR', _FLOAT64, nullable=False),
    pa.field('first_column_partial', _BOOL, nullable=False),
    pa.field('last_column_partial', _BOOL, nullable=False),
    pa.field('first_row_partial', _BOOL, nullable=False),
    pa.field('last_row_partial', _BOOL, nullable=False),
    pa.field('last_column_unfinished', _BOOL, nullable=False),
    pa.field('volume', _FLOAT64, nullable=False),
    pa.field('trade_count', _UINT64, nullable=False),
    pa.field('taker_buy_volume', _FLOAT64, nullable=False),
    pa.field('taker_buy_trade_count', _UINT64, nullable=False),
    pa.field('poc', _FLOAT64),
    pa.field('taker_buy_poc', _FLOAT64),
    pa.field('cell_count', _UINT64, nullable=False),
    pa.field('data_cutoff', _MICROS, nullable=False),
    pa.field('canonical_through', _MICROS, nullable=False),
    pa.field('state_token', pa.string(), nullable=False),
    pa.field('created_at', _MICROS, nullable=False),
])


class RequestError(ValueError):
    """A request the cube cannot answer: 400 for invalid input, 409 for unavailable coverage."""

    def __init__(self, status: int, reason: str, detail: str, **fields: str) -> None:
        super().__init__(detail)
        self.status, self.reason, self.detail, self.fields = status, reason, detail, fields

    def body(self) -> dict[str, object]:
        if self.status == 409:
            return {'error': self.reason, **self.fields}
        return {'error': 'invalid_request', 'reason': self.reason, **self.fields, 'detail': self.detail}


@dataclass(frozen=True)
class Request:
    t1: datetime | None
    t2: datetime | None
    p1: Decimal | None
    p2: Decimal | None
    time_exponent: int
    price_exponent: int

    @property
    def time_resolution(self) -> float:
        return float(_TIME_BASE * 2**self.time_exponent)

    @property
    def price_resolution(self) -> float:
        return float(_PRICE_BASE * 2**self.price_exponent)

    def describe(self) -> dict[str, object]:
        return {
            't1': None if self.t1 is None else iso(self.t1),
            't2': None if self.t2 is None else iso(self.t2),
            'p1': None if self.p1 is None else str(self.p1),
            'p2': None if self.p2 is None else str(self.p2),
            'tR': self.time_resolution,
            'pR': self.price_resolution,
        }


@dataclass(frozen=True)
class Pin:
    """The contiguous cube coverage from 2021-01-01 in one accepted source state."""

    records: tuple[StateRecord, ...]
    cutoff: datetime
    canonical_through: datetime


def parse_request(raw: bytes) -> Request:
    """Decode and validate a request body; decimal literals reach validation exactly."""
    if len(raw) > MAX_BODY_BYTES:
        raise RequestError(400, 'invalid_json', f'The body exceeds {MAX_BODY_BYTES} bytes.')
    try:
        body = json.loads(raw, parse_float=Decimal, parse_constant=_constant, object_pairs_hook=_object)
    except (ValueError, RecursionError) as error:
        raise RequestError(400, 'invalid_json', f'The body is not valid JSON: {type(error).__name__}.') from error
    if not isinstance(body, dict):
        raise RequestError(400, 'invalid_json', 'The body must be a JSON object.')
    fields = cast(dict[str, object], body)
    unknown = sorted(set(fields) - set(FIELDS))
    if unknown:
        raise RequestError(400, 'unknown_field', f'Unknown fields: {", ".join(unknown)}.', field=unknown[0])
    t1, t2 = _time(fields.get('t1'), 't1'), _time(fields.get('t2'), 't2')
    p1, p2 = _price(fields.get('p1'), 'p1'), _price(fields.get('p2'), 'p2')
    if t1 is not None and t2 is not None and t1 >= t2:
        raise RequestError(400, 'bounds_out_of_order', 't1 must be earlier than t2.', field='t2')
    if p1 is not None and p2 is not None and p1 >= p2:
        raise RequestError(400, 'bounds_out_of_order', 'p1 must be lower than p2.', field='p2')
    return Request(
        t1, t2, p1, p2,
        _exponent(fields.get('tR'), _TIME_BASE, 'tR'),
        _exponent(fields.get('pR'), _PRICE_BASE, 'pR'),
    )


def statement_settings(result_id: str) -> dict[str, object]:
    """The declared settings for every statement of one result, attributed by ``log_comment``."""
    return {**QUERY_SETTINGS, 'log_comment': result_id}


def pin(store: SourceStore, settings: Mapping[str, object]) -> Pin:
    """Pin the current accepted partitions; coverage ends at the first one without the cube."""
    rows = store.execute(
        f"""SELECT partition_key, provisional, partition_start, partition_end, generation,
        revision, build_id, component_hashes
        FROM {store.table('source_current_partitions')} WHERE source_key=%(source)s
        ORDER BY partition_start, provisional""",
        {'source': SOURCE},
        settings,
    )
    cursor = canonical_through = CUBE_START
    pinned: list[StateRecord] = []
    for record in (_record(row) for row in rows):
        if record.partition.end <= CUBE_START:
            continue
        component = _CUBE_COMPONENTS[record.partition.provisional]
        if record.partition.start != cursor or component not in dict(record.component_hashes):
            break
        pinned.append(record)
        cursor = record.partition.end
        if not record.partition.provisional:
            canonical_through = cursor
    return Pin(tuple(pinned), cursor, canonical_through)


def write_result(
    runtime: SourceRuntime,
    request: Request,
    staging: Path,
    *,
    result_id: str,
    guard: Callable[[int], None],
) -> dict[str, object]:
    """Write ``cells.arrow`` and ``summary.arrow`` into ``staging``; return the response fields.

    ``guard`` receives the bytes written so far after every batch and raises to stop a result
    that would breach the storage budget. A ``SOURCE_MAINTENANCE`` error means a cleanup
    reclaimed a build this request read, or held the fence too long to prove it did not.
    """
    created = datetime.now(UTC)
    settings = statement_settings(result_id)
    started = time.perf_counter()
    state = pin(runtime.store, settings)
    pinned = time.perf_counter()
    plan = _plan(request, state)
    token = state_token(SOURCE, plan.records)
    client = _connect()
    try:
        prices = plan.prices(client, runtime.store, settings)
        priced = time.perf_counter()
        totals = _write_cells(client, runtime.store, plan, prices, staging / CELLS_FILE, result_id, token, guard, settings)
        written = time.perf_counter()
        _validate(runtime, client, plan.records, settings)
        validated = time.perf_counter()
    finally:
        client.close()
    fields: dict[str, object] = {
        'result_id': result_id,
        't1': plan.edge_time(plan.time_lower),
        't2': plan.edge_time(plan.time_upper),
        'p1': prices.effective(prices.lower),
        'p2': prices.effective(prices.upper),
        'tR': request.time_resolution,
        'pR': request.price_resolution,
        'first_column_partial': plan.partial(plan.time_lower, request.time_exponent),
        'last_column_partial': plan.partial(plan.time_upper, request.time_exponent),
        'first_row_partial': prices.partial(prices.lower, request.price_exponent),
        'last_row_partial': prices.partial(prices.upper, request.price_exponent),
        'last_column_unfinished': plan.unfinished,
        'volume': totals.volumes.total(),
        'trade_count': totals.trade_count,
        'taker_buy_volume': totals.taker_volumes.total(),
        'taker_buy_trade_count': totals.taker_buy_trade_count,
        'poc': _poc(totals.volumes.rows(), request.price_resolution),
        'taker_buy_poc': _poc(totals.taker_volumes.rows(), request.price_resolution),
        'cell_count': totals.cells,
        'data_cutoff': state.cutoff,
        'canonical_through': state.canonical_through,
        'state_token': token,
        'created_at': created,
    }
    summary = SUMMARY_SCHEMA.with_metadata(_metadata(result_id, request, plan, token))
    with ipc.new_file(str(staging / SUMMARY_FILE), summary) as writer:
        writer.write_batch(pa.RecordBatch.from_pylist([fields], schema=summary))
    log.info(
        'market state result %s cells=%d bytes=%d pin_ms=%d extent_ms=%d sql_ms=%d write_ms=%d validate_ms=%d',
        result_id, totals.cells, sum((staging / name).stat().st_size for name in (CELLS_FILE, SUMMARY_FILE)),
        _ms(pinned - started), _ms(priced - pinned), _ms(totals.waited), _ms(totals.writing), _ms(validated - written),
    )
    return {
        'result_id': result_id,
        'effective': {
            't1': iso(plan.edge_time(plan.time_lower)),
            't2': iso(plan.edge_time(plan.time_upper)),
            'p1': fields['p1'],
            'p2': fields['p2'],
            'tR': request.time_resolution,
            'pR': request.price_resolution,
        },
        'clipped': {'t1': plan.clipped_t1, 't2': plan.clipped_t2},
        'data_cutoff': iso(state.cutoff),
        'canonical_through': iso(state.canonical_through),
        'last_column_unfinished': plan.unfinished,
        'state_token': token,
        'cell_count': totals.cells,
    }


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec='microseconds')


@dataclass(frozen=True)
class _Plan:
    request: Request
    time_lower: int
    time_upper: int
    clipped_t1: bool
    clipped_t2: bool
    cutoff: datetime
    records: tuple[StateRecord, ...]

    @property
    def unfinished(self) -> bool:
        return self.time_lower < self.time_upper and self.edge_time(self.time_upper) > self.cutoff

    @staticmethod
    def edge_time(edge: int) -> datetime:
        return CUBE_START + timedelta(microseconds=edge * BASE_TIME_US)

    def partial(self, edge: int, exponent: int) -> bool:
        return self.time_lower < self.time_upper and edge % 2**exponent != 0

    def union(self, store: SourceStore, prices: tuple[int, int] | None) -> tuple[str, _External] | None:
        """The pinned base contributions inside the rectangle, and their identity tables."""
        if self.time_lower >= self.time_upper or not self.records:
            return None
        external = _external()
        first = self.edge_time(self.time_lower).date()
        last = (self.edge_time(self.time_upper) - timedelta(microseconds=1)).date()
        price = '' if prices is None else f' AND price_index >= {prices[0]} AND price_index < {prices[1]}'
        parts: list[str] = []
        for provisional in (False, True):
            records = [record for record in self.records if record.partition.provisional == provisional]
            if not records:
                continue
            name = 'provisional' if provisional else 'canonical'
            _add_identities(external, name, records)
            parts.append(
                f"""SELECT {_CELL_COLUMNS} FROM {store.component_table(_CUBE_COMPONENTS[provisional])}
                WHERE source_date >= '{first.isoformat()}' AND source_date <= '{last.isoformat()}'
                AND (partition_key, revision, build_id) IN (SELECT partition_key, revision, build_id FROM {name})
                AND time_index >= {self.time_lower} AND time_index < {self.time_upper}{price}"""
            )
        return ' UNION ALL '.join(parts), external

    def prices(self, client: _HttpClient, store: SourceStore, settings: Mapping[str, object]) -> _Prices:
        """Supplied bounds as rounded; an automatic bound is the time window's occupied extent.

        When the automatic side falls on the wrong side of a supplied bound, or the window
        has no trades, the empty interval collapses onto the supplied bound. Both bounds are
        null only when both are automatic and the window has no trades.
        """
        request = self.request
        lower = None if request.p1 is None else _price_edge(request.p1)
        upper = None if request.p2 is None else _price_edge(request.p2)
        if lower is not None and upper is not None:
            return _Prices(lower, upper)
        observed = self._observed(client, store, settings)
        if lower is not None:
            return _Prices(lower, lower if observed is None else max(observed[1], lower))
        if upper is not None:
            return _Prices(upper if observed is None else min(observed[0], upper), upper)
        return _Prices(None, None) if observed is None else _Prices(*observed)

    def _observed(
        self, client: _HttpClient, store: SourceStore, settings: Mapping[str, object]
    ) -> tuple[int, int] | None:
        """The occupied base rows inside the time window, whatever price bound was supplied."""
        selection = self.union(store, None)
        if selection is None:
            return None
        body, external = selection
        text = client.raw_query(
            f'SELECT count(), min(price_index), max(price_index) FROM ({body})',
            settings=settings, fmt='TabSeparated', external_data=external,
        ).decode().split()
        count, low, high = (int(value) for value in text)
        return None if count == 0 else (low, high + 1)


@dataclass(frozen=True)
class _Prices:
    lower: int | None
    upper: int | None

    @property
    def bounds(self) -> tuple[int, int] | None:
        if self.lower is None or self.upper is None or self.lower >= self.upper:
            return None
        return self.lower, self.upper

    @staticmethod
    def effective(edge: int | None) -> float | None:
        return None if edge is None else float(edge * BASE_PRICE_USDT)

    def partial(self, edge: int | None, exponent: int) -> bool:
        return self.bounds is not None and edge is not None and edge % 2**exponent != 0


class _ExactSums:
    """Correctly rounded sums of non-negative Float64 values by row, fed batch by batch.

    A Float64 is an integer mantissa times a power of two. Mantissas are added as integers per
    (row, exponent): their 26-bit halves go through ``bincount``, which is exact while a batch
    sums fewer than 2^26 values. One integer division then rounds each sum once, so every sum
    equals ``math.fsum`` over the same values, and the state is one integer per (row, exponent)
    however many cells stream through.
    """

    def __init__(self) -> None:
        self.parts: dict[int, int] = {}

    def add(self, rows: npt.NDArray[np.uint64], values: npt.NDArray[np.float64]) -> None:
        bits = values.view(np.uint64)
        exponents = (bits >> np.uint64(52)).astype(np.int64)
        if bool(np.any(bits >> np.uint64(63))) or bool(np.any(exponents == _EXPONENTS - 1)):
            raise ValueError('Cell volumes must be finite and not negative.')
        mantissas = np.where(exponents > 0, (bits & _MANTISSA) | _HIDDEN_BIT, bits & _MANTISSA)
        keys, slots = np.unique(rows.astype(np.int64) * _EXPONENTS + np.maximum(exponents, 1), return_inverse=True)
        high = np.bincount(slots, weights=(mantissas >> np.uint64(26)).astype(np.float64)).astype(np.int64)
        low = np.bincount(slots, weights=(mantissas & _LOW_BITS).astype(np.float64)).astype(np.int64)
        for key, upper, lower in zip(keys.tolist(), high.tolist(), low.tolist(), strict=True):
            self.parts[key] = self.parts.get(key, 0) + (upper << 26) + lower

    def rows(self) -> dict[int, float]:
        scaled: dict[int, int] = {}
        for key, mantissas in self.parts.items():
            row, exponent = divmod(key, _EXPONENTS)
            scaled[row] = scaled.get(row, 0) + (mantissas << exponent)
        return {row: total / _SCALE for row, total in scaled.items()}

    def total(self) -> float:
        return sum(mantissas << (key % _EXPONENTS) for key, mantissas in self.parts.items()) / _SCALE


@dataclass
class _Totals:
    cells: int = 0
    trade_count: int = 0
    taker_buy_trade_count: int = 0
    volumes: _ExactSums = field(default_factory=_ExactSums)
    taker_volumes: _ExactSums = field(default_factory=_ExactSums)
    waited: float = 0.0
    writing: float = 0.0


def _plan(request: Request, state: Pin) -> _Plan:
    cutoff_edge = -(-(_micros(state.cutoff) - _T0_US) // BASE_TIME_US)
    lower = 0 if request.t1 is None else _time_edge(request.t1)
    upper = cutoff_edge if request.t2 is None else _time_edge(request.t2)
    clipped_t1 = request.t1 is not None and lower < 0
    clipped_t2 = request.t2 is not None and upper > cutoff_edge
    if lower == upper and 0 <= lower <= cutoff_edge and cutoff_edge > 0:
        # Bounds that round onto one edge inside existing coverage select nothing, whichever
        # were supplied; with no coverage at all every request is outside it.
        low = high = lower
    else:
        low, high = max(lower, 0), min(upper, cutoff_edge)
        if low >= high:
            raise RequestError(
                409, 'outside_coverage',
                'The selected time range lies outside the covered cube history.',
                history_start=iso(CUBE_START), data_cutoff=iso(state.cutoff),
            )
    start, end = _Plan.edge_time(low), _Plan.edge_time(high)
    records = tuple(
        record for record in state.records
        if record.partition.start < end and record.partition.end > start
    )
    return _Plan(request, low, high, clipped_t1, clipped_t2, state.cutoff, records)


def _write_cells(
    client: _HttpClient,
    store: SourceStore,
    plan: _Plan,
    prices: _Prices,
    path: Path,
    result_id: str,
    token: str,
    guard: Callable[[int], None],
    settings: Mapping[str, object],
) -> _Totals:
    schema = CELLS_SCHEMA.with_metadata(_metadata(result_id, plan.request, plan, token))
    totals = _Totals()
    bounds = prices.bounds
    selection = None if bounds is None else plan.union(store, bounds)
    with ipc.new_file(str(path), schema) as writer:
        if selection is not None:
            body, external = selection
            query = f"""SELECT I, J, sumKahan(volume), sum(toUInt64(trade_count)),
                sumKahan(taker_buy_volume), sum(toUInt64(taker_buy_trade_count))
                FROM (SELECT {_shift('time_index', plan.request.time_exponent)} AS I,
                    {_shift('price_index', plan.request.price_exponent)} AS J, volume, trade_count,
                    taker_buy_volume, taker_buy_trade_count FROM ({body}))
                GROUP BY I, J ORDER BY I, J"""
            started = time.perf_counter()
            with client.raw_stream(query, settings=settings, fmt='ArrowStream', external_data=external) as stream:
                for batch in ipc.open_stream(stream):
                    began = time.perf_counter()
                    arrays = [batch.column(index).cast(schema.field(index).type) for index in range(6)]
                    writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
                    guard(path.stat().st_size)
                    rows = cast(npt.NDArray[np.uint64], arrays[1].to_numpy())
                    totals.cells += batch.num_rows
                    totals.trade_count += int(cast(npt.NDArray[np.uint64], arrays[3].to_numpy()).sum())
                    totals.taker_buy_trade_count += int(cast(npt.NDArray[np.uint64], arrays[5].to_numpy()).sum())
                    totals.volumes.add(rows, cast(npt.NDArray[np.float64], arrays[2].to_numpy()))
                    totals.taker_volumes.add(rows, cast(npt.NDArray[np.float64], arrays[4].to_numpy()))
                    totals.writing += time.perf_counter() - began
            totals.waited = time.perf_counter() - started - totals.writing
    return totals


def _validate(
    runtime: SourceRuntime, client: _HttpClient, records: tuple[StateRecord, ...], settings: Mapping[str, object]
) -> None:
    """Prove no cleanup reclaimed a build this request read.

    Cleanup holds the fence exclusively while it deletes and logs; once the shared fence is
    ours, every deletion that could have overlapped the read is in the log. The log's
    ``completed_at`` is the cleanup's start, so the check has no time filter: a build that
    was current when pinned appears there only if it was reclaimed.
    """
    if not records:
        return
    deadline = time.monotonic() + FENCE_WAIT_SECONDS
    while True:
        try:
            with source_lock(runtime.lock_root, SOURCE, 'heavy', shared=True):
                external = _external()
                _add_identities(external, 'reads', records)
                reclaimed = client.raw_query(
                    f"""SELECT count() FROM {runtime.store.table('source_cleanup_log')}
                    WHERE source_key='{SOURCE}' AND build_id IN (SELECT build_id FROM reads)""",
                    settings=settings, fmt='TabSeparated', external_data=external,
                ).decode().strip()
            break
        except SourceError as error:
            if error.code != 'SOURCE_LOCK_BUSY':
                raise
            if time.monotonic() >= deadline:
                raise SourceError('SOURCE_MAINTENANCE', 'A cleanup held the source fence past the query deadline.') from error
            time.sleep(0.5)
    if int(reclaimed):
        raise SourceError('SOURCE_MAINTENANCE', 'A cleanup reclaimed a build this query read.')


def _poc(rows: Mapping[int, float], price_resolution: float) -> float | None:
    """The centre ``(J + 0.5) * pR`` of the row with the largest sum; the lower row wins a tie."""
    best = max(rows.values(), default=0.0)
    if best <= 0.0:
        return None
    return (min(row for row, total in rows.items() if total == best) + 0.5) * price_resolution


def _ms(seconds: float) -> int:
    return int(seconds * 1000)


def _shift(column: str, exponent: int) -> str:
    """``floor(index / 2^exponent)``; any exponent of 64 or more maps every UInt64 to 0."""
    return f'bitShiftRight({column}, {exponent})' if exponent < 64 else 'toUInt64(0)'


def _metadata(result_id: str, request: Request, plan: _Plan, token: str) -> dict[str, str]:
    return {
        METADATA_KEY: json.dumps(
            {
                'schema_version': SCHEMA_VERSION,
                'result_id': result_id,
                'request': request.describe(),
                'grid': {
                    't0': iso(CUBE_START),
                    'tR': request.time_resolution,
                    'pR': request.price_resolution,
                    'time_exponent': request.time_exponent,
                    'price_exponent': request.price_exponent,
                },
                'data_cutoff': iso(plan.cutoff),
                'state_token': token,
                'pins': [
                    [record.partition.key, record.generation, record.revision, str(record.build_id)]
                    for record in plan.records
                ],
            },
            sort_keys=True,
        )
    }


def _connect() -> _HttpClient:
    factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _HttpClient,
        factory(
            host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            port=int(os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')),
            username=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ['CLICKHOUSE_PASSWORD'],
            connect_timeout=10,
            send_receive_timeout=RECEIVE_TIMEOUT_SECONDS,
        ),
    )


def _external() -> _External:
    return cast(_External, getattr(import_module('clickhouse_connect.driver.external'), 'ExternalData')())


def _add_identities(external: _External, name: str, records: Sequence[StateRecord]) -> None:
    external.add_file(
        file_name=name,
        data='\n'.join(
            f'{record.partition.key}\t{record.revision}\t{record.build_id}' for record in records
        ).encode(),
        fmt='TabSeparated',
        structure=_PIN_STRUCTURE,
    )


def _record(row: Sequence[object]) -> StateRecord:
    key, provisional, start, end, generation, revision, build_id, hashes = row
    pairs: list[tuple[str, str]] = []
    for pair in json.loads(str(hashes)):
        if not isinstance(pair, list) or len(cast(list[object], pair)) != 2:
            raise ValueError(f'Malformed component hashes for {key!r}.')
        name, digest = cast(list[object], pair)
        pairs.append((str(name), str(digest)))
    return StateRecord(
        Partition(str(key), _utc(start), _utc(end), bool(provisional)),
        _integer(generation),
        str(revision),
        UUID(str(build_id)),
        tuple(sorted(pairs)),
    )


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError('duplicate keys')
    return dict(pairs)


def _constant(value: str) -> object:
    raise ValueError(f'unsupported constant {value}')


def _time(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > _MAX_TEXT:
        raise RequestError(400, 'invalid_time', f'{name} must be an ISO 8601 time string.', field=name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise RequestError(400, 'invalid_time', f'{name} is not an ISO 8601 time: {value!r}.', field=name) from error
    if parsed.tzinfo is None:
        raise RequestError(400, 'time_zone_required', f'{name} needs an explicit UTC offset.', field=name)
    try:
        return parsed.astimezone(UTC)
    except OverflowError as error:
        raise RequestError(400, 'invalid_time', f'{name} is outside the representable UTC range.', field=name) from error


def _price(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | Decimal | str) or len(str(value)) > _MAX_TEXT:
        raise RequestError(400, 'invalid_price', f'{name} must be a number or a decimal string.', field=name)
    try:
        price = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation as error:
        raise RequestError(400, 'invalid_price', f'{name} is not a number: {value!r}.', field=name) from error
    if not price.is_finite() or price < 0 or price >= _PRICE_LIMIT:
        raise RequestError(
            400, 'invalid_price',
            f'{name} must be finite, not negative and at most {_MAX_PRICE_EDGE * BASE_PRICE_USDT} USDT.',
            field=name,
        )
    return price


def _exponent(value: object, base: Fraction, name: str) -> int:
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise RequestError(400, 'unsupported_resolution', f'{name} must be a JSON number.', field=name)
    if isinstance(value, Decimal) and value.is_finite() and not 0 <= value.adjusted() <= 308:
        # Resolutions lie between 56.25 and the Float64 maximum; refusing other magnitudes
        # first keeps a short literal such as 1e10000000000 from expanding in Fraction.
        raise RequestError(400, 'unsupported_resolution', f'{name} is outside the Float64 range.', field=name)
    try:
        ratio = Fraction(value) / base
    except (ValueError, OverflowError) as error:
        raise RequestError(400, 'unsupported_resolution', f'{name} must be finite.', field=name) from error
    numerator = ratio.numerator
    if ratio.denominator != 1 or numerator < 1 or numerator & (numerator - 1):
        raise RequestError(
            400, 'unsupported_resolution',
            f'{name} must be {float(base)} multiplied by a power of two, got {value}.', field=name,
        )
    if base * numerator > _FLOAT64_MAX:
        raise RequestError(400, 'unsupported_resolution', f'{name} exceeds the Float64 range.', field=name)
    return numerator.bit_length() - 1


def _time_edge(value: datetime) -> int:
    return (_micros(value) - _T0_US + BASE_TIME_US // 2) // BASE_TIME_US


def _price_edge(value: Decimal) -> int:
    # Exact rational arithmetic: Decimal's 28-digit context would round a long literal
    # just below a midpoint up across it. Prices below the first midpoint are edge 0,
    # which also keeps a literal such as 1e-10000000000 from expanding in Fraction.
    if value < _FIRST_MIDPOINT:
        return 0
    return int((Fraction(value) + Fraction(BASE_PRICE_USDT, 2)) // BASE_PRICE_USDT)


def _micros(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f'Expected a datetime, got {type(value).__name__}.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _integer(value: object) -> int:
    if not isinstance(value, int):
        raise TypeError(f'Expected an integer, got {type(value).__name__}.')
    return value
