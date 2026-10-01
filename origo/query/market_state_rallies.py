"""Immutable rally discovery from a held market-state pack and exact native trades."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import socket
import threading
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, cast
from urllib.parse import unquote_plus, urlsplit
from uuid import UUID

import numpy as np
from numpy.typing import NDArray
from urllib3 import HTTPConnectionPool, PoolManager, Timeout
from urllib3.connection import HTTPConnection
from urllib3.exceptions import HTTPError
from urllib3.response import BaseHTTPResponse

from origo.query import market_state as cube
from origo.query import rally_detection as detector
from origo.query.rally_detection import (
    RallyBar,
    RallyDefinition,
    RallyDetection,
    RallyEvent,
    RallyTrades,
    definition_fingerprint,
    detect_rallies,
)
from origo.sources.contracts import SourceError, StateRecord
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.profiles.market_state import BASE_PRICE_USDT, BASE_TIME_US, CUBE_START

if TYPE_CHECKING:
    from urllib3._base_connection import BaseHTTPConnection

RALLY_FILES: Final = ('rallies.arrow', 'rally_cells.arrow', 'summary.arrow')
MAX_RALLY_BULK_SECONDS: Final = 48 * 3600
MAX_RALLY_INPUT_ROWS: Final = 8_000_000
MAX_RALLY_INPUT_BYTES: Final = 256 * 1024**2
MAX_RALLY_OUTPUT_BYTES: Final = 512 * 1024**2
RALLY_DEADLINE_SECONDS: Final = 295.0
METADATA_KEY: Final = 'origo.market_state_rallies'
log = logging.getLogger('origo.query.market_state_rallies')
AVAILABLE_EVENT_MEASURES: Final = ('volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count')
UNAVAILABLE_EVENT_MEASURES: Final = ('path_length', 'dwell', 'indicators')

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_US: Final = timedelta(microseconds=1)
_MINUTE_US: Final = 60_000_000
_BAR_US: Final = 15 * _MINUTE_US
_CUBE_US: Final = cube.utc_micros(CUBE_START)
_NATIVE_ROW_BYTES: Final = 33
_KEY_DTYPE: Final[np.dtype[np.void]] = np.dtype([('time', np.uint64), ('price', np.uint64)])
_LE_U64: Final[np.dtype[np.uint64]] = np.dtype('<u8')
_WORKER_RSS_BUDGET: Final = 1536 * 1024**2
_SCRATCH_RESERVE_BYTES: Final = 160 * 1024**2
_DIGEST: Final = re.compile('[0-9a-f]{64}', re.ASCII)
_RAW: Final = {False: 'raw', True: 'raw_latest'}
_FLOAT_FIELDS: Final = {
    'reference_price', 'start_price', 'end_price', 'return_bps', 'duration_seconds',
    'max_drawdown', 'volume', 'taker_buy_volume',
}
_UINT_FIELDS: Final = {
    'reference_trade_id', 'start_trade_id', 'end_trade_id', 'confirmation_trade_id',
    'trade_count', 'taker_buy_trade_count', 'base_time_index', 'base_price_index',
    'first_trade_id', 'last_trade_id',
}
_TIME_FIELDS: Final = {'anchor_at', 'reference_at', 'start_at', 'end_at', 'confirmed_at', 'first_at', 'last_at'}


class _Arrow(cube.ArrowProtocol, Protocol):
    def uint8(self) -> object: ...


class _Http(cube.HttpClient, Protocol):
    timeout: object
    http_retries: int


class _Batch(cube.ArrowBatch, Protocol):
    def to_pylist(self) -> list[dict[str, object]]: ...


pa = cast(_Arrow, import_module('pyarrow'))
ipc = cube.ipc


@dataclass(frozen=True)
class ExpectedCubeState:
    data_cutoff: datetime
    pack_pin_digest: str


@dataclass(frozen=True)
class RallyDiscoveryRequest:
    definition: RallyDefinition
    analysis_start: datetime
    analysis_end: datetime
    expected_state: ExpectedCubeState


class RallyError(cube.RequestError):
    def __init__(self, status: int, reason: str, detail: str, **attributes: object) -> None:
        super().__init__(status, reason, detail)
        self.attributes = attributes

    def body(self) -> dict[str, object]:
        return {'error': self.reason, 'reason': self.reason, 'detail': self.detail, **self.attributes}


def _object(value: object, names: set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RallyError(400, 'invalid_request', f'{name} must be an object.')
    body = cast(dict[str, object], value)
    unknown = sorted(set(body) - names)
    if unknown:
        raise RallyError(400, 'unknown_field', f'Unknown {name} field {unknown[0]!r}.', field=unknown[0])
    return body


def _time(value: object, name: str) -> datetime:
    try:
        parsed = cube.parse_utc_time(value, name)
    except cube.RequestError as error:
        raise RallyError(error.status, error.reason, error.detail, **error.fields) from error
    if parsed is None:
        raise RallyError(400, 'invalid_time', f'{name} is required.', field=name)
    return parsed


def _quantity(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise RallyError(400, 'invalid_definition', f'{name} must be a JSON number.', field=name)
    return value if isinstance(value, Decimal) else Decimal(value)


def parse_rally_request(raw: bytes) -> RallyDiscoveryRequest:
    if len(raw) > cube.MAX_BODY_BYTES:
        raise RallyError(400, 'invalid_json', f'The body exceeds {cube.MAX_BODY_BYTES} bytes.')
    try:
        decoded: object = json.loads(raw, parse_float=Decimal, parse_constant=cube.reject_json_constant, object_pairs_hook=cube.unique_json_object)
    except (ValueError, RecursionError) as error:
        raise RallyError(400, 'invalid_json', f'The body is not valid JSON: {type(error).__name__}.') from error
    body = _object(decoded, {'definition', 'analysis', 'expected_state'}, 'request')
    values = _object(body.get('definition'), {'mode', 'scale', 'target', 'pullback', 'reversal', 'anchor_minutes'}, 'definition')
    mode, scale = values.get('mode'), values.get('scale')
    if mode not in ('first_hit', 'controlled_advance', 'swing') or scale not in ('bps', 'atr'):
        raise RallyError(400, 'invalid_definition', 'A supported mode and scale are required.')
    applicable = {'mode', 'scale', 'target'}
    if mode == 'swing':
        applicable.add('reversal')
    else:
        applicable.add('anchor_minutes')
        if mode == 'controlled_advance':
            applicable.add('pullback')
    irrelevant = set(values) - applicable
    if irrelevant:
        raise RallyError(400, 'invalid_definition', 'Definition fields must apply to the selected mode.', field=sorted(irrelevant)[0])
    definition = RallyDefinition(
        mode, scale, _quantity(values.get('target'), 'target'),
        _quantity(values['pullback'], 'pullback') if 'pullback' in values else None,
        _quantity(values['reversal'], 'reversal') if 'reversal' in values else None,
        cast(int | None, values.get('anchor_minutes')),
    )
    try:
        definition_fingerprint(definition)
    except ValueError as error:
        raise RallyError(400, 'invalid_definition', str(error)) from error
    analysis = _object(body.get('analysis'), {'start', 'end'}, 'analysis')
    start, end = _time(analysis.get('start'), 'analysis.start'), _time(analysis.get('end'), 'analysis.end')
    expected = _object(body.get('expected_state'), {'data_cutoff', 'pack_pin_digest'}, 'expected_state')
    cutoff = _time(expected.get('data_cutoff'), 'expected_state.data_cutoff')
    digest = expected.get('pack_pin_digest')
    if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
        raise RallyError(400, 'invalid_expected_state', 'pack_pin_digest must be lowercase SHA-256.')
    request = RallyDiscoveryRequest(definition, start, end, ExpectedCubeState(cutoff, digest))
    _validate_request(request)
    return request


def _validate_request(request: RallyDiscoveryRequest) -> None:
    for name, value in (
        ('analysis.start', request.analysis_start), ('analysis.end', request.analysis_end),
        ('expected_state.data_cutoff', request.expected_state.data_cutoff),
    ):
        if not isinstance(cast(object, value), datetime) or value.utcoffset() is None:
            raise RallyError(400, 'time_zone_required', f'{name} needs an explicit UTC offset.')
    if request.analysis_start < CUBE_START:
        raise RallyError(400, 'analysis_before_cube_start', 'The cube starts at 2021-01-01.', history_start=cube.iso(CUBE_START))
    if request.analysis_start >= min(request.analysis_end, request.expected_state.data_cutoff):
        raise RallyError(400, 'bounds_out_of_order', 'analysis.start must precede the observation ceiling.')
    if not isinstance(cast(object, request.expected_state.pack_pin_digest), str) or _DIGEST.fullmatch(request.expected_state.pack_pin_digest) is None:
        raise RallyError(400, 'invalid_expected_state', 'pack_pin_digest must be lowercase SHA-256.')
    try:
        definition_fingerprint(request.definition)
    except ValueError as error:
        raise RallyError(400, 'invalid_definition', str(error)) from error


def _z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _at(value: int) -> datetime:
    return _EPOCH + timedelta(microseconds=value)


def _normalized(definition: RallyDefinition) -> dict[str, str | int]:
    result: dict[str, str | int] = {'mode': definition.mode, 'scale': definition.scale}
    for name in ('target', 'pullback', 'reversal'):
        value = cast(Decimal | None, getattr(definition, name))
        if value is not None:
            text = format(value, 'f')
            result[name] = text.rstrip('0').rstrip('.') if '.' in text else text
    if definition.anchor_minutes is not None:
        result['anchor_minutes'] = definition.anchor_minutes
    return result


def _current_rss() -> int:
    if sys.platform.startswith('linux'):
        return int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    result = subprocess.run(['ps', '-o', 'rss=', '-p', str(os.getpid())], capture_output=True, text=True, check=True)
    return int(result.stdout.strip()) * 1024


def _resource_guard(deadline: float, additional: int = 0) -> None:
    _remaining(deadline)
    required = _current_rss() + additional + _SCRATCH_RESERVE_BYTES
    if required > _WORKER_RSS_BUDGET:
        raise RallyError(413, 'rally_working_memory_exceeded', 'The worker cannot reserve this bounded operation.', required_bytes=required, budget_bytes=_WORKER_RSS_BUDGET)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise RallyError(504, 'rally_deadline_exceeded', 'The rally discovery wall deadline expired.')
    return remaining


class _DeadlinePool(PoolManager):
    def __init__(self, deadline: float) -> None:
        super().__init__(num_pools=1, retries=False)
        self.deadline = deadline
        self._sockets: set[socket.socket] = set()
        self._socket_lock = threading.Lock()
        self._expired = False
        owner = self

        class DeadlineConnection(HTTPConnection):
            def connect(self) -> None:
                super().connect()
                if self.sock is not None:
                    owner._register(self.sock)

        class DeadlineConnectionPool(HTTPConnectionPool):
            ConnectionCls = cast('type[BaseHTTPConnection]', DeadlineConnection)

        self.pool_classes_by_scheme = {**self.pool_classes_by_scheme, 'http': DeadlineConnectionPool}
        self._timer = threading.Timer(_remaining(deadline), self._interrupt)
        self._timer.daemon = True
        self._timer.start()

    @staticmethod
    def _shutdown(connection: socket.socket) -> None:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            log.debug('Rally deadline transport socket already closed.', exc_info=True)

    def _register(self, connection: socket.socket) -> None:
        with self._socket_lock:
            expired = self._expired
            if not expired:
                self._sockets.add(connection)
        if expired:
            self._shutdown(connection)
            _remaining(self.deadline)

    def _interrupt(self) -> None:
        with self._socket_lock:
            self._expired = True
            connections = tuple(self._sockets)
        for connection in connections:
            self._shutdown(connection)

    def close(self) -> None:
        self._timer.cancel()
        with self._socket_lock:
            connections = tuple(self._sockets)
            self._sockets.clear()
        for connection in connections:
            self._shutdown(connection)
        super().clear()

    def urlopen(self, method: str, url: str, redirect: bool = True, **kw: object) -> BaseHTTPResponse:
        remaining = _remaining(self.deadline)
        parts = urlsplit(url)
        parameters = parts.query.split('&') if parts.query else []
        execution_limit = f'max_execution_time={min(60.0, remaining)}'
        for index, parameter in enumerate(parameters):
            if unquote_plus(parameter.partition('=')[0]) == 'max_execution_time':
                parameters[index] = execution_limit
                break
        else:
            parameters.append(execution_limit)
        url = parts._replace(query='&'.join(parameters)).geturl()
        kw['timeout'] = Timeout(total=remaining, connect=min(10.0, remaining), read=remaining)
        kw['retries'] = False
        try:
            return super().urlopen(method, url, redirect=redirect, **kw)
        except HTTPError:
            _remaining(self.deadline)
            raise


@dataclass
class _Reads:
    runtime: SourceRuntime
    deadline: float
    result_id: str
    client: _Http | None = field(default=None, init=False)
    pool: _DeadlinePool | None = field(default=None, init=False)

    def settings(self) -> dict[str, object]:
        return {**cube.statement_settings(self.result_id), 'max_execution_time': min(60.0, _remaining(self.deadline))}

    def connect(self) -> _Http:
        if self.client is None:
            _remaining(self.deadline)
            self.pool = _DeadlinePool(self.deadline)
            factory = getattr(import_module('clickhouse_connect'), 'get_client')
            try:
                self.client = cast(_Http, factory(
                    host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
                    port=int(os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')),
                    username=os.environ.get('CLICKHOUSE_USER', 'default'),
                    password=os.environ['CLICKHOUSE_PASSWORD'],
                    connect_timeout=10, send_receive_timeout=60,
                    autogenerate_session_id=False, query_retries=0, pool_mgr=self.pool, settings=self.settings(),
                ))
            finally:
                _remaining(self.deadline)
            self.client.http_retries = 0
        remaining = _remaining(self.deadline)
        self.client.timeout = Timeout(total=remaining, connect=min(10.0, remaining), read=remaining)
        return self.client

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
        if self.pool is not None:
            self.pool.close()

    def scalar(self, query: str, external: cube.ExternalData | None = None) -> int:
        try:
            return int(self.connect().raw_query(query, settings=self.settings(), fmt='TabSeparated', external_data=external).decode().strip())
        finally:
            _remaining(self.deadline)

    @contextmanager
    def batches(self, query: str, external: cube.ExternalData | None = None) -> Iterator[Iterator[cube.ArrowBatch]]:
        try:
            with self.connect().raw_stream(query, settings=self.settings(), fmt='ArrowStream', external_data=external) as stream:
                yield ipc.open_stream(stream)
        finally:
            _remaining(self.deadline)


def rally_prelude(runtime: SourceRuntime, *, result_id: str, deadline: float) -> dict[str, int]:
    domain_id = UUID((runtime.lock_root / runtime.spec.key / 'domain_id').read_text())
    reads = _Reads(runtime, deadline, result_id)
    try:
        registered: list[str] = []
        query = f"SELECT DISTINCT toString(domain_id) AS domain_id FROM {runtime.store.table('source_lock_domain')} WHERE source_key='{cube.SOURCE}'"
        with reads.batches(query) as batches:
            for batch in batches:
                registered.extend(str(row['domain_id']) for row in cast(_Batch, batch).to_pylist())
        if registered != [str(domain_id)]:
            raise RuntimeError('Workers do not share the registered source lock mount.')
        measured: dict[str, int] = {}
        query = f"SELECT source_key, max(working_set_bytes) AS working_set_bytes FROM {runtime.store.table('source_capacity_log')} GROUP BY source_key"
        with reads.batches(query) as batches:
            for batch in batches:
                for row in cast(_Batch, batch).to_pylist():
                    measured[str(row['source_key'])] = int(str(row['working_set_bytes']))
        return measured
    finally:
        reads.close()


def _accepted(reads: _Reads) -> tuple[StateRecord, ...]:
    names = ('partition_key', 'provisional', 'partition_start', 'partition_end', 'generation', 'revision', 'build_id', 'component_hashes')
    columns = ', '.join('toString(build_id) AS build_id' if name == 'build_id' else name for name in names)
    query = f"""SELECT {columns} FROM {reads.runtime.store.table('source_current_partitions')}
        WHERE source_key='{cube.SOURCE}' ORDER BY partition_start, provisional"""
    records: list[StateRecord] = []
    with reads.batches(query) as batches:
        for batch in batches:
            _remaining(reads.deadline)
            records.extend(cube.state_record([row[name] for name in names]) for row in cast(_Batch, batch).to_pylist())
    return tuple(records)


def _pin(reads: _Reads) -> cube.Pin:
    cursor = canonical = CUBE_START
    records: list[StateRecord] = []
    for record in _accepted(reads):
        if record.partition.end > CUBE_START:
            if record.partition.start != cursor or cube.CUBE_COMPONENTS[record.partition.provisional] not in dict(record.component_hashes):
                break
            records.append(record)
            cursor = record.partition.end
            if not record.partition.provisional:
                canonical = cursor
    return cube.Pin(tuple(records), cursor, canonical)


def _pack_digest(cutoff: datetime, records: Sequence[StateRecord]) -> str:
    pins = {record.partition.key: [record.revision, str(record.build_id)] for record in records}
    encoded = json.dumps([cube.iso(cutoff), sorted(pins.items())], separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class _Plan:
    request: RallyDiscoveryRequest
    state: cube.Pin
    records: tuple[StateRecord, ...]
    bulk_start: int
    bulk_end: int
    probes: tuple[tuple[int, int], ...]
    base_lower: int
    base_upper: int
    required: tuple[tuple[int, int], ...]

    @property
    def coverage(self) -> tuple[tuple[datetime, datetime], ...]:
        return tuple(
            (record.partition.start, min(record.partition.end, self.state.cutoff))
            for record in self.state.records
        )


def _plan(request: RallyDiscoveryRequest, reads: _Reads) -> _Plan:
    current = _pin(reads)
    cutoff = request.expected_state.data_cutoff.astimezone(UTC)
    held = tuple(record for record in current.records if record.partition.start < cutoff)
    if current.cutoff < cutoff or _pack_digest(cutoff, held) != request.expected_state.pack_pin_digest:
        raise RallyError(
            409, 'pack_state_changed', 'Refresh the held cube pack explicitly.',
            required_cutoff=cube.iso(cutoff), required_span={'start': cube.iso(CUBE_START), 'end': cube.iso(cutoff)},
        )
    state = cube.Pin(held, cutoff, min(current.canonical_through, cutoff))
    origin = cube.utc_micros(request.analysis_start)
    edge = cube.utc_micros(min(request.analysis_end, cutoff))
    freeze = origin
    if request.definition.mode == 'swing' and request.definition.scale == 'atr':
        lookahead = min(edge, -(-origin // _BAR_US) * _BAR_US + 15 * _BAR_US)
        query, identities = _union(
            reads, held, _RAW, 'toUnixTimestamp64Micro(datetime) AS timestamp_us',
            f'datetime >= fromUnixTimestamp64Micro({origin}, \'UTC\') AND datetime < fromUnixTimestamp64Micro({lookahead}, \'UTC\')',
            _at(origin), _at(lookahead),
        )
        freeze = reads.scalar(f'SELECT if(count()=0, toInt64({lookahead}), min(timestamp_us)) FROM ({query})', identities)
    if request.definition.mode != 'swing':
        cadence = cast(int, request.definition.anchor_minutes) * _MINUTE_US
        freeze = -(-origin // cadence) * cadence
    first = origin if request.definition.scale == 'bps' or freeze >= edge else min(origin, freeze // _BAR_US * _BAR_US - 15 * _BAR_US)
    first = max(first, _CUBE_US)
    if edge - first > MAX_RALLY_BULK_SECONDS * 1_000_000:
        raise RallyError(
            400, 'analysis_read_span_exceeded', 'The actual bulk scan exceeds 48 hours.',
            bulk_start=_z(_at(first)), bulk_end=_z(_at(edge)), maximum_bulk_seconds=MAX_RALLY_BULK_SECONDS,
            maximum_analysis_end=_z(_at(first + MAX_RALLY_BULK_SECONDS * 1_000_000)),
        )
    probes: tuple[tuple[int, int], ...] = ()
    if request.definition.mode != 'swing':
        cadence = cast(int, request.definition.anchor_minutes) * _MINUTE_US
        anchor = -(-origin // cadence) * cadence
        if anchor < edge:
            low = max(_CUBE_US, anchor - 86400 * 1_000_000)
            if low < anchor:
                probes = ((low, anchor),)
    base_lower = (origin - _CUBE_US) // BASE_TIME_US
    base_upper = -(-(edge - _CUBE_US) // BASE_TIME_US)
    base_start, base_end = _CUBE_US + base_lower * BASE_TIME_US, min(_CUBE_US + base_upper * BASE_TIME_US, cube.utc_micros(cutoff))
    required = ((first, edge), *probes, (base_start, base_end))
    records = tuple(record for record in held if any(
        cube.utc_micros(record.partition.start) < high and cube.utc_micros(record.partition.end) > low
        for low, high in required
    ))
    return _Plan(request, state, records, first, edge, probes, base_lower, base_upper, required)


def _source_digest(plan: _Plan) -> str:
    fields: dict[str, object] = {
        'bulk_read': [_z(_at(plan.bulk_start)), _z(_at(plan.bulk_end))],
        'reference_windows': sorted([_z(_at(low)), _z(_at(high))] for low, high in plan.probes),
        'pins': sorted([record.partition.key, record.revision, str(record.build_id)] for record in plan.records),
    }
    return hashlib.sha256(b'rally_source_v1\n' + json.dumps(fields, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('utf-8')).hexdigest()


def _union(
    reads: _Reads, records: Sequence[StateRecord], components: Mapping[bool, str], columns: str,
    condition: str, start: datetime, end: datetime,
) -> tuple[str, cube.ExternalData]:
    external = cube.external_identities()
    statements: list[str] = []
    for provisional in (False, True):
        selected = [record for record in records if record.partition.provisional == provisional]
        if selected:
            name = 'provisional' if provisional else 'canonical'
            cube.add_identities(external, name, selected)
            statements.append(f"""SELECT {columns} FROM {reads.runtime.store.component_table(components[provisional])}
                WHERE source_date >= '{start.date().isoformat()}'
                AND source_date <= '{(end - _US).date().isoformat()}'
                AND (partition_key, revision, build_id) IN (SELECT partition_key, revision, build_id FROM {name})
                AND {condition}""")
    if not statements:
        raise RallyError(409, 'source_state_changed', 'No admitted source identities cover the read.')
    return ' UNION ALL '.join(statements), external


def _raw_selection(reads: _Reads, plan: _Plan, low: int, high: int) -> tuple[str, cube.ExternalData]:
    return _union(
        reads, plan.records, _RAW,
        'trade_id, toUnixTimestamp64Micro(datetime) AS timestamp_us, price, quote_quantity, toBool(is_buyer_maker) AS is_buyer_maker',
        f'datetime >= fromUnixTimestamp64Micro({low}, \'UTC\') AND datetime < fromUnixTimestamp64Micro({high}, \'UTC\')',
        _at(low), _at(high),
    )


def _input_budget(rows: int) -> None:
    size = rows * _NATIVE_ROW_BYTES
    if rows > MAX_RALLY_INPUT_ROWS or size > MAX_RALLY_INPUT_BYTES:
        raise RallyError(
            413, 'rally_input_budget_exceeded', 'The native column reservation exceeds admission.',
            required_rows=rows, budget_rows=MAX_RALLY_INPUT_ROWS,
            required_bytes=size, budget_bytes=MAX_RALLY_INPUT_BYTES,
        )


def _trade_arrays(size: int) -> RallyTrades:
    return RallyTrades(
        np.empty(size, dtype=np.uint64), np.empty(size, dtype=np.int64),
        np.empty(size, dtype=np.float64), np.empty(size, dtype=np.float64), np.empty(size, dtype=np.bool_),
    )


def _copy_batch(batch: cube.ArrowBatch, target: RallyTrades, offset: int) -> int:
    arrays = (target.trade_id, target.timestamp_us, target.price, target.quote_quantity, target.is_buyer_maker)
    stop = offset + batch.num_rows
    if stop > len(target.trade_id):
        raise RallyError(409, 'source_state_changed', 'Native rows exceed their pinned count reservation.')
    for index, column in enumerate(arrays):
        value = cast(NDArray[np.generic], batch.column(index).to_numpy(zero_copy_only=False))
        column[offset:stop] = value
    return stop


def _load_trades(reads: _Reads, plan: _Plan) -> tuple[RallyTrades, int, int]:
    query, external = _raw_selection(reads, plan, plan.bulk_start, plan.bulk_end)
    count = reads.scalar(f'SELECT count() FROM ({query})', external)
    _input_budget(count + len(plan.probes))
    preceding = _trade_arrays(len(plan.probes))
    returned = 0
    for low, high in plan.probes:
        probe, identities = _raw_selection(reads, plan, low, high)
        with reads.batches(f'SELECT * FROM ({probe}) ORDER BY trade_id DESC LIMIT 1', identities) as batches:
            for batch in batches:
                _remaining(reads.deadline)
                returned = _copy_batch(batch, preceding, returned)
    prepend = returned if returned and int(preceding.timestamp_us[0]) < plan.bulk_start else 0
    _resource_guard(reads.deadline, (count + prepend) * _NATIVE_ROW_BYTES)
    trades = _trade_arrays(count + prepend)
    if prepend:
        for source, target in zip(
            (preceding.trade_id, preceding.timestamp_us, preceding.price, preceding.quote_quantity, preceding.is_buyer_maker),
            (trades.trade_id, trades.timestamp_us, trades.price, trades.quote_quantity, trades.is_buyer_maker), strict=True,
        ):
            target[:prepend] = source[:prepend]
    offset = prepend
    with reads.batches(f'SELECT * FROM ({query}) ORDER BY trade_id', external) as batches:
        for batch in batches:
            _remaining(reads.deadline)
            offset = _copy_batch(batch, trades, offset)
    if offset != count + prepend:
        _revalidate(reads, plan)
        raise RallyError(409, 'source_state_changed', 'Native rows differ from their pinned count reservation.')
    return trades, count, returned


def _native_bars(trades: RallyTrades, plan: _Plan, deadline: float) -> tuple[RallyBar, ...]:
    if plan.request.definition.scale == 'bps' or not len(trades.trade_id):
        return ()
    first = int(np.searchsorted(trades.timestamp_us, plan.bulk_start))
    times = trades.timestamp_us[first:]
    if not len(times):
        return ()
    _resource_guard(deadline, len(times) * 9)
    periods = times // _BAR_US * _BAR_US
    starts = np.flatnonzero(np.r_[True, periods[1:] != periods[:-1]])
    stops = np.r_[starts[1:], len(periods)]
    bars: list[RallyBar] = []
    for low, high in zip(starts, stops, strict=True):
        _remaining(deadline)
        start = int(periods[low])
        if start >= plan.bulk_start and start + _BAR_US <= plan.bulk_end:
            prices = trades.price[first + int(low):first + int(high)]
            bars.append(RallyBar(_at(start), _at(start + _BAR_US), float(np.max(prices)), float(np.min(prices)), float(prices[-1])))
    return tuple(bars)


def _detect(trades: RallyTrades, bars: Sequence[RallyBar], plan: _Plan, deadline: float, *, result_id: str) -> RallyDetection:
    _remaining(deadline)
    budget_scope = cast(Callable[[Callable[[], None]], AbstractContextManager[None]], getattr(detector, '_budget_scope'))
    with budget_scope(lambda: _resource_guard(deadline)):
        log.info('market state rally %s detector_call', result_id)
        result = detect_rallies(
            trades=trades, definition=plan.request.definition,
            source=cube.SOURCE, instrument='BTCUSDT',
            analysis_start=plan.request.analysis_start, analysis_end=plan.request.analysis_end,
            known_at=_at(plan.bulk_end), coverage=plan.coverage, bars=bars,
        )
    _remaining(deadline)
    return result


@dataclass(frozen=True)
class _Whole:
    keys: NDArray[np.void]
    counts: NDArray[np.uint64]
    volumes: NDArray[np.float64]

    def get(self, time_index: int, price_index: int) -> tuple[int, float]:
        key = np.array([(time_index, price_index)], dtype=_KEY_DTYPE)
        slot = int(np.searchsorted(self.keys, key)[0])
        if slot >= len(self.keys) or self.keys[slot] != key[0]:
            raise RallyError(409, 'source_state_changed', 'A member cell lacks coherent whole-cell context.')
        return int(self.counts[slot]), float(self.volumes[slot])


def _whole(reads: _Reads, plan: _Plan) -> _Whole:
    start, end = _at(_CUBE_US + plan.base_lower * BASE_TIME_US), _at(_CUBE_US + plan.base_upper * BASE_TIME_US)
    query, external = _union(
        reads, plan.records, cube.CUBE_COMPONENTS, 'time_index, price_index, trade_count, volume',
        f'time_index >= {plan.base_lower} AND time_index < {plan.base_upper}', start, end,
    )
    grouped = f'SELECT time_index, price_index, sum(toUInt64(trade_count)), sumKahan(volume) FROM ({query}) GROUP BY time_index, price_index'
    count = reads.scalar(f'SELECT count() FROM ({grouped})', external)
    if count > MAX_RALLY_INPUT_ROWS or count * 32 > MAX_RALLY_INPUT_BYTES:
        raise RallyError(413, 'rally_input_budget_exceeded', 'Whole-cell column context exceeds admission.', required_rows=count, budget_rows=MAX_RALLY_INPUT_ROWS, required_bytes=count * 32, budget_bytes=MAX_RALLY_INPUT_BYTES)
    _resource_guard(reads.deadline, count * 32)
    keys = np.empty(count, dtype=_KEY_DTYPE)
    counts, volumes = np.empty(count, np.uint64), np.empty(count, np.float64)
    offset = 0
    with reads.batches(f'{grouped} ORDER BY time_index, price_index', external) as batches:
        for batch in batches:
            _remaining(reads.deadline)
            stop = offset + batch.num_rows
            if stop > count:
                raise RallyError(409, 'source_state_changed', 'Whole-cell rows exceed their pinned count.')
            for index, column in enumerate((keys['time'], keys['price'], counts, volumes)):
                column[offset:stop] = cast(NDArray[np.generic], batch.column(index).to_numpy(zero_copy_only=False))
            offset = stop
    if offset != count:
        _revalidate(reads, plan)
        raise RallyError(409, 'source_state_changed', 'Whole-cell rows differ from their pinned count.')
    return _Whole(keys, counts, volumes)


def _member_quotes(
    quotes: NDArray[np.float64], deadline: float, makers: NDArray[np.bool_] | None = None,
) -> Iterator[float]:
    for index, value in enumerate(quotes):
        if index % 4096 == 0:
            _resource_guard(deadline)
        if makers is None or not bool(makers[index]):
            yield float(value)


def _member_rows(
    trades: RallyTrades, event: RallyEvent, whole: _Whole, deadline: float,
) -> Iterator[dict[str, object]]:
    first = int(np.searchsorted(trades.trade_id, np.uint64(event.start_trade_id)))
    stop = int(np.searchsorted(trades.trade_id, np.uint64(event.end_trade_id), side='right'))
    if stop - first != event.trade_count:
        raise RallyError(409, 'source_state_changed', 'Event endpoints do not delimit their exact native members.')
    _resource_guard(deadline, (stop - first) * 72)
    times = (trades.timestamp_us[first:stop] - _CUBE_US) // BASE_TIME_US
    prices = np.floor(trades.price[first:stop] / BASE_PRICE_USDT).astype(np.uint64)
    order = np.lexsort((prices, times))
    sorted_times, sorted_prices = times[order], prices[order]
    starts = np.flatnonzero(np.r_[True, (sorted_times[1:] != sorted_times[:-1]) | (sorted_prices[1:] != sorted_prices[:-1])])
    ends = np.r_[starts[1:], len(order)]
    for low, high in zip(starts, ends, strict=True):
        _remaining(deadline)
        positions = order[int(low):int(high)] + first
        time_index, price_index = int(sorted_times[low]), int(sorted_prices[low])
        whole_count, whole_volume = whole.get(time_index, price_index)
        quotes, makers = trades.quote_quantity[positions], trades.is_buyer_maker[positions]
        count = len(positions)
        if count > whole_count:
            raise RallyError(409, 'source_state_changed', 'Event membership exceeds its whole-cell count.')
        opening, closing = int(positions[0]), int(positions[-1])
        yield {
            'rally_id': event.rally_id, 'base_time_index': time_index, 'base_price_index': price_index,
            'volume': math.fsum(_member_quotes(quotes, deadline)), 'trade_count': count,
            'taker_buy_volume': math.fsum(_member_quotes(quotes, deadline, makers)),
            'taker_buy_trade_count': count - int(np.count_nonzero(makers)),
            'whole_base_trade_count': whole_count, 'whole_base_volume': whole_volume,
            'partial': count < whole_count,
            'first_trade_id': int(trades.trade_id[opening]), 'last_trade_id': int(trades.trade_id[closing]),
            'first_at': _at(int(trades.timestamp_us[opening])), 'last_at': _at(int(trades.timestamp_us[closing])),
        }


def _event_record(event: RallyEvent) -> dict[str, object]:
    return {field.name: cast(object, getattr(event, field.name)) for field in fields(event)}


def _hash_json(row: Mapping[str, object], excluded: Set[str] = frozenset[str]()) -> bytes:
    values: dict[str, object] = {}
    for name, value in row.items():
        if name not in excluded:
            if value is None:
                values[name] = None
            elif name in _UINT_FIELDS:
                values[name] = str(value)
            elif name in _FLOAT_FIELDS:
                values[name] = cast(float, value).hex()
            elif name in _TIME_FIELDS:
                values[name] = _z(cast(datetime, value))
            else:
                values[name] = value
    return json.dumps(values, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('utf-8')


def _schemas(metadata: dict[str, str]) -> tuple[cube.ArrowSchema, cube.ArrowSchema, cube.ArrowSchema]:
    names = tuple(field.name for field in fields(RallyEvent))
    event_fields: list[object] = []
    for name in names:
        kind = pa.uint64() if name in _UINT_FIELDS else pa.float64() if name in _FLOAT_FIELDS else pa.timestamp('us', 'UTC') if name in _TIME_FIELDS else pa.string()
        event_fields.append(pa.field(name, kind, nullable=name == 'anchor_at'))
    event_fields += [pa.field('event_evidence_hash', pa.string(), nullable=False), pa.field('evidence_version', pa.uint8(), nullable=False)]
    cell_fields = [('rally_id', pa.string()), ('base_time_index', pa.uint64()), ('base_price_index', pa.uint64())]
    cell_fields += [(name, pa.float64() if name in ('volume', 'taker_buy_volume', 'whole_base_volume') else pa.bool_() if name == 'partial' else pa.timestamp('us', 'UTC') if name in ('first_at', 'last_at') else pa.uint64()) for name in (
        'volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count', 'whole_base_trade_count',
        'whole_base_volume', 'partial', 'first_trade_id', 'last_trade_id', 'first_at', 'last_at',
    )]
    summary_fields = [(name, pa.uint64()) for name in ('rally_count', 'left_censored_count', 'right_censored_count', 'unknown_context_count')]
    summary_fields.append(('diagnostics_as_of', pa.timestamp('us', 'UTC')))
    return (
        pa.schema(event_fields).with_metadata(metadata),
        pa.schema([pa.field(name, kind, nullable=False) for name, kind in cell_fields]).with_metadata(metadata),
        pa.schema([pa.field(name, kind, nullable=False) for name, kind in summary_fields]).with_metadata(metadata),
    )


def _output_guard(staging: Path, guard: Callable[[int], None], deadline: float) -> int:
    _remaining(deadline)
    size = sum((staging / name).stat().st_size for name in RALLY_FILES if (staging / name).exists())
    if size > MAX_RALLY_OUTPUT_BYTES:
        raise RallyError(413, 'rally_output_budget_exceeded', 'The canonical result exceeds its output budget.', required_bytes=size, budget_bytes=MAX_RALLY_OUTPUT_BYTES)
    guard(size)
    return size


def _write_rows(
    path: Path, schema: cube.ArrowSchema, rows: Iterator[dict[str, object]],
    staging: Path, guard: Callable[[int], None], deadline: float,
) -> None:
    with ipc.new_file(str(path), schema) as writer:
        held: list[dict[str, object]] = []
        for row in rows:
            held.append(row)
            if len(held) >= cube.BATCH_ROWS:
                writer.write_batch(pa.RecordBatch.from_pylist(held, schema=schema))
                held.clear()
                _output_guard(staging, guard, deadline)
        if held:
            writer.write_batch(pa.RecordBatch.from_pylist(held, schema=schema))
    _output_guard(staging, guard, deadline)


def _write_members(
    trades: RallyTrades, events: Sequence[RallyEvent], whole: _Whole,
    staging: Path, schema: cube.ArrowSchema, guard: Callable[[int], None], deadline: float,
) -> dict[str, str]:
    hashes: dict[str, str] = {}

    def rows() -> Iterator[dict[str, object]]:
        for event in sorted(events, key=lambda value: value.rally_id):
            _remaining(deadline)
            digest = hashlib.sha256(b'rally_evidence_v1\n')
            digest.update(_hash_json(_event_record(event)))
            digest.update(b'\nmember_trade_ids_le_u64\n')
            first = int(np.searchsorted(trades.trade_id, np.uint64(event.start_trade_id)))
            stop = int(np.searchsorted(trades.trade_id, np.uint64(event.end_trade_id), side='right'))
            for offset in range(first, stop, cube.BATCH_ROWS):
                ids = trades.trade_id[offset:min(offset + cube.BATCH_ROWS, stop)].astype(_LE_U64, copy=False)
                digest.update(ids.data.cast('B'))
                _remaining(deadline)
            digest.update(b'\nmember_base_rows\n')
            for row in _member_rows(trades, event, whole, deadline):
                digest.update(_hash_json(row, {'whole_base_trade_count', 'whole_base_volume', 'partial', 'rally_id'}))
                digest.update(b'\n')
                yield row
            hashes[event.rally_id] = digest.hexdigest()

    _write_rows(staging / RALLY_FILES[1], schema, rows(), staging, guard, deadline)
    return hashes


def _revalidate(reads: _Reads, plan: _Plan) -> None:
    fence_deadline = min(reads.deadline, time.monotonic() + cube.FENCE_WAIT_SECONDS)
    relevant_keys = {record.partition.key for record in plan.records}
    while True:
        _remaining(reads.deadline)
        try:
            with source_lock(reads.runtime.lock_root, cube.SOURCE, 'heavy', shared=True):
                current = {record.partition.key: record for record in _accepted(reads)}
                for record in plan.records:
                    now = current.get(record.partition.key)
                    if now is None or now.revision != record.revision or now.build_id != record.build_id:
                        raise RallyError(409, 'source_state_changed', 'A relevant raw identity changed.', partition_key=record.partition.key)
                    if cube.CUBE_COMPONENTS[now.partition.provisional] not in dict(now.component_hashes):
                        raise RallyError(409, 'source_state_changed', 'Relevant cube coverage became unavailable.', partition_key=record.partition.key)
                for low, high in plan.required:
                    cursor = low
                    for record in sorted(current.values(), key=lambda value: value.partition.start):
                        start, end = cube.utc_micros(record.partition.start), cube.utc_micros(record.partition.end)
                        if end > cursor and start <= cursor and record.partition.key in relevant_keys:
                            cursor = end
                    if cursor < high:
                        raise RallyError(409, 'source_state_changed', 'Previously admitted coverage is missing.')
                external = cube.external_identities()
                cube.add_identities(external, 'reads', plan.records)
                reclaimed = reads.scalar(
                    f"SELECT count() FROM {reads.runtime.store.table('source_cleanup_log')} WHERE source_key='{cube.SOURCE}' AND build_id IN (SELECT build_id FROM reads)", external,
                )
                if reclaimed:
                    raise SourceError('SOURCE_MAINTENANCE', 'A cleanup reclaimed a build this discovery read.')
            return
        except SourceError as error:
            if error.code != 'SOURCE_LOCK_BUSY':
                raise
            if time.monotonic() >= fence_deadline:
                _remaining(reads.deadline)
                raise SourceError('SOURCE_MAINTENANCE', 'Cleanup held the source fence past its wait budget.') from error
            time.sleep(min(0.5, _remaining(reads.deadline)))


def write_rally_result(
    runtime: SourceRuntime,
    request: RallyDiscoveryRequest,
    staging: Path,
    *,
    result_id: str,
    guard: Callable[[int], None],
    deadline: float,
) -> dict[str, object]:
    _validate_request(request)
    reads = _Reads(runtime, deadline, result_id)
    try:
        plan = _plan(request, reads)
        source_digest = _source_digest(plan)
        trades, bulk_rows, probe_rows = _load_trades(reads, plan)
        bars = _native_bars(trades, plan, deadline)
        detected = _detect(trades, bars, plan, deadline, result_id=result_id)
        counts = Counter(diagnostic.status for diagnostic in detected.diagnostics)
        reasons = Counter(diagnostic.reason for diagnostic in detected.diagnostics)
        normalized = _normalized(request.definition)
        bulk = {'start': _z(_at(plan.bulk_start)), 'end': _z(_at(plan.bulk_end)), 'rows': bulk_rows}
        probes = {'max_lookback_seconds': 86400, 'max_rows_per_anchor': 1, 'returned_rows': probe_rows}
        analysis = {'start': _z(request.analysis_start), 'end': _z(request.analysis_end)}
        diagnostic_counts = {f'{name}_count': counts[name] for name in ('left_censored', 'right_censored', 'unknown_context')}
        common: dict[str, object] = {
            'schema_version': 1, 'membership_version': 1, 'result_id': result_id,
            'source': cube.SOURCE, 'instrument': 'BTCUSDT',
            'definition_version': detector.DEFINITION_VERSION,
            'definition_fingerprint': definition_fingerprint(request.definition), 'normalized_definition': normalized,
            'analysis': analysis, 'observation_ceiling': _z(_at(plan.bulk_end)),
            'data_cutoff': cube.iso(plan.state.cutoff), 'canonical_through': cube.iso(plan.state.canonical_through),
            'pack_pin_digest': request.expected_state.pack_pin_digest, 'relevant_pin_digest': source_digest,
            'relevant_pins': sorted([record.partition.key, record.revision, str(record.build_id)] for record in plan.records),
            'bulk_read': bulk, 'reference_probe': probes,
            'detector_build': hashlib.sha256(Path(detector.__file__).read_bytes()).hexdigest(),
            'source_builds': sorted({str(record.build_id) for record in plan.records}),
            'diagnostics_as_of': _z(_at(plan.bulk_end)), 'diagnostic_counts': diagnostic_counts,
            'diagnostic_reasons': dict(sorted(reasons.items())), 'evidence_version': 1,
            'available_event_measures': list(AVAILABLE_EVENT_MEASURES),
            'unavailable_event_measures': list(UNAVAILABLE_EVENT_MEASURES),
        }
        metadata = {METADATA_KEY: json.dumps(common, sort_keys=True, separators=(',', ':'), ensure_ascii=True)}
        events_schema, cells_schema, summary_schema = _schemas(metadata)
        whole = _whole(reads, plan) if detected.events else _Whole(np.empty(0, _KEY_DTYPE), np.empty(0, np.uint64), np.empty(0, np.float64))
        hashes = _write_members(trades, detected.events, whole, staging, cells_schema, guard, deadline)
        events = iter(
            {**_event_record(event), 'event_evidence_hash': hashes[event.rally_id], 'evidence_version': 1}
            for event in sorted(detected.events, key=lambda value: (value.start_at, value.rally_id))
        )
        _write_rows(staging / RALLY_FILES[0], events_schema, events, staging, guard, deadline)
        summary: dict[str, object] = {'rally_count': len(detected.events), **diagnostic_counts, 'diagnostics_as_of': _at(plan.bulk_end)}
        _write_rows(staging / RALLY_FILES[2], summary_schema, iter([summary]), staging, guard, deadline)
        _revalidate(reads, plan)
        size = _output_guard(staging, guard, deadline)
        return {
            'result_id': result_id,
            'definition_version': detector.DEFINITION_VERSION, 'definition_fingerprint': common['definition_fingerprint'],
            'membership_version': 1, 'analysis': analysis,
            'observation_ceiling': common['observation_ceiling'], 'data_cutoff': common['data_cutoff'],
            'canonical_through': common['canonical_through'], 'pack_pin_digest': common['pack_pin_digest'],
            'relevant_pin_digest': source_digest, 'bulk_read': bulk, 'reference_probe': probes,
            'rally_count': len(detected.events), **diagnostic_counts, 'diagnostics_as_of': common['diagnostics_as_of'],
            'available_event_measures': list(AVAILABLE_EVENT_MEASURES), 'unavailable_event_measures': list(UNAVAILABLE_EVENT_MEASURES),
            'input_bytes': (bulk_rows + probe_rows) * _NATIVE_ROW_BYTES, 'output_bytes': size,
        }
    finally:
        reads.close()
