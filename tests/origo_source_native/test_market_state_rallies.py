"""Cube rally contract against unchanged pinned production records and native receipts."""
from __future__ import annotations

import gzip
import hashlib
import importlib
import inspect
import json
import math
import os
import sqlite3
import shutil
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import cache
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Protocol, cast
from uuid import UUID, uuid4

import numpy as np
import polars as pl
import pytest
from numpy.typing import NDArray

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import market_state, market_state_rallies, market_state_reader, market_state_results
from origo.query.market_state import RequestError, iso
from origo.query.market_state_rallies import RALLY_FILES, RallyDiscoveryRequest, RallyError, parse_rally_request, write_rally_result
from origo.query.market_state_reader import read_table
from origo.query.market_state_results import DiskSample, ResultStore, parse_result_path
from origo.query import rally_detection
from origo.query.rally_detection import RallyDefinition, RallyTrades, definition_fingerprint, detect_rallies
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Row
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.storage import SourceStore
from origo.workers.market_state_api import MarketStateApi, serve
from origo.workers.report import Reporter

from . import test_binance_rallies as legacy_fixture
from .test_rally_detection import test_legacy_r30v1_outputs_match_baseline as _legacy_baseline

FIXTURES = Path(__file__).parent / 'fixtures/market_state_rallies'
SOURCE = 'binance_spot_trades'
START = datetime(2026, 6, 27, 11, 39, tzinfo=UTC)
END = datetime(2026, 6, 27, 11, 55, tzinfo=UTC)
BASE_US = 56_250_000
T0_US = 1_609_459_200_000_000
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ABS_TOLERANCE, REL_TOLERANCE = 1e-8, 1e-12


def _object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _list(value: object) -> list[object]:
    assert isinstance(value, list)
    return cast(list[object], value)


def _integer(value: object) -> int:
    assert isinstance(value, int) and not isinstance(value, bool)
    return value


def _time(value: object) -> datetime:
    assert isinstance(value, str)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=UTC)


def _us(moment: datetime) -> int:
    delta = moment - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


@cache
def _proof() -> dict[str, object]:
    return _object(json.loads((FIXTURES / 'provenance.json').read_text()))


@cache
def _state() -> dict[str, object]:
    capture = FIXTURES / 'source-state.json.gz'
    evidence = _object(_proof()['source_state'])
    assert hashlib.sha256(capture.read_bytes()).hexdigest() == evidence['sha256']
    plain = gzip.decompress(capture.read_bytes())
    assert hashlib.sha256(plain).hexdigest() == evidence['uncompressed_sha256']
    return _object(json.loads(plain))


def _rows(key: str) -> list[Row]:
    section = _object(_state()[key])
    columns = [_list(column) for column in _list(section['columns'])]
    result: list[Row] = []
    for raw in _list(section['rows']):
        values: list[object] = []
        for column, value in zip(columns, _list(raw), strict=True):
            kind = str(column[1])
            if kind.startswith('DateTime64'):
                values.append(_time(value))
            elif kind == 'UUID':
                values.append(UUID(str(value)))
            else:
                values.append(value)
        result.append(tuple(values))
    return result


@cache
def _frame(name: str) -> pl.DataFrame:
    path = FIXTURES / name
    entry = _object(_object(_proof()['files'])[name])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entry['sha256']
    result = pl.read_parquet(path)
    assert result.height == entry['row_count']
    return result


def _native(name: str = 'june-27-raw.parquet') -> RallyTrades:
    frame = _frame(name)
    return RallyTrades(
        frame['trade_id'].to_numpy(), frame['datetime'].dt.epoch('us').to_numpy(),
        frame['price'].to_numpy(), frame['quote_quantity'].to_numpy(),
        frame['is_buyer_maker'].cast(pl.Boolean).to_numpy(),
    )


def _body(
    *, mode: str = 'first_hit', scale: str = 'bps', target: object = 30,
    start: datetime = START, end: datetime = END,
) -> dict[str, object]:
    definition: dict[str, object] = {'mode': mode, 'scale': scale, 'target': target}
    if mode != 'swing':
        definition['anchor_minutes'] = 1
    if mode == 'controlled_advance':
        definition['pullback'] = 10 if scale == 'bps' else 0.5
    if mode == 'swing':
        definition['reversal'] = 1 if scale == 'bps' else 0.1
    proof = _object(_proof()['source_state'])
    return {
        'definition': definition, 'analysis': {'start': iso(start), 'end': iso(end)},
        'expected_state': {'data_cutoff': proof['data_cutoff'], 'pack_pin_digest': proof['pack_pin_digest']},
    }


@dataclass
class Clock:
    now: int

    def __call__(self) -> int:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += seconds * 1_000_000_000


def _roomy(path: Path) -> DiskSample:
    return DiskSample(total=10**13, free=10**13, inodes=10**7, free_inodes=10**7)



class ReceiptReporter(Reporter):
    def __init__(self) -> None:
        super().__init__('http://127.0.0.1:9', timeout_seconds=0.1)
        self.calls: list[tuple[str, Mapping[str, object]]] = []

    def materialized(self, asset_key: str, *, partition: str | None, metadata: Mapping[str, object]) -> bool:
        self.calls.append((asset_key, dict(metadata)))
        return True

@dataclass
class NativeService:
    runtime: SourceRuntime
    store: ResultStore
    clock: Clock
    api: MarketStateApi
    url: str

    def post(self, route: str, body: object) -> tuple[int, dict[str, object]]:
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        request = urllib.request.Request(self.url + route, data=raw, headers={'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                return response.status, _object(json.loads(response.read()))
        except urllib.error.HTTPError as error:
            return error.code, _object(json.loads(error.read()))

    def discover(self, body: dict[str, object] | None = None) -> dict[str, object]:
        status, response = self.post('/v1/market-state/rallies', _body() if body is None else body)
        assert status == 200, response
        return response


@pytest.fixture
def native_service(origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[NativeService]:
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', BINANCE_SPOT_TRADES_SPEC)
    anchor = _rows('anchors')[0][1]
    assert isinstance(anchor, datetime)
    store.setup(anchor=anchor)
    for key, table in (
        ('activation', 'source_activation_log'), ('component', 'source_component_log'),
        ('startup_component', 'source_component_log'), ('build', 'source_build_log'),
        ('cleanup', 'source_cleanup_log'), ('rollout', 'source_component_rollout_log'),
    ):
        rows = _rows(key)
        if rows:
            client.execute(f'INSERT INTO origo.{table} VALUES', rows, settings={'max_partitions_per_insert_block': 10000})
    for raw, cells in (
        ('june-27-raw.parquet', 'june-27-cells.parquet'),
        ('march-24-empty-raw.parquet', 'march-24-cells.parquet'),
        ('startup-raw.parquet', 'startup-cells.parquet'),
    ):
        for name, table in ((raw, 'raw'), (cells, 'market_state')):
            frame = _frame(name)
            values: list[Row] = []
            for row in frame.iter_rows():
                values.append((*row[:3], UUID(str(row[3])), *row[4:]))
            client.execute(f'INSERT INTO origo.binance_spot_trades_{table}_revisions VALUES', values)
    locks = tmp_path / 'locks'
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(locks))
    runtime = SourceRuntime(BINANCE_SPOT_TRADES_SPEC, store, locks, 'captured-native-fixture')
    runtime.setup(anchor=anchor)
    clock = Clock(1_900_000_000_000_000_000)
    results = ResultStore(tmp_path / 'results-store', clock=clock, disk=_roomy)
    api = MarketStateApi(results, ReceiptReporter(), locks)
    server = serve(api, port=0)
    api.port = server.server_address[1]
    try:
        yield NativeService(runtime, results, clock, api, f'http://127.0.0.1:{server.server_address[1]}')
    finally:
        server.shutdown()
        server.server_close()
        client.disconnect()


def _metadata(path: object) -> dict[str, object]:
    assert isinstance(path, str)
    reader = _ipc().open_file(path)
    metadata = reader.schema.metadata
    assert metadata is not None
    return _object(json.loads(metadata[b'origo.market_state_rallies']))


class ArrowSchema(Protocol):
    @property
    def metadata(self) -> dict[bytes, bytes] | None: ...
    def field(self, name: str) -> object: ...


class ArrowTable(Protocol):
    @property
    def schema(self) -> ArrowSchema: ...
    @property
    def num_rows(self) -> int: ...
    def to_pylist(self) -> list[dict[str, object]]: ...


class ArrowReader(Protocol):
    @property
    def schema(self) -> ArrowSchema: ...
    def read_all(self) -> ArrowTable: ...


class ArrowIPC(Protocol):
    def open_file(self, path: object) -> ArrowReader: ...


def _ipc() -> ArrowIPC:
    return cast(ArrowIPC, importlib.import_module('pyarrow.ipc'))


def _table(response: Mapping[str, object], key: str) -> pl.DataFrame:
    return pl.read_ipc(str(response[key]))


class DeadlineReads(Protocol):
    def connect(self) -> object: ...
    def close(self) -> None: ...


class DeadlinePool(Protocol):
    def urlopen(self, method: str, url: str, *, preload_content: bool) -> object: ...
    def close(self) -> None: ...


class Benchmark(Protocol):
    def frozen_manifest(self) -> dict[str, object]: ...
    def validate_report(self, report: Mapping[str, object], manifest: Mapping[str, object] | None = None, evidence_root: Path | None = None) -> list[str]: ...


def _wire_bytes(record: Mapping[str, object]) -> bytes:
    unsigned = {'reference_trade_id', 'start_trade_id', 'end_trade_id', 'confirmation_trade_id', 'trade_count', 'taker_buy_trade_count', 'base_time_index', 'base_price_index', 'first_trade_id', 'last_trade_id'}
    converted: dict[str, object] = {}
    for name, value in record.items():
        if value is None:
            converted[name] = None
        elif name in unsigned:
            converted[name] = str(value)
        elif isinstance(value, float):
            converted[name] = value.hex()
        elif isinstance(value, datetime):
            converted[name] = value.astimezone(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        else:
            converted[name] = value
    return json.dumps(converted, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def _evidence_oracle(event: Mapping[str, object], cells: pl.DataFrame, ids: NDArray[np.uint64]) -> str:
    digest = hashlib.sha256(b'rally_evidence_v1\n')
    digest.update(_wire_bytes({name: value for name, value in event.items() if name not in {'event_evidence_hash', 'evidence_version'}}))
    digest.update(b'\nmember_trade_ids_le_u64\n')
    digest.update(ids.astype('<u8', copy=False).tobytes())
    digest.update(b'\nmember_base_rows\n')
    for row in cells.sort('base_time_index', 'base_price_index').to_dicts():
        digest.update(_wire_bytes({name: value for name, value in row.items() if name not in {'whole_base_trade_count', 'whole_base_volume', 'partial', 'rally_id'}}))
        digest.update(b'\n')
    return digest.hexdigest()


@cache
def _native_bars() -> dict[int, tuple[float, float, float]]:
    trades = _native()
    buckets = trades.timestamp_us // 900_000_000
    boundaries = np.flatnonzero(np.r_[True, buckets[1:] != buckets[:-1]])
    stops = np.r_[boundaries[1:], len(buckets)]
    result: dict[int, tuple[float, float, float]] = {}
    earliest = _us(datetime(2026, 6, 27, 7, 44, 3, 750000, tzinfo=UTC))
    latest = _us(datetime(2026, 6, 27, 11, 55, 18, 750000, tzinfo=UTC))
    for first, stop in zip(boundaries, stops, strict=True):
        bucket = int(buckets[first])
        if bucket * 900_000_000 >= earliest and (bucket + 1) * 900_000_000 <= latest:
            prices = trades.price[first:stop]
            result[bucket] = (float(prices.max()), float(prices.min()), float(prices[-1]))
    return result


def _frozen_atr(freeze_us: int) -> float:
    edge = freeze_us // 900_000_000
    native = _native_bars()
    required = [native[bucket] for bucket in range(edge - 15, edge)]
    previous_close = required[0][2]
    ranges: list[float] = []
    for high, low, close in required[1:]:
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        previous_close = close
    assert len(ranges) == 14
    return math.fsum(ranges) / 14.0


def _oracle(definition: RallyDefinition, trades: RallyTrades, start: datetime, end: datetime) -> list[tuple[int, int, int, int | None]]:
    """Raw native scans return reference/start/end/confirmation positions."""
    first = int(np.searchsorted(trades.timestamp_us, _us(start), side='left'))
    stop = int(np.searchsorted(trades.timestamp_us, _us(end), side='left'))
    if definition.mode == 'swing':
        assert definition.reversal is not None
        high = first
        retreat = None
        for index in range(first + 1, stop):
            if trades.price[index] > trades.price[high]:
                high = index
            retreats = (
                float(trades.price[index]) <= float(trades.price[high]) * (1.0 - float(definition.reversal) / 10000.0)
                if definition.scale == 'bps' else float(trades.price[high]) - float(trades.price[index]) >= _frozen_atr(int(trades.timestamp_us[high])) * float(definition.reversal)
            )
            if retreats:
                retreat = index
                break
        if retreat is None:
            return []
        events: list[tuple[int, int, int, int | None]] = []
        cursor = retreat
        while cursor < stop:
            trough = cursor
            qualification = None
            for index in range(cursor + 1, stop):
                if trades.price[index] < trades.price[trough]:
                    trough = index
                threshold = (float(trades.price[trough]) * (1.0 + float(definition.target) / 10000.0)
                             if definition.scale == 'bps' else float(trades.price[trough]) + _frozen_atr(int(trades.timestamp_us[trough])) * float(definition.target))
                if trades.price[index] >= threshold:
                    qualification = index
                    break
            if qualification is None:
                break
            peak = qualification
            confirmation = None
            for index in range(qualification + 1, stop):
                if trades.price[index] > trades.price[peak]:
                    peak = index
                reverses = (
                    float(trades.price[index]) <= float(trades.price[peak]) * (1.0 - float(definition.reversal) / 10000.0)
                    if definition.scale == 'bps' else float(trades.price[peak]) - float(trades.price[index]) >= _frozen_atr(int(trades.timestamp_us[trough])) * float(definition.reversal)
                )
                if reverses:
                    confirmation = index
                    break
            if confirmation is None:
                break
            events.append((trough, trough, peak, confirmation))
            cursor = confirmation
        return events
    assert definition.anchor_minutes is not None
    step = definition.anchor_minutes * 60_000_000
    events = []
    for anchor in range(((_us(start) + step - 1) // step) * step, _us(end), step):
        member_start = int(np.searchsorted(trades.timestamp_us, anchor, side='left'))
        reference = member_start - 1
        if reference < 0:
            continue
        reference_price = float(trades.price[reference])
        distance = None if definition.scale == 'bps' else _frozen_atr(anchor)
        threshold = reference_price * (1.0 + float(definition.target) / 10000.0) if distance is None else reference_price + distance * float(definition.target)
        barrier = None if definition.pullback is None else (reference_price * float(definition.pullback) / 10000.0 if distance is None else distance * float(definition.pullback))
        high = reference_price
        limit = int(np.searchsorted(trades.timestamp_us, min(anchor + 240 * 60_000_000, _us(end)), side='left'))
        for index in range(member_start, limit):
            high = max(high, float(trades.price[index]))
            if barrier is not None and high - float(trades.price[index]) > barrier:
                break
            if trades.price[index] >= threshold:
                events.append((reference, member_start, index, index))
                break
    return events


def _measure_rows(event: dict[str, object], trades: RallyTrades) -> dict[tuple[int, int], tuple[float, int, float, int, int, int]]:
    first = int(np.searchsorted(trades.trade_id, _integer(event['start_trade_id'])))
    last = int(np.searchsorted(trades.trade_id, _integer(event['end_trade_id'])))
    members: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index in range(first, last + 1):
        cell = ((int(trades.timestamp_us[index]) - T0_US) // BASE_US, int(float(trades.price[index]) // 125.0))
        members[cell].append(index)
    result: dict[tuple[int, int], tuple[float, int, float, int, int, int]] = {}
    for cell, indices in members.items():
        buys = [index for index in indices if not trades.is_buyer_maker[index]]
        result[cell] = (
            math.fsum(float(trades.quote_quantity[index]) for index in indices), len(indices),
            math.fsum(float(trades.quote_quantity[index]) for index in buys), len(buys),
            int(trades.trade_id[indices[0]]), int(trades.trade_id[indices[-1]]),
        )
    return result


def _assert_native_membership(response: Mapping[str, object], definition: RallyDefinition) -> None:
    trades = _native()
    events = _table(response, 'rallies').to_dicts()
    expected = _oracle(definition, trades, START, END)
    assert [(event['reference_trade_id'], event['start_trade_id'], event['end_trade_id'], event['confirmation_trade_id']) for event in events] == [
        (int(trades.trade_id[reference]), int(trades.trade_id[first]), int(trades.trade_id[last]), int(trades.trade_id[confirmation]))
        for reference, first, last, confirmation in expected if confirmation is not None
    ]
    cells = _table(response, 'rally_cells')
    whole = {(row[4], row[5]): (row[7], row[8]) for row in _frame('june-27-cells.parquet').iter_rows()}
    for event in events:
        assert isinstance(event['rally_id'], str)
        membership = _measure_rows(event, trades)
        actual = cells.filter(pl.col('rally_id') == event['rally_id']).to_dicts()
        assert [(row['base_time_index'], row['base_price_index']) for row in actual] == sorted(membership)
        for row in actual:
            key = (_integer(row['base_time_index']), _integer(row['base_price_index']))
            volume, count, buy_volume, buy_count, first_id, last_id = membership[key]
            assert math.isclose(float(row['volume']), volume, abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE)
            assert math.isclose(float(row['taker_buy_volume']), buy_volume, abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE)
            assert (row['trade_count'], row['taker_buy_trade_count'], row['first_trade_id'], row['last_trade_id']) == (count, buy_count, first_id, last_id)
            assert row['whole_base_trade_count'] == whole[key][1]
            assert math.isclose(float(row['whole_base_volume']), float(whole[key][0]), abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE)
            assert row['partial'] == (count < _integer(whole[key][1]))



def _withdraw(service: NativeService, key: str, *, after_generation: int | None = None) -> None:
    predicate = f' AND generation > {after_generation}' if after_generation is not None else ''
    service.runtime.store.client.execute(
        'ALTER TABLE origo.source_activation_log DELETE WHERE source_key=%(source)s AND partition_key=%(key)s' + predicate,
        {'source': SOURCE, 'key': key}, settings={'mutations_sync': 2},
    )


def _restore(service: NativeService, key: str, *, after_generation: int | None = None) -> None:
    rows = [row for row in _rows('activation') if row[1] == key and (after_generation is None or _integer(row[5]) > after_generation)]
    assert rows
    service.runtime.store.execute('INSERT INTO origo.source_activation_log VALUES', rows)


def _at_final_staging(service: NativeService, monkeypatch: pytest.MonkeyPatch, action: Callable[[], None]) -> list[bool]:
    original = cast(Callable[[str, int, int, float], None], getattr(service.api, '_rally_staging'))
    called: list[bool] = []

    def guard(identity: str, size: int, floor: int, deadline: float) -> None:
        if not called and all((service.store.staging / identity / name).is_file() for name in RALLY_FILES):
            action()
            called.append(True)
        original(identity, size, floor, deadline)

    monkeypatch.setattr(service.api, '_rally_staging', guard)
    return called

def test_compact_pack_commitment_through_http_handler(native_service: NativeService) -> None:
    state = _object(_proof()['source_state'])
    assert _integer(state['held_pin_bytes']) > market_state.MAX_BODY_BYTES
    assert _integer(state['held_cube_pins']) > 2000
    body = _body()
    assert len(json.dumps(body).encode()) < 1024
    response = native_service.discover(body)
    assert response['pack_pin_digest'] == state['pack_pin_digest']
    for name in RALLY_FILES:
        assert (native_service.store.results / str(response['result_id']) / name).is_file()
    for raw in (b'x' * (market_state.MAX_BODY_BYTES + 1), b'{', b'{"definition":1,"definition":2}'):
        status, _ = native_service.post('/v1/market-state/rallies', raw)
        assert status == 400
    invalid = {**body, 'expected_state': {**_object(body['expected_state']), 'pack_pin_digest': '0' * 64}}
    status, answer = native_service.post('/v1/market-state/rallies', invalid)
    assert status == 409 and answer['error'] == 'pack_state_changed'


def test_server_context_and_two_state_identities(native_service: NativeService, monkeypatch: pytest.MonkeyPatch) -> None:
    response = native_service.discover()
    metadata = _metadata(response['rallies'])
    assert metadata['pack_pin_digest'] == response['pack_pin_digest']
    assert metadata['relevant_pin_digest'] == response['relevant_pin_digest']
    assert response['pack_pin_digest'] != response['relevant_pin_digest']
    def z(moment: datetime) -> str:
        return moment.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    source_fields = {'bulk_read': [z(START), z(END)], 'reference_windows': [[z(START - timedelta(days=1)), z(START)]], 'pins': metadata['relevant_pins']}
    independent_digest = hashlib.sha256(b'rally_source_v1\n' + json.dumps(source_fields, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    assert response['relevant_pin_digest'] == independent_digest
    relevant = [_list(row) for row in _list(metadata['relevant_pins'])]
    assert relevant and len(relevant) < _integer(_object(_proof()['source_state'])['held_cube_pins'])
    held = {str(row[0]): (row[5], str(row[6])) for row in _rows('accepted')}
    assert all((row[1], row[2]) == held[str(row[0])] for row in relevant)
    activations = [row for row in _rows('activation') if row[1] == '2026-06-27']
    assert {row[5] for row in activations} >= {1, 2, 3}
    assert len({(row[6], row[7]) for row in activations}) == 1
    _withdraw(native_service, '2026-06-27', after_generation=2)
    try:
        generation_two = native_service.discover()
    finally:
        _restore(native_service, '2026-06-27', after_generation=2)
    generation_three = native_service.discover()
    for current in (generation_two, generation_three):
        assert _table(current, 'rallies')['rally_id'].to_list() == _table(response, 'rallies')['rally_id'].to_list()
        assert _table(current, 'rallies')['event_evidence_hash'].to_list() == _table(response, 'rallies')['event_evidence_hash'].to_list()
        assert current['pack_pin_digest'] == response['pack_pin_digest']
        assert current['relevant_pin_digest'] == response['relevant_pin_digest']
    unrelated = '2026-06-25'
    assert unrelated not in {str(row[0]) for row in relevant}
    called = _at_final_staging(native_service, monkeypatch, lambda: _withdraw(native_service, unrelated))
    try:
        while_unrelated_missing = native_service.discover()
        assert called == [True]
        assert while_unrelated_missing['relevant_pin_digest'] == response['relevant_pin_digest']
        assert _table(while_unrelated_missing, 'rallies')['event_evidence_hash'].to_list() == _table(response, 'rallies')['event_evidence_hash'].to_list()
    finally:
        _restore(native_service, unrelated)


def test_bulk_span_reference_probe_and_cube_start(native_service: NativeService) -> None:
    response = native_service.discover(_body(scale='atr', target=0.1))
    bulk = _object(response['bulk_read'])
    assert _time(bulk['start']) == datetime(2026, 6, 27, 7, 45, tzinfo=UTC)
    assert _time(bulk['end']) == END
    probe = _object(response['reference_probe'])
    assert probe['max_lookback_seconds'] == 86400 and probe['max_rows_per_anchor'] == 1
    assert _integer(probe['returned_rows']) <= 16
    for body, reason in (
        (_body(start=datetime(2020, 12, 31, tzinfo=UTC)), 'analysis_before_cube_start'),
        (_body(start=START, end=START + timedelta(hours=48, microseconds=1)), 'analysis_read_span_exceeded'),
        (_body(scale='atr', target=1, start=datetime(2026, 6, 25, 3, 45, tzinfo=UTC), end=datetime(2026, 6, 27, 0, 0, 0, 1, tzinfo=UTC)), 'analysis_read_span_exceeded'),
    ):
        status, answer = native_service.post('/v1/market-state/rallies', body)
        assert status == 400 and answer['error'] == reason
    empty = native_service.discover(_body(scale='atr', target=1, start=datetime(2023, 3, 24, 14, tzinfo=UTC), end=datetime(2023, 3, 24, 14, 5, tzinfo=UTC)))
    empty_meta = _metadata(empty['summary'])
    assert _table(empty, 'rallies').height == 0
    assert 'empty_atr_bar' in str(empty_meta['diagnostic_reasons'])
    boundary = native_service.discover(_body(start=datetime(2021, 1, 1, tzinfo=UTC), end=datetime(2021, 1, 1, 0, 5, tzinfo=UTC)))
    assert 'reference' in str(_metadata(boundary['summary'])['diagnostic_reasons'])


def test_exact_membership_confirmation_and_base_partiality(native_service: NativeService) -> None:
    definition = RallyDefinition('first_hit', 'bps', Decimal('30'), anchor_minutes=1)
    response = native_service.discover()
    _assert_native_membership(response, definition)
    events = _table(response, 'rallies')
    assert events.height == 4
    tied = events.filter(pl.col('anchor_at') == datetime(2026, 6, 27, 11, 43, tzinfo=UTC)).row(0, named=True)
    assert tied['end_trade_id'] == tied['confirmation_trade_id'] == 6453974454
    trades = _native()
    endpoint = int(np.searchsorted(trades.trade_id, 6453974454))
    simultaneous = np.flatnonzero(trades.timestamp_us == trades.timestamp_us[endpoint])
    assert len(simultaneous) == 13 and endpoint < int(simultaneous[-1])
    # Same-timestamp IDs after the first hit are outside exact membership.
    assert tied['trade_count'] == endpoint - int(np.searchsorted(trades.trade_id, _integer(tied['start_trade_id']))) + 1
    first = int(np.searchsorted(trades.trade_id, _integer(tied['start_trade_id'])))
    times = (trades.timestamp_us - T0_US) // BASE_US
    prices = (trades.price // 125).astype(np.int64)
    nonmembers = (times == times[endpoint]) & (prices == prices[endpoint]) & (trades.trade_id > trades.trade_id[endpoint])
    assert np.count_nonzero(nonmembers) == 1633
    assert float(trades.price[nonmembers].max()) > float(trades.price[first:endpoint + 1].max())
    assert _table(response, 'rally_cells')['partial'].any()
    swing = native_service.discover(_body(mode='swing', target=3))
    _assert_native_membership(swing, RallyDefinition('swing', 'bps', Decimal('3'), reversal=Decimal('1')))
    swing_events = _table(swing, 'rallies')
    assert swing_events.height > 0
    assert (swing_events['confirmation_trade_id'] > swing_events['end_trade_id']).all()
    for mode, scale, target, definition in (
        ('controlled_advance', 'bps', 30, RallyDefinition('controlled_advance', 'bps', Decimal('30'), pullback=Decimal('10'), anchor_minutes=1)),
        ('first_hit', 'atr', 0.1, RallyDefinition('first_hit', 'atr', Decimal('0.1'), anchor_minutes=1)),
        ('controlled_advance', 'atr', 0.1, RallyDefinition('controlled_advance', 'atr', Decimal('0.1'), pullback=Decimal('0.5'), anchor_minutes=1)),
        ('swing', 'atr', 0.1, RallyDefinition('swing', 'atr', Decimal('0.1'), reversal=Decimal('0.1'))),
    ):
        case = native_service.discover(_body(mode=mode, scale=scale, target=target))
        assert _table(case, 'rallies').height > 0
        _assert_native_membership(case, definition)


def test_overlaps_and_single_pass_columnar_aggregation(native_service: NativeService, monkeypatch: pytest.MonkeyPatch) -> None:
    reads_class = getattr(market_state_rallies, '_Reads')
    original = cast(Callable[[object, str, object | None], AbstractContextManager[Iterator[object]]], getattr(reads_class, 'batches'))
    statements: list[str] = []

    def recording_batches(reads: object, query: str, external: object | None = None) -> AbstractContextManager[Iterator[object]]:
        statements.append(query)
        return original(reads, query, external)

    monkeypatch.setattr(reads_class, 'batches', recording_batches)
    response = native_service.discover()
    native_streams = [query for query in statements if 'timestamp_us' in query and 'ORDER BY trade_id' in query]
    assert len(native_streams) == 2
    assert sum(query.endswith('ORDER BY trade_id') for query in native_streams) == 1
    assert sum(query.endswith('ORDER BY trade_id DESC LIMIT 1') for query in native_streams) == 1
    cells = _table(response, 'rally_cells')
    shared = cells.group_by('base_time_index', 'base_price_index').agg(pl.col('rally_id').n_unique().alias('events'))
    assert _integer(shared['events'].max()) > 1
    assert cells.group_by('rally_id', 'base_time_index', 'base_price_index').len()['len'].max() == 1
    _assert_native_membership(response, RallyDefinition('first_hit', 'bps', Decimal('30'), anchor_minutes=1))
    source = inspect.getsource(getattr(market_state_rallies, '_copy_batch'))
    assert 'iter_rows' not in source and 'to_pylist' not in source
    assert 'to_numpy' in source


def test_canonical_publication_and_replay_contract(native_service: NativeService) -> None:
    invalid_fields: tuple[tuple[str, object], ...] = (('result_id', str(uuid4())), ('grid', {}), ('filters', {}), ('known_at', iso(END)), ('selected_id', 'x'))
    for field, value in invalid_fields:
        status, _ = native_service.post('/v1/market-state/rallies', {**_body(), field: value})
        assert status == 400
    response = native_service.discover()
    metadata = _metadata(response['rallies'])
    assert _time(response['observation_ceiling']) == END
    assert _time(metadata['diagnostics_as_of']) == END
    events = _table(response, 'rallies')
    cutoff = events['confirmed_at'][1]
    assert isinstance(cutoff, datetime)
    eligible = events.filter(pl.col('confirmed_at') < cutoff)
    trades = _native()
    definition = RallyDefinition('first_hit', 'bps', Decimal('30'), anchor_minutes=1)
    expected = _oracle(definition, trades, START, cutoff)
    assert eligible['end_trade_id'].to_list() == [int(trades.trade_id[last]) for _, _, last, _ in expected]
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (native_service.store.results / str(response['result_id'])).iterdir()}
    for exponent in range(20):
        _table(response, 'rally_cells').with_columns((pl.col('base_time_index') // 2**exponent).alias('display_time')).group_by('rally_id', 'display_time').agg(pl.col('trade_count').sum())
    assert before == {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (native_service.store.results / str(response['result_id'])).iterdir()}


def test_event_totals_and_capability_metadata(native_service: NativeService) -> None:
    response = native_service.discover()
    metadata = [_metadata(response[key]) for key in ('rallies', 'rally_cells', 'summary')]
    assert metadata[0] == metadata[1] == metadata[2]
    assert set(_list(metadata[0]['available_event_measures'])) == {'volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count'}
    assert set(_list(metadata[0]['unavailable_event_measures'])) == {'path_length', 'dwell', 'indicators'}
    cells = _table(response, 'rally_cells')
    for event in _table(response, 'rallies').to_dicts():
        members = cells.filter(pl.col('rally_id') == event['rally_id'])
        for count in ('trade_count', 'taker_buy_trade_count'):
            assert event[count] == members[count].sum()
        for volume in ('volume', 'taker_buy_volume'):
            assert math.isclose(float(event[volume]), math.fsum(float(q) for q in members[volume]), abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE)
    assert _table(response, 'summary').height == 1


def test_content_evidence_hash_and_lossless_ids(native_service: NativeService) -> None:
    response = native_service.discover()
    events = _table(response, 'rallies')
    assert events['start_trade_id'].dtype == events['end_trade_id'].dtype == events['confirmation_trade_id'].dtype == pl.UInt64
    assert _integer(events['start_trade_id'].min()) > 2**32
    assert events['event_evidence_hash'].str.len_chars().to_list() == [64] * events.height
    assert events['evidence_version'].to_list() == [1] * events.height
    trades = _native()
    cells = _table(response, 'rally_cells')
    for event in events.to_dicts():
        first = int(np.searchsorted(trades.trade_id, int(event['start_trade_id'])))
        stop = int(np.searchsorted(trades.trade_id, int(event['end_trade_id']), side='right'))
        actual_ids = trades.trade_id[first:stop]
        member_cells = cells.filter(pl.col('rally_id') == event['rally_id'])
        assert _evidence_oracle(event, member_cells, actual_ids) == event['event_evidence_hash']
        # Withholding an authentic internal ID is a hash sensitivity probe, not a corrected revision.
        assert len(actual_ids) > 2
        withheld = np.concatenate((actual_ids[:1], actual_ids[2:]))
        assert _evidence_oracle(event, member_cells, withheld) != event['event_evidence_hash']
    again = native_service.discover()
    assert _table(again, 'rallies')['event_evidence_hash'].to_list() == events['event_evidence_hash'].to_list()
    assert _list(_object(_state()['revised_candidates'])['rows']) == []
    assert 'corrected' in str(_object(_proof()['missing_proofs'])['authentic_corrected_revision'])


def test_mixed_result_publication_renewal_recovery_expiry(native_service: NativeService, monkeypatch: pytest.MonkeyPatch) -> None:
    response = native_service.discover()
    ordinary_status, ordinary = native_service.post('/v1/market-state/query', {'t1': iso(START), 't2': iso(END)})
    assert ordinary_status == 200
    rally_id, ordinary_id = str(response['result_id']), str(ordinary['result_id'])
    native_service.clock.advance(3600)
    assert native_service.store.access(rally_id, 'rally_cells.arrow') is not None
    assert native_service.store.access(ordinary_id, 'cells.arrow') is not None
    connection = sqlite3.connect(native_service.store.database)
    try:
        renewed = connection.execute('SELECT name,last_access_ns FROM files WHERE result_id=? ORDER BY name', (rally_id,)).fetchall()
        assert len({row[1] for row in renewed}) == 1 and len(renewed) == 3
        ordinary_access = connection.execute('SELECT name,last_access_ns FROM files WHERE result_id=? ORDER BY name', (ordinary_id,)).fetchall()
        assert len({row[1] for row in ordinary_access}) == 2
    finally:
        connection.close()
    partial = str(uuid4())
    partial_stage = native_service.store.register(partial)
    shutil.copyfile(str(response['rallies']), partial_stage / 'rallies.arrow')
    rollover = str(uuid4())
    rollover_stage = native_service.store.register(rollover)
    for key, name in zip(('rallies', 'rally_cells', 'summary'), RALLY_FILES, strict=True):
        shutil.copyfile(str(response[key]), rollover_stage / name)
    original_rename = Path.rename

    def interrupted_rename(path: Path, target: str | Path) -> Path:
        if path == rollover_stage:
            raise OSError('Crash after inventory fsync and lifecycle commit, before directory rename.')
        return original_rename(path, target)

    with monkeypatch.context() as crash:
        crash.setattr(Path, 'rename', interrupted_rename)
        with pytest.raises(OSError):
            native_service.store.publish(rollover, files=RALLY_FILES)
    recovered = ResultStore(native_service.store.root, clock=native_service.clock, disk=_roomy)
    assert recovered.recover() == 1
    assert not partial_stage.exists()
    assert not rollover_stage.exists()
    assert all((recovered.results / rollover / name).is_file() for name in RALLY_FILES)
    assert recovered.access(rollover, 'summary.arrow') is not None
    raced = str(uuid4())
    raced_stage = recovered.register(raced)
    for name in RALLY_FILES:
        shutil.copyfile(recovered.results / rollover / name, raced_stage / name)
    recovered.publish(raced, files=RALLY_FILES)
    original_owned = cast(Callable[[Path], bool], getattr(market_state_results, '_owned_directory'))
    interleaved: list[bool] = []

    def discard_after_snapshot(path: Path) -> bool:
        if path == recovered.results / raced and not interleaved:
            interleaved.append(True)
            recovered.discard(raced)
        return original_owned(path)

    native_service.clock.advance(24 * 3600 + 1)
    with monkeypatch.context() as race:
        race.setattr(market_state_results, '_owned_directory', discard_after_snapshot)
        assert recovered.expire() == 8
    assert interleaved == [True] and not (recovered.results / raced).exists()
    assert not (recovered.results / rally_id).exists()
    assert not (recovered.results / ordinary_id).exists()
    for inventory in (('rallies.arrow', 'summary.arrow'), ('foreign.arrow', 'summary.arrow')):
        identity = str(uuid4())
        recovered.register(identity)
        with pytest.raises(ValueError):
            recovered.publish(identity, files=inventory)
        recovered.discard(identity)


def test_state_cleanup_disconnect_and_deadline(native_service: NativeService, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_state, 'FENCE_WAIT_SECONDS', 0.05)
    with source_lock(native_service.runtime.lock_root, SOURCE, 'heavy'):
        status, answer = native_service.post('/v1/market-state/rallies', _body())
    assert status == 503 and answer['error'] == 'source_maintenance'
    delivered: list[tuple[int, dict[str, object], dict[str, str]]] = []

    def disconnect(answer: tuple[int, dict[str, object], dict[str, str]]) -> bool:
        delivered.append(answer)
        return False

    native_service.api.rallies(json.dumps(_body()).encode(), disconnect)
    assert delivered[0][0] == 200
    assert not (native_service.store.results / str(delivered[0][1]['result_id'])).exists()
    assert native_service.store.usage() == (0, 0)
    called = _at_final_staging(native_service, monkeypatch, lambda: _withdraw(native_service, '2026-06-27'))
    try:
        status, answer = native_service.post('/v1/market-state/rallies', _body())
        assert called == [True]
        assert status == 409 and answer['error'] == 'source_state_changed'
        assert not tuple(native_service.store.staging.iterdir())
        assert native_service.store.usage() == (0, 0)
    finally:
        _restore(native_service, '2026-06-27')
    identity = str(uuid4())
    staging = native_service.store.register(identity)
    with pytest.raises(RequestError) as deadline:
        write_rally_result(native_service.runtime, parse_rally_request(json.dumps(_body()).encode()), staging, result_id=identity, guard=lambda size: None, deadline=time.monotonic() - 1)
    assert deadline.value.status == 504
    native_service.store.discard(identity)
    assert not staging.exists()
    # Delay only delivery of unchanged authentic Arrow bytes, including headers during
    # client construction: urllib3's relative inactivity timeout alone cannot stop this.
    payload = _frame('june-27-raw.parquet').write_ipc_stream(None).getvalue()
    slow_headers = [False]

    class DelayedHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._send_capture()

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get('Content-Length', '0')))
            self._send_capture()

        def _send_capture(self) -> None:
            header = b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(payload)).encode() + b'\r\nConnection: close\r\n\r\n'
            delayed = header if slow_headers[0] else payload[:64]
            try:
                if not slow_headers[0]:
                    self.wfile.write(header)
                for byte in delayed:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.02)
                self.wfile.write(payload if slow_headers[0] else payload[64:])
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True

    delayed_server = HTTPServer(('127.0.0.1', 0), DelayedHandler)
    delivery = threading.Thread(target=lambda: delayed_server.serve_forever(poll_interval=0.01), daemon=True)
    delivery.start()
    try:
        for operation in ('scalar', 'stream', 'constructor_headers'):
            slow_headers[0] = operation == 'constructor_headers'
            started = time.monotonic()
            wall = started + 0.2
            if operation == 'constructor_headers':
                reads = cast(Callable[[SourceRuntime, float, str], DeadlineReads], getattr(market_state_rallies, '_Reads'))(native_service.runtime, wall, str(uuid4()))
                with monkeypatch.context() as address:
                    address.setenv('CLICKHOUSE_HOST', '127.0.0.1')
                    address.setenv('CLICKHOUSE_HTTP_PORT', str(delayed_server.server_port))
                    try:
                        with pytest.raises(RallyError) as interrupted:
                            reads.connect()
                    finally:
                        reads.close()
            else:
                pool = cast(Callable[[float], DeadlinePool], getattr(market_state_rallies, '_DeadlinePool'))(wall)
                try:
                    with pytest.raises(RallyError) as interrupted:
                        try:
                            response = pool.urlopen('GET', f'http://127.0.0.1:{delayed_server.server_port}/', preload_content=operation == 'scalar')
                            if operation == 'stream':
                                tuple(market_state.ipc.open_stream(response))
                        finally:
                            cast(Callable[[float], float], getattr(market_state_rallies, '_remaining'))(wall)
                finally:
                    pool.close()
            assert interrupted.value.reason == 'rally_deadline_exceeded'
            assert time.monotonic() - started < 0.8, operation
    finally:
        delayed_server.shutdown()
        delayed_server.server_close()
        delivery.join(timeout=2)
    assert not delivery.is_alive()

    build = str(_frame('june-27-raw.parquet')['build_id'][0])

    def reclaim_actual_build() -> None:
        # Exercise native cleanup against the captured build in the disposable database.
        _withdraw(native_service, '2026-06-27')
        assert build in native_service.runtime.cleanup(dry_run=False)
        _restore(native_service, '2026-06-27')

    reclaimed = _at_final_staging(native_service, monkeypatch, reclaim_actual_build)
    status, answer = native_service.post('/v1/market-state/rallies', _body())
    assert reclaimed == [True]
    assert status == 503 and answer['error'] == 'source_maintenance'
    assert not tuple(native_service.store.staging.iterdir())
    assert native_service.store.usage() == (0, 0)


def test_shared_slots_resource_admission_and_receipts(native_service: NativeService, monkeypatch: pytest.MonkeyPatch) -> None:
    prior = native_service.discover()
    entered, release = threading.Event(), threading.Event()
    original = cast(Callable[[SourceRuntime, RallyDiscoveryRequest, str, float], tuple[dict[str, object], tuple[Path, ...]]], getattr(native_service.api, '_publish_rally'))
    answers: list[tuple[int, dict[str, object]]] = []

    def blocked(runtime: SourceRuntime, request: RallyDiscoveryRequest, identity: str, deadline: float) -> tuple[dict[str, object], tuple[Path, ...]]:
        entered.set()
        assert release.wait(30)
        return original(runtime, request, identity, deadline)

    monkeypatch.setattr(native_service.api, '_publish_rally', blocked)
    worker = threading.Thread(target=lambda: answers.append(native_service.post('/v1/market-state/rallies', _body())))
    worker.start()
    try:
        assert entered.wait(30)
        status, answer = native_service.post('/v1/market-state/rallies', _body())
        assert status == 503 and answer['error'] == 'busy'
        status, _ = native_service.post('/v1/market-state/query', {'t1': iso(START), 't2': iso(END)})
        assert status == 200
        started = time.monotonic()
        status, _ = native_service.post('/v1/market-state/access', {'path': prior['rally_cells']})
        assert status == 200 and time.monotonic() - started < 1
    finally:
        release.set()
        worker.join(30)
    assert not worker.is_alive() and answers[0][0] == 200
    monkeypatch.setattr(native_service.api, '_publish_rally', original)
    native_service.api.queries.acquire()
    native_service.api.queries.acquire()
    try:
        status, answer = native_service.post('/v1/market-state/rallies', _body())
    finally:
        native_service.api.queries.release()
        native_service.api.queries.release()
    assert status == 503 and answer['error'] == 'busy'
    with monkeypatch.context() as budget:
        budget.setattr(market_state_rallies, 'MAX_RALLY_INPUT_ROWS', 1)
        status, answer = native_service.post('/v1/market-state/rallies', _body())
        assert status == 413 and answer['error'] == 'rally_input_budget_exceeded'
        assert _integer(answer['required_rows']) > _integer(answer['budget_rows']) == 1
    with monkeypatch.context() as budget:
        budget.setattr(market_state_rallies, 'MAX_RALLY_OUTPUT_BYTES', 1)
        status, answer = native_service.post('/v1/market-state/rallies', _body())
        assert status == 413 and answer['error'] == 'rally_output_budget_exceeded'
        assert _integer(answer['required_bytes']) > _integer(answer['budget_bytes']) == 1
    with monkeypatch.context() as memory:
        memory.setattr(market_state_rallies, '_WORKER_RSS_BUDGET', 1)
        status, answer = native_service.post('/v1/market-state/rallies', _body())
        assert status == 413 and answer['error'] == 'rally_working_memory_exceeded'
        assert _integer(answer['required_bytes']) > _integer(answer['budget_bytes']) == 1
    calls: list[int] = []

    def interrupt() -> None:
        calls.append(len(calls))
        if len(calls) == 3:
            raise RallyError(504, 'rally_deadline_exceeded', 'Test cooperative interruption on unchanged native records.')

    budget_scope = cast(Callable[[Callable[[], None]], AbstractContextManager[None]], getattr(rally_detection, '_budget_scope'))
    with budget_scope(interrupt), pytest.raises(RallyError) as interrupted:
        detect_rallies(trades=_native(), definition=RallyDefinition('swing', 'bps', Decimal('3'), reversal=Decimal('1')), source=SOURCE, instrument='BTCUSDT', analysis_start=START, analysis_end=END, known_at=END, coverage=((START, END),))
    assert interrupted.value.status == 504 and len(calls) == 3
    assert not tuple(native_service.store.staging.iterdir())
    native_service.api.tick(datetime.now(UTC).replace(second=0, microsecond=0))
    receipts = native_service.runtime.store.execute('SELECT series, status, error_code, error, rows, sha256 FROM origo.worker_minute_log ORDER BY series')
    query = [row for row in receipts if row[0] == 'binance_spot_trades:query']
    assert len(query) == 1
    assert 'rally_discovery_ok=2' in str(query[0][3])
    assert 'rally_discovery_rejected=5' in str(query[0][3])
    assert 'rally_discovery_failed=0' in str(query[0][3])
    assert 'rally_input_bytes=' in str(query[0][3]) and 'rally_output_bytes=' in str(query[0][3])
    assert all(row[2] != 'CLEANUP_FAILED' for row in receipts)


def test_public_reader_numbers_timeout_and_rally_paths(native_service: NativeService) -> None:
    identities: list[list[dict[str, object]]] = []
    for target in (30, 30.0, Decimal('30.0')):
        result = market_state_reader.rallies(_body(target=target), url=native_service.url, timeout_seconds=300)
        assert result.response['definition_fingerprint'] == definition_fingerprint(RallyDefinition('first_hit', 'bps', Decimal('30'), anchor_minutes=1))
        identities.append(read_table(result.rallies, url=native_service.url).to_pylist())
        for path in (result.rallies, result.rally_cells, result.summary):
            assert parse_result_path(path) is not None
            assert read_table(path, url=native_service.url).num_rows >= 1
    assert identities[0] == identities[1] == identities[2]
    decimal = market_state_reader.rallies(_body(target=Decimal('30.5')), url=native_service.url)
    assert decimal.response['definition_fingerprint'] == definition_fingerprint(RallyDefinition('first_hit', 'bps', Decimal('30.5'), anchor_minutes=1))
    for definition in (
        RallyDefinition('first_hit', 'atr', Decimal('0.1'), anchor_minutes=1),
        RallyDefinition('controlled_advance', 'atr', Decimal('0.1'), pullback=Decimal('0.1'), anchor_minutes=1),
        RallyDefinition('swing', 'atr', Decimal('0.1'), reversal=Decimal('0.1')),
    ):
        request = _body(mode=definition.mode, scale=definition.scale, target=definition.target)
        fields = _object(request['definition'])
        if definition.pullback is not None:
            fields['pullback'] = definition.pullback
        if definition.reversal is not None:
            fields['reversal'] = definition.reversal
        result = market_state_reader.rallies(request, url=native_service.url)
        assert result.response['definition_fingerprint'] == definition_fingerprint(definition)
        _assert_native_membership(result.response, definition)
    for timeout in (0.0, -1.0, 300.1, float('inf'), float('nan')):
        with pytest.raises(ValueError):
            market_state_reader.rallies(_body(), url=native_service.url, timeout_seconds=timeout)
    with pytest.raises(ValueError):
        market_state_reader.rallies(_body(scale='atr', target=Decimal('0.1000000000000000001')), url=native_service.url)


def test_frozen_acceptance_protocol_and_report_verdict(tmp_path: Path) -> None:
    benchmark = cast(Benchmark, importlib.import_module('tools.benchmark_market_state_rallies'))
    failure_policy = cast(Callable[[object, object], bool], getattr(benchmark, '_workload_failure'))
    # Scalar policy probes; no production receipt or resource report is fabricated.
    for feed in ('market_state_api', 'provisional', 'depth'):
        assert failure_policy(feed, 'FAILED')
        assert not failure_policy(feed, 'OK')
        assert not failure_policy(feed, 'REJECTED')
    assert not failure_policy('unrelated', 'FAILED')
    assert not failure_policy(None, 'FAILED')
    assert not failure_policy('market_state_api', None)
    # Scalar binding probes use unchanged capture identities and an actual local
    # timing; they do not manufacture container observations or production reports.
    same_deployment = cast(Callable[[object, object, object, object, object, object], bool], getattr(benchmark, '_same_deployment'))
    entries = _object(_proof()['files'])
    first, second = _object(entries['june-27-raw.parquet']), _object(entries['startup-raw.parquet'])
    first_id = _object(_object(first['acquisition'])['params'])['build']
    second_id = _object(_object(second['acquisition'])['params'])['build']
    pid = os.getpid()
    assert same_deployment(first_id, first['sha256'], pid, first_id, first['sha256'], pid)
    for observed_id, observed_image, observed_pid in (
        (second_id, first['sha256'], pid),
        (first_id, second['sha256'], pid),
        (first_id, first['sha256'], os.getppid()),
    ):
        assert not same_deployment(first_id, first['sha256'], pid, observed_id, observed_image, observed_pid)
    assert not same_deployment('', first['sha256'], pid, '', first['sha256'], pid)
    assert not same_deployment(first_id, '', pid, first_id, '', pid)
    assert not same_deployment(first_id, first['sha256'], True, first_id, first['sha256'], True)
    started = time.perf_counter()
    checksum = hashlib.sha256((FIXTURES / 'june-27-raw.parquet').read_bytes()).hexdigest()
    elapsed = time.perf_counter() - started
    assert checksum == first['sha256'] and elapsed > 0
    retained = _object(json.loads(json.dumps({'seconds': elapsed})))['seconds']
    same_latency = cast(Callable[[object, object], bool], getattr(benchmark, '_same_latency'))
    assert same_latency(elapsed, retained)
    assert not same_latency(math.nextafter(elapsed, math.inf), retained)
    assert not same_latency(-elapsed, -elapsed)
    for invalid in (math.nan, None, True):
        with pytest.raises(ValueError):
            same_latency(invalid, retained)
    manifest = benchmark.frozen_manifest()
    assert 'market_state_api.heartbeat' in _list(manifest['required_heartbeats'])
    assert 'perp_capture.heartbeat' in _list(manifest['required_heartbeats'])
    cases = [_object(case) for case in _list(manifest['cases'])]
    assert len(cases) == len({case['id'] for case in cases}) == 24
    assert _list(manifest['rounds']) == ['cold', 'warm1', 'warm2', 'warm3', 'warm4', 'warm5']
    assert len(_list(manifest['proof_cases'])) == 8
    assert _object(manifest['corrected_revision'])['status'] == 'deferred'
    assert benchmark.validate_report({}, manifest, tmp_path)
    # These deliberately incomplete reports prove rejection; no production PASS is invented.
    declared: dict[str, object] = {'schema_version': 1, 'status': 'PASS', 'evidence': [], 'samples': [], 'proof_samples': []}
    assert any('Missing required evidence' in failure for failure in benchmark.validate_report(declared, manifest, tmp_path))
    changed = {**manifest, 'limits': {**_object(manifest['limits']), 'input_rows': 8_000_001}}
    assert any('Frozen corpus' in failure for failure in benchmark.validate_report(declared, changed, tmp_path))
    for altered in ({**manifest, 'cases': cases[:-1]}, {**manifest, 'tolerance': {**_object(manifest['tolerance']), 'relative': 1e-6}}, {**manifest, 'corrected_revision': {'status': 'PASS'}}):
        assert any('Frozen corpus' in failure for failure in benchmark.validate_report(declared, altered, tmp_path))
    native = FIXTURES / 'june-27-raw.parquet'
    shutil.copyfile(native, tmp_path / native.name)
    inventory = [{'file': native.name, 'bytes': native.stat().st_size, 'sha256': hashlib.sha256(native.read_bytes()).hexdigest()}]
    authentic = {**declared, 'evidence': inventory}
    assert not any('altered immutable' in failure for failure in benchmark.validate_report(authentic, manifest, tmp_path))
    incorrect_checksum = {**declared, 'evidence': [{**inventory[0], 'sha256': '0' * 64}]}
    assert any('altered immutable' in failure for failure in benchmark.validate_report(incorrect_checksum, manifest, tmp_path))
    for name, entry in _object(_proof()['files']).items():
        evidence = _object(entry)
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == evidence['sha256']
        if 'native_column_sha256' in evidence:
            frame = _frame(name)
            digest = hashlib.sha256(b''.join(frame[column].to_numpy().tobytes() for column in ('trade_id', 'timestamp', 'price', 'quote_quantity', 'is_buyer_maker'))).hexdigest()
            assert digest == evidence['native_column_sha256']
            multiplier = 1000 if evidence['native_timestamp_unit'] == 'milliseconds' else 1
            assert (frame['timestamp'].cast(pl.Int64) * multiplier == frame['datetime'].dt.epoch('us')).all()
            assert frame['trade_id'].diff().drop_nulls().to_list() == [1] * (frame.height - 1)
    assert _integer(_object(_proof()['source_state'])['held_pin_bytes']) > 65_536


def test_ordinary_query_and_legacy_export_remain_compatible(native_service: NativeService, origo_test_env: dict[str, str], origo_assets: dict[str, object], tmp_path: Path) -> None:
    ordinary = market_state_reader.query(t1=iso(START), t2=iso(END), url=native_service.url)
    assert read_table(ordinary.summary, url=native_service.url).num_rows == 1
    assert set(ordinary.response) == {'result_id', 'cells', 'summary', 'expires_after_seconds', 'expires_at', 'effective', 'clipped', 'data_cutoff', 'canonical_through', 'last_column_unfinished', 'state_token', 'cell_count'}
    native_service.runtime.store.execute('DROP DATABASE origo SYNC')
    initialize = cast(Callable[[dict[str, str], dict[str, object]], None], inspect.unwrap(legacy_fixture.rally_data))
    initialize(origo_test_env, origo_assets)
    _legacy_baseline(None, tmp_path)
