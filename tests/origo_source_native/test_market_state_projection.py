from __future__ import annotations

import csv
import hashlib
import json
import math
import struct
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from clickhouse_driver.errors import ServerException

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters import binance_spot_rest as rest
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.columnar import HASH_CHUNK_ROWS
from origo.sources.contracts import BuildContext, Client, ComponentSpec, Partition, Revision, Row
from origo.sources.hashing import content_hash
from origo.sources.profiles.market_state import (
    BASE_PRICE_USDT,
    BASE_TIME_US,
    CUBE_START,
    MARKET_STATE_COMPONENTS,
    build_market_state,
)
from origo.sources.profiles.spot import SPOT_COMPONENTS
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES, REST

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ANCHOR_US = 1_609_459_200_000_000
_ABS_TOL = 1e-8
_REL_TOL = 1e-12


@dataclass(frozen=True)
class Cell:
    first_trade_at: datetime
    volume: float
    trade_count: int
    taker_buy_volume: float
    taker_buy_trade_count: int


def _archive(day: str) -> tuple[bytes, Partition, tuple[Row, ...]]:
    body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_bytes()
    provenance = json.loads((ARCHIVES / f'BTCUSDT-trades-{day}.provenance.json').read_text())
    assert hashlib.sha256(body).hexdigest() == provenance['selected_sha256']
    partition = daily.BinanceSpotDaily().partition(day)
    return body, partition, tuple(daily.spot_csv_rows(body, partition))


def _micros(timestamp: str) -> int:
    assert len(timestamp) in (13, 16)
    return int(timestamp) * (1000 if len(timestamp) == 13 else 1)


def _reference(body: bytes, selected_ids: set[int] | None = None) -> dict[tuple[int, int], Cell]:
    groups: dict[tuple[int, int], list[tuple[int, float, bool]]] = defaultdict(list)
    for fields in csv.reader(body.decode().splitlines()):
        micros = _micros(fields[4])
        if micros >= _ANCHOR_US and (selected_ids is None or int(fields[0]) in selected_ids):
            key = ((micros - _ANCHOR_US) // 56_250_000, int(Decimal(fields[1]) // Decimal(125)))
            groups[key].append((micros, float(fields[3]), fields[5].lower() == 'false'))
    return {
        key: Cell(
            _EPOCH + timedelta(microseconds=min(row[0] for row in rows)),
            math.fsum(row[1] for row in rows),
            len(rows),
            math.fsum(row[1] for row in rows if row[2]),
            sum(row[2] for row in rows),
        )
        for key, rows in groups.items()
    }


def _assert_cells(
    rows: list[Row], expected: dict[tuple[int, int], Cell], *, milliseconds: bool = False
) -> None:
    assert len(rows) == len(expected)
    actual_keys = {(int(str(row[0])), int(str(row[1]))) for row in rows}
    assert actual_keys == set(expected)
    for row in rows:
        reference = expected[(int(str(row[0])), int(str(row[1])))]
        assert isinstance(row[2], datetime)
        first = reference.first_trade_at
        if milliseconds:
            first = first.replace(microsecond=(first.microsecond // 1000) * 1000)
        assert row[2].replace(tzinfo=UTC) == first
        assert row[4] == reference.trade_count
        assert row[6] == reference.taker_buy_trade_count
        assert float(str(row[3])) == pytest.approx(reference.volume, abs=_ABS_TOL, rel=_REL_TOL)
        assert float(str(row[5])) == pytest.approx(
            reference.taker_buy_volume, abs=_ABS_TOL, rel=_REL_TOL
        )


@pytest.fixture()
def cube_client(origo_test_env: dict[str, str]) -> Iterator[Client]:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute('CREATE DATABASE origo')
        yield client
    finally:
        client.disconnect()


def _create_table(client: Client, component: ComponentSpec) -> None:
    client.execute(f'DROP TABLE IF EXISTS origo.{component.key} SYNC')
    columns = ', '.join(f'{column.name} {column.sql_type}' for column in component.columns)
    ordering = ', '.join(component.primary_key)
    client.execute(
        f'CREATE TABLE origo.{component.key} ({columns}) ENGINE=MergeTree ORDER BY ({ordering})'
    )


def _build(client: Client, partition: Partition, rows: tuple[Row, ...]) -> ComponentSpec:
    raw_key = 'raw_latest' if partition.provisional else 'raw'
    raw = next(component for component in SPOT_COMPONENTS if component.key == raw_key)
    target = next(
        component
        for component in MARKET_STATE_COMPONENTS
        if component.provisional == partition.provisional
    )
    _create_table(client, raw)
    _create_table(client, target)
    if rows:
        inserted = tuple((partition.start, *row) for row in rows) if partition.provisional else rows
        client.execute(f'INSERT INTO origo.{raw.key} VALUES', inserted)
    revision = Revision(
        content_hash(rows, schema_version=1),
        content_hash(rows, schema_version=1),
        '{}',
        len(rows),
        lambda: iter(rows),
    )
    build_market_state(BuildContext(client, 'origo', partition, revision, uuid4()))
    return target


def _read(client: Client, component: ComponentSpec) -> list[Row]:
    return client.execute(f'SELECT * FROM origo.{component.key} ORDER BY time_index, price_index')


@pytest.mark.parametrize('day', ['2024-12-31', '2025-01-01'])
def test_market_state_matches_authentic_archive(cube_client: Client, day: str) -> None:
    assert CUBE_START == datetime(2021, 1, 1, tzinfo=UTC)
    assert BASE_TIME_US == 56_250_000
    assert BASE_PRICE_USDT == 125
    body, partition, rows = _archive(day)
    assert len(rows) == 12000
    fields = list(csv.reader(body.decode().splitlines()))
    assert any(Decimal(row[1]) % Decimal(125) == 0 for row in fields)
    assert len({_micros(row[4]) // 56_250_000 for row in fields}) > 1
    expected = _reference(body)
    component = _build(cube_client, partition, rows)
    assert component.key == 'market_state'
    assert component.primary_key == ('time_index', 'price_index')
    assert component.time_column == 'first_trade_at'
    assert [
        (row[0], row[1]) for row in cube_client.execute('DESCRIBE TABLE origo.market_state')
    ] == [
        ('time_index', 'UInt64'),
        ('price_index', 'UInt64'),
        ('first_trade_at', 'DateTime64(6)'),
        ('volume', 'Float64'),
        ('trade_count', 'UInt32'),
        ('taker_buy_volume', 'Float64'),
        ('taker_buy_trade_count', 'UInt32'),
    ]
    first = _read(cube_client, component)
    _assert_cells(first, expected)
    assert sum(cell.trade_count for cell in expected.values()) == len(rows)
    assert sum(cell.taker_buy_trade_count for cell in expected.values()) == sum(
        row[5] == 0 for row in rows
    )
    cube_client.execute('TRUNCATE TABLE origo.market_state')
    revision = Revision('repeat', 'repeat', '{}', len(rows), lambda: iter(rows))
    build_market_state(BuildContext(cube_client, 'origo', partition, revision, uuid4()))
    assert _read(cube_client, component) == first


def test_market_state_fragments_recombine_adjacent_minutes(cube_client: Client) -> None:
    body, partition, rows = _archive('2025-01-01')
    canonical = _build(cube_client, partition, rows)
    expected = _reference(body)
    _assert_cells(_read(cube_client, canonical), expected)
    cube_client.execute('CREATE TABLE origo.fragments AS origo.market_state')
    minutes: dict[datetime, list[Row]] = defaultdict(list)
    for row in rows:
        assert isinstance(row[-1], datetime)
        minutes[row[-1].replace(second=0, microsecond=0)].append(row)
    assert len(minutes) > 1
    store = SourceStore(cube_client, 'origo', BINANCE_SPOT_TRADES_SPEC)
    previous: datetime | None = None
    for minute, selected in sorted(minutes.items()):
        if previous is not None:
            assert minute == previous + timedelta(minutes=1)
        previous = minute
        provisional = Partition(
            minute.strftime('%Y-%m-%dT%H:%M:%SZ'), minute, minute + timedelta(minutes=1), True
        )
        component = _build(cube_client, provisional, tuple(selected))
        assert component.key == 'market_state_latest'
        assert component.current_target == 'market_state'
        _assert_cells(
            _read(cube_client, component),
            _reference(body, {int(str(row[0])) for row in selected}),
            milliseconds=True,
        )
        validated, _ = store.validate_component(component, 'origo.market_state_latest', provisional)
        assert validated == len(_read(cube_client, component))
        cube_client.execute('INSERT INTO origo.fragments SELECT * FROM origo.market_state_latest')
    shared = cube_client.execute(
        'SELECT count() FROM (SELECT time_index, price_index FROM origo.fragments '
        'GROUP BY time_index, price_index HAVING count()>1)'
    )[0][0]
    assert int(str(shared)) > 0
    recombined = cube_client.execute(
        'SELECT time_index, price_index, min(first_trade_at), sumKahan(volume), '
        'sum(toUInt64(trade_count)), sumKahan(taker_buy_volume), '
        'sum(toUInt64(taker_buy_trade_count)) FROM origo.fragments '
        'GROUP BY time_index, price_index ORDER BY time_index, price_index',
        settings={'max_threads': 1},
    )
    _assert_cells(recombined, expected, milliseconds=True)
    assert cube_client.execute(
        'SELECT toTypeName(sum(toUInt64(trade_count))), '
        'toTypeName(sum(toUInt64(taker_buy_trade_count))) FROM origo.fragments'
    ) == [('UInt64', 'UInt64')]


def test_market_state_uses_normalized_rest_timestamp(
    cube_client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    provenance = json.loads((REST / 'provenance.json').read_text())
    requests = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        request = requests.pop(0)
        assert url == request['url']
        assert params == request['params']
        assert headers == {}
        assert weight == (4 if url.endswith('/aggTrades') else 25)
        body = (REST / request['file']).read_bytes()
        assert hashlib.sha256(body).hexdigest() == request['sha256']
        return daily.Response(body, request['response_headers'], request['status'])

    monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinanceSpotProvisional()
    partition = adapter.partition('2025-01-01T00:00:00Z')
    revision = adapter.fetch(partition)
    rows = tuple(revision.rows())
    assert not requests
    assert revision.complete and len(rows) > 1000
    body, canonical_partition, archive_rows = _archive('2025-01-01')
    matching = tuple(
        row
        for row in archive_rows
        if isinstance(row[-1], datetime) and partition.start <= row[-1] < partition.end
    )
    assert {row[0] for row in rows} == {row[0] for row in matching}
    assert all(len(str(row[4])) == 13 for row in rows)
    assert all(len(str(row[4])) == 16 for row in matching)
    expected = _reference(body, {int(str(row[0])) for row in rows})
    canonical = _build(cube_client, canonical_partition, matching)
    provisional = _build(cube_client, partition, rows)
    _assert_cells(_read(cube_client, canonical), expected)
    actual = _read(cube_client, provisional)
    assert {(row[0], row[1], row[4], row[6]) for row in actual} == {
        (row[0], row[1], row[4], row[6]) for row in _read(cube_client, canonical)
    }
    for row in actual:
        reference = expected[(int(str(row[0])), int(str(row[1])))]
        assert isinstance(row[2], datetime)
        # REST preserves milliseconds; the archive retains original microseconds.
        assert row[2].replace(tzinfo=UTC) == reference.first_trade_at.replace(
            microsecond=(reference.first_trade_at.microsecond // 1000) * 1000
        )
        assert float(str(row[3])) == pytest.approx(reference.volume, abs=_ABS_TOL, rel=_REL_TOL)
        assert float(str(row[5])) == pytest.approx(
            reference.taker_buy_volume, abs=_ABS_TOL, rel=_REL_TOL
        )


def test_market_state_excludes_pre2021(cube_client: Client) -> None:
    body, partition, rows = _archive('2017-08-17')
    assert len(rows) == 3427
    assert _reference(body) == {}
    component = _build(cube_client, partition, rows)
    assert _read(cube_client, component) == []
    assert cube_client.execute('SELECT count() FROM origo.raw') == [(3427,)]


def _raw_binary_hash(rows: tuple[Row, ...], component: ComponentSpec) -> str:
    header = json.dumps([(c.name, c.sql_type) for c in component.columns], separators=(',', ':'))
    root = hashlib.sha256(b'origo-source-rowbinary-chunks-v2\n' + header.encode() + b'\n')
    root.update((1).to_bytes(8, 'big'))
    root.update(HASH_CHUNK_ROWS.to_bytes(8, 'big'))
    block = bytearray()
    for row in rows:
        assert isinstance(row[-1], datetime)
        delta = row[-1] - _EPOCH
        micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        block.extend(struct.pack('<QdddQBBq', *row[:-1], micros))
    assert len(rows) < HASH_CHUNK_ROWS
    root.update((0).to_bytes(8, 'big'))
    root.update(len(rows).to_bytes(8, 'big'))
    root.update(hashlib.sha256(block).digest())
    return 'v2:' + root.hexdigest()


def test_market_state_uint32_overflow_and_hash_compatibility(cube_client: Client) -> None:
    assert cube_client.execute("SELECT accurateCast(toUInt64(4294967295), 'UInt32')") == [
        (4294967295,)
    ]
    with pytest.raises(ServerException, match='cannot be safely converted'):
        cube_client.execute("SELECT accurateCast(toUInt64(4294967296), 'UInt32')")
    _, partition, rows = _archive('2025-01-01')
    component = _build(cube_client, partition, rows)
    raw = next(item for item in SPOT_COMPONENTS if item.key == 'raw')
    store = SourceStore(cube_client, 'origo', BINANCE_SPOT_TRADES_SPEC)
    assert store.validate_component(raw, 'origo.raw', partition) == (
        len(rows),
        _raw_binary_hash(rows, raw),
    )
    assert store.validate_component(raw, 'origo.raw', partition, legacy_hash=True) == (
        len(rows),
        content_hash(rows, schema_version=1),
    )
    current = store.validate_component(component, 'origo.market_state', partition)
    legacy = store.validate_component(component, 'origo.market_state', partition, legacy_hash=True)
    assert current[0] == legacy[0] == len(_read(cube_client, component))
    assert current[1].startswith('v2:')
    assert legacy[1] == content_hash(_read(cube_client, component), schema_version=1)
    assert store.validate_component(component, 'origo.market_state', partition) == current
    assert (
        store.validate_component(component, 'origo.market_state', partition, legacy_hash=True)
        == legacy
    )


def test_market_state_is_registered_with_applicability_and_rollout_group() -> None:
    expected = (
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
        'market_state',
        'market_state_latest',
    )
    assert tuple(component.key for component in SPOT_COMPONENTS) == expected
    assert BINANCE_SPOT_TRADES_SPEC.components == SPOT_COMPONENTS
    assert len(MARKET_STATE_COMPONENTS) == 2
    assert set(MARKET_STATE_COMPONENTS) <= set(SPOT_COMPONENTS)
    assert all(item.start_at == CUBE_START for item in MARKET_STATE_COMPONENTS)
    assert all(item.activation_group == 'market_state' for item in MARKET_STATE_COMPONENTS)
