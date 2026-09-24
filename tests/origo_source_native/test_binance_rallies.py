"""Acceptance tests for the Binance spot rally export (#314), on authentic data only.

The 2017-08-17 day is the official Binance archive already used by the source tests; the
2026-06-27 11:38-11:55 UTC excerpt is Origo's own spot trades and depth-200 snapshots,
captured read-only from production (see fixtures/binance_rallies/provenance.json).
"""

from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl
import pyarrow as pa
import pyarrow.ipc
import pytest
from clickhouse_driver import Client
from dagster import materialize

import origo.query.binance_rallies as rallies_module
from origo.query.binance_rallies import (
    BOOK_FIELDS,
    METADATA_KEY,
    OUTPUT_FILENAMES,
    RALLY_FIELDS,
    TRADE_FIELDS,
    export_binance_rallies,
)

from .helpers import BINANCE_FIXTURE_ROOT, SEED_BUILD_ID, SEED_REVISION, seed_spot_source

FIXTURES = Path(__file__).parent / 'fixtures' / 'binance_rallies'
EXPECTED = json.loads((FIXTURES / 'expected.json').read_text())
DAY_2017 = (
    BINANCE_FIXTURE_ROOT / 'spot' / 'daily' / 'trades' / 'revisioned' / 'BTCUSDT-trades-2017-08-17.csv'
)
TRADES_2026 = FIXTURES / 'BTCUSDT-spot-trades-2026-06-27T1138-1155.parquet'
BOOK_2026 = FIXTURES / 'BTCUSDT-spot-depth200-2026-06-27T1138-1155.parquet'
MICROSECOND = timedelta(microseconds=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
LEVELS = pa.list_(pa.struct([pa.field('price', pa.float64()), pa.field('quantity', pa.float64())]))
TYPES = {
    'rally_id': pa.string(),
    'anchor_time': pa.timestamp('us', tz='UTC'),
    'reference_trade_id': pa.uint64(),
    'reference_time': pa.timestamp('us', tz='UTC'),
    'reference_price': pa.float64(),
    'hit_trade_id': pa.uint64(),
    'hit_time': pa.timestamp('us', tz='UTC'),
    'hit_price': pa.float64(),
    'time_to_hit': pa.duration('us'),
    'first_trade_id': pa.uint64(),
    'first_snapshot_time': pa.timestamp('ms', tz='UTC'),
    'last_snapshot_time': pa.timestamp('ms', tz='UTC'),
    'trade_id': pa.uint64(),
    'timestamp': pa.timestamp('us', tz='UTC'),
    'price': pa.float64(),
    'quantity': pa.float64(),
    'quote_quantity': pa.float64(),
    'is_buyer_maker': pa.bool_(),
    'is_best_match': pa.bool_(),
    'observed_at': pa.timestamp('ms', tz='UTC'),
    'last_update_id': pa.uint64(),
    'bids': LEVELS,
    'asks': LEVELS,
}
NULLABLE = {'first_snapshot_time', 'last_snapshot_time'}
# The four rallies anchored in [11:39, 11:55) on 2026-06-27, in anchor order.
WINDOW_ANCHORS = ['11:39', '11:41', '11:42', '11:43']


def _at(clock: str, day: str = '2026-06-27') -> datetime:
    return datetime.fromisoformat(f'{day}T{clock}').replace(tzinfo=UTC)


def _trade_rows_2017() -> list[tuple[Any, ...]]:
    with DAY_2017.open(newline='') as handle:
        return [
            (
                date(2017, 8, 17), '2017-08-17', SEED_REVISION, SEED_BUILD_ID,
                int(trade_id), float(price), float(quantity), float(quote_quantity),
                int(time_ms), int(maker == 'True'), int(best == 'True'),
                EPOCH + timedelta(milliseconds=int(time_ms)),
            )
            for trade_id, price, quantity, quote_quantity, time_ms, maker, best in csv.reader(handle)
        ]


def _reactivate_2017() -> None:
    """Activate a second revision of 2017-08-17 holding the same official rows."""
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
    from origo.sources.contracts import StateRecord
    from origo.sources.storage import SourceStore

    build = UUID(int=2)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute(
            """INSERT INTO origo.binance_spot_trades_raw_revisions
            (source_date, partition_key, revision, build_id, trade_id, price, quantity,
             quote_quantity, timestamp, is_buyer_maker, is_best_match, datetime) VALUES""",
            [(*row[:2], 'reseed', build, *row[4:]) for row in _trade_rows_2017()],
        )
        store = SourceStore(client, 'origo', BINANCE_SPOT_TRADES_SPEC)
        partition = BINANCE_SPOT_TRADES_SPEC.canonical.partition('2017-08-17')
        store.insert_activation(StateRecord(partition, 2, 'reseed', build, ()), 'reseed')
    finally:
        client.disconnect()


def _fixture_trades() -> pl.DataFrame:
    return pl.read_parquet(TRADES_2026)


def _fixture_book() -> pl.DataFrame:
    rename = pl.element().struct.rename_fields(['price', 'quantity'])
    return pl.read_parquet(BOOK_2026).with_columns(
        pl.col('bids').list.eval(rename), pl.col('asks').list.eval(rename)
    )


@pytest.fixture()
def rally_data(origo_test_env: dict[str, str], origo_assets: dict[str, Any]) -> None:
    created = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_snapshots_table_origo'],
        ]
    )
    assert created.success
    for day in ('2017-08-17', '2026-06-27'):
        seed_spot_source(day)
    trades_2026 = [
        (date(2026, 6, 27), '2026-06-27', SEED_REVISION, SEED_BUILD_ID, *row)
        for row in _fixture_trades().iter_rows()
    ]
    book = [
        (observed_at, source_ms, update_id, [tuple(level.values()) for level in bids],
         [tuple(level.values()) for level in asks])
        for observed_at, source_ms, update_id, bids, asks in pl.read_parquet(BOOK_2026).iter_rows()
    ]
    client = Client(
        host=origo_test_env['CLICKHOUSE_HOST'],
        port=int(origo_test_env['CLICKHOUSE_PORT']),
        user=origo_test_env['CLICKHOUSE_USER'],
        password=origo_test_env['CLICKHOUSE_PASSWORD'],
    )
    try:
        client.execute(
            """INSERT INTO origo.binance_spot_trades_raw_revisions
            (source_date, partition_key, revision, build_id, trade_id, price, quantity,
             quote_quantity, timestamp, is_buyer_maker, is_best_match, datetime) VALUES""",
            _trade_rows_2017() + trades_2026,
        )
        client.execute(
            """INSERT INTO origo.binance_spot_depth200_snapshots
            (datetime, source_timestamp_ms, last_update_id, bids, asks) VALUES""",
            book,
        )
    finally:
        client.disconnect()


_names = count()


def _export(tmp_path: Path, **selector: Any) -> dict[str, pa.Table]:
    paths = export_binance_rallies(output_dir=tmp_path / f'export-{next(_names)}', **selector)
    assert [path.name for path in paths] == list(OUTPUT_FILENAMES)
    return {path.name: pyarrow.ipc.open_file(str(path)).read_all() for path in paths}


def _rallies(tables: dict[str, pa.Table]) -> pl.DataFrame:
    frame = pl.from_arrow(tables['rallies.arrow'])
    assert isinstance(frame, pl.DataFrame)
    return frame


def _anchors(tables: dict[str, pa.Table]) -> list[str]:
    return _rallies(tables)['anchor_time'].dt.strftime('%H:%M').to_list()


def _window(tmp_path: Path, **selector: Any) -> dict[str, pa.Table]:
    return _export(tmp_path, start=_at('11:39'), end=_at('11:55'), **selector)


def _selected(tables: dict[str, pa.Table]) -> tuple[list[int], list[int]]:
    """Trades and snapshots each rally's bounds select from the shared files."""
    trades = pl.from_arrow(tables['trades.arrow'])
    book = pl.from_arrow(tables['book.arrow'])
    assert isinstance(trades, pl.DataFrame) and isinstance(book, pl.DataFrame)
    trade_counts, snapshot_counts = [], []
    for rally in _rallies(tables).iter_rows(named=True):
        ids = pl.col('trade_id').is_between(rally['first_trade_id'], rally['hit_trade_id'])
        trade_counts.append(trades.filter(ids).height)
        times = pl.col('observed_at').is_between(
            rally['first_snapshot_time'], rally['last_snapshot_time']
        )
        snapshot_counts.append(book.filter(times).height)
    return trade_counts, snapshot_counts


def test_first_hit_definition(rally_data: None, tmp_path: Path) -> None:
    day = _export(tmp_path, start=_at('00:00', '2017-08-17'), end=_at('00:00', '2017-08-18'))
    rallies = _rallies(day)
    labels = rallies.select(
        pl.col('anchor_time').dt.epoch('s'),
        'reference_trade_id',
        'hit_trade_id',
        pl.col('time_to_hit').dt.total_microseconds(),
    ).rows()
    expected = EXPECTED['BTCUSDT-trades-2017-08-17']
    assert [list(label) for label in labels] == expected['rallies']
    assert len(labels) == 989
    # The 241 anchors up to 04:00 precede the first trade (04:00:28.322) and have no reference.
    assert expected['anchors_without_reference'] == 241
    assert rallies['anchor_time'].min() == _at('04:01', '2017-08-17')
    by_anchor = {row[0]: row[3] for row in labels}
    assert round(by_anchor[int(_at('17:16', '2017-08-17').timestamp())] / 60e6, 1) == 238.8
    assert int(_at('16:56', '2017-08-17').timestamp()) not in by_anchor
    horizons = [
        rallies.filter(pl.col('time_to_hit') < timedelta(minutes=minutes)).height
        for minutes in rallies_module.HORIZON_GRID_MINUTES
    ]
    assert horizons == [96, 217, 310, 562, 700, 830, 954, 989]

    # The 2026 window: the reference is the last trade before the anchor and the hit is the
    # first trade from the anchor on at or above reference x 1.003, in trade-ID order.
    window = _rallies(_window(tmp_path))
    assert [list(row) for row in window.select(
        pl.col('anchor_time').dt.epoch('s'), 'reference_trade_id', 'hit_trade_id',
        pl.col('time_to_hit').dt.total_microseconds(),
    ).rows()] == EXPECTED['BTCUSDT-spot-trades-2026-06-27T1138-1155']['rallies']
    source = _fixture_trades()
    for rally in window.iter_rows(named=True):
        before = source.filter(pl.col('datetime') < rally['anchor_time'])
        assert before['trade_id'].max() == rally['reference_trade_id']
        target = rally['reference_price'] * 1.003
        run = source.filter(
            pl.col('trade_id').is_between(rally['reference_trade_id'] + 1, rally['hit_trade_id'])
        )
        assert (run['price'] >= target).arg_true().to_list() == [run.height - 1]
    tie = source.filter(pl.col('datetime') == _at('11:45:40.878016'))
    assert tie['trade_id'].to_list() == list(range(6453974448, 6453974461))
    assert window['hit_trade_id'][3] == 6453974454 == tie['trade_id'][6]


def test_id_selection(
    rally_data: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ranged = _window(tmp_path)
    ids = _rallies(ranged)['rally_id'].to_list()
    assert ids == [
        f'binance:spot:BTCUSDT:r30v1:t{int(_at(clock).timestamp())}' for clock in WINDOW_ANCHORS
    ]
    selected = _export(tmp_path, rally_ids=[ids[2], ids[0], ids[3], ids[1], ids[0]])
    reordered = _export(tmp_path, rally_ids=list(reversed(ids)))
    for name in OUTPUT_FILENAMES:
        assert selected[name].equals(ranged[name])
        assert selected[name].equals(reordered[name], check_metadata=True)
    request = json.loads(selected['rallies.arrow'].schema.metadata[METADATA_KEY.encode()])['request']
    assert request == {'rally_ids': ids, 'boundary': 'after'}

    anchor = int(_at('11:39').timestamp())
    for value in (
        'nonsense',
        f'binance:spot:BTCUSDT:r31v1:t{anchor}',
        f'binance:perp:BTCUSDT:r30v1:t{anchor}',
        f'binance:spot:BTCUSDT:r30v1:t{anchor + 1}',
        f'binance:spot:BTCUSDT:r30v1:t{anchor + 60}',
    ):
        output = tmp_path / f'rejected-{next(_names)}'
        with pytest.raises(ValueError):
            export_binance_rallies(output_dir=output, rally_ids=[value])
        assert not output.exists()

    # IDs more than 240 minutes apart are read in separate windows from one pin: a revision
    # activated after the first trade read is not read by the later window.
    labels = EXPECTED['BTCUSDT-trades-2017-08-17']['rallies']
    early = labels[0]
    late = next(label for label in labels if label[0] >= early[0] + 241 * 60)
    read = rallies_module._Reader.trades

    def reactivate_after_first_read(self: Any, *args: Any, **kwargs: Any) -> pl.DataFrame:
        if not reactivated:
            _reactivate_2017()
            reactivated.append(True)
        return read(self, *args, **kwargs)

    reactivated: list[bool] = []
    monkeypatch.setattr(rallies_module._Reader, 'trades', reactivate_after_first_read)
    windows = _export(
        tmp_path, rally_ids=[f'binance:spot:BTCUSDT:r30v1:t{label[0]}' for label in (late, early)]
    )
    assert reactivated == [True]
    metadata = json.loads(windows['rallies.arrow'].schema.metadata[METADATA_KEY.encode()])
    assert metadata['sources']['trades']['partitions'] == [
        {'partition_key': '2017-08-17', 'provisional': False, 'revision': SEED_REVISION,
         'build_id': str(SEED_BUILD_ID)}
    ]
    assert _rallies(windows).select(
        pl.col('anchor_time').dt.epoch('s'), 'reference_trade_id', 'hit_trade_id',
        pl.col('time_to_hit').dt.total_microseconds(),
    ).rows() == [tuple(early), tuple(late)]


def test_full_containment(rally_data: None, tmp_path: Path) -> None:
    hit = _at('11:45:40.878016')
    assert _anchors(_window(tmp_path)) == WINDOW_ANCHORS
    later = _export(tmp_path, start=_at('11:40'), end=_at('11:55'))
    assert _anchors(later) == ['11:41', '11:42', '11:43']
    assert _anchors(_export(tmp_path, start=_at('11:39') + MICROSECOND, end=_at('11:55'))) == [
        '11:41', '11:42', '11:43'
    ]
    assert _anchors(_export(tmp_path, start=_at('11:39'), end=hit)) == ['11:39', '11:41', '11:42']
    assert _anchors(_export(tmp_path, start=_at('11:39'), end=hit + MICROSECOND)) == WINDOW_ANCHORS
    by_id = _export(tmp_path, rally_ids=_rallies(later)['rally_id'].to_list())
    for name in OUTPUT_FILENAMES:
        assert by_id[name].equals(later[name])


def test_three_file_export(rally_data: None, tmp_path: Path) -> None:
    output = tmp_path / 'three'
    paths = export_binance_rallies(output_dir=output, start=_at('11:39'), end=_at('11:55'))
    assert sorted(path.name for path in output.iterdir()) == sorted(OUTPUT_FILENAMES)
    tables = {path.name: pyarrow.ipc.open_file(str(path)).read_all() for path in paths}
    for name, fields in zip(OUTPUT_FILENAMES, (RALLY_FIELDS, TRADE_FIELDS, BOOK_FIELDS)):
        schema = tables[name].schema
        assert tuple(schema.names) == fields
        for field in schema:
            assert field.type == TYPES[field.name], field.name
            assert field.nullable == (field.name in NULLABLE), field.name
    metadata = {tables[name].schema.metadata[METADATA_KEY.encode()] for name in OUTPUT_FILENAMES}
    assert len(metadata) == 1
    described = json.loads(metadata.pop())
    assert described['definition']['version'] == 'r30v1'
    assert described['request'] == {
        'start': '2026-06-27T11:39:00+00:00', 'end': '2026-06-27T11:55:00+00:00', 'boundary': 'after'
    }
    assert described['sources']['trades']['partitions'] == [
        {'partition_key': '2026-06-27', 'provisional': False, 'revision': SEED_REVISION,
         'build_id': str(SEED_BUILD_ID)}
    ]

    trades, book = tables['trades.arrow'], tables['book.arrow']
    assert trades.num_rows == 10_369 == len(set(trades['trade_id'].to_pylist()))
    assert book.num_rows == 401 == len(set(book['observed_at'].to_pylist()))
    assert _selected(tables) == ([9792, 7611, 6443, 5909], [399, 279, 219, 161])

    source = _fixture_trades().filter(pl.col('trade_id').is_between(6453964086, 6453974454))
    assert pl.from_arrow(trades).equals(
        source.select(
            'trade_id', pl.col('datetime').alias('timestamp'), 'price', 'quantity',
            'quote_quantity', pl.col('is_buyer_maker').cast(pl.Boolean),
            pl.col('is_best_match').cast(pl.Boolean),
        )
    )
    snapshots = _fixture_book().filter(
        pl.col('datetime').is_between(_at('11:39:00.394'), _at('11:45:40.392'))
    )
    assert pl.from_arrow(book).equals(
        snapshots.select(pl.col('datetime').alias('observed_at'), 'last_update_id', 'bids', 'asks')
    )


def test_boundary_setting(rally_data: None, tmp_path: Path) -> None:
    after, before = _window(tmp_path), _window(tmp_path, boundary='before')
    detection = [
        'rally_id', 'anchor_time', 'reference_trade_id', 'reference_time', 'reference_price',
        'hit_trade_id', 'hit_time', 'hit_price', 'time_to_hit',
    ]
    ra, rb = _rallies(after), _rallies(before)
    assert ra.select(detection).equals(rb.select(detection))

    source = _fixture_trades()
    snapshots = _fixture_book()
    for rally_after, rally_before in zip(ra.iter_rows(named=True), rb.iter_rows(named=True)):
        anchor = rally_after['anchor_time']
        assert rally_after['first_trade_id'] == source.filter(pl.col('datetime') >= anchor)['trade_id'].min()
        assert rally_before['first_trade_id'] == rally_before['reference_trade_id']
        assert rally_after['first_snapshot_time'] == snapshots.filter(pl.col('datetime') >= anchor)['datetime'].min()
        assert rally_before['first_snapshot_time'] == snapshots.filter(pl.col('datetime') < anchor)['datetime'].max()
        assert rally_after['last_snapshot_time'] == rally_before['last_snapshot_time']
    assert rb['first_snapshot_time'][0] == _at('11:38:59.392')
    assert rb['last_snapshot_time'].to_list() == [
        _at('11:45:38.393'), _at('11:45:38.393'), _at('11:45:38.393'), _at('11:45:40.392')
    ]
    assert (after['trades.arrow'].num_rows, after['book.arrow'].num_rows) == (10_369, 401)
    assert (before['trades.arrow'].num_rows, before['book.arrow'].num_rows) == (10_370, 402)
    assert _selected(before) == ([9793, 7612, 6444, 5910], [400, 280, 220, 162])

    # The 11:39 hit is the first of nine trades sharing its timestamp; the eight after it
    # stay in trades.arrow for the later rallies but outside the 11:39 rally's bounds.
    first = ra.row(0, named=True)
    assert first['hit_trade_id'] == 6453973877
    shared = source.filter(pl.col('datetime') == first['hit_time'])['trade_id'].to_list()
    assert shared == list(range(6453973877, 6453973886))
    trade_ids = set(after['trades.arrow']['trade_id'].to_pylist())
    assert set(shared) <= trade_ids
    assert all(trade_id > first['hit_trade_id'] for trade_id in shared[1:])


def test_empty_and_invalid_selectors(rally_data: None, tmp_path: Path) -> None:
    for selector in ({'rally_ids': []}, {'start': _at('11:46'), 'end': _at('11:55')}):
        tables = _export(tmp_path, **selector)
        for name, fields in zip(OUTPUT_FILENAMES, (RALLY_FIELDS, TRADE_FIELDS, BOOK_FIELDS)):
            assert tables[name].num_rows == 0
            assert tuple(tables[name].schema.names) == fields
            assert [field.type for field in tables[name].schema] == [TYPES[f] for f in fields]

    anchor_id = f'binance:spot:BTCUSDT:r30v1:t{int(_at("11:39").timestamp())}'
    for selector in (
        {'rally_ids': [anchor_id], 'start': _at('11:39'), 'end': _at('11:55')},
        {},
        {'start': _at('11:39')},
        {'end': _at('11:55')},
        {'start': datetime(2026, 6, 27, 11, 39), 'end': datetime(2026, 6, 27, 11, 55)},
        {'start': _at('11:55'), 'end': _at('11:39')},
        {'start': _at('11:39'), 'end': _at('11:39')},
        {'start': _at('11:39'), 'end': _at('11:55'), 'boundary': 'middle'},
    ):
        output = tmp_path / f'invalid-{next(_names)}'
        with pytest.raises(ValueError):
            export_binance_rallies(output_dir=output, **selector)
        assert not output.exists()


def test_deterministic_atomic_output(
    rally_data: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = _window(tmp_path), _window(tmp_path)
    for name in OUTPUT_FILENAMES:
        assert first[name].equals(second[name], check_metadata=True)

    exports = sorted(path.name for path in tmp_path.iterdir())
    write = rallies_module._write

    def fail_on_book(path: Path, frame: pl.DataFrame, schema: object) -> None:
        if path.name == 'book.arrow':
            raise OSError('No space left on device')
        write(path, frame, schema)

    monkeypatch.setattr(rallies_module, '_write', fail_on_book)
    with pytest.raises(OSError):
        export_binance_rallies(output_dir=tmp_path / 'failed', start=_at('11:39'), end=_at('11:55'))
    assert sorted(path.name for path in tmp_path.iterdir()) == exports
    monkeypatch.undo()

    existing = tmp_path / 'existing'
    existing.mkdir()
    (existing / 'kept.txt').write_text('kept')
    with pytest.raises(FileExistsError):
        export_binance_rallies(output_dir=existing, start=_at('11:39'), end=_at('11:55'))
    assert [path.name for path in existing.iterdir()] == ['kept.txt']
    assert (existing / 'kept.txt').read_text() == 'kept'
