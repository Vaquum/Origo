from __future__ import annotations

import csv
import json
import math
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import market_state
from origo.query.market_state import (
    CELLS_SCHEMA,
    METADATA_KEY,
    QUERY_SETTINGS,
    SUMMARY_SCHEMA,
    RequestError,
    parse_request,
    pin,
    write_result,
)
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Partition, PartitionPolicy, Revision, SourceError
from origo.sources.hashing import content_hash, state_token
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES
from .test_market_state_registration import CapturedArchive, _capture

T0_US = 1_609_459_200_000_000
BASE_US = 56_250_000
ABS_TOL, REL_TOL = 1e-8, 1e-12
DAY1, DAY2, DAY3 = '2021-01-01', '2021-01-02', '2021-01-03'
MINUTES = ('2021-01-02T00:00:00Z', '2021-01-02T00:01:00Z', '2021-01-02T00:02:00Z')


@dataclass
class CapturedMinutes:
    """Provisional minutes cut from the committed, checksum-proven daily captures."""

    fetched: int = 0

    def partition(self, key: str) -> Partition:
        start = datetime.fromisoformat(key)
        return Partition(key, start, start + timedelta(minutes=1), True)

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        return ()

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        self.fetched += 1
        _, archive = _capture(partition.start.date().isoformat())
        rows = tuple(
            row for row in archive
            if isinstance(row[-1], datetime) and partition.start <= row[-1] < partition.end
        )
        assert rows
        digest = content_hash(rows, schema_version=1)
        return Revision(digest, digest, '{}', len(rows), lambda: rows)


@pytest.fixture
def cube(origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SourceRuntime]:
    """The spot source anchored at the cube's history start, 2021-01-01; nothing built yet."""
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    spec = replace(
        BINANCE_SPOT_TRADES_SPEC,
        canonical=CapturedArchive(),
        partitions=PartitionPolicy(date(2021, 1, 1)),
        provisional=CapturedMinutes(),
        orchestration=replace(BINANCE_SPOT_TRADES_SPEC.orchestration, retry_count=0),
    )
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4()))
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
    try:
        runtime.setup(anchor=datetime(2021, 1, 1, tzinfo=UTC))
        yield runtime
    finally:
        client.disconnect()


def built(runtime: SourceRuntime, *days: str, minutes: tuple[str, ...] = ()) -> SourceRuntime:
    runtime.enable_components('market_state')
    for day in days:
        runtime.build(day)
    for minute in minutes:
        runtime.build(minute, provisional=True)
    return runtime


@dataclass(frozen=True)
class Result:
    answer: dict[str, Any]
    cells: list[dict[str, Any]]
    summary: dict[str, Any]
    staging: Path


def run(runtime: SourceRuntime, tmp_path: Path, **fields: object) -> Result:
    staging = tmp_path / 'staging' / str(uuid4())
    staging.mkdir(parents=True)
    answer = write_result(
        runtime, parse_request(json.dumps(fields).encode()), staging,
        result_id=staging.name, guard=lambda staged: None,
    )
    cells = ipc.open_file(staging / 'cells.arrow').read_all().to_pylist()
    summary = ipc.open_file(staging / 'summary.arrow').read_all().to_pylist()
    assert len(summary) == 1
    return Result(answer, cells, summary[0], staging)


@dataclass(frozen=True)
class Trade:
    micros: int
    price: Decimal
    quote: float
    taker: bool

    @property
    def base(self) -> tuple[int, int]:
        return (self.micros - T0_US) // BASE_US, int(self.price // 125)


def trades(day: str, start: str | None = None, end: str | None = None) -> list[Trade]:
    """The authentic capture's trades, optionally within ``[start, end)`` (ISO UTC)."""
    body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_text()
    low = None if start is None else _micros(start)
    high = None if end is None else _micros(end)
    selected: list[Trade] = []
    for fields in csv.reader(body.splitlines()):
        stamp = int(fields[4]) * (1000 if len(fields[4]) == 13 else 1)
        if (low is None or stamp >= low) and (high is None or stamp < high):
            selected.append(Trade(stamp, Decimal(fields[1]), float(fields[3]), fields[5].lower() == 'false'))
    return selected


def reference(
    selected: list[Trade], n: int, m: int, *, rows: tuple[int, int] | None = None, columns: tuple[int, int] | None = None
) -> dict[tuple[int, int], tuple[float, int, float, int]]:
    """An independent cell reference: exact membership, ``math.fsum`` volumes."""
    groups: dict[tuple[int, int], list[Trade]] = defaultdict(list)
    for trade in selected:
        i, j = trade.base
        if columns is not None and not columns[0] <= i < columns[1]:
            continue
        if rows is not None and not rows[0] <= j < rows[1]:
            continue
        groups[(i >> n if n < 64 else 0, j >> m if m < 64 else 0)].append(trade)
    return {
        key: (
            math.fsum(trade.quote for trade in group), len(group),
            math.fsum(trade.quote for trade in group if trade.taker), sum(trade.taker for trade in group),
        )
        for key, group in groups.items()
    }


def assert_cells(cells: list[dict[str, Any]], expected: dict[tuple[int, int], tuple[float, int, float, int]]) -> None:
    keys = [(cell['time_index'], cell['price_index']) for cell in cells]
    assert keys == sorted(expected)
    for cell in cells:
        volume, count, taker_volume, taker_count = expected[(cell['time_index'], cell['price_index'])]
        assert cell['trade_count'] == count and cell['taker_buy_trade_count'] == taker_count
        assert cell['volume'] == pytest.approx(volume, abs=ABS_TOL, rel=REL_TOL)
        assert cell['taker_buy_volume'] == pytest.approx(taker_volume, abs=ABS_TOL, rel=REL_TOL)


def _micros(value: str) -> int:
    delta = datetime.fromisoformat(value) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def iso(value: str) -> str:
    return datetime.fromisoformat(value).astimezone(UTC).isoformat(timespec='microseconds')


def metadata(path: Path) -> dict[str, Any]:
    return json.loads(ipc.open_file(path).schema.metadata[METADATA_KEY.encode()])


@pytest.mark.parametrize(('n', 'm'), [(0, 0), (1, 0), (0, 1), (2, 3), (6, 0), (11, 2), (64, 64), (1018, 1017)])
def test_cells_match_raw_trades_at_independent_resolutions(
    cube: SourceRuntime, tmp_path: Path, n: int, m: int
) -> None:
    runtime = built(cube, DAY1, minutes=MINUTES)
    tR, pR = 225 * 2**n // 4 if n >= 2 else [56.25, 112.5][n], 125 * 2**m
    result = run(runtime, tmp_path, tR=tR, pR=pR)
    # 2021-01-01's capture and three provisional minutes of 2021-01-02: cells spanning the
    # day boundary at coarse tR, and base cells split across minute partitions, are each
    # summed once.
    expected = reference(trades(DAY1) + trades(DAY2, end='2021-01-02T00:03:00Z'), n, m)
    assert_cells(result.cells, expected)
    assert result.summary['cell_count'] == len(expected)
    if n == 0 and m == 0:
        fragments = runtime.store.execute(
            'SELECT count() FROM (SELECT time_index, price_index, count() AS c '
            'FROM origo.binance_spot_trades_market_state_current GROUP BY time_index, price_index HAVING c > 1)'
        )[0][0]
        assert fragments > 0, 'the minute captures must split at least one base cell'


def test_supplied_bounds_round_to_nearest_base_edges(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1)
    day = trades(DAY1)
    # Column 62 spans 00:58:07.5-00:59:03.75; its midpoint 00:58:35.625 rounds up to edge 63,
    # and digits past microseconds are truncated, so .6249999 rounds down to edge 62.
    up = run(runtime, tmp_path, t1='2021-01-01T00:58:35.625Z', t2='2021-01-01T01:00:00Z', p1='28937.5')
    assert up.answer['effective']['t1'] == '2021-01-01T00:59:03.750000+00:00'
    assert up.answer['effective']['t2'] == '2021-01-01T01:00:00.000000+00:00'
    assert up.answer['effective']['p1'] == 29000.0
    assert_cells(up.cells, reference(day, 0, 0, columns=(63, 64), rows=(232, 233)))
    down = run(runtime, tmp_path, t1='2021-01-01T00:58:35.6249999Z', t2='2021-01-01T01:00:00Z', p1='28937.49')
    assert down.answer['effective']['t1'] == '2021-01-01T00:58:07.500000+00:00'
    assert down.answer['effective']['p1'] == 28875.0
    # Rounding is exact however long the literal: this one lies just below the midpoint.
    long = run(runtime, tmp_path, t1='2021-01-01T00:58:35.6249999Z', t2='2021-01-01T01:00:00Z', p1='28937.4999999999999999999999999999')
    assert long.answer['effective']['p1'] == 28875.0 and long.cells == down.cells
    assert_cells(down.cells, reference(day, 0, 0, columns=(62, 64), rows=(231, 233)))
    # A coarse grid whose rectangle cuts its first/last column and both rows aggregates only
    # the selected base cells.
    partial = run(
        runtime, tmp_path, t1='2021-01-01T00:57:11.25Z', t2='2021-01-01T00:59:03.75Z',
        p1=28875, p2=29125, tR=225, pR=250,
    )
    summary = partial.summary
    assert (summary['first_column_partial'], summary['last_column_partial']) == (True, True)
    assert (summary['first_row_partial'], summary['last_row_partial']) == (True, True)
    assert_cells(partial.cells, reference(day, 2, 1, columns=(61, 63), rows=(231, 233)))


def test_omitted_bounds_and_empty_rectangles(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1)
    day = trades(DAY1)
    rows = sorted({trade.base[1] for trade in day})
    everything = run(runtime, tmp_path)
    assert everything.answer['effective'] == {
        't1': '2021-01-01T00:00:00.000000+00:00', 't2': '2021-01-02T00:00:00.000000+00:00',
        'p1': rows[0] * 125.0, 'p2': (rows[-1] + 1) * 125.0, 'tR': 56.25, 'pR': 125.0,
    }
    assert everything.answer['clipped'] == {'t1': False, 't2': False}
    assert everything.answer['last_column_unfinished'] is False
    assert_cells(everything.cells, reference(day, 0, 0))
    # A supplied bound beyond the other side's automatic extent: the empty interval
    # collapses onto the supplied bound.
    above = run(runtime, tmp_path, p1='30000')
    below = run(runtime, tmp_path, p2='20000')
    assert (above.answer['effective']['p1'], above.answer['effective']['p2']) == (30000.0, 30000.0)
    assert (below.answer['effective']['p1'], below.answer['effective']['p2']) == (20000.0, 20000.0)
    # Supplied time bounds rounding onto one edge; a trade-free window with automatic prices.
    edge = run(runtime, tmp_path, t1='2021-01-01T00:58:10Z', t2='2021-01-01T00:58:20Z')
    assert edge.answer['effective']['t1'] == edge.answer['effective']['t2'] == '2021-01-01T00:58:07.500000+00:00'
    quiet = run(runtime, tmp_path, t1='2021-01-01T12:00:00Z', t2='2021-01-01T13:00:00Z')
    assert (quiet.answer['effective']['p1'], quiet.answer['effective']['p2']) == (None, None)
    # One supplied bound rounding onto the other side's automatic edge is empty too.
    start = run(runtime, tmp_path, t2='2021-01-01T00:00:20Z')
    assert start.answer['effective']['t1'] == start.answer['effective']['t2'] == '2021-01-01T00:00:00.000000+00:00'
    end = run(runtime, tmp_path, t1='2021-01-02T00:00:00Z')
    assert end.answer['effective']['t1'] == end.answer['effective']['t2'] == '2021-01-02T00:00:00.000000+00:00'
    for empty in (above, below, edge, quiet, start, end):
        assert empty.cells == [] and empty.answer['cell_count'] == 0
        summary = empty.summary
        assert (summary['volume'], summary['trade_count']) == (0.0, 0)
        assert (summary['taker_buy_volume'], summary['taker_buy_trade_count']) == (0.0, 0)
        assert summary['poc'] is None and summary['taker_buy_poc'] is None
        assert summary['first_row_partial'] is False and summary['last_row_partial'] is False
        assert metadata(empty.staging / 'summary.arrow')['request'] == metadata(empty.staging / 'cells.arrow')['request']
    assert metadata(quiet.staging / 'summary.arrow')['request'] == {
        't1': '2021-01-01T12:00:00.000000+00:00', 't2': '2021-01-01T13:00:00.000000+00:00',
        'p1': None, 'p2': None, 'tR': 56.25, 'pR': 125.0,
    }


def _row_sums(cells: list[dict[str, Any]], measure: str) -> dict[int, float]:
    grouped: dict[int, list[float]] = defaultdict(list)
    for cell in cells:
        grouped[cell['price_index']].append(cell[measure])
    return {row: math.fsum(values) for row, values in grouped.items()}


def _reference_poc(sums: dict[int, float], pR: float) -> float | None:
    best = max(sums.values(), default=0.0)
    return None if best <= 0.0 else (min(row for row, total in sums.items() if total == best) + 0.5) * pR


@pytest.mark.parametrize(('n', 'm'), [(0, 0), (2, 0), (0, 1), (6, 3)])
def test_pocs_and_totals_follow_emitted_cells(cube: SourceRuntime, tmp_path: Path, n: int, m: int) -> None:
    runtime = built(cube, DAY1)
    tR, pR = [56.25, 112.5, 225, 450, 900, 1800, 3600][n], 125 * 2**m
    result = run(runtime, tmp_path, tR=tR, pR=pR)
    summary = result.summary
    # Totals are fsum over all emitted cells, not over row sums; both reproduce bit for bit.
    assert summary['volume'] == math.fsum(cell['volume'] for cell in result.cells)
    assert summary['taker_buy_volume'] == math.fsum(cell['taker_buy_volume'] for cell in result.cells)
    assert summary['trade_count'] == sum(cell['trade_count'] for cell in result.cells)
    assert summary['taker_buy_trade_count'] == sum(cell['taker_buy_trade_count'] for cell in result.cells)
    assert summary['poc'] == _reference_poc(_row_sums(result.cells, 'volume'), float(pR))
    assert summary['taker_buy_poc'] == _reference_poc(_row_sums(result.cells, 'taker_buy_volume'), float(pR))
    # The authentic taker-free cell: column 62, row 231 (28,875-29,000 USDT) holds five
    # maker-buy trades, so the taker-buy POC has no qualifying volume.
    if (n, m) == (0, 0):
        free = run(
            runtime, tmp_path, t1='2021-01-01T00:58:07.5Z', t2='2021-01-01T00:59:03.75Z', p1=28875, p2=29000
        )
        assert [(cell['time_index'], cell['price_index'], cell['trade_count']) for cell in free.cells] == [(62, 231, 5)]
        assert free.summary['taker_buy_trade_count'] == 0 and free.summary['taker_buy_volume'] == 0.0
        assert free.summary['taker_buy_poc'] is None and free.summary['poc'] == 28937.5


@pytest.mark.parametrize('scenario', ['missing_first_day', 'interior_day_without_the_cube', 'missing_minute'])
def test_coverage_starts_at_2021_and_stops_at_the_first_gap(
    cube: SourceRuntime, tmp_path: Path, scenario: str
) -> None:
    runtime = cube
    if scenario == 'missing_first_day':
        # The first day of the cube's history is missing: no coverage, and every request is a 409.
        runtime.enable_components('market_state')
        runtime.build(DAY2)
        state = pin(runtime.store, QUERY_SETTINGS)
        assert state.records == () and state.cutoff == datetime(2021, 1, 1, tzinfo=UTC)
        with pytest.raises(RequestError) as missing:
            run(runtime, tmp_path)
        assert missing.value.status == 409 and missing.value.body() == {
            'error': 'outside_coverage',
            'history_start': '2021-01-01T00:00:00.000000+00:00',
            'data_cutoff': '2021-01-01T00:00:00.000000+00:00',
        }
        runtime.build(DAY1)
        assert pin(runtime.store, QUERY_SETTINGS).cutoff == datetime(2021, 1, 3, tzinfo=UTC)
        runtime.build(DAY3)
        assert pin(runtime.store, QUERY_SETTINGS).cutoff == datetime(2021, 1, 4, tzinfo=UTC)
        return
    if scenario == 'interior_day_without_the_cube':
        runtime.build(DAY2)  # accepted before the cube was enabled: no market_state component
        runtime.enable_components('market_state')
        runtime.build(DAY1)
        runtime.build(DAY3)
        expected_keys, cutoff = [DAY1], datetime(2021, 1, 2, tzinfo=UTC)
        read = trades(DAY1)
    else:
        built(runtime, DAY1, minutes=(MINUTES[0], MINUTES[2]))
        expected_keys, cutoff = [DAY1, MINUTES[0]], datetime(2021, 1, 2, 0, 1, tzinfo=UTC)
        read = trades(DAY1) + trades(DAY2, end='2021-01-02T00:01:00Z')
    state = pin(runtime.store, QUERY_SETTINGS)
    assert [record.partition.key for record in state.records] == expected_keys
    assert state.cutoff == cutoff
    assert_cells(run(runtime, tmp_path).cells, reference(read, 0, 0))
    with pytest.raises(RequestError) as later:
        run(runtime, tmp_path, t1='2021-01-03T00:00:00Z')
    assert later.value.status == 409


def test_unfinished_tail_and_canonical_through(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1, minutes=MINUTES)
    tail = run(runtime, tmp_path, t1='2021-01-02T00:00:00Z')
    # The cutoff 00:03:00 lies inside base column 1536+3 (00:02:48.75-00:03:45).
    assert tail.answer['data_cutoff'] == '2021-01-02T00:03:00.000000+00:00'
    assert tail.answer['canonical_through'] == '2021-01-02T00:00:00.000000+00:00'
    assert tail.answer['effective']['t2'] == '2021-01-02T00:03:45.000000+00:00'
    assert tail.answer['last_column_unfinished'] is True and tail.summary['last_column_unfinished'] is True
    assert_cells(tail.cells, reference(trades(DAY2, end='2021-01-02T00:03:00Z'), 0, 0))
    # A coarse column may end after the cutoff while the selection itself ends before it.
    covered = run(runtime, tmp_path, t1='2021-01-02T00:00:00Z', t2='2021-01-02T00:02:48.75Z', tR=3600)
    assert covered.answer['last_column_unfinished'] is False
    assert covered.summary['last_column_partial'] is True
    # Clipping at both ends is reported, never silent.
    clipped = run(runtime, tmp_path, t1='2020-12-31T00:00:00Z', t2='2021-01-02T06:00:00Z')
    assert clipped.answer['clipped'] == {'t1': True, 't2': True}
    assert clipped.answer['effective']['t1'] == '2021-01-01T00:00:00.000000+00:00'
    assert clipped.answer['effective']['t2'] == '2021-01-02T00:03:45.000000+00:00'


def test_one_pinned_state_serves_the_whole_request(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = built(cube, DAY1, minutes=MINUTES)
    pinned = pin(runtime.store, QUERY_SETTINGS)
    other = SourceRuntime(runtime.spec, SourceStore(make_clickhouse_client(get_clickhouse_settings()), 'origo', runtime.spec), runtime.lock_root, str(uuid4()))
    connect = market_state._connect
    changes: list[str] = []

    def concurrent_change() -> object:
        # After the pin and before any read: accept the canonical day that supersedes the
        # pinned minutes, and run maintenance that must not wait for or fail on the query.
        other.build(DAY2)
        other.cleanup(dry_run=True)
        changes.append('applied')
        return connect()

    monkeypatch.setattr(market_state, '_connect', concurrent_change)
    try:
        result = run(runtime, tmp_path, t1='2021-01-02T00:00:00Z')
    finally:
        other.store.client.disconnect()
    assert changes == ['applied']
    assert result.answer['data_cutoff'] == '2021-01-02T00:03:00.000000+00:00'
    read = [record for record in pinned.records if record.partition.provisional]
    assert result.answer['state_token'] == state_token('binance_spot_trades', tuple(read))
    assert [pin_[0] for pin_ in metadata(result.staging / 'summary.arrow')['pins']] == list(MINUTES)
    assert_cells(result.cells, reference(trades(DAY2, end='2021-01-02T00:03:00Z'), 0, 0))
    # The next request pins the new canonical day.
    fresh = run(runtime, tmp_path, t1='2021-01-02T00:00:00Z')
    assert [pin_[0] for pin_ in metadata(fresh.staging / 'summary.arrow')['pins']] == [DAY2]


def test_reclaimed_pinned_build_discards_the_result(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = built(cube, DAY1, minutes=MINUTES)
    target = next(record for record in pin(runtime.store, QUERY_SETTINGS).records if record.partition.key == MINUTES[1])
    connect = market_state._connect

    def cleanup_during_read() -> object:
        # What cleanup does to a build: delete its rows, then log the build id with the
        # cleanup's start time, which precedes this request.
        client = make_clickhouse_client(get_clickhouse_settings())
        try:
            for component in ('raw_latest', 'time_latest', 'market_state_latest'):
                client.execute(
                    f'ALTER TABLE origo.binance_spot_trades_{component}_revisions DELETE '
                    'WHERE partition_key=%(partition)s AND build_id=%(build)s',
                    {'partition': target.partition.key, 'build': target.build_id}, settings={'mutations_sync': 2},
                )
            client.execute(
                'INSERT INTO origo.source_cleanup_log VALUES',
                [('binance_spot_trades', target.partition.key, target.build_id, 'test-cleanup', datetime(2021, 1, 1, tzinfo=UTC))],
            )
        finally:
            client.disconnect()
        return connect()

    monkeypatch.setattr(market_state, '_connect', cleanup_during_read)
    with pytest.raises(SourceError) as reclaimed:
        run(runtime, tmp_path, t1='2021-01-02T00:00:00Z')
    assert reclaimed.value.code == 'SOURCE_MAINTENANCE'
    # A cleanup still holding the fence past the deadline is refused the same way.
    monkeypatch.setattr(market_state, '_connect', connect)
    monkeypatch.setattr(market_state, 'FENCE_WAIT_SECONDS', 1.0)
    with source_lock(runtime.lock_root, 'binance_spot_trades', 'heavy'):
        with pytest.raises(SourceError) as held:
            run(runtime, tmp_path, t1='2021-01-01T00:00:00Z', t2='2021-01-01T01:00:00Z')
    assert held.value.code == 'SOURCE_MAINTENANCE'
    # Any other lock failure stays itself: a persistent fault is not a retryable 503.

    @contextmanager
    def broken_lock(*args: object, **kwargs: object) -> Iterator[None]:
        raise SourceError('SOURCE_LOCK_INVALID', 'The lock file could not be opened.')
        yield

    monkeypatch.setattr(market_state, 'source_lock', broken_lock)
    with pytest.raises(SourceError) as broken:
        run(runtime, tmp_path, t1='2021-01-01T00:00:00Z', t2='2021-01-01T01:00:00Z')
    assert broken.value.code == 'SOURCE_LOCK_INVALID'


@pytest.mark.parametrize(
    ('raw', 'reason', 'field'),
    [
        (b'{', 'invalid_json', None),
        (b'[]', 'invalid_json', None),
        (b'[' * 10_000, 'invalid_json', None),
        (b'', 'invalid_json', None),
        (b'{"tR": 1e10000000000}', 'unsupported_resolution', 'tR'),
        (b'{"pR": 1e-10000000000}', 'unsupported_resolution', 'pR'),
        (b'{"t1": "0001-01-01T00:00:00+01:00"}', 'invalid_time', 't1'),
        (b'{"t2": "9999-12-31T23:59:59-01:00"}', 'invalid_time', 't2'),
        (b'{"tR": NaN}', 'invalid_json', None),
        (b'{"tR": 900, "tR": 1800}', 'invalid_json', None),
        (b'{"x": 1}', 'unknown_field', 'x'),
        (b'{"t1": 5}', 'invalid_time', 't1'),
        (b'{"t1": "yesterday"}', 'invalid_time', 't1'),
        (b'{"t1": "2021-01-01"}', 'time_zone_required', 't1'),
        (b'{"t2": "2021-01-01T00:00:00"}', 'time_zone_required', 't2'),
        (b'{"t1": "2021-01-02T00:00:00Z", "t2": "2021-01-01T00:00:00Z"}', 'bounds_out_of_order', 't2'),
        (b'{"p1": true}', 'invalid_price', 'p1'),
        (b'{"p1": -1}', 'invalid_price', 'p1'),
        (b'{"p1": "abc"}', 'invalid_price', 'p1'),
        (b'{"p2": "NaN"}', 'invalid_price', 'p2'),
        (b'{"p2": "1e400"}', 'invalid_price', 'p2'),
        (b'{"p1": 30000, "p2": 20000}', 'bounds_out_of_order', 'p2'),
        (b'{"tR": 60}', 'unsupported_resolution', 'tR'),
        (b'{"tR": "900"}', 'unsupported_resolution', 'tR'),
        (b'{"tR": true}', 'unsupported_resolution', 'tR'),
        (b'{"tR": 0}', 'unsupported_resolution', 'tR'),
        (b'{"tR": -56.25}', 'unsupported_resolution', 'tR'),
        (b'{"tR": 56.250000000000001}', 'unsupported_resolution', 'tR'),
        (b'{"pR": 100}', 'unsupported_resolution', 'pR'),
    ],
)
def test_invalid_requests_are_rejected_with_reasons(raw: bytes, reason: str, field: str | None) -> None:
    with pytest.raises(RequestError) as rejected:
        parse_request(raw)
    body = rejected.value.body()
    assert rejected.value.status == 400 and body['error'] == 'invalid_request' and body['reason'] == reason
    assert body.get('field') == field
    assert json.dumps(body, allow_nan=False)


def test_resolutions_up_to_the_float64_range_are_exact() -> None:
    started = time.perf_counter()
    tiny = parse_request(b'{"p1": 1e-10000000000}')
    assert tiny.p1 == Decimal('1e-10000000000') and time.perf_counter() - started < 1
    largest_time = 225 * 2**1016  # 56.25 x 2^1018
    largest_price = 125 * 2**1017
    request = parse_request(json.dumps({'tR': largest_time, 'pR': largest_price}).encode())
    assert (request.time_exponent, request.price_exponent) == (1018, 1017)
    assert math.isfinite(request.time_resolution) and math.isfinite(request.price_resolution)
    for field, value in (('tR', largest_time * 2), ('pR', largest_price * 2)):
        with pytest.raises(RequestError) as beyond:
            parse_request(json.dumps({field: value}).encode())
        assert beyond.value.reason == 'unsupported_resolution'


@pytest.mark.parametrize(('n', 'm'), [(0, 0), (3, 0), (0, 2), (11, 5)])
def test_row_sums_and_totals_stream_exactly(cube: SourceRuntime, tmp_path: Path, n: int, m: int) -> None:
    runtime = built(cube, DAY1, DAY2, DAY3)
    pR = 125.0 * 2**m
    result = run(runtime, tmp_path, tR=56.25 * 2**n, pR=pR)
    cells = result.cells
    assert result.summary['volume'] == math.fsum(cell['volume'] for cell in cells)
    assert result.summary['taker_buy_volume'] == math.fsum(cell['taker_buy_volume'] for cell in cells)
    assert result.summary['poc'] == _reference_poc(_row_sums(cells, 'volume'), pR)
    assert result.summary['taker_buy_poc'] == _reference_poc(_row_sums(cells, 'taker_buy_volume'), pR)
    rows = np.array([cell['price_index'] for cell in cells], dtype=np.uint64)
    for measure in ('volume', 'taker_buy_volume'):
        values = np.array([cell[measure] for cell in cells], dtype=np.float64)
        expected_keys = {
            int(row) * 2048 + max(int(bits) >> 52, 1) for row, bits in zip(rows, values.view(np.uint64), strict=True)
        }
        # Any batching gives math.fsum's result, from one integer per (row, exponent).
        for size in (1, 7, 65_536):
            sums = market_state._ExactSums()
            for start in range(0, len(values), size):
                sums.add(rows[start:start + size], values[start:start + size])
            assert sums.rows() == _row_sums(cells, measure)
            assert sums.total() == math.fsum(values.tolist())
            assert set(sums.parts) == expected_keys
    # A volume the exact sum cannot represent stops the result instead of summing wrongly.
    with pytest.raises(ValueError, match='finite and not negative'):
        market_state._ExactSums().add(np.array([0], dtype=np.uint64), np.array([math.inf]))


def _statement(query: str) -> str:
    for marker, name in (
        ('component_hashes', 'pin'), ('min(price_index)', 'extent'), ('sumKahan(volume)', 'cells'), ('source_cleanup_log', 'validate'),
    ):
        if marker in query:
            return name
    return query


def test_query_runs_with_declared_clickhouse_settings(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1)
    result = run(runtime, tmp_path, tR=900, pR=250)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute('SYSTEM FLUSH LOGS')
        rows = client.execute(
            "SELECT query, Settings FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = %(result)s "
            'ORDER BY event_time_microseconds',
            {'result': result.staging.name},
        )
        # query_log records only settings that differ from the server default.
        defaults = dict(client.execute(
            "SELECT name, value FROM system.settings WHERE name IN ('timeout_overflow_mode')"
        ))
    finally:
        client.disconnect()
    # Every statement of the request, the pin included, carries the declared bounds and its result ID.
    assert [_statement(query) for query, _ in rows] == ['pin', 'extent', 'cells', 'validate']
    for _, settings in rows:
        effective = {**defaults, **settings}
        assert effective['max_threads'] == '4'
        assert effective['max_memory_usage'] == str(4 * 1024**3)
        assert effective['max_bytes_before_external_group_by'] == str(2 * 1024**3)
        assert effective['max_bytes_before_external_sort'] == str(2 * 1024**3)
        assert effective['max_execution_time'] == '60'
        assert effective['timeout_overflow_mode'] == 'throw'
        assert effective['max_block_size'] == '65536'
        assert effective['log_comment'] == result.staging.name
    assert SUMMARY_SCHEMA.field('p1').nullable and not SUMMARY_SCHEMA.field('first_row_partial').nullable
    assert [field.name for field in CELLS_SCHEMA] == [
        'time_index', 'price_index', 'volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count'
    ]
