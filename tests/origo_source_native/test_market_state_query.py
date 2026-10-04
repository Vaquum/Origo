from __future__ import annotations

import csv
import hashlib
import json
import math
import time
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
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
    MEASURES,
    METADATA_KEY,
    QUERY_SETTINGS,
    SUMMARY_SCHEMA,
    Pin,
    RequestError,
    parse_request,
    pin,
    write_result,
)
from origo.query.market_state_reader import query, read_table
from origo.query.market_state_results import ResultStore
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Partition, PartitionPolicy, Revision, Row, SourceError
from origo.sources.hashing import content_hash, state_token
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.storage import SourceStore
from origo.workers.market_state_api import MarketStateApi, serve

from .test_binance_daily_source_adapter import ARCHIVES
from .test_market_state_detail import Cell
from .test_market_state_detail import Trade as Traded
from .test_market_state_detail import _reference as _detail_reference
from .test_market_state_detail import _trades as _detail_trades
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


@pytest.mark.parametrize('scenario', [
    'empty', 'missing_first_day', 'canonical_gap', 'cube_component_gap', 'minute_gap',
    'contiguous', 'canonical_replacement', 'detail_component_gap', 'repeated_anchor',
])
@pytest.mark.parametrize('detail', [False, True])
def test_pin_matches_current_view_for_recorded_coverage(
    cube: SourceRuntime, monkeypatch: pytest.MonkeyPatch, scenario: str, detail: bool
) -> None:
    runtime = cube
    if scenario == 'cube_component_gap':
        runtime.build(DAY2)
    runtime.enable_components('market_state')
    if scenario != 'detail_component_gap':
        runtime.enable_components('market_state_detail')
    if scenario == 'missing_first_day':
        runtime.build(DAY2)
    elif scenario in ('canonical_gap', 'cube_component_gap'):
        runtime.build(DAY1)
        runtime.build(DAY3)
    elif scenario == 'minute_gap':
        built(runtime, DAY1, minutes=(MINUTES[0], MINUTES[2]))
    elif scenario in ('contiguous', 'canonical_replacement', 'repeated_anchor'):
        built(runtime, DAY1, minutes=MINUTES)
        if scenario == 'canonical_replacement':
            runtime.build(DAY2)
    elif scenario == 'detail_component_gap':
        runtime.build(DAY1)
        runtime.enable_components('market_state_detail')
        runtime.build(DAY2)

    if scenario == 'repeated_anchor':
        runtime.store.execute(
            "INSERT INTO origo.source_anchor_log SELECT * FROM origo.source_anchor_log "
            "WHERE source_key=%(source)s", {'source': runtime.spec.key},
        )

    # Both selectors read states built from checksum-proven canonical captures and their
    # actual provisional minutes, including a canonical day superseding those minutes.
    current_rows = runtime.store.execute(
        "SELECT partition_key, provisional, partition_start, partition_end, generation, "
        "revision, build_id, component_hashes FROM origo.source_current_partitions "
        "WHERE source_key=%(source)s ORDER BY partition_start, provisional",
        {'source': runtime.spec.key}, QUERY_SETTINGS,
    )
    execute = runtime.store.execute
    selected: list[Row] = []

    def capture(
        statement: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        rows = execute(statement, params, settings)
        selected.extend(rows)
        return rows

    with monkeypatch.context() as patch:
        patch.setattr(runtime.store, 'execute', capture)
        actual = pin(runtime.store, QUERY_SETTINGS, detail=detail)
    with monkeypatch.context() as patch:
        patch.setattr(runtime.store, 'execute', lambda *args: current_rows)
        expected = pin(runtime.store, QUERY_SETTINGS, detail=detail)
    assert selected == current_rows
    assert actual == expected
    if scenario == 'detail_component_gap':
        assert [record.partition.key for record in actual.records] == ([] if detail else [DAY1, DAY2])
    elif scenario == 'canonical_replacement':
        assert [record.partition.key for record in actual.records] == [DAY1, DAY2]


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
        (b'{"measures": "dwell"}', 'invalid_measures', 'measures'),
        (b'{"measures": {"dwell": true}}', 'invalid_measures', 'measures'),
        (b'{"measures": ["dwell", "dwell"]}', 'invalid_measures', 'measures'),
        (b'{"measures": ["volume"]}', 'invalid_measures', 'measures'),
        (b'{"measures": ["Dwell"]}', 'invalid_measures', 'measures'),
        (b'{"measures": [1]}', 'invalid_measures', 'measures'),
        (b'{"measures": [null]}', 'invalid_measures', 'measures'),
        (b'{"measures": [["dwell"]]}', 'invalid_measures', 'measures'),
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
def test_row_sums_and_totals_stream_exactly(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, n: int, m: int
) -> None:
    runtime = built(cube, DAY1, DAY2, DAY3)
    pR = 125.0 * 2**m
    # One cell per batch, so every sum crosses batch boundaries.
    monkeypatch.setattr(market_state, 'QUERY_SETTINGS', {**market_state.QUERY_SETTINGS, 'max_block_size': 1})
    result = run(runtime, tmp_path, tR=56.25 * 2**n, pR=pR)
    cells = result.cells
    assert len(cells) >= 2
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


def test_exact_sums_match_fsum_across_the_float64_range() -> None:
    # Arithmetic edge cases, not market data: zeros, subnormals, the largest mantissas and
    # exponents far apart, split over batches, where rounding each row first would differ.
    tiny, huge = math.ulp(0.0), float.fromhex('0x1.fffffffffffffp+1000')
    values = [0.0, tiny, 5 * tiny, 2.0**-1022, math.nextafter(2.0**52, 0.0), 2.0**52, 1e16, 1.0, 1e-16, 1e-16, huge, 3.0]
    rows = [0, 0, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4]
    for size in (1, 2, 5, 12):
        sums = market_state._ExactSums()
        for start in range(0, len(values), size):
            sums.add(np.array(rows[start:start + size], dtype=np.uint64), np.array(values[start:start + size]))
        grouped: dict[int, list[float]] = defaultdict(list)
        for row, value in zip(rows, values, strict=True):
            grouped[row].append(value)
        assert sums.rows() == {row: math.fsum(parts) for row, parts in grouped.items()}
        assert sums.total() == math.fsum(values)
    # The total is the correctly rounded sum of every value, not a sum of rounded row totals.
    split = market_state._ExactSums()
    split.add(np.array([0, 0, 1], dtype=np.uint64), np.array([1.0, 1e-16, 1e-16]))
    assert split.rows() == {0: 1.0, 1: 1e-16}
    assert split.total() == math.fsum([1.0, 1e-16, 1e-16]) != math.fsum([1.0, 1e-16])


def server_defaults(client: Any) -> dict[str, str]:
    """The server's default settings, which query_log's Settings omits: a setting equal to its
    default, such as ``max_threads`` 4 on a four-core machine, is not recorded as changed."""
    return dict(client.execute('SELECT name, value FROM system.settings'))


def effective(settings: dict[str, str], defaults: dict[str, str], name: str) -> str:
    # system.settings shows the automatic thread count quoted, as 'auto(4)'.
    value = settings.get(name, defaults.get(name, ''))
    return value.strip("'").removeprefix('auto(').removesuffix(')')


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
        defaults = server_defaults(client)
    finally:
        client.disconnect()
    # Every statement of the request, the pin included, carries the declared bounds and its result ID.
    assert [_statement(query) for query, _ in rows] == ['pin', 'extent', 'cells', 'validate']
    for _, settings in rows:
        assert effective(settings, defaults, 'max_threads') == '4'
        assert effective(settings, defaults, 'max_memory_usage') == str(4 * 1024**3)
        # Nothing spills: neither the absolute triggers nor 25.3's default ratio triggers are on.
        for trigger in ('max_bytes_before_external_group_by', 'max_bytes_before_external_sort',
                        'max_bytes_ratio_before_external_group_by', 'max_bytes_ratio_before_external_sort'):
            assert effective(settings, defaults, trigger) == '0', trigger
        assert effective(settings, defaults, 'max_execution_time') == '60'
        assert effective(settings, defaults, 'timeout_overflow_mode') == 'throw'
        assert effective(settings, defaults, 'max_block_size') == '65536'
        assert settings['log_comment'] == result.staging.name
    assert SUMMARY_SCHEMA.field('p1').nullable and not SUMMARY_SCHEMA.field('first_row_partial').nullable
    assert [field.name for field in CELLS_SCHEMA] == [
        'time_index', 'price_index', 'volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count'
    ]


def test_statements_open_no_clickhouse_session(origo_test_env: dict[str, str]) -> None:
    # ClickHouse releases a session only after its answer is sent, so statements sent back to back
    # on one session intermittently fail with SESSION_IS_LOCKED; without one nothing carries over.
    client = market_state._connect()
    try:
        client.raw_query('SET max_threads = 7', settings={}, fmt=None, external_data=None)
        carried = client.raw_query("SELECT getSetting('max_threads') = 7", settings={}, fmt='TabSeparated', external_data=None)
    finally:
        client.close()
    assert carried == b'0\n'


# PRD-0023: measures from the detail component. The reference is the detail tests' independent
# per-partition build from the capture text, grouped here to the requested grid.

_UNITS = {'base_volume': 100_000_000, 'path_length': 100, 'dwell': 1_000_000}
_PRICES = ('high', 'low', 'open', 'open_at', 'close', 'close_at')


def detailed(runtime: SourceRuntime, *days: str, minutes: tuple[str, ...] = ()) -> SourceRuntime:
    """The cube and its detail component enabled, then the days and minutes built."""
    runtime.enable_components('market_state_detail')
    return built(runtime, *days, minutes=minutes)


def alone(runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch, day: str) -> SourceRuntime:
    """A later authentic capture, queried by itself.

    Coverage runs from 2021-01-01 to the first missing day, so the capture's day is pinned on
    its own, with both components.
    """
    detailed(runtime, day)
    record = runtime.store.record(CapturedArchive().partition(day))
    assert record is not None
    assert {'market_state', 'market_state_detail'} <= dict(record.component_hashes).keys()

    def pinned(store: SourceStore, settings: object, *, detail: bool = False) -> Pin:
        return Pin((record,), record.partition.end, record.partition.end)

    monkeypatch.setattr(market_state, 'pin', pinned)
    return runtime


def partition_cells(day: str, start: str, end: str, *, provisional: bool = False) -> dict[tuple[int, int], Cell]:
    """One built partition's detail cells: a canonical day, or a provisional minute in milliseconds."""
    body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_bytes()
    low, high = _micros(start), _micros(end)
    selected = [trade for trade in _detail_trades(body, milliseconds=provisional) if low <= trade.micros < high]
    return _detail_reference(selected, low, high)


def measured(
    parts: list[dict[tuple[int, int], Cell]], n: int, m: int, *,
    columns: tuple[int, int] | None = None, rows: tuple[int, int] | None = None,
) -> dict[tuple[int, int], Cell]:
    """Base cells of every partition grouped into (I, J) cells, trades pooled, sums added."""
    grouped: dict[tuple[int, int], Cell] = {}
    for cells in parts:
        for (column, row), cell in cells.items():
            if (columns is not None and not columns[0] <= column < columns[1]) or (
                rows is not None and not rows[0] <= row < rows[1]
            ):
                continue
            group = grouped.setdefault((column >> n if n < 64 else 0, row >> m if m < 64 else 0), Cell(cell.event))
            group.trades += cell.trades
            group.path += cell.path
            group.dwell += cell.dwell
    return grouped


def _moment(micros: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def expected_measures(cell: Cell, measures: tuple[str, ...]) -> dict[str, object]:
    """A cell's measure columns: integer sums divided once, prices of its trades or null."""
    sums = {'base_volume': sum(trade.sats for trade in cell.trades), 'path_length': cell.path, 'dwell': cell.dwell}
    values: dict[str, object] = {name: sums[name] / _UNITS[name] for name in _UNITS if name in measures}
    first = min(cell.trades, key=lambda trade: trade.id, default=None)
    last = max(cell.trades, key=lambda trade: trade.id, default=None)
    prices = {
        'high': max((trade.price for trade in cell.trades), default=None),
        'low': min((trade.price for trade in cell.trades), default=None),
        'open': None if first is None else first.price, 'open_at': None if first is None else _moment(first.micros),
        'close': None if last is None else last.price, 'close_at': None if last is None else _moment(last.micros),
    }
    values.update({name: prices[name] for name in _PRICES if name.removesuffix('_at') in measures})
    return values


def assert_measures(cells: list[dict[str, Any]], expected: dict[tuple[int, int], Cell], measures: tuple[str, ...]) -> None:
    moved = not {'path_length', 'dwell'}.isdisjoint(measures)
    emitted = {key: cell for key, cell in expected.items() if cell.trades or moved}
    assert [(cell['time_index'], cell['price_index']) for cell in cells] == sorted(emitted)
    names = [name for name in (*_UNITS, *_PRICES) if name.removesuffix('_at') in measures]
    for cell in cells:
        assert {name: cell[name] for name in names} == expected_measures(
            emitted[(cell['time_index'], cell['price_index'])], measures
        )


def statement_hashes(result_id: str) -> list[str]:
    """SHA-256 of every statement one result ran, in order."""
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute('SYSTEM FLUSH LOGS')
        rows = client.execute(
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = %(result)s "
            'ORDER BY event_time_microseconds',
            {'result': result_id},
        )
    finally:
        client.disconnect()
    return [hashlib.sha256(str(row[0]).encode()).hexdigest() for row in rows]


@pytest.mark.parametrize(('n', 'm'), [(0, 0), (1, 0), (0, 1), (2, 0), (2, 3), (6, 0), (11, 2), (64, 64), (1018, 1017)])
def test_measures_match_raw_trades_at_independent_resolutions(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, n: int, m: int
) -> None:
    runtime = detailed(cube, DAY1, minutes=MINUTES)
    tR, pR = 225 * 2**n // 4 if n >= 2 else [56.25, 112.5][n], 125 * 2**m
    result = run(runtime, tmp_path, tR=tR, pR=pR, measures=list(MEASURES))
    # 2021-01-01's capture as its day and three provisional minutes of 2021-01-02, each built
    # from its own trades; base cells split across minutes are summed once.
    parts = [partition_cells(DAY1, '2021-01-01T00:00:00+00:00', '2021-01-02T00:00:00+00:00')] + [
        partition_cells(DAY2, minute, (datetime.fromisoformat(minute) + timedelta(minutes=1)).isoformat(), provisional=True)
        for minute in MINUTES
    ]
    expected = measured(parts, n, m)
    assert_measures(result.cells, expected, MEASURES)
    assert result.summary['cell_count'] == len(result.cells) == len(expected)
    totals = {'base_volume': sum(trade.sats for cell in expected.values() for trade in cell.trades),
              'path_length': sum(cell.path for cell in expected.values()),
              'dwell': sum(cell.dwell for cell in expected.values())}
    assert {name: result.summary[name] for name in _UNITS} == {name: totals[name] / _UNITS[name] for name in _UNITS}
    # Over the same covered rectangle, PRD-0022's measures, totals and POCs are bit-identical.
    effective = result.answer['effective']
    plain = run(runtime, tmp_path, t1=effective['t1'], t2=effective['t2'], p1=effective['p1'], p2=effective['p2'], tR=tR, pR=pR)
    names = [field.name for field in CELLS_SCHEMA]
    assert [{name: cell[name] for name in names} for cell in result.cells if cell['trade_count']] == plain.cells
    assert all(
        (cell['volume'], cell['taker_buy_volume'], cell['taker_buy_trade_count']) == (0.0, 0.0, 0)
        for cell in result.cells if not cell['trade_count']
    )
    for name in ('volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count', 'poc', 'taker_buy_poc'):
        assert result.summary[name] == plain.summary[name], name
    if (n, m) == (2, 0):
        # The new totals are the integer totals divided once, not fsum of the emitted cells.
        alone(runtime, monkeypatch, '2025-01-01')
        day = run(runtime, tmp_path, t1='2025-01-01T00:00:00Z', t2='2025-01-02T00:00:00Z', tR=225, pR=125, measures=['base_volume'])
        satoshis = sum(trade.sats for trade in _detail_trades((ARCHIVES / 'BTCUSDT-trades-2025-01-01.csv').read_bytes()))
        assert len(day.cells) == 5 and satoshis == 12_348_987_000
        assert day.summary['base_volume'] == satoshis / 10**8 == 123.48987
        assert math.fsum(cell['base_volume'] for cell in day.cells) == 123.48987000000001


def test_measures_selection_through_the_reader(cube: SourceRuntime, tmp_path: Path) -> None:
    # The API tests import this module, so their helpers are imported here, once both exist.
    from .test_market_state_api import RecordingReporter, roomy

    detailed(cube, DAY1)
    api = MarketStateApi(ResultStore(tmp_path / 'market-state', disk=roomy), RecordingReporter(), cube.lock_root, interrupted=1)
    server = serve(api, port=0)
    api.port = server.server_address[1]
    url = f'http://127.0.0.1:{api.port}'
    base = [field.name for field in CELLS_SCHEMA]
    try:
        for measures, columns in (
            (None, []),
            ([], []),
            (['dwell'], ['dwell']),
            (['close', 'dwell', 'high', 'open'], ['dwell', 'high', 'open', 'open_at', 'close', 'close_at']),
        ):
            result = query(t1='2021-01-01T00:00:00Z', t2='2021-01-02T00:00:00Z', measures=measures, url=url)
            cells, summary = read_table(result.cells, url=url), read_table(result.summary, url=url)
            assert cells.column_names == base + columns
            assert summary.column_names == [field.name for field in SUMMARY_SCHEMA] + (['dwell'] if 'dwell' in columns else [])
            chosen = [name for name in MEASURES if name in (measures or [])]
            for table in (cells, summary):
                meta = json.loads(table.schema.metadata[METADATA_KEY.encode()])
                assert meta['schema_version'] == (2 if chosen else 1)
                assert meta['request'].get('measures', []) == chosen and ('measures' in meta['request']) == bool(chosen)
            rows = cells.to_pylist()
            if 'high' in columns:
                assert any(not row['trade_count'] for row in rows)
                for row in rows:
                    assert all((row[name] is None) == (not row['trade_count']) for name in columns[1:]), row
        with pytest.raises(TypeError, match='not one string'):
            query(measures='dwell', url=url)
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize('day', [DAY1, '2021-05-19'])
def test_column_prices_at_a_coarse_price_resolution(
    cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, day: str
) -> None:
    runtime = detailed(cube, DAY1) if day == DAY1 else alone(cube, monkeypatch, day)
    body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_bytes()
    trades = _detail_trades(body)
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    cells = _detail_reference(trades, _micros(start.isoformat()), _micros((start + timedelta(days=1)).isoformat()))
    assert max(key[1] for key in cells) < 2**9
    # Base columns, then columns of 32 hours that hold the day's untraded head and tail with its trades.
    for n in (0, 11):
        columns: dict[int, list[Traded]] = defaultdict(list)
        for trade in trades:
            columns[trade.column >> n].append(trade)
        # 2021-05-19's column 212796 holds a row the path crossed eight times without a trade.
        mixed = {column >> n for (column, _), cell in cells.items() if not cell.trades} & set(columns)
        assert bool(mixed) == (n == 11 or day != DAY1)
        result = run(
            runtime, tmp_path, t1=start.isoformat(), t2=(start + timedelta(days=1)).isoformat(),
            tR=56.25 * 2**n, pR=125 * 2**9, measures=['path_length', 'high', 'low', 'open', 'close'],
        )
        assert {cell['price_index'] for cell in result.cells} == {0}
        assert {cell['time_index'] for cell in result.cells if cell['trade_count']} == set(columns)
        for cell in result.cells:
            group = columns.get(cell['time_index'], [])
            first = min(group, key=lambda trade: trade.id, default=None)
            last = max(group, key=lambda trade: trade.id, default=None)
            assert (cell['open'], cell['high'], cell['low'], cell['close'], cell['open_at'], cell['close_at']) == (
                (first.price, max(trade.price for trade in group), min(trade.price for trade in group), last.price,
                 _moment(first.micros), _moment(last.micros))
                if first is not None and last is not None else (None,) * 6
            )


def _bounds(result: Result) -> tuple[object, object]:
    return result.answer['effective']['p1'], result.answer['effective']['p2']


def test_measures_automatic_price_bounds(cube: SourceRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = detailed(cube, DAY1)
    trades = _detail_trades((ARCHIVES / f'BTCUSDT-trades-{DAY1}.csv').read_bytes())
    # Both bounds automatic: the extent of the emitted cells.
    whole = run(runtime, tmp_path, measures=['dwell'])
    rows = sorted({cell['price_index'] for cell in whole.cells})
    assert _bounds(whole) == (rows[0] * 125.0, (rows[-1] + 1) * 125.0)
    # After the day's last trade its price holds, untraded, until midnight.
    last = trades[-1]
    assert last.micros < _micros('2021-01-01T01:00:00+00:00')
    tail = run(runtime, tmp_path, t1='2021-01-01T01:00:00Z', measures=['dwell'])
    assert _bounds(tail) == (last.row * 125.0, (last.row + 1) * 125.0)
    assert {(cell['price_index'], cell['trade_count']) for cell in tail.cells} == {(last.row, 0)}
    assert tail.summary['dwell'] == 23 * 3600.0
    assert (tail.summary['volume'], tail.summary['trade_count'], tail.summary['poc'], tail.summary['taker_buy_poc']) == (0.0, 0, None, None)
    for other in (run(runtime, tmp_path, t1='2021-01-01T01:00:00Z', measures=['high']), run(runtime, tmp_path, t1='2021-01-01T01:00:00Z')):
        assert _bounds(other) == (None, None) and other.cells == []
    # One bound supplied: the other comes from the extent.
    upper = run(runtime, tmp_path, p1=rows[-1] * 125, measures=['dwell'])
    assert _bounds(upper) == (rows[-1] * 125.0, (rows[-1] + 1) * 125.0)
    assert {cell['price_index'] for cell in upper.cells} == {rows[-1]}
    # An empty rectangle: a supplied bound beyond the extent collapses the interval onto it.
    empty = run(runtime, tmp_path, p1='30000', measures=['dwell', 'high'])
    assert _bounds(empty) == (30000.0, 30000.0) and empty.cells == []
    assert (empty.summary['dwell'], empty.summary['poc'], empty.summary['cell_count']) == (0.0, None, 0)
    # A window that starts in another row than the trade before it: 2021-05-19's column 212796
    # opens at row 267, where the price held from the column's start until its first trade at 268.
    alone(runtime, monkeypatch, '2021-05-19')
    jumpy = _detail_trades((ARCHIVES / 'BTCUSDT-trades-2021-05-19.csv').read_bytes())
    first = next(index for index, trade in enumerate(jumpy) if trade.column == 212_796)
    assert (jumpy[first - 1].row, jumpy[first].row) == (267, 268)
    window = run(runtime, tmp_path, t1='2021-05-19T12:56:15Z', t2='2021-05-19T12:57:11.25Z', measures=['dwell'])
    rows = sorted({cell['price_index'] for cell in window.cells})
    assert 267 in rows and _bounds(window) == (rows[0] * 125.0, (rows[-1] + 1) * 125.0)
    # A rectangle holding only moved-through cells: the 2023-03-24 halt.
    alone(runtime, monkeypatch, '2023-03-24')
    halt = run(runtime, tmp_path, t1='2023-03-24T12:00:00Z', t2='2023-03-24T13:00:00Z', measures=['dwell'])
    assert _bounds(halt) == (28000.0, 28125.0)
    assert {(cell['price_index'], cell['trade_count'], cell['dwell']) for cell in halt.cells} == {(224, 0, 56.25)}
    assert (halt.summary['volume'], halt.summary['poc'], halt.summary['taker_buy_poc']) == (0.0, None, None)
    for other in (run(runtime, tmp_path, t1='2023-03-24T12:00:00Z', t2='2023-03-24T13:00:00Z', measures=['low']),
                  run(runtime, tmp_path, t1='2023-03-24T12:00:00Z', t2='2023-03-24T13:00:00Z')):
        assert _bounds(other) == (None, None) and other.cells == []


def test_integer_measures_divide_exactly_above_2_53() -> None:
    # Full-history base volume is about 2^53 satoshis, where Float64 stops holding every integer.
    probes = [0, 1, 12_348_987_000, 2**53 - 1, 2**53, 2**53 + 1, 2**53 + 3, 2**60 + 12_345, 2**64 - 1]
    for scale in _UNITS.values():
        units = market_state._units(np.array(probes, dtype=np.uint64), scale)
        assert units.tolist() == [float(Fraction(value, scale)) for value in probes]
        # Converting 2^53 + 1 to Float64 first rounds it before the division rounds again.
        assert float(2**53 + 1) / scale != float(Fraction(2**53 + 1, scale)) == units[5]


# SHA-256 of every statement each request below ran on v3.27.2 (7a516f9), recorded with this
# test's captures: the pin, the automatic price extent when a bound was omitted, the cells and
# the validation.
_V3_27_2_STATEMENTS: dict[str, list[str]] = {
    'everything': [
        '0d80189b467fe176a4525a5ee2816636c9771a530836757a7e771233441f4345',
        '3dc590e05df9463a6d56844e0ca28d6651bf32aa046c20f6cc09ca179e247afa',
        '86c65d6ce39841d1d8a626ced4f2e89c981154bbca60549f381fd0198ad2118d',
        'd5c62590764aee78b6fa98d93302fb20316c857474694e6f6107b089c023a83a',
    ],
    'window': [
        '0d80189b467fe176a4525a5ee2816636c9771a530836757a7e771233441f4345',
        '2fc2a648da423687f9b24af77ef2f1ea8acaded6b14dfbf270eaa1353736f0c8',
        '863b6ff219685bbe1e37c41a2e0636cd7ca8733693068c7464da5d69de02f3af',
        'd5c62590764aee78b6fa98d93302fb20316c857474694e6f6107b089c023a83a',
    ],
    'bounded': [
        '0d80189b467fe176a4525a5ee2816636c9771a530836757a7e771233441f4345',
        '66d5a802ccd37b441138d1c3fdb1479ad2c39d2dadbc6a1d931a4fe34953c94a',
        'd5c62590764aee78b6fa98d93302fb20316c857474694e6f6107b089c023a83a',
    ],
    'tail': [
        '0d80189b467fe176a4525a5ee2816636c9771a530836757a7e771233441f4345',
        '432c0b269375ac991a03107e7ee842f19c1f360947679db0d4726ed9e527b9a4',
        '82955091e7c8bbb182527e2a6ef416b4468cae3a4ec18b0fb14eaae56d76e060',
        'd5c62590764aee78b6fa98d93302fb20316c857474694e6f6107b089c023a83a',
    ],
}


def test_default_requests_keep_their_statements_and_schema(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = detailed(cube, DAY1, minutes=MINUTES)
    requests: dict[str, dict[str, object]] = {
        'everything': {},
        'window': {'t1': '2021-01-01T00:57:11.25Z', 't2': '2021-01-01T01:00:00Z', 'tR': 225, 'pR': 250},
        'bounded': {'t1': '2021-01-01T00:00:00Z', 't2': '2021-01-02T00:03:00Z', 'p1': 28000, 'p2': 30000, 'tR': 450, 'pR': 500},
        'tail': {'t1': '2021-01-02T00:00:00Z', 'pR': 250},
    }
    for name, fields in requests.items():
        for absent in ({}, {'measures': None}, {'measures': []}):
            result = run(runtime, tmp_path, **fields, **absent)
            # Only the coverage pin changed in #501; every data/validation statement is frozen.
            expected = ['c568497fd23856869d2687d89c5f456f0a5c67309b2006cb8dc0ab879f64d1b8',
                        *_V3_27_2_STATEMENTS[name][1:]]
            assert statement_hashes(result.staging.name) == expected, (name, absent)
            for file, schema in (('cells.arrow', CELLS_SCHEMA), ('summary.arrow', SUMMARY_SCHEMA)):
                assert ipc.open_file(result.staging / file).schema.remove_metadata() == schema
                meta = metadata(result.staging / file)
                assert set(meta) == {'schema_version', 'result_id', 'request', 'grid', 'data_cutoff', 'state_token', 'pins'}
                assert meta['schema_version'] == 1 and set(meta['request']) == {'t1', 't2', 'p1', 'p2', 'tR', 'pR'}


def test_measures_coverage_stops_where_detail_is_missing(cube: SourceRuntime, tmp_path: Path) -> None:
    runtime = built(cube, DAY1, DAY2)
    runtime.enable_components('market_state_detail')
    runtime.upgrade_components(DAY1)
    assert pin(runtime.store, QUERY_SETTINGS).cutoff == datetime(2021, 1, 3, tzinfo=UTC)
    assert pin(runtime.store, QUERY_SETTINGS, detail=True).cutoff == datetime(2021, 1, 2, tzinfo=UTC)
    # A later day with both components does not extend coverage past the day without detail.
    runtime.build(DAY3)
    detailed_state = pin(runtime.store, QUERY_SETTINGS, detail=True)
    assert [record.partition.key for record in detailed_state.records] == [DAY1]
    measured_days = run(runtime, tmp_path, measures=['dwell'])
    assert measured_days.answer['data_cutoff'] == '2021-01-02T00:00:00.000000+00:00'
    assert [pin_[0] for pin_ in metadata(measured_days.staging / 'summary.arrow')['pins']] == [DAY1]
    with pytest.raises(RequestError) as later:
        run(runtime, tmp_path, t1='2021-01-03T00:00:00Z', measures=['dwell'])
    assert later.value.status == 409 and later.value.body() == {
        'error': 'outside_coverage',
        'history_start': '2021-01-01T00:00:00.000000+00:00',
        'data_cutoff': '2021-01-02T00:00:00.000000+00:00',
    }
    # Without measures, coverage is the cube's own.
    plain = run(runtime, tmp_path, t1='2021-01-03T00:00:00Z')
    assert plain.answer['data_cutoff'] == '2021-01-04T00:00:00.000000+00:00' and plain.cells
    runtime.upgrade_components(DAY2)
    assert pin(runtime.store, QUERY_SETTINGS, detail=True).cutoff == datetime(2021, 1, 4, tzinfo=UTC)
