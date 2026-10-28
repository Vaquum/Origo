"""Pinned spot microstructure reads for the Explorer's Micro Strip."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from bisect import bisect_left
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from origo.query.market_state import RequestError
from origo.sources.binance_spot_aggtrades import BINANCE_SPOT_AGGTRADES_SPEC
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.binance_top20 import BINANCE_SPOT_TOP20_SPEC
from origo.sources.contracts import Client, Row, StateRecord
from origo.sources.locking import source_lock
from origo.sources.storage import SourceStore

SCHEMA = 'origo-micro@1'
NEAR_BPS = 0.1
MAX_REPLY = 4 * 1024 * 1024
MAX_BINS = 4096
MAX_SPAN = 31 * 86400_000
NORMAL_WINDOW = 3600_000
NORMAL_SECONDS = 1800
SETTINGS: Mapping[str, object] = {
    'max_threads': 2, 'max_memory_usage': 2 * 1024**3,
    'max_execution_time': 10, 'max_rows_to_read': 5_000_000,
    'max_bytes_to_read': 2 * 1024**3, 'read_overflow_mode': 'throw',
    'max_result_rows': 20_000, 'result_overflow_mode': 'throw',
}
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SPECS = (BINANCE_SPOT_TRADES_SPEC, BINANCE_SPOT_AGGTRADES_SPEC, BINANCE_SPOT_TOP20_SPEC)


def millis(value: datetime) -> int:
    return (value.replace(tzinfo=UTC) - EPOCH) // timedelta(milliseconds=1)


@dataclass(frozen=True)
class MicroRequest:
    start: int
    end: int
    step: int
    as_of: int
    trade_pin_digest: str

    @property
    def detail(self) -> bool:
        return self.step == 1000


class MicroCache:
    """Bounded request results, reused only after all contributing generations are pinned."""

    def __init__(self) -> None:
        self.answers: OrderedDict[tuple[MicroRequest, tuple[str, ...]], tuple[str, dict[str, object]]] = OrderedDict()
        self.lock = threading.Lock()

    def get(self, request: MicroRequest, version: str) -> dict[str, object] | None:
        with self.lock:
            for key, (held_version, answer) in self.answers.items():
                if key[0] == request and held_version == version:
                    self.answers.move_to_end(key)
                    return answer
            return None

    def get_pinned(self, key: tuple[MicroRequest, tuple[str, ...]]) -> dict[str, object] | None:
        with self.lock:
            held = self.answers.get(key)
            return held[1] if held is not None else None

    def put(self, key: tuple[MicroRequest, tuple[str, ...]], version: str, answer: dict[str, object]) -> None:
        with self.lock:
            self.answers[key] = (version, answer)
            self.answers.move_to_end(key)
            while len(self.answers) > 8:
                self.answers.popitem(last=False)


def parse_micro_request(raw: bytes) -> MicroRequest:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate field.')
            result[key] = value
        return result

    try:
        value: object = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, RecursionError) as error:
        raise RequestError(400, 'invalid_micro_request', 'Expected a JSON object.') from error
    if not isinstance(value, dict):
        raise RequestError(400, 'invalid_micro_request', 'Expected a JSON object.')
    body = cast(dict[str, object], value)
    if set(body) != {'from', 'to', 'step', 'as_of', 'trade_pin_digest'}:
        raise RequestError(400, 'invalid_micro_request', 'Unexpected or missing Micro fields.')
    numbers = [body[key] for key in ('from', 'to', 'step', 'as_of')]
    if any(isinstance(item, bool) or not isinstance(item, int) for item in numbers):
        raise RequestError(400, 'invalid_micro_request', 'Times and step must be integer UTC milliseconds.')
    start, end, step, as_of = (cast(int, item) for item in numbers)
    if not (0 <= start < end <= as_of <= 8_000_000_000_000):
        raise RequestError(400, 'invalid_micro_request', 'Require 0 ≤ from < to ≤ as_of.')
    if (step != 1000 and (step < 60_000 or step % 60_000)) or start % step or end % step:
        raise RequestError(400, 'invalid_micro_request', 'Use aligned whole seconds for detail or whole minutes for overview.')
    if end - start > (30_000 if step == 1000 else MAX_SPAN) or (end - start) // step > MAX_BINS:
        raise RequestError(400, 'invalid_micro_request', 'Micro range exceeds the detail or overview bound.')
    digest = body['trade_pin_digest']
    if not isinstance(digest, str) or re.fullmatch('[a-f0-9]{64}', digest) is None:
        raise RequestError(400, 'invalid_micro_request', 'A relevant cube trade-pin digest is required.')
    return MicroRequest(start, end, step, as_of, digest)


def trade_digest(records: Sequence[StateRecord]) -> str:
    identities = sorted((record.partition.key, record.revision, str(record.build_id)) for record in records)
    return hashlib.sha256(json.dumps(identities, separators=(',', ':')).encode()).hexdigest()


@dataclass(frozen=True)
class _Bounded:
    client: Client

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None,
    ) -> list[Row]:
        return self.client.execute(query, params, settings={**SETTINGS, **(settings or {})})

    def disconnect(self) -> None:
        self.client.disconnect()


@dataclass(frozen=True)
class _Pinned:
    store: SourceStore
    records: tuple[StateRecord, ...]

    def union(self, component: str, fields: str, request: MicroRequest, *, book: bool = False) -> tuple[str, dict[str, object]]:
        low = request.start - (1000 if request.detail else NORMAL_WINDOW) if book else request.start
        bindings: dict[str, object] = {
            'low': low, 'high': request.end, 'as_of': request.as_of,
            'first': (EPOCH + timedelta(milliseconds=low)).date(),
            'last': (EPOCH + timedelta(milliseconds=request.end - 1)).date(),
            'step': request.step, 'start': request.start,
        }
        parts: list[str] = []
        for provisional in (False, True):
            records = [record for record in self.records if record.partition.provisional == provisional]
            if not records:
                continue
            key = 'latest' if provisional else 'canonical'
            bindings[key] = [(record.partition.key, record.revision, record.build_id) for record in records]
            physical = self.store.component_table(component + ('_latest' if provisional else ''))
            clock = 'intDiv(exchange_time_us, 1000)' if book else 'toUnixTimestamp64Milli(toDateTime64(datetime, 6))'
            receipt = ' AND observed_at_us < %(as_of)s * 1000' if book else ''
            parts.append(f'''SELECT {fields} FROM {physical}
                WHERE source_date >= %(first)s AND source_date <= %(last)s
                AND (partition_key, revision, build_id) IN %({key})s
                AND {clock} >= %(low)s AND {clock} < %(high)s{receipt}''')
        return ' UNION ALL '.join(parts), bindings

    def coverage(self, request: MicroRequest) -> list[int]:
        covered = [0] * ((request.end - request.start) // request.step)
        for record in self.records:
            start = max(request.start, millis(record.partition.start))
            end = min(request.end, millis(record.partition.end))
            if start >= end:
                continue
            first = (start - request.start) // request.step
            last = (end - 1 - request.start) // request.step
            for index in range(first, last + 1):
                at = request.start + index * request.step
                covered[index] += min(end, at + request.step) - max(start, at)
        return covered

    def describe(self) -> dict[str, object]:
        return {
            'source': self.store.spec.key,
            'partitions': [[r.partition.key, r.revision, str(r.build_id), millis(r.partition.start), millis(r.partition.end)] for r in self.records],
        }


def _book(pinned: _Pinned, request: MicroRequest) -> dict[int, dict[str, object]]:
    sql, params = pinned.union('depth20', 'exchange_time_us, observed_at_us, bids, asks', request, book=True)
    if not sql:
        return {}
    rows = pinned.store.execute(f'''WITH samples AS ({sql}), measured AS (
        SELECT *, (bids[1].1 + asks[1].1) / 2 AS mid,
            arraySum(arrayMap(l -> if(l.1 >= mid * (1 - 0.00001), l.1 * l.2, 0), bids)) AS bid,
            arraySum(arrayMap(l -> if(l.1 <= mid * (1 + 0.00001), l.1 * l.2, 0), asks)) AS ask,
            (mid - bids[-1].1) / mid * 10000 AS bid_reach,
            (asks[-1].1 - mid) / mid * 10000 AS ask_reach,
            (asks[1].1 - bids[1].1) / mid * 10000 AS spread,
            intDiv(exchange_time_us, 1000000) AS second,
            tuple(exchange_time_us, observed_at_us, mid, bid, ask, bid_reach, ask_reach, spread) AS sample
        FROM samples
    ), periods AS (
        SELECT intDiv(second, intDiv(least(%(step)s, 60000), 1000)) * least(%(step)s, 60000) AS at,
            count() AS seconds, argMin(sample, second) AS first, argMax(sample, second) AS last,
            sumKahanIf(sample.4, sample.6 >= 0.1 AND sample.7 >= 0.1) AS bid_sum,
            sumKahanIf(sample.5, sample.6 >= 0.1 AND sample.7 >= 0.1) AS ask_sum,
            countIf(sample.6 >= 0.1 AND sample.7 >= 0.1) AS near_seconds,
            sumKahanIf(sample.4 + sample.5, sample.6 >= 0.1 AND sample.7 >= 0.1
                AND sample.2 < (intDiv(second, 60) + 1) * 60000000) AS normal_sum,
            countIf(sample.6 >= 0.1 AND sample.7 >= 0.1
                AND sample.2 < (intDiv(second, 60) + 1) * 60000000) AS normal_seconds,
            sumKahan(sample.8) AS spread_sum, max(sample.2) AS last_receipt
        FROM measured GROUP BY at
    ) SELECT toInt64(intDiv(at, %(step)s)) - toInt64(intDiv(%(start)s, %(step)s)) AS bin,
        sum(seconds), argMax(last, at), sumKahan(bid_sum), sumKahan(ask_sum), sum(near_seconds),
        argMin(first, at), sumKahan(spread_sum) / sum(seconds),
        groupArray(tuple(at, normal_sum, normal_seconds)), max(last_receipt)
        FROM periods GROUP BY bin ORDER BY bin''', params)
    history = sorted(
        (int(str(period[0])), float(str(period[1])), int(str(period[2])))
        for row in rows for period in cast(list[tuple[object, ...]], row[8])
    )
    times = [period[0] for period in history]
    sums, counts = [0.0], [0]
    for _at, amount, count in history:
        sums.append(sums[-1] + amount)
        counts.append(counts[-1] + count)
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row[2], tuple):
            raise TypeError('Invalid book sample.')
        sample = cast(tuple[object, ...], row[2])
        if len(sample) != 8:
            raise TypeError('Invalid book sample.')
        exchange, receipt, mid, bid, ask, bid_reach, ask_reach, spread = sample
        index = int(str(row[0]))
        at = request.start + index * request.step
        low, high = bisect_left(times, at - NORMAL_WINDOW), bisect_left(times, at)
        normal_seconds = counts[high] - counts[low] if not request.detail else 0
        near_seconds = int(str(row[5]))
        first = cast(tuple[object, ...], row[6])
        result[index] = {
            'seconds': row[1], 'exchangeTimeUs': exchange, 'observedAtUs': receipt, 'mid': mid,
            'bid': bid if float(str(bid_reach)) >= NEAR_BPS else None,
            'ask': ask if float(str(ask_reach)) >= NEAR_BPS else None,
            'bidReachBps': bid_reach, 'askReachBps': ask_reach, 'spreadBps': spread,
            'meanBid': float(str(row[3])) / near_seconds if near_seconds else None,
            'meanAsk': float(str(row[4])) / near_seconds if near_seconds else None,
            'nearSeconds': near_seconds, 'meanSpreadBps': row[7],
            'firstMid': first[2], 'firstExchangeTimeUs': first[0], 'firstObservedAtUs': first[1],
            'normalDepth': (sums[high] - sums[low]) / normal_seconds if normal_seconds >= NORMAL_SECONDS else None,
            'normalSeconds': normal_seconds,
            'maxObservedAtUs': row[9],
        }
    return result


def _flow(pinned: _Pinned, request: MicroRequest, *, groups: bool = False) -> dict[int, dict[str, object]]:
    if request.detail:
        fields = 'datetime, price, quantity, is_buyer_maker'
        measures = '''sumKahanIf(price * quantity, is_buyer_maker=0),
            sumKahanIf(price * quantity, is_buyer_maker=1), count(),
            sumKahanIf(quantity, is_buyer_maker=0), sumKahanIf(quantity, is_buyer_maker=1)'''
        if groups:
            measures += ', max(price * quantity)'
    else:
        fields = 'datetime, liquidity_sum, maker_liquidity, no_of_trades, volume, maker_volume'
        measures = '''sumKahan(liquidity_sum - maker_liquidity), sumKahan(maker_liquidity),
            sum(no_of_trades), sumKahan(volume - maker_volume), sumKahan(maker_volume)'''
    sql, params = pinned.union('raw' if request.detail else 'time', fields, request)
    if not sql:
        return {}
    rows = pinned.store.execute(f'''SELECT intDiv(toUnixTimestamp64Milli(toDateTime64(datetime, 6)) - %(start)s, %(step)s) AS bin,
        {measures} FROM ({sql}) GROUP BY bin ORDER BY bin''', params)
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        buy, sell, count, buy_base, sell_base = (float(str(v)) for v in row[1:6])
        item: dict[str, object] = {'buy': buy, 'sell': sell, 'count': int(count)}
        if not groups:
            item |= {'buyVwap': buy / buy_base if buy_base else None, 'sellVwap': sell / sell_base if sell_base else None}
        else:
            item |= {'meanNotional': (buy + sell) / count if count else None,
                     'maxNotional': row[6] if request.detail else None}
        result[int(str(row[0]))] = item
    return result


def read_micro(
    client: Client, database: str, lock_root: Path, request: MicroRequest, *, cache: MicroCache | None = None,
) -> dict[str, object]:
    """Read immutable accepted generations under the existing source cleanup fences."""
    bounded = _Bounded(client)
    with ExitStack() as locks:
        for spec in sorted(SPECS, key=lambda source: source.key):
            locks.enter_context(source_lock(lock_root, spec.key, 'heavy', shared=True))
        version = ''
        if cache is not None:
            # Activations are append-only. Any new activation or reclaimed generation
            # changes this watermark; immutable anchors participate in selection too.
            catalog = SourceStore(bounded, database, SPECS[0])
            rows = bounded.execute(f'''SELECT a.source_key, count(), max(toString(a.activated_at)),
                max(toString(n.anchor)) FROM {catalog.table('source_activation_log')} a
                INNER JOIN {catalog.table('source_anchor_log')} n ON a.source_key=n.source_key
                WHERE a.source_key IN %(sources)s GROUP BY a.source_key ORDER BY a.source_key''',
                {'sources': tuple(spec.key for spec in SPECS)})
            version = hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()
            held = cache.get(request, version)
            if held is not None:
                return held
        catalog = SourceStore(bounded, database, SPECS[0])
        starts = {spec.key: EPOCH + timedelta(milliseconds=request.start) for spec in SPECS}
        starts[BINANCE_SPOT_TOP20_SPEC.key] -= timedelta(milliseconds=1000 if request.detail else NORMAL_WINDOW)
        records = catalog.current_records_by_source(starts, EPOCH + timedelta(milliseconds=request.end))
        pinned = [_Pinned(SourceStore(bounded, database, spec), records[spec.key]) for spec in SPECS]
        trades, aggregates, books = pinned
        digest = trade_digest(trades.records)
        if digest != request.trade_pin_digest:
            raise RequestError(409, 'micro_cube_changed', 'The held cube and current relevant trade generations differ.')
        key = (request, tuple(trade_digest(source.records) for source in pinned))
        held = cache.get_pinned(key) if cache is not None else None
        if held is not None:
            if cache is not None:
                cache.put(key, version, held)
            return held
        book_rows, flows, grouping = _book(books, request), _flow(trades, request), _flow(aggregates, request, groups=True)
        coverage_bins = {name: source.coverage(request) for name, source in zip(('trades', 'groups', 'book'), pinned, strict=True)}
        bins: list[dict[str, object]] = []
        for index, at in enumerate(range(request.start, request.end, request.step)):
            end = at + request.step
            coverage = {name: values[index] for name, values in coverage_bins.items()}
            bins.append({
                'at': at, 'until': end, 'book': book_rows.get(index),
                'flow': flows.get(index, {'buy': 0.0, 'sell': 0.0, 'count': 0, 'buyVwap': None, 'sellVwap': None}) if coverage['trades'] == request.step else None,
                'groups': grouping.get(index, {'buy': 0.0, 'sell': 0.0, 'count': 0, 'meanNotional': None, 'maxNotional': None}) if coverage['groups'] == request.step else None,
                'coveredMs': coverage,
            })
        answer: dict[str, object] = {
            'schema': SCHEMA, 'from': request.start, 'to': request.end, 'step': request.step, 'asOf': request.as_of,
            'nearBps': NEAR_BPS, 'anchor': book_rows.get(-1) if request.detail else None, 'bins': bins,
            'sources': {name: source.describe() for name, source in zip(('trades', 'groups', 'book'), pinned, strict=True)},
            'tradePinDigest': digest,
            'method': {'book': 'One last eligible quote per observed second; means use seconds with both 0.1 bp radii captured; last quote retained separately',
                       'normal': 'Mean total near depth in the preceding hour, excluding the displayed interval; at least 1800 observed near-depth seconds',
                       'notional': 'price * quantity, USDT; aggregate grouping is never added to execution flow',
                       'flow': 'Individual executions' if request.detail else 'Accepted one-minute bars',
                       'grouping': 'Exchange aggregate trades, not parent-order identity',
                       'clocks': 'Half-open exchange-time intervals; book receipt strictly before asOf'},
        }
        if len(json.dumps(answer, allow_nan=False).encode()) > MAX_REPLY:
            raise RequestError(413, 'micro_result_too_large', 'Choose a shorter Micro range.')
        if cache is not None:
            cache.put(key, version, answer)
        return answer
