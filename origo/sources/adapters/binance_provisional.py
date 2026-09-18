"""Shared Binance provisional REST base (PRD-0013 rows 3, 11, 12).

Spot and perp declare their provisional parameters on subclasses; the base owns
minute-partition math, the 36h/5 candidate window, the locate+page loop, the
100-page cap, and the empty-minute two-observation evidence. Per-source hooks
stay on the subclasses: the row mapper, and HTTP plus the clock, which resolve
through the subclass modules so the test patch seams keep working.

Row 11 (paging start) is the ``PAGING_BACKTRACK_IDS`` parameter: spot pages
from the locator id and treats a pre-minute row as a boundary violation, while
fapi backtracks (an aggregate's open time can hide in-minute trades) and
requires pre-minute rows as the completeness proof. Row 12 (weights) is
enforced structurally: every request weight is a named declaration field, never
a literal at a call site.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, cast

from ..contracts import Partition, Revision, Row, SourceError
from ..hashing import content_hash

if TYPE_CHECKING:
    from .binance_daily import Response


def _objects(body: bytes) -> tuple[dict[str, object], ...]:
    payload: object = json.loads(body)
    if not isinstance(payload, list):
        raise ValueError('Binance response must be a list.')
    result: list[dict[str, object]] = []
    for item in cast(list[object], payload):
        if not isinstance(item, dict):
            raise ValueError('Binance response rows must be objects.')
        row: dict[str, object] = {}
        for key, value in cast(dict[object, object], item).items():
            if not isinstance(key, str):
                raise ValueError('Binance response keys must be strings.')
            row[key] = value
        result.append(row)
    return tuple(result)


def _int(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'Binance {name} must be an unsigned integer.')
    return value


def _bool(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if not isinstance(value, bool):
        raise ValueError(f'Binance {name} must be a boolean.')
    return int(value)


def _text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str):
        raise ValueError(f'Binance {name} must be decimal text.')
    return value


@dataclass(frozen=True)
class BinanceProvisionalBase:
    """Locate+page closed minutes; subclasses declare host, paths, weights, and policy."""

    SOURCE_NOUN: ClassVar[str]
    REST_BASE_URL_ENV: ClassVar[str]
    REST_BASE_URL_DEFAULT: ClassVar[str]
    LATEST_SYMBOL_ENV: ClassVar[str]
    AGG_TRADES_PATH: ClassVar[str]
    HISTORICAL_TRADES_PATH: ClassVar[str]
    WEIGHT_LOCATOR: ClassVar[int]
    WEIGHT_BOUNDARY: ClassVar[int]
    WEIGHT_HISTORICAL: ClassVar[int]
    PAGE_LIMIT: ClassVar[int]
    CREDENTIAL_REQUIRED: ClassVar[bool]
    PAGING_BACKTRACK_IDS: ClassVar[int]

    def map_row(self, row: Mapping[str, object]) -> Row:
        raise NotImplementedError('Provisional subclasses map their own row shape.')

    def _get_response(
        self,
        url: str,
        params: dict[str, str | int],
        headers: dict[str, str],
        weight: int,
    ) -> Response:
        raise NotImplementedError('Provisional subclasses resolve HTTP through their own module.')

    def _now_utc(self) -> datetime:
        raise NotImplementedError(
            'Provisional subclasses resolve the clock through their own module.'
        )

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        last = now.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        if last < anchor:
            return ()
        missing: list[Partition] = []
        # The 36-hour lookback also stays inside fapi's 2-day aggTrades search window.
        cursor = max(anchor, last - timedelta(hours=36))
        while cursor < last and len(missing) < 5:
            if not any(interval.start <= cursor < interval.end for interval in covered):
                missing.append(self.partition(cursor.strftime('%Y-%m-%dT%H:%M:%SZ')))
            cursor += timedelta(minutes=1)
        current = (
            ()
            if any(interval.start <= last < interval.end for interval in covered)
            else (self.partition(last.strftime('%Y-%m-%dT%H:%M:%SZ')),)
        )
        return (*current, *missing)

    def partition(self, key: str) -> Partition:
        start = datetime.strptime(key, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)
        if start.second:
            raise ValueError('Provisional partitions start on minute boundaries.')
        return Partition(key, start, start + timedelta(minutes=1), provisional=True)

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        if not partition.provisional or partition.end > self._now_utc():
            raise ValueError(f'Only closed {self.SOURCE_NOUN} minutes may be fetched.')
        base = os.environ.get(self.REST_BASE_URL_ENV, self.REST_BASE_URL_DEFAULT).rstrip('/')
        symbol = os.environ.get(self.LATEST_SYMBOL_ENV, 'BTCUSDT')
        if symbol != 'BTCUSDT':
            raise ValueError('This source declares BTCUSDT only.')
        if self.CREDENTIAL_REQUIRED and not os.environ.get('BINANCE_API_KEY'):
            raise SourceError(
                'PROVIDER_CREDENTIAL_MISSING',
                f'BINANCE_API_KEY is required for the {self.SOURCE_NOUN} provisional tail.',
            )
        start_ms = int(partition.start.timestamp()) * 1000
        end_ms = int(partition.end.timestamp()) * 1000
        requests: list[dict[str, object]] = []

        def request(
            path: str, params: dict[str, str | int], weight: int
        ) -> tuple[dict[str, object], ...]:
            api_key = os.environ.get('BINANCE_API_KEY')
            headers = {'X-MBX-APIKEY': api_key} if api_key else {}
            response = self._get_response(
                base + path, params=params, headers=headers, weight=weight
            )
            requests.append(
                {
                    'path': path,
                    'params': params,
                    'status': response.status,
                    'body_sha256': hashlib.sha256(response.body).hexdigest(),
                    'completed_at': self._now_utc().isoformat(),
                }
            )
            return _objects(response.body)

        locator = request(
            self.AGG_TRADES_PATH,
            {'symbol': symbol, 'startTime': start_ms, 'endTime': end_ms - 1, 'limit': 1},
            self.WEIGHT_LOCATOR,
        )
        if not locator:
            tick = self._now_utc().replace(second=0, microsecond=0).isoformat()
            evidence: dict[str, object] = {
                'requests': requests,
                'empty_partition': partition.key,
                'empty_tick': tick,
            }
            complete = False
            if previous_evidence is not None:
                decoded: object = json.loads(previous_evidence)
                if not isinstance(decoded, dict):
                    raise ValueError('Stored provisional evidence must be an object.')
                prior = cast(dict[str, object], decoded)
                previous_tick = prior.get('empty_tick')
                if (
                    prior.get('empty_partition') == partition.key
                    and isinstance(previous_tick, str)
                    and previous_tick != tick
                ):
                    later = request(
                        self.AGG_TRADES_PATH,
                        {'symbol': symbol, 'startTime': end_ms, 'limit': 1},
                        self.WEIGHT_BOUNDARY,
                    )
                    complete = bool(later) and _int(later[0], 'T') >= end_ms
                    evidence['previous_empty_observation'] = prior
            digest = content_hash((), schema_version=1)
            return Revision(
                digest,
                digest,
                json.dumps(evidence, sort_keys=True),
                0,
                lambda: iter(()),
                complete=complete,
            )
        next_id = _int(locator[0], 'f')
        if _int(locator[0], 'T') < start_ms or _int(locator[0], 'l') < next_id:
            raise ValueError('Invalid Binance individual-trade locator range.')
        rows: list[Row] = []
        # A zero backtrack pages from the locator; fapi pages from before it because an
        # aggregate straddling the boundary can hide in-minute trades before the first id.
        first_id = max(1, next_id - self.PAGING_BACKTRACK_IDS)
        next_id = first_id
        previous_id, previous_time = (
            next_id - 1,
            (start_ms if self.PAGING_BACKTRACK_IDS == 0 else 0),
        )
        skipped = 0
        complete = False
        for _ in range(100):
            page = request(
                self.HISTORICAL_TRADES_PATH,
                {'symbol': symbol, 'fromId': next_id, 'limit': self.PAGE_LIMIT},
                self.WEIGHT_HISTORICAL,
            )
            if not page:
                raise RuntimeError('Historical-trade paging ended before the minute boundary.')
            for value in page:
                trade_id, instant = _int(value, 'id'), _int(value, 'time')
                if trade_id <= previous_id or instant < previous_time:
                    raise ValueError('Historical trades are unordered or duplicated.')
                previous_id, previous_time = trade_id, instant
                parsed = self.map_row(value)
                if instant >= end_ms:
                    complete = True
                    break
                if instant < start_ms:
                    if self.PAGING_BACKTRACK_IDS == 0:
                        raise ValueError('Historical trade precedes its locator boundary.')
                    skipped += 1
                    continue
                rows.append(parsed)
            if complete:
                break
            next_id = _int(page[-1], 'id') + 1
        if not complete:
            raise RuntimeError(
                f'{self.SOURCE_NOUN.capitalize()} closed-minute paging exceeded the 100-page cap.'
            )
        if self.PAGING_BACKTRACK_IDS > 0 and skipped == 0 and first_id > 1:
            raise RuntimeError(
                f'{self.SOURCE_NOUN.capitalize()} minute paging never reached pre-minute '
                'evidence; the backtrack is too short.'
            )
        if not rows:
            raise RuntimeError('Empty minute lacks independent completeness evidence.')
        result = tuple(rows)
        digest = content_hash(result, schema_version=1)
        return Revision(
            digest,
            digest,
            json.dumps({'requests': requests}, sort_keys=True),
            len(result),
            lambda: iter(result),
        )
