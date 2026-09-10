from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from ..contracts import Partition, Revision, Row
from ..hashing import content_hash
from .binance_daily import get_response, parse_decimal, timestamp_datetime


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


def historical_row(row: Mapping[str, object]) -> Row:
    timestamp = _int(row, 'time')
    if len(str(timestamp)) != 13:
        raise ValueError('The frozen provisional spot timestamp contract is milliseconds.')
    return (
        _int(row, 'id'),
        parse_decimal(_text(row, 'price')),
        parse_decimal(_text(row, 'qty')),
        parse_decimal(_text(row, 'quoteQty')),
        timestamp,
        _bool(row, 'isBuyerMaker'),
        _bool(row, 'isBestMatch'),
        timestamp_datetime(timestamp),
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class BinanceSpotProvisional:
    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        last = now.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        if last < anchor:
            return ()
        missing: list[Partition] = []
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
        if not partition.provisional or partition.end > now_utc():
            raise ValueError('Only closed spot minutes may be fetched.')
        base = os.environ.get('BINANCE_SPOT_REST_BASE_URL', 'https://api.binance.com').rstrip('/')
        symbol = os.environ.get('BINANCE_SPOT_LATEST_SYMBOL', 'BTCUSDT')
        if symbol != 'BTCUSDT':
            raise ValueError('This source declares BTCUSDT only.')
        start_ms = int(partition.start.timestamp()) * 1000
        end_ms = int(partition.end.timestamp()) * 1000
        requests: list[dict[str, object]] = []

        def request(
            path: str, params: dict[str, str | int], weight: int
        ) -> tuple[dict[str, object], ...]:
            api_key = os.environ.get('BINANCE_API_KEY')
            headers = {'X-MBX-APIKEY': api_key} if api_key else {}
            response = get_response(base + path, params=params, headers=headers, weight=weight)
            requests.append(
                {
                    'path': path,
                    'params': params,
                    'status': response.status,
                    'body_sha256': hashlib.sha256(response.body).hexdigest(),
                    'completed_at': now_utc().isoformat(),
                }
            )
            return _objects(response.body)

        locator = request(
            '/api/v3/aggTrades',
            {'symbol': symbol, 'startTime': start_ms, 'endTime': end_ms - 1, 'limit': 1},
            4,
        )
        if not locator:
            tick = now_utc().replace(second=0, microsecond=0).isoformat()
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
                        '/api/v3/aggTrades', {'symbol': symbol, 'startTime': end_ms, 'limit': 1}, 4
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
        previous_id, previous_time = next_id - 1, start_ms
        complete = False
        for _ in range(100):
            page = request(
                '/api/v3/historicalTrades', {'symbol': symbol, 'fromId': next_id, 'limit': 1000}, 25
            )
            if not page:
                raise RuntimeError('Historical-trade paging ended before the minute boundary.')
            for value in page:
                trade_id, instant = _int(value, 'id'), _int(value, 'time')
                if trade_id <= previous_id or instant < previous_time:
                    raise ValueError('Historical trades are unordered or duplicated.')
                previous_id, previous_time = trade_id, instant
                parsed = historical_row(value)
                if instant >= end_ms:
                    complete = True
                    break
                if instant < start_ms:
                    raise ValueError('Historical trade precedes its locator boundary.')
                rows.append(parsed)
            if complete:
                break
            next_id = _int(page[-1], 'id') + 1
        if not complete:
            raise RuntimeError('Spot closed-minute paging exceeded the 100-page cap.')
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
