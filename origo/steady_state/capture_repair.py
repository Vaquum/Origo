"""Repair only uncovered native-ID ranges between verified capture segments.

Integer ID continuity is never assumed. A historical request must overlap the
known row on each side of a gap, with exactly matching normalized values.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import cast

from origo.sources.adapters.binance_daily import Response
from origo.sources.contracts import Partition, Revision, Row, SourceError
from origo.sources.hashing import content_hash
from .trade_spool import TradeSpool

MAX_ROWS = 200_000
MAX_PAGES = 256
PAGE_LIMIT = 500
MAX_EDGE_AGE_MS = 60_000
FetchPage = Callable[[int], Response]
FIELDS = 'segment,id,price,qty,quote_qty,time,is_buyer_maker,is_rpi'


def _values(row: tuple[object, ...]) -> dict[str, object]:
    return {
        'id': int(str(row[1])),
        'price': str(row[2]),
        'qty': str(row[3]),
        'quoteQty': str(row[4]),
        'time': int(str(row[5])),
        'isBuyerMaker': bool(row[6]),
        'isRPITrade': bool(row[7]),
    }


def _page(response: Response) -> list[dict[str, object]]:
    if response.status != 200:
        raise SourceError('CAPTURE_REPAIR_HTTP', 'Historical gap repair did not return HTTP 200.')
    decoded: object = json.loads(response.body)
    if not isinstance(decoded, list):
        raise SourceError('CAPTURE_REPAIR_SCHEMA', 'Historical repair response must be a list.')
    entries = cast(list[object], decoded)
    if not 1 <= len(entries) <= PAGE_LIMIT:
        raise SourceError('CAPTURE_REPAIR_SCHEMA', 'Historical repair page size is invalid.')
    result: list[dict[str, object]] = []
    for item in entries:
        if not isinstance(item, dict):
            raise SourceError('CAPTURE_REPAIR_SCHEMA', 'Historical repair must return objects.')
        result.append(cast(dict[str, object], item))
    return result


def repair_minute(spool: TradeSpool, partition: Partition, fetch: FetchPage) -> Revision | None:
    """Return a fully proven minute, or None when capture cannot bracket it cheaply."""
    start, end = int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000)
    connection = spool.connection
    left = connection.execute(
        f'SELECT {FIELDS} FROM trades WHERE time < ? ORDER BY time DESC,id DESC LIMIT 1', (start,)
    ).fetchone()
    right = connection.execute(
        f'SELECT {FIELDS} FROM trades WHERE time >= ? ORDER BY time,id LIMIT 1', (end,)
    ).fetchone()
    if left is None or right is None:
        return None
    if start - left[5] > MAX_EDGE_AGE_MS or right[5] - end > MAX_EDGE_AGE_MS:
        return None
    captured = connection.execute(
        f'SELECT {FIELDS} FROM trades WHERE time >= ? AND time < ? ORDER BY id,segment LIMIT ?',
        (start, end, MAX_ROWS + 1),
    ).fetchall()
    if len(captured) > MAX_ROWS:
        raise SourceError('CAPTURE_REPAIR_BOUND', 'Captured minute exceeds the row bound.')
    records = [left, *captured, right]
    normalized: dict[int, Row] = {}
    provider: dict[int, dict[str, object]] = {}
    for record in records:
        raw = _values(tuple(record))
        mapped = spool.mapper(raw)
        key = int(str(mapped[0]))
        if key in normalized and normalized[key] != mapped:
            raise SourceError('CAPTURE_CONFLICT', 'Capture segments disagree on one native trade.')
        normalized[key], provider[key] = mapped, raw
    first, last = int(left[1]), int(right[1])
    segments = sorted({int(record[0]) for record in records})
    if len(segments) > 1000:
        raise SourceError('CAPTURE_REPAIR_BOUND', 'Capture has too many disconnected segments.')
    ranges = connection.execute(
        'SELECT segment,min_id,max_id FROM segments WHERE segment IN ('
        + ','.join('?' for _ in segments)
        + ') ORDER BY min_id',
        segments,
    ).fetchall()
    merged: list[tuple[int, int]] = []
    for _, lower, upper in ranges:
        lower, upper = max(first, int(lower)), min(last, int(upper))
        if lower > upper:
            continue
        if merged and lower <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(upper, merged[-1][1]))
        else:
            merged.append((lower, upper))
    if not merged or merged[0][0] != first or merged[-1][1] != last:
        return None
    requests: list[dict[str, object]] = []
    gaps = [(previous[1], following[0]) for previous, following in zip(merged, merged[1:])]
    for lower, upper in gaps:
        if lower not in normalized or upper not in normalized:
            raise SourceError('CAPTURE_REPAIR_EVIDENCE', 'Gap endpoints have no retained trade.')
        next_id = lower
        reached = False
        previous_id, previous_time = lower - 1, -1
        while not reached:
            if len(requests) >= MAX_PAGES:
                raise SourceError('CAPTURE_REPAIR_BOUND', 'Gap repair exceeded its page bound.')
            response = fetch(next_id)
            page = _page(response)
            requests.append(
                {
                    'fromId': next_id,
                    'limit': PAGE_LIMIT,
                    'status': response.status,
                    'body_sha256': hashlib.sha256(response.body).hexdigest(),
                    'weight': 200,
                    'latency_ms': response.cost.latency_ms,
                    'pace_wait_ms': response.cost.pace_wait_ms,
                }
            )
            for raw in page:
                mapped = spool.mapper(raw)
                key, instant = int(str(mapped[0])), int(str(mapped[4]))
                if key < next_id or key <= previous_id or instant < previous_time:
                    raise SourceError('CAPTURE_REPAIR_ORDER', 'Historical gap page is unordered.')
                if previous_id == lower - 1 and key != lower:
                    raise SourceError(
                        'CAPTURE_REPAIR_OVERLAP', 'Historical reply omitted the left anchor.'
                    )
                previous_id, previous_time = key, instant
                if key in normalized and normalized[key] != mapped:
                    raise SourceError(
                        'CAPTURE_CONFLICT', 'Historical and captured values disagree.'
                    )
                if key > upper:
                    raise SourceError(
                        'CAPTURE_REPAIR_OVERLAP', 'Historical reply omitted the right anchor.'
                    )
                normalized[key], provider[key] = mapped, raw
                if len(normalized) > MAX_ROWS:
                    raise SourceError(
                        'CAPTURE_REPAIR_BOUND', 'Repaired minute exceeds the row bound.'
                    )
                if key == upper:
                    reached = True
                    break
            next_id = previous_id + 1
    ordered = [normalized[key] for key in sorted(normalized)]
    if any(int(str(a[4])) > int(str(b[4])) for a, b in zip(ordered, ordered[1:])):
        raise SourceError('CAPTURE_REPAIR_ORDER', 'Reconstructed native trades regress in time.')
    rows = tuple(row for row in ordered if start <= int(str(row[4])) < end)
    if not rows:
        return None
    digest = content_hash(rows, schema_version=1)
    evidence = {
        'acquisition': 'verified_capture_with_historical_gap_repair',
        'partition': partition.key,
        'segments': [list(item) for item in ranges],
        'left_id': first,
        'right_id': last,
        'gaps': [list(gap) for gap in gaps],
        'requests': requests,
        'row_count': len(rows),
        'content_hash': digest,
    }
    return Revision(
        digest, digest, json.dumps(evidence, sort_keys=True), len(rows), lambda: iter(rows)
    )
