"""Bounded reads of certified interval metadata, never raw market rows."""

from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from origo.sources.contracts import Partition, SourceError

if TYPE_CHECKING:
    from origo.sources.storage import SourceStore

COVERAGE_QUERY_SETTINGS = {
    'max_memory_usage': 512 * 1024 * 1024,
    'max_execution_time': 5,
    'max_threads': 2,
    'max_rows_to_read': 5_000_000,
    'max_result_rows': 100_000,
    'max_result_bytes': 32 * 1024 * 1024,
    'result_overflow_mode': 'throw',
    'read_overflow_mode': 'throw',
}


@dataclass(frozen=True)
class Coverage:
    anchor: datetime
    due: datetime
    intervals: tuple[Partition, ...]
    canonical_end: datetime
    contiguous_end: datetime
    newest_end: datetime
    missing_minutes: int
    oldest_missing: datetime | None
    incomplete_partitions: tuple[str, ...] = ()


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError('Coverage bounds must be datetimes.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _pairs(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError('Component evidence must be a sequence of pairs.')
    result: list[tuple[str, str]] = []
    for item in cast(list[object] | tuple[object, ...], value):
        if not isinstance(item, (list, tuple)):
            raise TypeError('Component evidence entry must be a pair.')
        pair = cast(list[object] | tuple[object, ...], item)
        if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[1], str):
            raise TypeError('Component evidence must contain two strings.')
        result.append((pair[0], pair[1]))
    return tuple(sorted(result))


def selected_intervals(intervals: tuple[Partition, ...]) -> tuple[Partition, ...]:
    """Canonical intervals mask their contained provisional intervals in O(n log n)."""
    canonical = sorted(
        (item for item in intervals if not item.provisional), key=lambda item: item.start
    )
    starts: list[datetime] = []
    ends: list[datetime] = []
    for item in canonical:
        starts.append(item.start)
        ends.append(max(item.end, ends[-1]) if ends else item.end)
    selected = list(canonical)
    for item in intervals:
        if item.provisional:
            index = bisect_right(starts, item.start) - 1
            if index < 0 or ends[index] <= item.start:
                selected.append(item)
            elif ends[index] < item.end:
                raise SourceError(
                    'COVERAGE_PARTIAL_AUTHORITY',
                    'Canonical coverage splits a provisional interval.',
                )
    return tuple(sorted(selected, key=lambda item: (item.start, item.provisional, item.end)))


def contiguous_end(anchor: datetime, intervals: tuple[Partition, ...]) -> datetime:
    end = anchor
    for item in sorted(intervals, key=lambda item: item.start):
        if item.start > end:
            break
        end = max(end, item.end)
    return end


def coverage_from_intervals(
    anchor: datetime,
    intervals: tuple[Partition, ...],
    now: datetime,
    *,
    incomplete_partitions: tuple[str, ...] = (),
) -> Coverage:
    if anchor.tzinfo is None or now.tzinfo is None:
        raise ValueError('Coverage requires timezone-aware timestamps.')
    anchor = anchor.astimezone(UTC)
    due = now.astimezone(UTC).replace(second=0, microsecond=0)
    selected = selected_intervals(intervals)
    canonical = tuple(item for item in selected if not item.provisional)
    frontier = contiguous_end(anchor, selected)
    cursor = anchor
    missing_seconds = 0.0
    oldest: datetime | None = None
    for item in selected:
        start = min(item.start, due)
        if start > cursor:
            if oldest is None:
                oldest = cursor
            missing_seconds += (start - cursor).total_seconds()
        cursor = max(cursor, min(item.end, due))
        if cursor >= due:
            break
    if cursor < due:
        if oldest is None:
            oldest = cursor
        missing_seconds += (due - cursor).total_seconds()
    return Coverage(
        anchor=anchor,
        due=due,
        intervals=selected,
        canonical_end=contiguous_end(anchor, canonical),
        contiguous_end=frontier,
        newest_end=max((item.end for item in selected), default=anchor),
        missing_minutes=int(missing_seconds / 60),
        oldest_missing=oldest,
        incomplete_partitions=incomplete_partitions,
    )


def read_coverage(store: SourceStore, now: datetime) -> Coverage:
    """Two bounded queries; one proof row per active partition, not an interval-array join."""
    anchors = store.execute(
        f'SELECT anchor FROM {store.table("source_anchor_log")} WHERE source_key=%(source)s',
        {'source': store.spec.key},
        settings=COVERAGE_QUERY_SETTINGS,
    )
    if len(anchors) != 1:
        raise SourceError(
            'COVERAGE_ANCHOR_INVALID', 'Source must have one immutable coverage anchor.'
        )
    rows = store.execute(
        f"""SELECT a.partition_key, a.partition_start, a.partition_end, a.provisional,
                   a.component_hashes, c.proofs
        FROM (
            SELECT partition_key, partition_start, partition_end, provisional,
                   revision, build_id, component_hashes
            FROM {store.table('source_active_partitions')}
            WHERE source_key=%(source)s
        ) AS a
        LEFT JOIN (
            SELECT partition_key, provisional, revision, build_id,
                   groupUniqArray((component, content_hash)) AS proofs
            FROM {store.table('source_component_log')}
            WHERE source_key=%(source)s
            GROUP BY partition_key, provisional, revision, build_id
        ) AS c USING (partition_key, provisional, revision, build_id)
        ORDER BY a.partition_start, a.provisional""",
        {'source': store.spec.key},
        settings=COVERAGE_QUERY_SETTINGS,
    )
    expected = {
        provisional: {
            component.key
            for component in store.spec.components
            if component.provisional == provisional
        }
        for provisional in (False, True)
    }
    intervals: list[Partition] = []
    incomplete: list[str] = []
    for key, start, end, provisional, hashes, proofs in rows:
        declared: object = json.loads(str(hashes))
        claimed = _pairs(declared)
        actual = _pairs(proofs)
        keys = [name for name, _ in claimed]
        if (
            not claimed
            or any(not digest for _, digest in claimed)
            or len(keys) != len(set(keys))
            or set(keys) != expected[bool(provisional)]
            or claimed != actual
        ):
            incomplete.append(str(key))
        else:
            intervals.append(Partition(str(key), _utc(start), _utc(end), bool(provisional)))
    return coverage_from_intervals(
        _utc(anchors[0][0]), tuple(intervals), now, incomplete_partitions=tuple(incomplete)
    )
