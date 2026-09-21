"""Stable prerequisite accounting on the existing source failure log (S439 SS-06).

A consumer blocked on a persistent prerequisite (the unseeded full-history mount that
``RENDER_DEFERRED`` names) fails once per attempt, but its identity is the failure key
``lifecycle.publish`` already records: source, operation, consumer and error code. That
key does not move with the pinned state token, so the retry budget and backoff below
survive every new minute; ``RECOVERED`` on the same key is the verified resolution that
resets them. Nothing here is a second store: the failure log is read, never copied.

``render_active`` is the only scheduling fact a worker needs before it asks for a
publication: whether the consumer lock the renderer holds is held right now.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from origo.sources.contracts import RevisionedSourceSpec, SourceError
from origo.sources.locking import source_lock

if TYPE_CHECKING:
    from origo.sources.storage import SourceStore

log = logging.getLogger(__name__)

PREREQUISITE_CODES: tuple[str, ...] = ('RENDER_DEFERRED',)
PREREQUISITE_QUERY_SETTINGS = {
    'max_execution_time': 5,
    'max_threads': 2,
    'max_memory_usage': 128 * 1024 * 1024,
    'max_rows_to_read': 1_000_000,
    'max_result_rows': 10_000,
    'result_overflow_mode': 'throw',
    'read_overflow_mode': 'throw',
}


def prerequisite_key(source_key: str, consumer: str, error_code: str) -> str:
    """The failure key ``FailureLog.record`` derives for a consumer-scoped publication
    failure: stable across state tokens, attempts and processes."""
    identity = json.dumps([source_key, 'consumer', None, consumer, None, error_code])
    return hashlib.sha256(identity.encode()).hexdigest()


@dataclass(frozen=True)
class Prerequisite:
    source_key: str
    consumer: str
    error_code: str
    failure_key: str
    attempts: int
    first_failed_at: datetime
    last_failed_at: datetime


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError('Failure event times must be datetimes.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def open_prerequisites(
    store: SourceStore,
    *,
    consumer: str | None = None,
    codes: tuple[str, ...] = PREREQUISITE_CODES,
) -> tuple[Prerequisite, ...]:
    """Every consumer prerequisite with FAILED events after its last RECOVERED, with the
    attempt count and bounds of that open run. One bounded read of the failure log."""
    table = store.table('source_failure_log')
    rows = store.execute(
        f"""SELECT f.failure_key, ifNull(any(f.consumer), ''), any(f.error_code),
            countIf(f.event_type = 'FAILED' AND f.event_time > r.recovered_at) AS attempts,
            minIf(f.event_time, f.event_type = 'FAILED' AND f.event_time > r.recovered_at),
            maxIf(f.event_time, f.event_type = 'FAILED' AND f.event_time > r.recovered_at)
        FROM {table} AS f
        INNER JOIN (
            SELECT failure_key, maxIf(event_time, event_type = 'RECOVERED') AS recovered_at
            FROM {table}
            WHERE source_key = %(source)s AND operation = 'consumer' AND error_code IN %(codes)s
            GROUP BY failure_key
        ) AS r USING failure_key
        WHERE f.source_key = %(source)s AND f.operation = 'consumer' AND f.error_code IN %(codes)s
          AND (%(consumer)s = '' OR f.consumer = %(consumer)s)
        GROUP BY f.failure_key HAVING attempts > 0 ORDER BY f.failure_key""",
        {'source': store.spec.key, 'codes': tuple(codes), 'consumer': consumer or ''},
        settings=PREREQUISITE_QUERY_SETTINGS,
    )
    found: list[Prerequisite] = []
    for key, name, code, attempts, first, last in rows:
        found.append(
            Prerequisite(
                store.spec.key,
                str(name),
                str(code),
                str(key),
                int(str(attempts)),
                _utc(first),
                _utc(last),
            )
        )
    return tuple(found)


def prerequisite_attempts(
    store: SourceStore, *, consumer: str, error_code: str
) -> tuple[int, datetime | None]:
    """Open attempts of one prerequisite and when the last one failed; ``(0, None)`` when
    the prerequisite is resolved or never failed."""
    found = open_prerequisites(store, consumer=consumer, codes=(error_code,))
    if not found:
        return 0, None
    if len(found) != 1:
        raise SourceError(
            'PREREQUISITE_AMBIGUOUS', 'One consumer prerequisite maps to several failure keys.'
        )
    return found[0].attempts, found[0].last_failed_at


def may_attempt_prerequisite(
    store: SourceStore,
    spec: RevisionedSourceSpec,
    *,
    consumer: str,
    error_code: str,
    now: datetime,
) -> bool:
    """Whether the stable prerequisite admits another publication attempt now: the doubling
    delay from one minute to ``retry_delay`` since the last failure has elapsed, and at
    most ``retry_count`` attempts have failed since the prerequisite was last resolved.
    A new state token changes nothing here."""
    attempts, last_failed = prerequisite_attempts(store, consumer=consumer, error_code=error_code)
    if attempts == 0 or last_failed is None:
        return True
    if attempts > spec.orchestration.retry_count:
        log.error(
            'source=%s consumer=%s prerequisite %s exhausted after %d attempts; '
            'it stays open until a full render resolves it',
            spec.key,
            consumer,
            error_code,
            attempts,
        )
        return False
    delay = min(spec.orchestration.retry_delay, 60 * 2 ** min(attempts - 1, 60))
    return now >= last_failed + timedelta(seconds=delay)


def render_active(lock_root: Path, source_key: str, consumer: str) -> bool:
    """Whether a publication of this consumer holds its lock right now.

    Positive evidence from the same lock ``SourceRuntime.publish`` takes, so a minute
    worker can skip a publication instead of blocking behind a full-history render; the
    next attempt against the then-current token is the coalesced follow-up.
    """
    try:
        with source_lock(lock_root, source_key, 'consumer_' + consumer):
            return False
    except SourceError as error:
        if error.code != 'SOURCE_LOCK_BUSY':
            raise
        return True
