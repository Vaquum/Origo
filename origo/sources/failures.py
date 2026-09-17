from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dagster import get_dagster_logger

from .locking import source_lock
from .storage import SourceStore

# Error codes of the retired legacy parity comparison. Open failures carrying one of
# these codes describe a policy that no longer exists and are retired, never rewritten.
REMOVED_PARITY_CODES = frozenset(
    {
        'LEGACY_PARITY_MISMATCH',
        'LEGACY_SCHEMA_MISMATCH',
        'RETAINED_PARITY_FAILED',
        'PARITY_INPUT_MISSING',
        'PARITY_INPUT_INVALID',
        'PARITY_EVIDENCE_MISSING',
        'PARITY_EVIDENCE_INVALID',
        'PARITY_VERIFIER_MISSING',
        'PARITY_REVISION_CHANGED',
    }
)


class FailureLog:
    def __init__(self, store: SourceStore, lock_root: Path, run_id: str) -> None:
        self.store, self.lock_root, self.run_id = store, lock_root, run_id

    def record(
        self,
        *,
        operation: str,
        error_code: str,
        scope: str,
        partition: str | None = None,
        build_id: UUID | None = None,
        revision: str | None = None,
        component: str | None = None,
        consumer: str | None = None,
        event_type: str = 'FAILED',
        severity: str = 'ERROR',
        message: str | None = None,
        related_event: UUID | None = None,
        details: dict[str, object] | None = None,
    ) -> str:
        if scope not in ('NONE', 'CONSUMER', 'PARTITION', 'SOURCE', 'ROUTE'):
            raise ValueError('Unknown failure blocking scope.')
        identity = json.dumps(
            [self.store.spec.key, operation, partition, consumer, component, error_code]
        )
        failure_key = hashlib.sha256(identity.encode()).hexdigest()
        event_id = uuid5(
            NAMESPACE_URL,
            json.dumps([failure_key, self.run_id, str(build_id), event_type, str(related_event)]),
        )
        safe_details = json.dumps(details or {}, sort_keys=True)
        if len(safe_details) > 8192:
            raise ValueError('Failure details exceed the bounded source-log contract.')
        values = [
            (
                event_id,
                datetime.now(UTC),
                failure_key,
                self.store.spec.key,
                event_type,
                severity,
                scope,
                operation,
                partition,
                revision,
                build_id,
                component,
                consumer,
                self.run_id,
                error_code,
                (message or f'{operation}: {error_code}')[:1024],
                safe_details,
            )
        ]
        logger = get_dagster_logger('origo.sources')
        logger.log(
            40 if event_type == 'FAILED' else 20,
            'source=%s partition=%s operation=%s component=%s consumer=%s build=%s '
            'origin_run=%s event=%s code=%s failure_key=%s message=%s',
            self.store.spec.key,
            partition,
            operation,
            component,
            consumer,
            build_id,
            self.run_id,
            event_type,
            error_code,
            failure_key,
            message or f'{operation}: {error_code}',
        )
        with source_lock(self.lock_root, self.store.spec.key, 'failure_' + failure_key):
            found = self.store.execute(
                f'SELECT event_id FROM {self.store.table("source_failure_log")} WHERE event_id=%(event)s',
                {'event': event_id},
            )
            if found:
                if found != [(event_id,)]:
                    raise RuntimeError('Failure event identity is duplicated.')
                return failure_key
            try:
                self.store.execute(
                    f'INSERT INTO {self.store.table("source_failure_log")} VALUES', values
                )
            except Exception as error:
                found = self.store.execute(
                    f'SELECT event_id FROM {self.store.table("source_failure_log")} WHERE event_id=%(event)s',
                    {'event': event_id},
                )
                if found != [(event_id,)]:
                    raise RuntimeError('Failure event could not be committed.') from error
        return failure_key

    def retire_removed_codes(self, *, partition: str | None) -> int:
        """Append RECOVERED to every open failure whose code belongs to the removed parity."""
        condition = '1' if partition is None else "ifNull(partition_key, '')=%(partition)s"
        rows = self.store.execute(
            f"""SELECT failure_key, any(operation), argMax(error_code, event_time),
            argMax(partition_key, event_time), argMax(consumer, event_time),
            argMax(component, event_time), argMax(blocking_scope, event_time),
            argMax(event_id, event_time)
            FROM {self.store.table('source_failure_log')}
            WHERE source_key=%(source)s AND error_code IN %(codes)s AND {condition}
            GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
            {
                'source': self.store.spec.key,
                'codes': tuple(sorted(REMOVED_PARITY_CODES)),
                'partition': partition or '',
            },
        )
        for row in rows:
            if not isinstance(row[7], UUID):
                raise TypeError('Retirement must reference a concrete failure event.')
            self.record(
                operation=str(row[1]),
                error_code=str(row[2]),
                scope=str(row[6]),
                partition=None if row[3] is None else str(row[3]),
                consumer=None if row[4] is None else str(row[4]),
                component=None if row[5] is None else str(row[5]),
                event_type='RECOVERED',
                related_event=row[7],
                details={'reason': 'legacy parity removed'},
            )
        return len(rows)

    def recover(
        self, *, operation: str, partition: str | None = None, consumer: str | None = None
    ) -> None:
        rows = self.store.execute(
            f"""SELECT failure_key, argMax(error_code, event_time),
            argMax(component, event_time), argMax(blocking_scope, event_time), argMax(event_id, event_time)
            FROM {self.store.table('source_failure_log')}
            WHERE source_key=%(source)s AND operation=%(operation)s
              AND ifNull(partition_key, '')=%(partition)s AND ifNull(consumer, '')=%(consumer)s
            GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
            {
                'source': self.store.spec.key,
                'operation': operation,
                'partition': partition or '',
                'consumer': consumer or '',
            },
        )
        for row in rows:
            if not isinstance(row[4], UUID):
                raise TypeError('Recovery must reference a concrete failure event.')
            self.record(
                operation=operation,
                error_code=str(row[1]),
                scope=str(row[3]),
                partition=partition,
                consumer=consumer,
                component=None if row[2] is None else str(row[2]),
                event_type='RECOVERED',
                related_event=row[4],
            )
