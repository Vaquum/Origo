from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from .locking import source_lock
from .storage import SourceStore


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
