"""Retain current partition facts in Dagster's existing index when history expires.

No replacement materializations are emitted: IDs, timestamps, tags, data versions
and the asset cache stay intact. Retired run details expire; their current asset
facts remain available through the same Dagster APIs until superseded.
"""

import sqlite3
import time
from pathlib import Path
from typing import Self

from dagster import RunsFilter
from dagster._core.events.log import EventLogEntry
from dagster._core.instance import RUNLESS_RUN_ID
from dagster._core.storage.event_log.sqlite.sqlite_event_log import SqliteEventLogStorage
from dagster._core.storage.sqlite_storage import SqliteStorageConfig
from dagster._serdes import ConfigurableClassData, deserialize_value

from .run_locks import run_lock
from .sqlite import connection

_CURRENT_TYPES = ('ASSET_MATERIALIZATION', 'ASSET_OBSERVATION')


def _current(database: sqlite3.Connection, event: sqlite3.Row) -> bool:
    if event['dagster_event_type'] not in _CURRENT_TYPES:
        return False
    newer = database.execute(
        """SELECT id FROM event_logs WHERE asset_key=? AND dagster_event_type=?
           AND partition IS ? AND id>? LIMIT 1""",
        (event['asset_key'], event['dagster_event_type'], event['partition'], event['id']),
    ).fetchone()
    return newer is None


def _delete_index_events(database: sqlite3.Connection, ids: list[int]) -> None:
    if ids:
        placeholders = ','.join('?' for _ in ids)
        for row in database.execute(
            f"SELECT event,run_id,dagster_event_type FROM event_logs WHERE id IN ({placeholders}) AND dagster_event_type IN ('ASSET_CHECK_EVALUATION','ASSET_CHECK_EVALUATION_PLANNED')",
            ids,
        ).fetchall():
            event = deserialize_value(str(row['event']), EventLogEntry).dagster_event
            if event is None:
                raise ValueError('Asset check evaluation has no payload.')
            payload = (
                event.asset_check_evaluation_data
                if row['dagster_event_type'] == 'ASSET_CHECK_EVALUATION'
                else event.asset_check_planned_data
            )
            database.execute(
                'DELETE FROM asset_check_executions WHERE asset_key=? AND check_name=? AND run_id=?',
                (payload.asset_key.to_string(), payload.check_name, row['run_id']),
            )
        database.execute(f'DELETE FROM asset_event_tags WHERE event_id IN ({placeholders})', ids)
        database.execute(f'DELETE FROM event_logs WHERE id IN ({placeholders})', ids)


class OrigoSqliteEventLogStorage(SqliteEventLogStorage):
    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: SqliteStorageConfig
    ) -> Self:
        return cls(inst_data=inst_data, **config_value)

    def writer_lock_path(self) -> Path:
        return Path(self.path_for_shard('index')).parent / 'operational-maintenance' / 'run-locks'

    def store_event(self, event: EventLogEntry) -> None:
        storage_id = 0
        if event.run_id != RUNLESS_RUN_ID:
            records = self._instance.get_run_records(RunsFilter(run_ids=[event.run_id]), limit=1)
            if not records:
                raise RuntimeError(f'Refusing an event for an absent run: {event.run_id}')
            storage_id = records[0].storage_id
        with run_lock(self.writer_lock_path(), storage_id, 5):
            if (
                event.run_id != RUNLESS_RUN_ID
                and self._instance.get_run_by_id(event.run_id) is None
            ):
                raise RuntimeError(f'Refusing an event for an absent run: {event.run_id}')
            super().store_event(event)

    def delete_events(self, run_id: str) -> None:
        # This API is called only after run storage has retired the run. Keep the
        # upstream per-run deletion behavior; physical reclamation is a later stage.
        with self.run_connection(run_id) as database:
            self.delete_events_for_run(database, run_id)
        path = Path(self.path_for_shard('index'))
        with connection(path, time.monotonic() + 10, 1, write=True) as database:
            database.execute('BEGIN IMMEDIATE')
            cursor = 0
            while rows := database.execute(
                'SELECT id,asset_key,dagster_event_type,partition FROM event_logs WHERE run_id=? AND id>? ORDER BY id LIMIT 500',
                (run_id, cursor),
            ).fetchall():
                _delete_index_events(
                    database, [int(row['id']) for row in rows if not _current(database, row)]
                )
                cursor = int(rows[-1]['id'])
            database.commit()
        if self.supports_global_concurrency_limits:
            self.free_concurrency_slots_for_run(run_id)

    def compact_retired_state(
        self, runs_path: Path, after_id: int, limit: int, deadline: float, lock_wait: float
    ) -> tuple[int, int, int]:
        """Scan a bounded index page; remove only superseded facts of absent runs."""
        path = Path(self.path_for_shard('index'))
        with (
            connection(runs_path, deadline, lock_wait) as runs,
            connection(path, deadline, lock_wait, write=True) as database,
        ):
            database.execute('BEGIN IMMEDIATE')
            rows = database.execute(
                """SELECT id,run_id,asset_key,dagster_event_type,partition
                   FROM event_logs WHERE id>? ORDER BY id LIMIT ?""",
                (after_id, limit),
            ).fetchall()
            retired: list[int] = []
            for row in rows:
                run_id = str(row['run_id'])
                # Runless events belong to Dagster's runless shard and are not retired history.
                if (
                    len(run_id) == 36
                    and runs.execute('SELECT 1 FROM runs WHERE run_id=?', (run_id,)).fetchone()
                    is None
                    and not _current(database, row)
                ):
                    retired.append(int(row['id']))
            _delete_index_events(database, retired)
            database.commit()
        return (int(rows[-1]['id']) if rows else 0, len(rows), len(retired))
