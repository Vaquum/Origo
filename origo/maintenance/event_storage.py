"""Retain current partition facts in Dagster's existing index when history expires.

No replacement materializations are emitted: IDs, timestamps, tags, data versions
and the asset cache stay intact. Retired run details expire; their current asset
facts remain available through the same Dagster APIs until superseded.
"""

import sqlite3
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Self
from uuid import UUID

from dagster import DagsterRunStatus, RunsFilter
from dagster._core.events.log import EventLogEntry
from dagster._core.instance import RUNLESS_RUN_ID
from dagster._core.storage.event_log.sqlite import sqlite_event_log as upstream
from dagster._core.storage.event_log.sqlite.sqlite_event_log import SqliteEventLogStorage
from dagster._core.storage.sql import get_alembic_config, run_alembic_upgrade
from dagster._core.storage.sqlite_storage import SqliteStorageConfig
from dagster._serdes import ConfigurableClassData, deserialize_value
from sqlalchemy import Connection, create_engine
from sqlalchemy.pool import NullPool

from .archive import (
    archive_path,
    read_image,
    remove_image,
    restore_for_write,
    snapshot_image,
    store_image,
    sync_directory,
    transition_lock,
)
from .codec import compress_run_json, json_rows
from .roles import run_role
from .run_locks import run_lock
from .run_storage import OrigoSqliteRunStorage
from .sqlite import MaintenanceDeadlineReached, allocated, connection

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

    @contextmanager
    def index_connection(self) -> Iterator[Connection]:
        with super().index_connection() as database, json_rows(database):
            yield database

    @contextmanager
    def run_connection(self, run_id: str | None = None) -> Iterator[Connection]:
        if run_id is None:
            raise ValueError('A run ID is required for an event connection.')
        base = Path(self.path_for_shard('index')).parent
        deadline = time.monotonic() + 5
        with transition_lock(base, run_id, deadline=deadline):
            payload = read_image(base, run_id, deadline)
            if payload is None:
                with super().run_connection(run_id) as database:
                    yield database
                return
        memory = sqlite3.connect(':memory:')
        memory.deserialize(payload)
        memory.execute('PRAGMA query_only=ON')
        engine = create_engine('sqlite://', creator=lambda: memory, poolclass=NullPool)
        try:
            with engine.connect() as database:
                yield database
        finally:
            engine.dispose()
            memory.close()

    def archive_run(self, run_id: str, deadline: float) -> int:
        if str(UUID(run_id)) != run_id:
            raise ValueError('A canonical run UUID is required for source compaction.')
        records = self._instance.get_run_records(RunsFilter(run_ids=[run_id]), limit=1)
        if not records or run_role(records[0].dagster_run) == 'projection':
            raise ValueError('Only retained source or unclassified runs can be compacted.')
        base = Path(self.path_for_shard('index')).parent
        shard = Path(self.path_for_shard(run_id))
        with run_lock(self.writer_lock_path(), records[0].storage_id, 1):
            current = self._instance.get_run_by_id(run_id)
            if current is None or current.status not in (
                DagsterRunStatus.SUCCESS,
                DagsterRunStatus.FAILURE,
                DagsterRunStatus.CANCELED,
            ):
                raise RuntimeError('Source compaction requires a retained terminal run.')
            with transition_lock(base, run_id, deadline=deadline):
                paths = [Path(str(shard) + suffix) for suffix in ('', '-wal', '-shm')]
                for path in paths:
                    if path.exists() and (path.is_symlink() or path.stat().st_nlink != 1):
                        raise ValueError(f'Unsafe source event shard: {path}')
                archive_files = [
                    Path(str(archive_path(base)) + suffix) for suffix in ('', '-wal', '-shm')
                ]
                archive_before = sum(allocated(path) for path in archive_files)
                image = read_image(base, run_id, deadline)
                if image is None:
                    image = snapshot_image(shard, deadline)
                    store_image(base, run_id, image, deadline)
                before = sum(allocated(path) for path in paths)
                for path in paths:
                    if path.exists():
                        path.unlink()
                sync_directory(base)
                archive_after = sum(allocated(path) for path in archive_files)
                released = max(0, before + archive_before - archive_after)
        runs = self._instance.run_storage
        if not isinstance(runs, OrigoSqliteRunStorage):
            raise TypeError('Source compaction requires Origo run storage.')
        runs.compress_run(run_id)
        while True:
            if time.monotonic() >= deadline:
                raise MaintenanceDeadlineReached(
                    'Shared source JSON compaction reached its deadline.'
                )
            with self.index_connection() as database:
                count = compress_run_json(database, 'event_logs', run_id)
            if count < 500:
                break
        return released

    def get_all_run_ids(self) -> Sequence[str]:
        ids = set(super().get_all_run_ids())
        path = archive_path(Path(self.path_for_shard('index')).parent)
        if path.exists():
            with connection(path, time.monotonic() + 10, 1) as database:
                ids.update(
                    str(row[0]) for row in database.execute('SELECT run_id FROM source_runs')
                )
        return sorted(ids)

    def upgrade(self) -> None:
        # Dagster migrations already require an outage. Upgrade one packed image
        # at a time, without expanding the complete source history on disk.
        base = Path(self.path_for_shard('index')).parent
        configuration = get_alembic_config(upstream.__file__)
        for run_id in self.get_all_run_ids():
            deadline = time.monotonic() + 120
            with transition_lock(base, run_id, deadline=deadline):
                payload = read_image(base, run_id, deadline)
                if payload is None:
                    with super().run_connection(run_id) as database:
                        run_alembic_upgrade(configuration, database, run_id)
                else:
                    memory = sqlite3.connect(':memory:')
                    memory.deserialize(payload)
                    engine = create_engine('sqlite://', creator=lambda: memory)
                    try:
                        with engine.begin() as database:
                            run_alembic_upgrade(configuration, database, run_id)
                        updated = memory.serialize()
                        store_image(base, run_id, updated, deadline)
                    finally:
                        engine.dispose()
                        memory.close()
        with self.index_connection() as database:
            run_alembic_upgrade(configuration, database, 'index')
        self._initialized_dbs = set[str]()

    def wipe(self) -> None:
        base = Path(self.path_for_shard('index')).parent
        # Like Dagster schema upgrades, an instance wipe requires quiesced workers.
        super().wipe()
        for suffix in ('', '-wal', '-shm'):
            path = Path(str(archive_path(base)) + suffix)
            if path.exists():
                path.unlink()
        if archive_path(base).parent.exists():
            sync_directory(archive_path(base).parent)

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
            base = Path(self.path_for_shard('index')).parent
            with transition_lock(base, event.run_id, deadline=time.monotonic() + 5):
                restore_for_write(
                    base,
                    Path(self.path_for_shard(event.run_id)),
                    event.run_id,
                    time.monotonic() + 5,
                )
            super().store_event(event)

    def update_event_log_record(self, record_id: int, event: EventLogEntry) -> None:
        records = self._instance.get_run_records(RunsFilter(run_ids=[event.run_id]), limit=1)
        if not records:
            raise RuntimeError('Cannot update an event of an absent run.')
        base = Path(self.path_for_shard('index')).parent
        with run_lock(self.writer_lock_path(), records[0].storage_id, 5):
            with transition_lock(base, event.run_id, deadline=time.monotonic() + 5):
                restore_for_write(
                    base,
                    Path(self.path_for_shard(event.run_id)),
                    event.run_id,
                    time.monotonic() + 5,
                )
            super().update_event_log_record(record_id, event)

    def delete_events(self, run_id: str) -> None:
        if self._instance.get_run_by_id(run_id) is not None:
            raise RuntimeError('Retire the run before deleting its execution history.')
        base = Path(self.path_for_shard('index')).parent
        deadline = time.monotonic() + 10
        with transition_lock(base, run_id, deadline=deadline):
            if read_image(base, run_id, deadline) is not None:
                remove_image(base, run_id, deadline)
            elif Path(self.path_for_shard(run_id)).exists():
                with super().run_connection(run_id) as database:
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
