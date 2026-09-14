"""SQLite 3.40+ maintenance for the pinned Dagster 1.13 storage layout."""

import fcntl
import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dagster import DagsterInstance, RunsFilter
from dagster._core.storage.event_log.sqlite.sqlite_event_log import SqliteEventLogStorage
from dagster._core.storage.local_compute_log_manager import LocalComputeLogManager
from dagster._core.storage.runs.sqlite.sqlite_run_storage import SqliteRunStorage
from dagster._core.storage.schedules.sqlite.sqlite_schedule_storage import SqliteScheduleStorage

from .codec import decoded_row

JOB_QUERY = """SELECT r.run_body,r.status FROM runs r
JOIN run_tags t ON r.run_id=t.run_id
AND t.key='.dagster/repository' AND t.value='__repository__@origo'
WHERE r.pipeline_name=? ORDER BY r.id DESC LIMIT 5"""
PROBE_JOBS = (
    'create_binance_spot_trades_source_origo_job',
    'build_bar_store_arrow_job',
)


@dataclass(frozen=True)
class Layout:
    runs: Path
    events: Path
    schedules: Path
    compute: Path
    artifact_root: Path | None = None

    @classmethod
    def from_instance(cls, instance: DagsterInstance) -> 'Layout':
        runs = instance.run_storage
        events = instance.event_log_storage
        schedules = instance.schedule_storage
        logs = instance.compute_log_manager
        if not (
            isinstance(runs, SqliteRunStorage)
            and isinstance(events, SqliteEventLogStorage)
            and isinstance(schedules, SqliteScheduleStorage)
            and isinstance(logs, LocalComputeLogManager)
        ):
            raise TypeError(
                'Operational maintenance requires the pinned local SQLite/log backends.'
            )
        with runs.connect() as connection:
            runs_path = Path(str(connection.exec_driver_sql('PRAGMA database_list').one()[2]))
        with schedules.connect() as connection:
            schedules_path = Path(str(connection.exec_driver_sql('PRAGMA database_list').one()[2]))
        log_path = Path(logs.get_captured_local_path(['maintenance-path-probe'], 'out')).parent
        return cls(
            runs_path.resolve(strict=True),
            Path(events.path_for_shard('index')).resolve(strict=True),
            schedules_path.resolve(strict=True),
            log_path.resolve(),
            Path(instance.storage_directory()).resolve(),
        )

    def shard(self, run_id: str) -> Path:
        # Historical Dagster IDs are UUIDs. Never accept a shard name or relative path.
        from uuid import UUID

        if str(UUID(run_id)) != run_id:
            raise ValueError(f'Noncanonical run ID: {run_id}')
        path = self.events.parent / f'{run_id}.db'
        if path.is_symlink() or path.resolve().parent != self.events.parent:
            raise ValueError(f'Unsafe event shard: {path}')
        return path


class MaintenanceDeadlineReached(TimeoutError):
    """A bounded maintenance operation exhausted its assigned work window."""


@contextmanager
def connection(
    path: Path, deadline: float, lock_wait: float, *, write: bool = False
) -> Iterator[sqlite3.Connection]:
    if time.monotonic() >= deadline:
        raise MaintenanceDeadlineReached('Maintenance deadline reached before opening SQLite.')
    mode = 'rw' if write else 'ro'
    database = sqlite3.connect(
        f'{path.as_uri()}?mode={mode}',
        uri=True,
        timeout=min(lock_wait, deadline - time.monotonic()),
    )
    database.row_factory = decoded_row
    database.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    try:
        yield database
    except sqlite3.OperationalError as error:
        if error.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT and time.monotonic() >= deadline:
            raise MaintenanceDeadlineReached(
                'SQLite exhausted its maintenance work window.'
            ) from error
        raise
    finally:
        database.close()


@contextmanager
def maintenance_lock(path: Path, wait_seconds: float) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as handle:
        deadline = time.monotonic() + wait_seconds
        acquired = False
        while not acquired:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError as error:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        'Operational metadata maintenance is already running.'
                    ) from error
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def allocated(path: Path) -> int:
    if path.is_symlink():
        raise ValueError(f'Refusing symlink in maintenance inventory: {path}')
    try:
        return path.stat().st_blocks * 512
    except FileNotFoundError:
        return 0


def artifacts(layout: Layout, run_id: str) -> tuple[Path, ...]:
    shard = layout.shard(run_id)
    return tuple(Path(str(shard) + suffix) for suffix in ('', '-wal', '-shm'))


def refresh_statistics(
    paths: tuple[Path, ...], deadline: float, lock_wait: float
) -> dict[str, float]:
    timings: dict[str, float] = {}
    for path in paths:
        started = time.monotonic()
        with connection(path, deadline, lock_wait, write=True) as database:
            # optimize alone on a fresh 3.40 connection can do nothing. Standard bounded
            # ANALYZE works on this deployed version and persists stats for new connections.
            database.execute('PRAGMA analysis_limit=1000')
            database.execute('ANALYZE')
            database.commit()
        timings[path.name] = time.monotonic() - started
    return timings


def query_latencies(instance: DagsterInstance, deadline: float) -> dict[str, float]:
    latencies: dict[str, float] = {}
    for job in PROBE_JOBS:
        timings: list[float] = []
        for _ in range(20):
            if time.monotonic() >= deadline:
                raise TimeoutError('Job-history latency probe exceeded its deadline.')
            started = time.monotonic()
            instance.get_runs(
                RunsFilter(job_name=job, tags={'.dagster/repository': '__repository__@origo'}),
                limit=5,
            )
            timings.append(time.monotonic() - started)
        latencies[job] = sorted(timings)[18]
    return latencies


def shared_bytes(path: Path, deadline: float, lock_wait: float) -> dict[str, int]:
    with connection(path, deadline, lock_wait) as database:
        page_size = int(database.execute('PRAGMA page_size').fetchone()[0])
        free_pages = int(database.execute('PRAGMA freelist_count').fetchone()[0])
    return {
        'allocated_bytes': sum(
            allocated(Path(str(path) + suffix)) for suffix in ('', '-wal', '-shm')
        ),
        'reusable_bytes': page_size * free_pages,
        'filesystem_free_bytes': os.statvfs(path).f_bavail * os.statvfs(path).f_frsize,
    }
