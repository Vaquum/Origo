from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from dagster import get_dagster_logger

from .contracts import SourceError
from .lifecycle import SourceRuntime


@dataclass(frozen=True)
class _Volume:
    identity: str
    path: Path

    def sample(self) -> tuple[int, int, int, int]:
        stats = os.statvfs(self.path)
        return (
            stats.f_blocks * stats.f_frsize,
            stats.f_bavail * stats.f_frsize,
            stats.f_files,
            stats.f_favail,
        )


def _volumes(runtime: SourceRuntime) -> tuple[_Volume, ...]:
    data = Path(os.environ.get('ORIGO_SOURCE_CLICKHOUSE_VOLUME_PATH', '/opt/origo/clickhouse-data'))
    if not data.is_absolute():
        raise SourceError('CAPACITY_VOLUME_INVALID', 'ClickHouse volume path must be absolute.')
    server = runtime.store.execute('SELECT serverUUID()')[0][0]
    if UUID((data / 'uuid').read_text().strip()) != UUID(str(server)):
        raise SourceError(
            'CAPACITY_VOLUME_MISMATCH',
            'Read-only volume does not belong to this ClickHouse server.',
        )
    disks = runtime.store.execute('SELECT name, path FROM system.disks')
    if disks != [('default', '/var/lib/clickhouse/')]:
        raise SourceError(
            'CAPACITY_DISKS_UNSUPPORTED',
            'Capacity proof requires the declared local ClickHouse volume.',
        )
    paths = (
        data,
        runtime.lock_root,
        Path(os.environ.get('ORIGO_SOURCE_DAGSTER_VOLUME_PATH', '/opt/dagster-instance')),
        Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')),
    )
    return tuple(_Volume(f'{path}:{path.stat().st_dev}:{server}', path) for path in paths)


class CapacityMonitor:
    """Measure source storage working sets and gate subsequent heavy runs."""

    def __init__(self, runtime: SourceRuntime, *, probe: bool | None) -> None:
        self.runtime, self.probe = runtime, probe
        self.volumes = _volumes(runtime)
        self.start: dict[str, int] = {}
        self.working: dict[str, int] = {}
        self.errors: list[Exception] = []
        self.retained_before = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._sample, name='source-capacity', daemon=True)

    def check(self) -> None:
        """Reject unmeasured, unhealthy or capacity-constrained storage before work."""
        runtime = self.runtime
        blocked = runtime.store.execute(
            f"""SELECT failure_key FROM {runtime.store.table('source_failure_log')}
            WHERE source_key=%(source)s AND blocking_scope='SOURCE' AND operation!='capacity'
            GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
            {'source': runtime.spec.key},
        )
        if blocked:
            raise SourceError(
                'SOURCE_HEALTH_BLOCKED', 'An unresolved source-wide failure blocks historical work.'
            )
        for volume in self.volumes:
            total, free, inodes, available = volume.sample()
            records = runtime.store.execute(
                f"""SELECT max(working_set_bytes), countIf(successful) FROM {runtime.store.table('source_capacity_log')}
                WHERE source_key=%(source)s AND volume_id=%(volume)s""",
                {'source': runtime.spec.key, 'volume': volume.identity},
            )
            measured = int(str(records[0][0]))
            if not records[0][1] and self.probe is None:
                self.probe = True
            if not records[0][1] and self.probe is False:
                raise SourceError(
                    'CAPACITY_MEASUREMENT_REQUIRED',
                    'Launch one representative day with capacity_probe enabled before a range backfill.',
                )
            reserve = max(
                (total * 3 + 9) // 10,
                measured * 2 * runtime.spec.orchestration.canonical_concurrency,
            )
            if total <= 0 or free < reserve or inodes <= 0 or available * 10 < inodes:
                raise SourceError(
                    'CAPACITY_RESERVE_BREACHED', f'Storage reserve breached on {volume.path}.'
                )
            self.start[volume.identity] = free
            self.working[volume.identity] = measured
            get_dagster_logger('origo.sources').info(
                'source=%s phase=capacity volume=%s free_bytes=%s reserve_bytes=%s '
                'available_inodes=%s total_inodes=%s probe=%s',
                runtime.spec.key,
                volume.path,
                free,
                reserve,
                available,
                inodes,
                self.probe,
            )
        self.retained_before = self._retained_bytes()
        runtime.failures.recover(operation='capacity')

    def _retained_bytes(self) -> int:
        rows = self.runtime.store.execute(
            'SELECT sum(bytes_on_disk) FROM system.parts WHERE active '
            'AND database=%(database)s AND startsWith(table, %(prefix)s)',
            {'database': self.runtime.store.database, 'prefix': self.runtime.spec.names.prefix},
        )
        return int(str(rows[0][0]))

    def _sample(self) -> None:
        try:
            while not self.stop.is_set():
                for volume in self.volumes:
                    _total, free, _inodes, _available = volume.sample()
                    self.working[volume.identity] = max(
                        self.working[volume.identity], self.start[volume.identity] - free
                    )
                self.stop.wait(0.1)
        except (OSError, ValueError) as error:
            self.errors.append(error)

    def start_sampling(self) -> None:
        """Sample mounted filesystems while the source run executes."""
        self.thread.start()

    def finish(self, *, successful: bool) -> None:
        """Persist valid samples; only successful verification admits future work."""
        self.stop.set()
        if self.thread.is_alive():
            self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise SourceError('CAPACITY_SAMPLER_STUCK', 'Storage sampling did not terminate.')
        if self.errors:
            raise ExceptionGroup('Storage sampling failed.', self.errors)
        # Retained source bytes provide a floor even when a short-lived working set
        # falls between filesystem samples. The probe includes both implementations.
        retained = max(0, self._retained_bytes() - self.retained_before)
        for volume in self.volumes:
            working = max(self.working[volume.identity], retained * 3, 1)
            self.runtime.store.execute(
                f'INSERT INTO {self.runtime.store.table("source_capacity_log")} VALUES',
                [
                    (
                        self.runtime.spec.key,
                        volume.identity,
                        working,
                        self.runtime.run_id,
                        int(successful),
                        datetime.now(UTC),
                    )
                ],
            )
