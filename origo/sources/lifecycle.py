from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from dagster import get_dagster_logger

from .component_upgrade import ComponentUpgradeError, attach_components
from .contracts import (
    ArchiveNotPublishedYet,
    BuildContext,
    Partition,
    Revision,
    RevisionedSourceSpec,
    Snapshot,
    SourceError,
    StateRecord,
    beat_worker,
    failure_code,
    failure_message,
)
from .failures import FailureLog
from .locking import partition_work, source_lock
from .storage import SourceStore


def _partition_lock(partition: Partition) -> str:
    return 'partition_' + hashlib.sha256(partition.key.encode()).hexdigest()


@dataclass(frozen=True)
class SourceRuntime:
    spec: RevisionedSourceSpec
    store: SourceStore
    lock_root: Path
    run_id: str

    @property
    def failures(self) -> FailureLog:
        return FailureLog(self.store, self.lock_root, self.run_id)

    def setup(self, *, anchor: datetime | None = None) -> None:
        self.spec.require_enabled('setup')
        selected = anchor or datetime.combine(
            self.spec.partitions.first_day, datetime.min.time(), UTC
        )
        with source_lock(self.lock_root, self.spec.key, 'setup', wait=True):
            self.store.setup(anchor=selected)
            marker = self.lock_root / self.spec.key / 'domain_id'
            if not marker.exists():
                with marker.open('x') as handle:
                    handle.write(str(uuid4()))
                    handle.flush()
                    os.fsync(handle.fileno())
            domain_id = UUID(marker.read_text())
            existing = self.store.execute(
                f'SELECT DISTINCT domain_id FROM {self.store.table("source_lock_domain")} WHERE source_key=%(source)s',
                {'source': self.spec.key},
            )
            if not existing:
                self.store.execute(
                    f'INSERT INTO {self.store.table("source_lock_domain")} VALUES',
                    [(self.spec.key, domain_id)],
                )
            self.require_shared_mount()

    def require_shared_mount(self) -> None:
        marker = self.lock_root / self.spec.key / 'domain_id'
        domain_id = UUID(marker.read_text())
        existing = self.store.execute(
            f'SELECT DISTINCT domain_id FROM {self.store.table("source_lock_domain")} WHERE source_key=%(source)s',
            {'source': self.spec.key},
        )
        if existing != [(domain_id,)]:
            raise RuntimeError('Workers do not share the registered source lock mount.')

    def discover(self, partition: Partition) -> str:
        self.spec.require_enabled('discover')
        self.require_shared_mount()
        with source_lock(
            self.lock_root,
            self.spec.key,
            'discovery_' + hashlib.sha256(partition.key.encode()).hexdigest(),
        ):
            found = self.store.execute(
                f'SELECT count() FROM {self.store.table("source_discovery_log")} '
                'WHERE source_key=%(source)s AND partition_key=%(partition)s',
                {'source': self.spec.key, 'partition': partition.key},
            )
            if not found[0][0]:
                self.store.execute(
                    f'INSERT INTO {self.store.table("source_discovery_log")} VALUES',
                    [(self.spec.key, partition.key, datetime.now(UTC))],
                )
        try:
            revision = self.spec.canonical.discover(partition)
            self.failures.recover(operation='discovery', partition=partition.key)
            return revision
        except Exception as error:
            if (
                isinstance(error, SourceError)
                and error.code == 'PROVIDER_HTTP_404'
                and partition.key == self.spec.canonical.candidate(datetime.now(UTC)).key
            ):
                raise ArchiveNotPublishedYet(
                    f'{self.spec.key} partition {partition.key} is not published yet.'
                ) from error
            self.failures.record(
                operation='discovery',
                error_code=failure_code(error),
                message=failure_message(error),
                scope='PARTITION',
                partition=partition.key,
            )
            raise

    def enable_components(self, group: str) -> None:
        self.spec.require_enabled('component_enablement')
        self.require_shared_mount()
        if group not in {component.activation_group for component in self.spec.components}:
            raise ValueError(f'Unknown component activation group: {group}')
        with source_lock(self.lock_root, self.spec.key, 'heavy', wait=True):
            if group not in self.store.enabled_groups():
                self.store.execute(
                    f'INSERT INTO {self.store.table("source_component_rollout_log")} VALUES',
                    [(self.spec.key, group, 1, datetime.now(UTC))],
                )
            if group not in self.store.enabled_groups():
                raise RuntimeError('Component enablement did not persist.')

    def upgrade_components(self, key: str, *, provisional: bool = False) -> StateRecord:
        self.spec.require_enabled('component_upgrade')
        self.require_shared_mount()
        adapter = self.spec.provisional if provisional else self.spec.canonical
        if adapter is None:
            raise ValueError('This source has no provisional adapter.')
        partition = adapter.partition(key)
        with partition_work(self.lock_root, self.spec.key, _partition_lock(partition)):
            record = self.store.record(partition)
            if record is None:
                raise SourceError('ACTIVE_PARTITION_MISSING', 'No selected generation is active.')
            self._validate_retained(record)
            return self._upgrade_components(record)

    def _upgrade_components(self, record: StateRecord) -> StateRecord:
        return attach_components(
            self.store, record, failure_log=self.failures,
            run_id=self.run_id, progress=self._beat_progress,
        )

    def build(self, key: str, *, provisional: bool = False) -> StateRecord:
        get_dagster_logger('origo.sources').info(
            'source=%s partition=%s phase=build_started provisional=%s run=%s',
            self.spec.key,
            key,
            provisional,
            self.run_id,
        )
        self.spec.require_enabled('build')
        self.require_shared_mount()
        adapter = self.spec.provisional if provisional else self.spec.canonical
        if adapter is None:
            raise ValueError('This source has no provisional adapter.')
        partition = adapter.partition(key)
        build_id = uuid4()
        operation = 'provisional' if provisional else 'canonical'
        lock = _partition_lock(partition)
        try:
            with partition_work(self.lock_root, self.spec.key, lock):
                expected = self.store.generation(partition)
                if provisional:
                    retained = self.store.record(partition)
                    if retained is not None and self.store.missing_components(retained):
                        self._validate_retained(retained)
                        return self._upgrade_components(retained)
                    previous = self.store.execute(
                        f"""SELECT evidence_json FROM {self.store.table('source_observation_log')}
                        WHERE source_key=%(source)s AND partition_key=%(partition)s
                        ORDER BY observed_at DESC LIMIT 1""",
                        {'source': self.spec.key, 'partition': key},
                    )
                    if self.spec.provisional is None:
                        raise ValueError('Source has no provisional adapter.')
                    revision = self.spec.provisional.fetch(
                        partition, None if not previous else str(previous[0][0])
                    )
                    self.store.execute(
                        f'INSERT INTO {self.store.table("source_observation_log")} VALUES',
                        [
                            (
                                self.spec.key,
                                key,
                                revision.evidence_json,
                                int(revision.complete),
                                datetime.now(UTC),
                            )
                        ],
                    )
                    if not revision.complete:
                        raise RuntimeError(
                            'Provisional interval is awaiting independent completeness evidence.'
                        )
                else:
                    current = next(
                        (
                            item
                            for item in self.store.records(canonical_only=True)
                            if item.partition.key == key
                        ),
                        None,
                    )
                    if (
                        current is not None
                        and self.spec.canonical.discover(partition) == current.revision
                    ):
                        self._validate_retained(current)
                        self.spec.canonical.revalidate(
                            partition, Revision(current.revision, '', '{}', 0, lambda: iter(()))
                        )
                        current = self._upgrade_components(current)
                        self.failures.recover_partition(partition=key)
                        self._recover_superseded(key)
                        return current
                    revision = adapter.fetch(partition)
                current = [
                    record
                    for record in self.store.records(canonical_only=not provisional)
                    if record.partition.key == key
                ]
                if current and current[0].revision == revision.key:
                    self._validate_retained(current[0])
                    retained = self._upgrade_components(current[0])
                    if provisional:
                        self.failures.recover(operation=operation, partition=key)
                        self.failures.recover(operation='component', partition=key)
                    else:
                        self.failures.recover_partition(partition=key)
                    return retained
                record = self._build_components(partition, revision, build_id, expected)
                if provisional:
                    self._activate(record, expected)
                    self.failures.recover(operation=operation, partition=key)
                    self.failures.recover(operation='component', partition=key)
                else:
                    self.spec.canonical.revalidate(partition, revision)
                    self._activate(record, expected)
                    self.failures.recover_partition(partition=key)
                    self._recover_superseded(key)
                return record
        except ComponentUpgradeError:
            raise
        except Exception as error:
            self._record_attempt_failure(operation, key, build_id, error)
            raise

    def _record_attempt_failure(
        self, operation: str, key: str, build_id: UUID, error: Exception
    ) -> None:
        recorded = self.store.execute(
            f'SELECT count() FROM {self.store.table("source_failure_log")} '
            "WHERE build_id=%(build)s AND event_type='FAILED' AND blocking_scope='PARTITION'",
            {'build': build_id},
        )
        if not recorded[0][0]:
            self.failures.record(
                operation=operation,
                error_code=failure_code(error),
                message=failure_message(error),
                scope='PARTITION',
                partition=key,
                build_id=build_id,
            )

    def _beat_progress(self, partition: Partition, build_id: UUID, component: str) -> None:
        """Best-effort worker beat between build phases.

        A heartbeat write error is telemetry failing, not the component: it is
        logged and never recorded, so it cannot misrecord as a component or
        build failure. A truly unwritable heartbeat still kills the worker via
        the watchdog within the bound, which pages through workers_alive.
        """
        try:
            beat_worker()
        except OSError as error:
            get_dagster_logger('origo.sources').warning(
                'source=%s partition=%s build=%s component=%s heartbeat write failed: %s',
                self.spec.key,
                partition.key,
                build_id,
                component,
                error,
            )

    def _build_components(
        self, partition: Partition, revision: Revision, build_id: UUID, expected: int
    ) -> StateRecord:
        database = 'source_build_' + build_id.hex
        context = BuildContext(self.store.client, database, partition, revision, build_id)
        hashes: list[tuple[str, str]] = []
        components = self.store.components(partition)
        self.store.execute(
            f'INSERT INTO {self.store.table("source_build_log")} VALUES',
            [
                (
                    self.spec.key,
                    partition.key,
                    int(partition.provisional),
                    partition.start,
                    partition.end,
                    revision.key,
                    build_id,
                    datetime.now(UTC),
                )
            ],
        )
        self.store.execute(f'CREATE DATABASE {database}')
        try:
            for component in components:
                columns = ', '.join(
                    f'{column.name} {column.sql_type}' for column in component.columns
                )
                self.store.execute(f"""CREATE TABLE {context.table(component.key)} ({columns})
                    ENGINE = MergeTree ORDER BY ({', '.join(component.primary_key)})""")
            for index, component in enumerate(components):
                try:
                    get_dagster_logger('origo.sources').info(
                        'source=%s partition=%s build=%s component=%s phase=started',
                        self.spec.key,
                        partition.key,
                        build_id,
                        component.key,
                    )
                    component.build(context)
                    # The Arrow build above is the slow phase; beat before the
                    # validate-and-insert stretch so neither half can look dead.
                    self._beat_progress(partition, build_id, component.key)
                    count, digest = self.store.validate_component(
                        component, context.table(component.key), partition
                    )
                    if index == 0 and count != revision.row_count:
                        raise SourceError(
                            'COMPONENT_CONTENT_INVALID',
                            'Source rows do not match the validated adapter count.',
                        )
                    if revision.row_count and count == 0:
                        raise SourceError(
                            'COMPONENT_CONTENT_INVALID',
                            'A required component is empty for a non-empty revision.',
                        )
                    params = {
                        'date': partition.start.date(),
                        'partition': partition.key,
                        'revision': revision.key,
                        'build': build_id,
                    }
                    self.store.execute(
                        f"""INSERT INTO {self.store.component_table(component.key)}
                        SELECT %(date)s, %(partition)s, %(revision)s, %(build)s, *
                        FROM {context.table(component.key)}""",
                        params,
                    )
                    # Retained contents are checked together immediately before activation,
                    # under this partition's exclusive lock. Rehashing each copy here
                    # would duplicate that complete fresh read without changing visibility.
                    self.store.execute(
                        f'INSERT INTO {self.store.table("source_component_log")} VALUES',
                        [
                            (
                                self.spec.key,
                                partition.key,
                                int(partition.provisional),
                                revision.key,
                                build_id,
                                component.key,
                                count,
                                digest,
                                revision.content_hash,
                                revision.evidence_json,
                                self.store.component_table(component.key),
                                self.run_id,
                                datetime.now(UTC),
                            )
                        ],
                    )
                    hashes.append((component.key, digest))
                    get_dagster_logger('origo.sources').info(
                        'source=%s partition=%s build=%s component=%s phase=validated rows=%s hash=%s',
                        self.spec.key,
                        partition.key,
                        build_id,
                        component.key,
                        count,
                        digest,
                    )
                except Exception as error:
                    self.failures.record(
                        operation='component',
                        error_code=failure_code(error),
                        message=failure_message(error),
                        scope='PARTITION',
                        partition=partition.key,
                        revision=revision.key,
                        build_id=build_id,
                        component=component.key,
                    )
                    raise
                # A fat minute spends minutes in Arrow builds and inserts
                # after its last REST request; each finished component proves
                # the worker is alive. Dagster runs skip this.
                self._beat_progress(partition, build_id, component.key)
            return StateRecord(partition, expected + 1, revision.key, build_id, tuple(hashes))
        finally:
            try:
                self.store.execute(f'DROP DATABASE {database} SYNC')
            except (OSError, RuntimeError) as error:
                self.failures.record(
                    operation='cleanup',
                    error_code=failure_code(error),
                    message=failure_message(error),
                    scope='NONE',
                    partition=partition.key,
                    build_id=build_id,
                    details={'staging_database': database},
                )

    def _validate_retained(self, record: StateRecord) -> None:
        components = self.store.accepted_components(record)
        expected = dict(record.component_hashes)
        params = {
            'source': self.spec.key,
            'source_date': record.partition.start.date(),
            'partition': record.partition.key,
            'revision': record.revision,
            'build': record.build_id,
        }
        for component in components:
            rows = self.store.execute(
                f"""SELECT row_count, content_hash FROM {self.store.table('source_component_log')}
                WHERE source_key=%(source)s AND partition_key=%(partition)s
                  AND revision=%(revision)s AND build_id=%(build)s AND component=%(component)s""",
                {**params, 'component': component.key},
            )
            try:
                actual = self.store.validate_component(
                    component,
                    self.store.component_table(component.key),
                    record.partition,
                    predicate='source_date=%(source_date)s AND partition_key=%(partition)s AND revision=%(revision)s AND build_id=%(build)s',
                    params=params,
                    legacy_hash=not expected[component.key].startswith('v2:'),
                )
            except SourceError as error:
                if error.code != 'COMPONENT_CONTENT_INVALID':
                    raise
                raise SourceError('RETAINED_CONTENT_INVALID', error.safe_message) from error
            if len(rows) != 1 or rows[0] != actual or actual[1] != expected[component.key]:
                raise SourceError(
                    'RETAINED_CONTENT_INVALID',
                    f'Retained {component.key} content or successful component evidence is incomplete.',
                )

    def _activate(self, record: StateRecord, expected: int) -> None:
        if self.store.generation(record.partition) != expected:
            raise RuntimeError('Stale expected generation; the build cannot activate.')
        self._validate_retained(record)
        self.store.insert_activation(record, self.run_id)

    def publish(self, consumer_key: str, destination: str, *, allow_full: bool = False) -> Snapshot:
        consumer = next(value for value in self.spec.consumers if value.key == consumer_key)
        self.spec.require_enabled('publish', public=consumer.public)
        self.require_shared_mount()
        try:
            with source_lock(
                self.lock_root, self.spec.key, 'consumer_' + consumer.key,
                wait=not self.run_id.startswith('worker:'),
            ):
                from .publication import publication_current

                # A canonical-only consumer publishes the canonical state. A consumer that
                # declares provisional components pins the partial-day rows present now and
                # republishes as they change; only canonical drift invalidates a render.
                canonical = self.store.snapshot(canonical_only=True)
                pinned = (
                    canonical
                    if consumer.canonical_only
                    else self.store.snapshot(canonical_only=False)
                )
                if not publication_current(
                    self.spec,
                    consumer.key,
                    pinned.token,
                    root=Path(destination).parent.parent,
                    pinned=not consumer.canonical_only,
                ):
                    consumer.publish(self.store, pinned, destination, allow_full=allow_full)
                self.failures.recover(operation='consumer', consumer=consumer.key)
                return canonical
        except Exception as error:
            self.failures.record(
                operation='consumer',
                error_code=failure_code(error),
                message=failure_message(error),
                scope='CONSUMER',
                consumer=consumer.key,
            )
            raise

    def rollback(
        self, record: StateRecord, *, operator: str, reason: str, quarantine: bool = False
    ) -> StateRecord:
        self.spec.require_enabled('rollback')
        self.require_shared_mount()
        if not operator.strip() or not reason.strip():
            raise ValueError('Rollback requires operator identity and reason.')
        with source_lock(self.lock_root, self.spec.key, 'heavy'):
            with source_lock(self.lock_root, self.spec.key, _partition_lock(record.partition)):
                self._validate_retained(record)
                revision = Revision(record.revision, '', '{}', 0, lambda: iter(()))
                try:
                    if not record.partition.provisional:
                        self.spec.canonical.revalidate(record.partition, revision)
                except SourceError as error:
                    if error.code != 'OFFICIAL_REVISION_CHANGED' or not quarantine:
                        self.failures.record(
                            operation='rollback',
                            error_code=error.code,
                            message=error.safe_message,
                            scope='PARTITION',
                            partition=record.partition.key,
                            revision=record.revision,
                        )
                        raise
                    self.failures.record(
                        operation='quarantine',
                        error_code='OLDER_OFFICIAL_REVISION',
                        scope='NONE',
                        partition=record.partition.key,
                        revision=record.revision,
                        severity='CRITICAL',
                        details={'operator': operator, 'reason': reason},
                    )
                expected = self.store.generation(record.partition)
                restored = StateRecord(
                    record.partition,
                    expected + 1,
                    record.revision,
                    record.build_id,
                    record.component_hashes,
                )
                self._activate(restored, expected)
                self.failures.recover(operation='rollback', partition=record.partition.key)
                return restored

    def audit(self) -> tuple[str, ...]:
        self.spec.require_enabled('audit')
        self.require_shared_mount()
        records = self.store.records(canonical_only=True)
        changed: list[str] = []
        cutoff = datetime.now(UTC) - timedelta(days=14)
        recent = [record for record in records if record.partition.end >= cutoff]
        older = [record for record in records if record.partition.end < cutoff]
        offset = (int(datetime.now(UTC).timestamp()) // 3600 * 50) % max(1, len(older))
        candidates = (
            recent + (older[offset : offset + 50] + older[: max(0, offset + 50 - len(older))])[:50]
        )
        pending = self.store.execute(
            f"""SELECT partition_key FROM {self.store.table('source_discovery_log')}
            WHERE source_key=%(source)s AND partition_key NOT IN (
                SELECT partition_key FROM {self.store.table('source_active_partitions')}
                WHERE source_key=%(source)s AND NOT provisional
            ) GROUP BY partition_key ORDER BY min(requested_at) LIMIT 5""",
            {'source': self.spec.key},
        )
        for row in pending:
            partition = self.spec.canonical.partition(str(row[0]))
            try:
                self.discover(partition)
                self.failures.recover(operation='audit', partition=partition.key)
                changed.append(partition.key)
            except ArchiveNotPublishedYet:
                get_dagster_logger('origo.sources').info(
                    'source=%s partition=%s phase=audit_pending_unpublished',
                    self.spec.key,
                    partition.key,
                )
                continue
            except (OSError, ValueError, RuntimeError) as error:
                self.failures.record(
                    operation='audit',
                    error_code=failure_code(error),
                    message=failure_message(error),
                    scope='NONE',
                    partition=partition.key,
                )
        for record in candidates:
            try:
                revision = self.spec.canonical.discover(record.partition)
                if revision != record.revision:
                    changed.append(record.partition.key)
                self.failures.recover(operation='audit', partition=record.partition.key)
            except (OSError, ValueError, RuntimeError) as error:
                self.failures.record(
                    operation='audit',
                    error_code=failure_code(error),
                    message=failure_message(error),
                    scope='NONE',
                    partition=record.partition.key,
                )
        return tuple(changed)

    def repair(self, key: str) -> StateRecord:
        self.spec.require_enabled('repair')
        self.require_shared_mount()
        record = next(
            (
                record
                for record in self.store.records(canonical_only=True)
                if record.partition.key == key
            ),
            None,
        )
        if record is None:
            return self.build(key)
        partition = self.spec.canonical.partition(key)
        build_id = uuid4()
        try:
            with partition_work(self.lock_root, self.spec.key, _partition_lock(partition)):
                current = self.store.record(partition)
                if current is None:
                    raise SourceError('ACTIVE_PARTITION_MISSING', 'No canonical generation is active.')
                record = current
                try:
                    self._validate_retained(record)
                except SourceError as error:
                    if error.code != 'RETAINED_CONTENT_INVALID':
                        raise
                    self.failures.record(
                        operation='repair',
                        error_code=error.code,
                        message=failure_message(error),
                        scope='PARTITION',
                        partition=key,
                        revision=record.revision,
                        build_id=record.build_id,
                    )
                else:
                    upgraded = self._upgrade_components(record)
                    self.failures.recover_partition(partition=key)
                    return upgraded
                expected = self.store.generation(partition)
                revision = self.spec.canonical.fetch(partition)
                rebuilt = self._build_components(partition, revision, build_id, expected)
                self.spec.canonical.revalidate(partition, revision)
                self._activate(rebuilt, expected)
                self.failures.recover_partition(partition=key)
                return rebuilt
        except ComponentUpgradeError:
            raise
        except Exception as error:
            self._record_attempt_failure('repair', key, build_id, error)
            raise

    def cleanup(self, *, dry_run: bool = True) -> tuple[str, ...]:
        self.spec.require_enabled('cleanup')
        self.require_shared_mount()
        planned: list[str] = []
        now = datetime.now(UTC)
        try:
            with source_lock(self.lock_root, self.spec.key, 'heavy'):
                attempts = self.store.execute(
                    f"""SELECT partition_key, provisional, partition_start,
                    partition_end, revision, build_id, started_at FROM {self.store.table('source_build_log')}
                    WHERE source_key=%(source)s AND build_id NOT IN (
                        SELECT build_id FROM {self.store.table('source_cleanup_log')} WHERE source_key=%(source)s
                    ) ORDER BY partition_start, build_id""",
                    {'source': self.spec.key},
                )
                for row in attempts:
                    if (
                        not isinstance(row[2], datetime)
                        or not isinstance(row[3], datetime)
                        or not isinstance(row[5], UUID)
                        or not isinstance(row[6], datetime)
                    ):
                        raise TypeError('Cleanup attempt metadata has invalid types.')
                    partition = Partition(
                        str(row[0]),
                        row[2].replace(tzinfo=UTC),
                        row[3].replace(tzinfo=UTC),
                        bool(row[1]),
                    )
                    build_id = row[5]
                    age = now - row[6].replace(tzinfo=UTC)
                    with source_lock(self.lock_root, self.spec.key, _partition_lock(partition)):
                        active = self.store.execute(
                            f"""SELECT build_id, max(a.activated_at) AS activated_at FROM {self.store.table('source_activation_log')} a
                            WHERE source_key=%(source)s AND partition_key=%(partition)s
                            GROUP BY build_id ORDER BY max(a.generation) DESC LIMIT 2""",
                            {'source': self.spec.key, 'partition': partition.key},
                        )
                        superseded_provisional = False
                        if partition.provisional:
                            canonical = self.store.execute(
                                f"""SELECT count(), max(activated_at) FROM {self.store.table('source_activation_log')}
                                WHERE source_key=%(source)s AND NOT provisional
                                  AND partition_start<=%(start)s AND partition_end>=%(end)s""",
                                {
                                    'source': self.spec.key,
                                    'start': partition.start,
                                    'end': partition.end,
                                },
                            )
                            if canonical[0][0]:
                                canonical_time = canonical[0][1]
                                if not isinstance(canonical_time, datetime):
                                    raise TypeError('Canonical activation time is invalid.')
                                if now - canonical_time.replace(tzinfo=UTC) < timedelta(days=7):
                                    continue
                                superseded_provisional = True
                        if active and active[0][0] == build_id and not superseded_provisional:
                            continue
                        if (
                            not partition.provisional
                            and len(active) == 2
                            and active[1][0] == build_id
                        ):
                            activation_time = active[0][1]
                            if not isinstance(activation_time, datetime):
                                raise TypeError('Cleanup activation time is invalid.')
                            if now - activation_time.replace(tzinfo=UTC) < timedelta(days=30):
                                continue
                        if age < timedelta(days=7):
                            continue
                        planned.append(str(build_id))
                        if not dry_run:
                            for component in self.store.known_components(partition):
                                self.store.client.execute(
                                    f"""ALTER TABLE {self.store.component_table(component.key)}
                                    DELETE WHERE partition_key=%(partition)s AND build_id=%(build)s""",
                                    {'partition': partition.key, 'build': build_id},
                                    settings={'mutations_sync': 2},
                                )
                            self.store.execute(
                                f'DROP DATABASE IF EXISTS source_build_{build_id.hex} SYNC'
                            )
                            self.store.execute(
                                f'INSERT INTO {self.store.table("source_cleanup_log")} VALUES',
                                [(self.spec.key, partition.key, build_id, self.run_id, now)],
                            )
                return tuple(planned)
        except Exception as error:
            self.failures.record(
                operation='cleanup',
                error_code=failure_code(error),
                message=failure_message(error),
                scope='NONE',
            )
            raise

    def _recover_superseded(self, day: str) -> None:
        """Close open provisional-interval failures inside a day the canonical build now covers."""
        rows = self.store.execute(
            f"""SELECT failure_key, any(operation), argMax(error_code, event_time),
            argMax(partition_key, event_time), argMax(component, event_time),
            argMax(blocking_scope, event_time), argMax(event_id, event_time)
            FROM {self.store.table('source_failure_log')}
            WHERE source_key=%(source)s AND operation IN ('provisional', 'component')
              AND startsWith(ifNull(partition_key, ''), %(prefix)s)
            GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
            {'source': self.spec.key, 'prefix': day + 'T'},
        )
        for row in rows:
            if not isinstance(row[6], UUID):
                raise TypeError('Superseded recovery must reference a concrete failure event.')
            self.failures.record(
                operation=str(row[1]),
                error_code=str(row[2]),
                scope=str(row[5]),
                partition=None if row[3] is None else str(row[3]),
                component=None if row[4] is None else str(row[4]),
                event_type='RECOVERED',
                related_event=row[6],
                details={'reason': 'superseded by the canonical day'},
            )

    def _recover_committed_failure(self, record: StateRecord) -> None:
        rows = self.store.execute(
            f"""SELECT failure_key, argMax(error_code, event_time), argMax(event_id, event_time),
            argMax(dagster_run_id, event_time) FROM {self.store.table('source_failure_log')}
            WHERE source_key=%(source)s AND partition_key=%(partition)s AND operation='canonical'
            GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
            {'source': self.spec.key, 'partition': record.partition.key},
        )
        committed = self.store.execute(
            f"""SELECT dagster_run_id FROM {self.store.table('source_activation_log')}
            WHERE source_key=%(source)s AND partition_key=%(partition)s
              AND build_id=%(build)s AND generation=%(generation)s""",
            {
                'source': self.spec.key,
                'partition': record.partition.key,
                'build': record.build_id,
                'generation': record.generation,
            },
        )
        for row in rows:
            if (row[3],) in committed:
                if not isinstance(row[2], UUID):
                    raise TypeError('Committed recovery must reference a concrete failure event.')
                self.failures.record(
                    operation='canonical',
                    partition=record.partition.key,
                    scope='PARTITION',
                    error_code=str(row[1]),
                    event_type='RECOVERED',
                    related_event=row[2],
                )

    def reconcile(self, key: str) -> StateRecord:
        """Re-check the retained contents and official revision of the active generation."""
        self.spec.require_enabled('reconcile')
        self.require_shared_mount()
        partition = self.spec.canonical.partition(key)
        try:
            with partition_work(self.lock_root, self.spec.key, _partition_lock(partition)):
                record = next(
                    (
                        item
                        for item in self.store.records(canonical_only=True)
                        if item.partition.key == key
                    ),
                    None,
                )
                if record is None:
                    raise SourceError(
                        'ACTIVE_PARTITION_MISSING', 'No canonical generation is active.'
                    )
                self.failures.retire_removed_codes(partition=key)
                self._validate_retained(record)
                self.spec.canonical.revalidate(
                    record.partition, Revision(record.revision, '', '{}', 0, lambda: iter(()))
                )
                record = self._upgrade_components(record)
                if self.store.generation(record.partition) != record.generation:
                    raise SourceError(
                        'GENERATION_CHANGED', 'Activation advanced during reconciliation.'
                    )
                # A validated active generation closes open integrity findings, the
                # retired comparison operation's findings and failed repair attempts.
                for operation in ('integrity', 'verification', 'repair'):
                    self.failures.recover(operation=operation, partition=key)
                self._recover_committed_failure(record)
                self._recover_superseded(key)
                pending = self.store.execute(
                    f"""SELECT failure_key FROM {self.store.table('source_failure_log')}
                    WHERE source_key=%(source)s AND partition_key=%(partition)s
                      AND blocking_scope='PARTITION'
                    GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED' """,
                    {'source': self.spec.key, 'partition': key},
                )
                if pending:
                    raise SourceError(
                        'INGESTION_FAILURE_UNRESOLVED',
                        'A failed ingestion attempt still requires a successful retry.',
                    )
                get_dagster_logger('origo.sources').info(
                    'source=%s partition=%s revision=%s build=%s generation=%s phase=reconciled',
                    self.spec.key,
                    key,
                    record.revision,
                    record.build_id,
                    record.generation,
                )
                return record
        except ComponentUpgradeError:
            raise
        except Exception as error:
            self.failures.record(
                operation='integrity',
                scope='PARTITION',
                partition=key,
                error_code=failure_code(error),
                message=failure_message(error),
            )
            raise

    def certify(self, key: str, *, review_state: str) -> Snapshot:
        self.spec.require_enabled('certify')
        self.require_shared_mount()
        if review_state not in ('PENDING', 'APPROVED'):
            raise ValueError('Certification review state must be explicit.')
        record = next(
            record
            for record in self.store.records(canonical_only=True)
            if record.partition.key == key
        )
        try:
            self._validate_retained(record)
            self.spec.canonical.revalidate(
                record.partition, Revision(record.revision, '', '{}', 0, lambda: iter(()))
            )
            snapshot = self.store.snapshot()
            checks = {
                'components': dict(record.component_hashes),
                'official_revision': record.revision,
                'activation_generation': record.generation,
            }
            self.store.execute(
                f'INSERT INTO {self.store.table("source_certification_log")} VALUES',
                [
                    (
                        self.spec.key,
                        key,
                        record.revision,
                        record.build_id,
                        record.generation,
                        snapshot.token,
                        json.dumps(checks, sort_keys=True),
                        self.run_id,
                        review_state,
                        datetime.now(UTC),
                    )
                ],
            )
            self.failures.recover(operation='certification', partition=key)
            return snapshot
        except Exception as error:
            self.failures.record(
                operation='certification',
                error_code=failure_code(error),
                message=failure_message(error),
                scope='PARTITION',
                partition=key,
            )
            raise
