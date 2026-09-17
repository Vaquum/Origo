from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    Client,
    ComponentSpec,
    Partition,
    RevisionedSourceSpec,
    Row,
    Snapshot,
    SourceError,
    StateRecord,
    identifier,
    table_name,
)
from .hashing import activation_id, content_hash, state_token

_FAILURE_DDL = """(
    event_id UUID, event_time DateTime64(6, 'UTC'), failure_key String,
    source_key LowCardinality(String),
    event_type Enum8('FAILED' = 1, 'RECOVERED' = 2, 'ACKNOWLEDGED' = 3),
    severity Enum8('WARNING' = 1, 'ERROR' = 2, 'CRITICAL' = 3),
    blocking_scope Enum8('NONE' = 0, 'CONSUMER' = 1, 'PARTITION' = 2, 'SOURCE' = 3, 'ROUTE' = 4),
    operation LowCardinality(String), partition_key Nullable(String), revision Nullable(String),
    build_id Nullable(UUID), component Nullable(String), consumer Nullable(String),
    dagster_run_id String, error_code LowCardinality(String), message String, details_json String
) ENGINE = MergeTree PARTITION BY toYYYYMM(event_time)
ORDER BY (source_key, event_time, event_id)"""


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError('State interval must be a datetime.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _uuid(value: object) -> UUID:
    if not isinstance(value, UUID):
        raise TypeError('State build identity must be a UUID.')
    return value


def _int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError('State counter must be an integer.')
    return value


def ordered_component_rows(
    execute: Callable[[str, object | None], list[Row]],
    component: ComponentSpec,
    table: str,
    *,
    predicate: str = '1',
    params: object | None = None,
) -> Iterator[Row]:
    """Seek through unique component keys, checking the page boundary before advancing."""
    if params is not None and not isinstance(params, dict):
        raise TypeError('Component query parameters must be a mapping.')
    bindings = dict(cast(dict[str, object], params)) if params is not None else {}
    names = ', '.join(column.name for column in component.columns)
    ordering = ', '.join(component.primary_key)
    indexes = [
        next(i for i, column in enumerate(component.columns) if column.name == key)
        for key in component.primary_key
    ]
    seek = '1'
    while True:
        rows = execute(
            f'SELECT {names} FROM {table} WHERE ({predicate}) AND ({seek}) '
            f'ORDER BY {ordering} LIMIT 50001',
            bindings,
        )
        yield from rows[:50000]
        if len(rows) <= 50000:
            break
        last = tuple(rows[49999][index] for index in indexes)
        if last == tuple(rows[50000][index] for index in indexes):
            raise SourceError(
                'COMPONENT_CONTENT_INVALID', f'Component {component.key} contains duplicate keys.'
            )
        casts: list[str] = []
        for position, index in enumerate(indexes):
            name = f'source_page_{position}'
            value = last[position]
            # Driver datetime parameters lose fractional seconds; explicit typed
            # casts retain the DateTime64 key at the page boundary.
            bindings[name] = (
                value.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(value, datetime) else value
            )
            casts.append(f'CAST(%({name})s AS {component.columns[index].sql_type})')
        seek = f'tuple({ordering}) > tuple({", ".join(casts)})'


class StorageError(RuntimeError):
    """A storage operation failed without changing its caller's failure scope."""


# Framework tables retired without a successor; setup drops them wherever they remain.
_RETIRED_TABLES = ('source_parity_log',)


class SourceStore:
    def __init__(self, client: Client, database: str, spec: RevisionedSourceSpec) -> None:
        from .columnar import BoundedClient

        self.client, self.database, self.spec = BoundedClient(client), identifier(database), spec

    def table(self, name: str) -> str:
        return f'{self.database}.{identifier(name)}'

    def component_table(self, component: str) -> str:
        return self.table(f'{self.spec.names.prefix}_{identifier(component)}_revisions')

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[Row]:
        try:
            return self.client.execute(query, params, settings)
        except Exception as error:
            raise StorageError(f'Storage operation failed: {type(error).__name__}') from error

    def run_receipt(self, identity: str) -> tuple[int, str, str] | None:
        rows = self.execute(
            f'SELECT attempt,status,dagster_run_id FROM {self.table("source_run_log")} '
            "WHERE source_key=%(source)s AND event_key=%(identity)s AND dagster_run_id!='' "
            'ORDER BY attempt DESC,recorded_at DESC LIMIT 1',
            {'source': self.spec.key, 'identity': identity},
        )
        return (_int(rows[0][0]), str(rows[0][1]), str(rows[0][2])) if rows else None

    def record_run_receipt(self, identity: str, attempt: int, status: str, run_id: str) -> None:
        event_id = uuid5(NAMESPACE_URL, f'{identity}:{attempt}:{run_id}:{status}')
        if not self.execute(
            f'SELECT event_id FROM {self.table("source_run_log")} WHERE event_id=%(event)s LIMIT 1',
            {'event': event_id},
        ):
            self.execute(
                f'INSERT INTO {self.table("source_run_log")} VALUES',
                [(event_id, self.spec.key, identity, attempt, status, run_id, datetime.now(UTC))],
            )

    def setup(self, *, anchor: datetime) -> None:
        anchor = _utc(anchor)
        self.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        self.execute(
            f'CREATE TABLE IF NOT EXISTS {self.table("source_failure_log")} {_FAILURE_DDL}'
        )
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_run_log')} (
            event_id UUID, source_key String, event_key String, attempt UInt64,
            status LowCardinality(String), dagster_run_id String, recorded_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree PARTITION BY toYYYYMM(recorded_at)
        ORDER BY (source_key, event_key, attempt, recorded_at)""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_backfill_log')} (
            source_key String, backfill_id String, partition_key String, revision String,
            build_id UUID, generation UInt64, dagster_run_id String
        ) ENGINE=MergeTree ORDER BY (source_key, backfill_id, partition_key)""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_discovery_log')} (
            source_key String, partition_key String, requested_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree ORDER BY (source_key, partition_key)""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_lock_domain')} (
            source_key String, domain_id UUID
        ) ENGINE=MergeTree ORDER BY source_key""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_observation_log')} (
            source_key String, partition_key String, evidence_json String,
            complete UInt8, observed_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree PARTITION BY toYYYYMM(observed_at)
        ORDER BY (source_key, partition_key, observed_at)""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_build_log')} (
            source_key String, partition_key String, provisional UInt8,
            partition_start DateTime64(6, 'UTC'), partition_end DateTime64(6, 'UTC'),
            revision String, build_id UUID, started_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree PARTITION BY toYYYYMM(partition_start)
        ORDER BY (source_key, partition_key, build_id)""")
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_certification_log')} (
            source_key String, partition_key String, revision String, build_id UUID,
            activation_generation UInt64, state_token String, check_results String,
            dagster_run_id String, review_state String, recorded_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree PARTITION BY toYYYYMM(recorded_at)
        ORDER BY (source_key, partition_key, recorded_at)""")
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_cleanup_log')} (
            source_key String, partition_key String, build_id UUID,
            dagster_run_id String, completed_at DateTime64(6, 'UTC')
        ) ENGINE=MergeTree ORDER BY (source_key, partition_key, build_id)""")

        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_component_log')} (
            source_key String, partition_key String, provisional UInt8, revision String,
            build_id UUID, component String, row_count UInt64, content_hash String,
            source_content_hash String, evidence_json String, artifact_ref String,
            dagster_run_id String, completed_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree PARTITION BY toYYYYMM(completed_at)
        ORDER BY (source_key, partition_key, revision, build_id, component)""")
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_activation_log')} (
            source_key String, partition_key String, provisional UInt8,
            partition_start DateTime64(6, 'UTC'), partition_end DateTime64(6, 'UTC'),
            generation UInt64, revision String, build_id UUID, activation_id FixedString(64),
            component_hashes String, dagster_run_id String, activated_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree PARTITION BY toYYYYMM(partition_start)
        ORDER BY (source_key, partition_key, generation)""")
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_anchor_log')} (
            source_key String, anchor DateTime64(6, 'UTC')
        ) ENGINE = MergeTree ORDER BY source_key""")
        previous = self.execute(
            f'SELECT anchor FROM {self.table("source_anchor_log")} WHERE source_key=%(source)s',
            {'source': self.spec.key},
        )
        if previous:
            if {_utc(row[0]) for row in previous} != {anchor}:
                raise RuntimeError('The source coverage anchor is immutable.')
        else:
            self.execute(
                f'INSERT INTO {self.table("source_anchor_log")} VALUES', [(self.spec.key, anchor)]
            )
        self.execute(f"""CREATE VIEW IF NOT EXISTS {self.table('source_active_partitions')} AS
            SELECT source_key, partition_key, provisional,
                argMax(a.partition_start, a.generation) AS partition_start,
                argMax(a.partition_end, a.generation) AS partition_end,
                max(a.generation) AS generation,
                argMax(a.revision, a.generation) AS revision,
                argMax(a.build_id, a.generation) AS build_id,
                argMax(a.component_hashes, a.generation) AS component_hashes
            FROM {self.table('source_activation_log')} a
            GROUP BY source_key, partition_key, provisional""")
        self.execute(f"""CREATE VIEW IF NOT EXISTS {self.table('source_current_partitions')} AS
            WITH eligible AS (
                SELECT a.* FROM {self.table('source_active_partitions')} a
                LEFT JOIN (
                    SELECT source_key, groupArray((partition_start, partition_end)) AS intervals
                    FROM {self.table('source_active_partitions')} WHERE NOT provisional GROUP BY source_key
                ) c ON a.source_key=c.source_key
                WHERE NOT a.provisional OR NOT arrayExists(
                    interval -> interval.1<=a.partition_start AND interval.2>a.partition_start, c.intervals)
            ), ranked AS (
                SELECT e.*, anchor,
                    max(partition_end) OVER (PARTITION BY e.source_key ORDER BY partition_start, partition_end
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_end
                FROM eligible e INNER JOIN {self.table('source_anchor_log')} n ON e.source_key=n.source_key
            ), frontiers AS (
                SELECT source_key,
                    if(countIf(partition_start>greatest(prior_end, anchor))=0,
                       max(partition_end), minIf(partition_start, partition_start>greatest(prior_end, anchor))) AS frontier
                FROM ranked GROUP BY source_key
            )
            SELECT e.* FROM eligible e INNER JOIN frontiers f ON e.source_key=f.source_key
            WHERE NOT e.provisional OR e.partition_end<=f.frontier""")
        self.execute(f"""CREATE TABLE IF NOT EXISTS {self.table('source_capacity_log')} (
            source_key String, volume_id String, working_set_bytes UInt64,
            dagster_run_id String, successful UInt8, measured_at DateTime64(6, 'UTC')
        ) ENGINE = MergeTree ORDER BY (source_key, volume_id, measured_at)""")
        for component in self.spec.components:
            columns = ', '.join(f'{column.name} {column.sql_type}' for column in component.columns)
            key = ', '.join(component.primary_key)
            self.execute(f"""CREATE TABLE IF NOT EXISTS {self.component_table(component.key)} (
                source_date Date, partition_key String, revision String, build_id UUID, {columns}
            ) ENGINE = MergeTree PARTITION BY toYYYYMM(source_date)
            ORDER BY (source_date, revision, build_id, {key})""")
        for component in self.spec.components:
            selections = [self._current_select(component, component)]
            selections.extend(
                self._current_select(component, provisional)
                for provisional in self.spec.components
                if provisional.current_target == component.key
            )
            self.execute(
                f'CREATE VIEW IF NOT EXISTS {self.table(self.spec.names.prefix + "_" + component.key + "_current")} AS '
                + ' UNION ALL '.join(selections)
            )
        for name in (*_RETIRED_TABLES, *self.spec.retired_tables):
            self._drop_table(name)
        for name, predicate in self.spec.retired_rows:
            self._delete_rows(name, predicate)
        for alias, key in self.spec.aliases:
            if self._engine(alias) not in (None, 'View'):
                self._drop_table(alias)
            component = next(item for item in self.spec.components if item.key == key)
            self.execute(
                f'CREATE OR REPLACE VIEW {self.table(alias)} AS '
                + self._current_select(component, component)
            )

    def _engine(self, name: str) -> str | None:
        rows = self.execute(
            'SELECT engine FROM system.tables WHERE database=%(database)s AND name=%(name)s',
            {'database': self.database, 'name': table_name(name)},
        )
        return str(rows[0][0]) if rows else None

    def _delete_rows(self, name: str, predicate: str) -> None:
        """Delete a retired pipeline's rows from a shared table that remains, once."""
        if self._engine(name) in (None, 'View'):
            return
        table = f'{self.database}.{table_name(name)}'
        if not self.execute(f'SELECT 1 FROM {table} WHERE {predicate} LIMIT 1'):
            return
        self.execute(
            f'ALTER TABLE {table} DELETE WHERE {predicate}', settings={'mutations_sync': 2}
        )

    def _drop_table(self, name: str) -> None:
        # Retired legacy tables exceed the server drop limit; lift it for this statement only.
        self.execute(
            f'DROP TABLE IF EXISTS {self.database}.{table_name(name)} SYNC',
            settings={'max_table_size_to_drop': 0},
        )

    def _current_select(self, target: ComponentSpec, source: ComponentSpec) -> str:
        selected = ', '.join(f'd.{column.name}' for column in target.columns)
        return f"""SELECT {selected} FROM {self.component_table(source.key)} d
            INNER JOIN {self.table('source_current_partitions')} a
            ON d.partition_key=a.partition_key AND d.revision=a.revision AND d.build_id=a.build_id
            WHERE a.source_key='{self.spec.key}' AND a.provisional={int(source.provisional)}"""

    def generation(self, partition: Partition) -> int:
        rows = self.execute(
            f"""SELECT max(generation) FROM {self.table('source_activation_log')}
            WHERE source_key=%(source)s AND partition_key=%(partition)s AND provisional=%(provisional)s""",
            {
                'source': self.spec.key,
                'partition': partition.key,
                'provisional': int(partition.provisional),
            },
        )
        return _int(rows[0][0])

    def components(self, partition: Partition) -> tuple[ComponentSpec, ...]:
        return tuple(
            component
            for component in self.spec.components
            if component.provisional == partition.provisional
        )

    def validate_component(
        self,
        component: ComponentSpec,
        table: str,
        partition: Partition,
        *,
        predicate: str = '1',
        params: object | None = None,
        legacy_hash: bool = False,
    ) -> tuple[int, str]:
        if not legacy_hash:
            from .columnar import binary_hash, validation_query

            native_count, unique, first, last, nonfinite = self.execute(
                validation_query(component, table, predicate), params
            )[0]
            if (
                native_count != unique
                or nonfinite
                or (
                    native_count
                    and not (partition.start <= _utc(first) <= _utc(last) < partition.end)
                )
            ):
                raise SourceError(
                    'COMPONENT_CONTENT_INVALID',
                    f'Component {component.key} contains duplicate, non-finite or out-of-bounds rows.',
                )
            return _int(native_count), binary_hash(
                self.client,
                component,
                table,
                schema_version=self.spec.schema_version,
                predicate=predicate,
                params=cast(dict[str, object], params) if params is not None else None,
            )
        indexes = tuple(
            next(i for i, column in enumerate(component.columns) if column.name == key)
            for key in component.primary_key
        )
        time_index = next(
            i for i, column in enumerate(component.columns) if column.name == component.time_column
        )
        count = 0

        def checked_rows() -> Iterator[Row]:
            nonlocal count
            previous: Row | None = None
            for row in ordered_component_rows(
                self.execute, component, table, predicate=predicate, params=params
            ):
                current = tuple(row[index] for index in indexes)
                if previous == current:
                    raise SourceError(
                        'COMPONENT_CONTENT_INVALID',
                        f'Component {component.key} contains duplicate keys.',
                    )
                if not partition.start <= _utc(row[time_index]) < partition.end:
                    raise SourceError(
                        'COMPONENT_CONTENT_INVALID',
                        f'Component {component.key} contains an out-of-bounds row.',
                    )
                previous = current
                count += 1
                yield row

        digest = content_hash(checked_rows(), schema_version=self.spec.schema_version)
        return count, digest

    def anchor(self) -> datetime:
        rows = self.execute(
            f'SELECT anchor FROM {self.table("source_anchor_log")} WHERE source_key=%(source)s',
            {'source': self.spec.key},
        )
        if len(rows) != 1:
            raise RuntimeError('A source must have exactly one immutable coverage anchor.')
        return _utc(rows[0][0])

    def active_intervals(self) -> tuple[Partition, ...]:
        rows = self.execute(
            f"""SELECT partition_key, partition_start, partition_end, provisional
            FROM {self.table('source_active_partitions')} WHERE source_key=%(source)s""",
            {'source': self.spec.key},
        )
        return tuple(
            Partition(str(row[0]), _utc(row[1]), _utc(row[2]), bool(row[3])) for row in rows
        )

    def records(self, *, canonical_only: bool = False) -> tuple[StateRecord, ...]:
        rows = self.execute(
            f"""SELECT partition_key, provisional, partition_start, partition_end,
            generation, revision, build_id, component_hashes
            FROM {self.table('source_active_partitions')} WHERE source_key=%(source)s
            ORDER BY partition_start, provisional""",
            {'source': self.spec.key},
        )
        result: list[StateRecord] = []
        for row in rows:
            hashes: object = json.loads(str(row[7]))
            if not isinstance(hashes, list):
                raise TypeError('Activation component hashes must be a list.')
            pairs: list[tuple[str, str]] = []
            for item in cast(list[object], hashes):
                if not isinstance(item, list):
                    raise TypeError('Activation component hash entry must be a pair.')
                pair = cast(list[object], item)
                if len(pair) != 2:
                    raise TypeError('Activation component hash entry must be a pair.')
                if not isinstance(pair[0], str) or not isinstance(pair[1], str):
                    raise TypeError('Activation component hash entry must contain strings.')
                pairs.append((pair[0], pair[1]))
            result.append(
                StateRecord(
                    Partition(str(row[0]), _utc(row[2]), _utc(row[3]), bool(row[1])),
                    _int(row[4]),
                    str(row[5]),
                    _uuid(row[6]),
                    tuple(pairs),
                )
            )
        canonical = [record for record in result if not record.partition.provisional]
        if canonical_only:
            return tuple(canonical)
        rows = self.execute(
            f'SELECT anchor FROM {self.table("source_anchor_log")} WHERE source_key=%(source)s',
            {'source': self.spec.key},
        )
        if len(rows) != 1:
            raise RuntimeError('A source must have exactly one immutable coverage anchor.')
        frontier = _utc(rows[0][0])
        eligible = [
            record
            for record in result
            if not record.partition.provisional
            or not any(
                day.partition.start <= record.partition.start < day.partition.end
                for day in canonical
            )
        ]
        for record in eligible:
            if record.partition.start > frontier:
                break
            frontier = max(frontier, record.partition.end)
        return tuple(
            record
            for record in eligible
            if not record.partition.provisional or record.partition.end <= frontier
        )

    def snapshot(self, *, canonical_only: bool = False) -> Snapshot:
        records = self.records(canonical_only=canonical_only)
        return Snapshot(state_token(self.spec.key, records), records)

    def canonical_token(self, snapshot: Snapshot) -> str:
        """The token of the canonical records pinned in a snapshot; publication currency."""
        return state_token(
            self.spec.key,
            tuple(record for record in snapshot.records if not record.partition.provisional),
        )

    def complete_builds(self) -> tuple[str, dict[str, object]]:
        """Subquery of builds whose component log carries every declared canonical component."""
        keys = tuple(
            component.key for component in self.spec.components if not component.provisional
        )
        return (
            f"""(SELECT source_key, partition_key, revision, build_id
            FROM {self.table('source_component_log')}
            WHERE source_key=%(source)s AND component IN %(components)s
            GROUP BY source_key, partition_key, revision, build_id
            HAVING uniqExact(component)=%(component_count)s)""",
            {'source': self.spec.key, 'components': keys, 'component_count': len(keys)},
        )

    def canonical_ready(self) -> bool:
        """Every active canonical generation has complete component evidence and no open partition failure."""
        builds, params = self.complete_builds()
        incomplete = self.execute(
            f"""SELECT count() FROM {self.table('source_active_partitions')} a
            LEFT ANTI JOIN {builds} c USING (source_key, partition_key, revision, build_id)
            WHERE a.source_key=%(source)s AND NOT a.provisional""",
            params,
        )
        blocked = self.execute(
            f"""SELECT count() FROM {self.table('source_active_partitions')} a
            INNER JOIN (
                SELECT ifNull(any(partition_key), '') AS partition_key
                FROM {self.table('source_failure_log')}
                WHERE source_key=%(source)s AND blocking_scope='PARTITION'
                GROUP BY failure_key HAVING argMax(event_type, event_time)='FAILED'
            ) f ON a.partition_key=f.partition_key
            WHERE a.source_key=%(source)s AND NOT a.provisional""",
            {'source': self.spec.key},
        )
        return incomplete == [(0,)] and blocked == [(0,)]

    def rows(self, component: str, snapshot: Snapshot) -> list[Row]:
        specification = next(item for item in self.spec.components if item.key == component)
        result: list[Row] = []
        names = ', '.join(column.name for column in specification.columns)
        for record in snapshot.records:
            if record.partition.provisional != specification.provisional:
                continue
            result.extend(
                self.execute(
                    f"""SELECT {names} FROM {self.component_table(component)}
                WHERE partition_key=%(partition)s AND revision=%(revision)s AND build_id=%(build)s
                ORDER BY {', '.join(specification.primary_key)}""",
                    {
                        'partition': record.partition.key,
                        'revision': record.revision,
                        'build': record.build_id,
                    },
                )
            )
        return result

    def insert_activation(self, record: StateRecord, run_id: str) -> None:
        identity = activation_id(self.spec.key, record)
        values = [
            (
                self.spec.key,
                record.partition.key,
                int(record.partition.provisional),
                record.partition.start,
                record.partition.end,
                record.generation,
                record.revision,
                record.build_id,
                identity,
                json.dumps(record.component_hashes),
                run_id,
                datetime.now(UTC),
            )
        ]
        try:
            self.execute(f'INSERT INTO {self.table("source_activation_log")} VALUES', values)
        except Exception as error:
            found = self.execute(
                f'SELECT activation_id FROM {self.table("source_activation_log")} WHERE activation_id=%(identity)s',
                {'identity': identity},
            )
            if found != [(identity,)]:
                raise RuntimeError(
                    'Activation insert did not produce one committed record.'
                ) from error
        found = self.execute(
            f"""SELECT activation_id FROM {self.table('source_activation_log')}
            WHERE source_key=%(source)s AND partition_key=%(partition)s AND generation=%(generation)s""",
            {
                'source': self.spec.key,
                'partition': record.partition.key,
                'generation': record.generation,
            },
        )
        if found != [(identity,)]:
            raise RuntimeError('Activation readback did not match the fenced generation.')
