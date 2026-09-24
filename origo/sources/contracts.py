from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import UUID

from dagster import AssetsDefinition, JobDefinition, ScheduleDefinition, SensorDefinition

Row = tuple[object, ...]


class SourceError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


class ArchiveNotPublishedYet(Exception):
    """The provider has not published this latest-day partition yet.

    A latest-day 404 is the normal pre-publish state, not an outage, so it
    skips instead of recording a failure. Once the day is no longer the
    latest, the same 404 fails loud as a mid-history gap. Deliberately not a
    RuntimeError, so the audit's failure handlers cannot catch it as a fault.
    """


WORKER_HEARTBEAT_ENV = 'ORIGO_WORKER_HEARTBEAT'


def beat_worker() -> None:
    """Touch the worker heartbeat when a worker owns this process.

    A slow minute holds the worker inside one tick for minutes; each
    completed unit of work — a REST request, a built component — proves the
    loop is alive so the watchdog does not mistake slow progress for a hang.
    Dagster runs never set the variable and skip this.
    """
    beat = os.environ.get(WORKER_HEARTBEAT_ENV, '')
    if not beat:
        return
    path = Path(beat)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'{time.time():.3f}\n')


def failure_code(error: Exception) -> str:
    return error.code if isinstance(error, SourceError) else type(error).__name__


def failure_message(error: Exception) -> str:
    return (
        error.safe_message
        if isinstance(error, SourceError)
        else f'Operation failed: {type(error).__name__}'
    )


def identifier(value: str) -> str:
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,119}', value):
        raise ValueError(f'Invalid source identifier: {value!r}')
    return value


def table_name(value: str) -> str:
    """A ClickHouse table name as a legacy pipeline spelled it, upper-case labels included."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,119}', value):
        raise ValueError(f'Invalid table name: {value!r}')
    return value


class Client(Protocol):
    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[Row]: ...

    def disconnect(self) -> None: ...


class RolloutStage(StrEnum):
    DORMANT = 'DORMANT'
    CANARY = 'CANARY'
    LIVE = 'LIVE'


@dataclass(frozen=True)
class Partition:
    key: str
    start: datetime
    end: datetime
    provisional: bool = False

    def __post_init__(self) -> None:
        if not self.key or len(self.key) > 200 or any(ord(char) < 32 for char in self.key):
            raise ValueError('Partition key must be a non-empty bounded opaque key.')
        if self.start.tzinfo != UTC or self.end.tzinfo != UTC or self.start >= self.end:
            raise ValueError('Partition bounds must be increasing UTC instants.')


@dataclass(frozen=True)
class Revision:
    key: str
    content_hash: str
    evidence_json: str
    row_count: int
    rows: Callable[[], Iterable[Row]]
    complete: bool = True
    insert_bulk: Callable[[str], None] | None = None


class CanonicalAdapter(Protocol):
    def candidate(self, now: datetime) -> Partition: ...

    def partition(self, key: str) -> Partition: ...

    def discover(self, partition: Partition) -> str: ...

    def fetch(self, partition: Partition) -> Revision: ...

    def revalidate(self, partition: Partition, revision: Revision) -> None: ...


class ProvisionalAdapter(Protocol):
    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]: ...

    def partition(self, key: str) -> Partition: ...

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision: ...


@dataclass(frozen=True)
class SourceNames:
    prefix: str

    def __post_init__(self) -> None:
        identifier(self.prefix)


@dataclass(frozen=True)
class PartitionPolicy:
    first_day: date


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str

    def __post_init__(self) -> None:
        identifier(self.name)
        if self.sql_type not in {
            'UInt8',
            'UInt32',
            'UInt64',
            'Int64',
            'Float64',
            'String',
            'LowCardinality(String)',
            'DateTime',
            'DateTime64(3)',
            'DateTime64(6)',
        }:
            raise ValueError(f'Unsupported component column type: {self.sql_type}')


@dataclass(frozen=True)
class BuildContext:
    client: Client
    database: str
    partition: Partition
    revision: Revision
    build_id: UUID

    def table(self, component: str) -> str:
        return f'{identifier(self.database)}.{identifier(component)}'


@dataclass(frozen=True)
class ComponentSpec:
    key: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    time_column: str
    build: Callable[[BuildContext], None]
    provisional: bool = False
    current_target: str | None = None
    start_at: datetime | None = None
    activation_group: str | None = None

    def __post_init__(self) -> None:
        identifier(self.key)
        if self.activation_group is not None:
            identifier(self.activation_group)
        if self.start_at is not None and self.start_at.tzinfo != UTC:
            raise ValueError('Component applicability must start at a UTC instant.')
        names = tuple(column.name for column in self.columns)
        if len(names) != len(set(names)) or not set(self.primary_key) <= set(names):
            raise ValueError('Component columns and primary key must be unambiguous.')
        if self.time_column not in names or not self.primary_key:
            raise ValueError('Component needs a time column and a primary key.')


@dataclass(frozen=True)
class StateRecord:
    partition: Partition
    generation: int
    revision: str
    build_id: UUID
    component_hashes: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Snapshot:
    token: str
    records: tuple[StateRecord, ...]


class SnapshotReader(Protocol):
    def rows(self, component: str, snapshot: Snapshot) -> list[Row]: ...


class ConsumerRenderer(Protocol):
    def __call__(
        self,
        reader: SnapshotReader,
        snapshot: Snapshot,
        destination: str,
        *,
        allow_full: bool = False,
    ) -> None: ...


@dataclass(frozen=True)
class ConsumerSpec:
    key: str
    publish: ConsumerRenderer
    canonical_only: bool = False
    public: bool = False

    def __post_init__(self) -> None:
        identifier(self.key)


@dataclass(frozen=True)
class OrchestrationSpec:
    canonical_cron: str
    provisional_cron: str
    audit_cron: str
    retry_count: int = 23
    retry_delay: int = 3600
    canonical_concurrency: int = 8

    def __post_init__(self) -> None:
        if self.canonical_concurrency < 1:
            raise ValueError('Canonical concurrency must be positive.')
        if self.provisional_cron != '* * * * *':
            raise ValueError('Provisional tails run in the provisional worker every minute.')


@dataclass(frozen=True)
class RevisionedSourceSpec:
    key: str
    rollout_stage: RolloutStage
    schema_version: int
    names: SourceNames
    partitions: PartitionPolicy
    canonical: CanonicalAdapter
    provisional: ProvisionalAdapter | None
    components: tuple[ComponentSpec, ...]
    consumers: tuple[ConsumerSpec, ...]
    orchestration: OrchestrationSpec
    # Legacy table names served as views over declared components, and legacy tables
    # without a successor that setup drops.
    aliases: tuple[tuple[str, str], ...] = ()
    retired_tables: tuple[str, ...] = ()
    # Rows a retired pipeline wrote into a table it shared with another pipeline, as
    # (table name, SQL predicate); setup deletes them wherever the table remains.
    retired_rows: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        identifier(self.key)
        keys = [component.key for component in self.components]
        consumers = [consumer.key for consumer in self.consumers]
        reserved = {f'{self.names.prefix}_{key}_current' for key in keys}
        aliases = [alias for alias, _ in self.aliases]
        for alias, component in self.aliases:
            if identifier(alias) in reserved or component not in keys:
                raise ValueError('An alias must map a distinct table name to a declared component.')
        for name in self.retired_tables:
            table_name(name)
        for name, predicate in self.retired_rows:
            if not predicate.strip() or ';' in predicate or table_name(name) in self.retired_tables:
                raise ValueError('Retired rows need a single predicate on a table that stays.')
        if len(aliases) != len(set(aliases)) or set(aliases) & set(self.retired_tables):
            raise ValueError('Alias and retired table names must be unique.')
        if self.schema_version < 1 or not keys or len(keys) != len(set(keys)):
            raise ValueError('Source requires a version and unique components.')
        for component in self.components:
            if component.current_target is not None:
                if not component.provisional or component.current_target not in keys:
                    raise ValueError(
                        'A provisional current target must be a declared canonical component.'
                    )
        if len(consumers) != len(set(consumers)):
            raise ValueError('Source consumer identities must be unique.')

    def require_enabled(self, operation: str, *, public: bool = False) -> None:
        if operation == 'setup':
            return
        if self.rollout_stage == RolloutStage.DORMANT:
            raise RuntimeError(f'{self.key} is DORMANT; {operation} is disabled.')
        if public and self.rollout_stage != RolloutStage.LIVE:
            raise RuntimeError(f'{self.key} must be LIVE for public publication.')


@dataclass(frozen=True)
class SourceBundle:
    assets: tuple[AssetsDefinition, ...]
    jobs: tuple[JobDefinition, ...]
    schedules: tuple[ScheduleDefinition, ...]
    sensors: tuple[SensorDefinition, ...]


def retryable_source_error(error: Exception) -> bool:
    """Only explicitly transient provider/republication failures use hourly op retries."""
    if not isinstance(error, SourceError):
        return False
    return error.code in {
        'PROVIDER_TRANSPORT_FAILED',
        'PROVIDER_RATE_CIRCUIT',
        'PROVIDER_HTTP_404',
        'PROVIDER_HTTP_408',
        'PROVIDER_HTTP_418',
        'PROVIDER_HTTP_429',
        'OFFICIAL_REVISION_CHANGED',
        'GENERATION_CHANGED',
    } or error.code.startswith('PROVIDER_HTTP_5')
