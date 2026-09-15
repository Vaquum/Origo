"""Keep repository membership correlated with selected runs on Dagster 1.13.21."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import cached_property
from typing import Self, cast

from dagster import RunsFilter
from dagster._core.storage.dagster_run import JobBucket, TagBucket
from dagster._core.storage.runs.schema import RunsTable, RunTagsTable
from dagster._core.storage.runs.sqlite.sqlite_run_storage import SqliteRunStorage
from dagster._core.storage.sqlite_storage import SqliteStorageConfig
from dagster._serdes import ConfigurableClassData
from sqlalchemy import Connection, Engine, Select, create_engine, delete, literal_column, select
from sqlalchemy.pool import NullPool

from . import roles
from .codec import compress_run_json, json_rows


class OrigoSqliteRunStorage(SqliteRunStorage):
    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: SqliteStorageConfig
    ) -> Self:
        return cls.from_local(inst_data=inst_data, **config_value)

    @cached_property
    def _origo_engine(self) -> Engine:
        return create_engine(self._conn_string, poolclass=NullPool)

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        with self._origo_engine.connect() as database, database.begin(), json_rows(database):
            yield database

    def dispose(self) -> None:
        if '_origo_engine' in self.__dict__:
            self._origo_engine.dispose()
        super().dispose()

    def compress_run(self, run_id: str) -> None:
        with self.connect() as database:
            compress_run_json(database, 'runs', run_id)

    def delete_run(self, run_id: str) -> None:
        source_tag = (
            select(1)
            .select_from(RunTagsTable)
            .where(
                RunTagsTable.c.run_id == RunsTable.c.run_id,
                RunTagsTable.c.key == 'origo_source_key',
                RunTagsTable.c.value != '',
            )
            .correlate(RunsTable)
        )
        # Classification is part of the DELETE itself: a concurrent source tag
        # cannot turn a protected run into a disposable projection after a read.
        query = delete(RunsTable).where(
            RunsTable.c.run_id == run_id,
            RunsTable.c.pipeline_name.in_(roles.PROJECTION_JOBS - roles.SOURCE_JOBS),
            ~source_tag.exists(),
        )
        with self.connect() as database:
            database.execute(query)
            if (
                database.execute(
                    select(1).select_from(RunsTable).where(RunsTable.c.run_id == run_id)
                ).first()
                is not None
            ):
                raise RuntimeError('Source and unclassified runs cannot be retired.')
            # SQLite does not enable foreign-key cascades on Dagster connections.
            # Remove tags in the same transaction, after the source guard passed.
            database.execute(delete(RunTagsTable).where(RunTagsTable.c.run_id == run_id))

    def _runs_query(
        self,
        filters: RunsFilter | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        ascending: bool = False,
        bucket_by: JobBucket | TagBucket | None = None,
    ) -> Select[tuple[object, ...]]:
        repository = filters.tags.get('.dagster/repository') if filters else None
        correlated = (
            filters is not None
            and repository is not None
            and (bool(filters.job_name) or len(filters.tags) > 1)
        )
        if correlated and filters is not None:
            filters = RunsFilter(
                run_ids=filters.run_ids,
                job_name=filters.job_name,
                statuses=filters.statuses,
                tags={k: v for k, v in filters.tags.items() if k != '.dagster/repository'},
                snapshot_id=filters.snapshot_id,
                updated_after=filters.updated_after,
                updated_before=filters.updated_before,
                created_after=filters.created_after,
                created_before=filters.created_before,
                exclude_subruns=filters.exclude_subruns,
            )
        query = cast(
            Select[tuple[object, ...]],
            super()._runs_query(filters, cursor, limit, columns, order_by, ascending, bucket_by),
        )
        if correlated and repository is not None:
            # SQLite STAT1 averages a nearly universal repository tag together with
            # unique run keys. Job, sensor, and schedule queries must probe membership
            # by run ID rather than rescan that repository for every matching run.
            membership = (
                select(1)
                .select_from(RunTagsTable)
                .where(
                    RunTagsTable.c.run_id == RunsTable.c.run_id,
                    literal_column('+run_tags.key') == '.dagster/repository',
                    RunTagsTable.c.value == repository
                    if isinstance(repository, str)
                    else RunTagsTable.c.value.in_(repository),
                )
                .correlate(RunsTable)
            )
            query = query.where(membership.exists())
        return query
