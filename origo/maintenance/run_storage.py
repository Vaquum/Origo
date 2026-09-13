"""Keep repository membership correlated with a selected job on Dagster 1.13.21."""

from collections.abc import Sequence
from typing import Self, cast

from dagster import RunsFilter
from dagster._core.storage.dagster_run import JobBucket, TagBucket
from dagster._core.storage.runs.schema import RunsTable, RunTagsTable
from dagster._core.storage.runs.sqlite.sqlite_run_storage import SqliteRunStorage
from dagster._core.storage.sqlite_storage import SqliteStorageConfig
from dagster._serdes import ConfigurableClassData
from sqlalchemy import Select, literal_column, select


class OrigoSqliteRunStorage(SqliteRunStorage):
    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: SqliteStorageConfig
    ) -> Self:
        return cls.from_local(inst_data=inst_data, **config_value)

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
        correlated = filters is not None and bool(filters.job_name) and repository is not None
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
            # unique run keys. Even full ANALYZE estimates six matches, versus 2.8M.
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
