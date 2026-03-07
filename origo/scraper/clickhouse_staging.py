from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from clickhouse_connect import get_client as _raw_get_client

from origo.query.native_core import resolve_clickhouse_http_settings

from .contracts import NormalizedMetricRecord
from .normalize import normalized_records_to_json_rows

get_client = cast(Any, _raw_get_client)

_STAGING_TABLE = 'scraper_normalized_staging'
_ETF_DAILY_TABLE = 'etf_daily_metrics_long'
_ETF_SOURCE_PREFIX = 'etf_'
_ETF_SOURCE_SUFFIX = '_daily'
_INSERT_COLUMN_NAMES = [
    'metric_id',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'observed_at_utc',
    'dimensions_json',
    'provenance_json',
    'ingested_at_utc',
]


def _ensure_staging_table(client: Any, database: str) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{_STAGING_TABLE} (
            metric_id String,
            source_id String,
            metric_name String,
            metric_unit Nullable(String),
            metric_value_string Nullable(String),
            metric_value_int Nullable(Int64),
            metric_value_float Nullable(Float64),
            metric_value_bool Nullable(UInt8),
            observed_at_utc DateTime64(3, 'UTC'),
            dimensions_json String,
            provenance_json Nullable(String),
            ingested_at_utc DateTime64(3, 'UTC')
        )
        ENGINE = MergeTree
        ORDER BY (source_id, observed_at_utc, metric_name, metric_id)
        """
    )


def _is_etf_daily_source_id(source_id: str) -> bool:
    return source_id.startswith(_ETF_SOURCE_PREFIX) and source_id.endswith(
        _ETF_SOURCE_SUFFIX
    )


def _validate_etf_daily_records(records: list[NormalizedMetricRecord]) -> None:
    for record in records:
        if not _is_etf_daily_source_id(record.source_id):
            raise RuntimeError(
                'ETF canonical load only accepts ETF daily source ids. '
                f'Got source_id={record.source_id}'
            )

        observed = record.observed_at_utc.astimezone(UTC)
        if (
            observed.hour != 0
            or observed.minute != 0
            or observed.second != 0
            or observed.microsecond != 0
        ):
            raise RuntimeError(
                'ETF canonical load requires UTC midnight observation timestamps. '
                f'Got observed_at_utc={record.observed_at_utc.isoformat()} '
                f'for source_id={record.source_id} metric_name={record.metric_name}'
            )


def _insert_rows(
    *,
    client: Any,
    table: str,
    rows: list[dict[str, Any]],
    ingested_at_utc: datetime,
) -> int:
    insert_rows: list[list[Any]] = []
    for row in rows:
        insert_row = {
            **row,
            'ingested_at_utc': ingested_at_utc,
        }
        try:
            insert_rows.append(
                [insert_row[column_name] for column_name in _INSERT_COLUMN_NAMES]
            )
        except KeyError as exc:
            missing_column = str(exc)
            raise RuntimeError(
                f'Normalized row is missing required ClickHouse column {missing_column}'
            ) from exc

    client.insert(
        table=table,
        data=insert_rows,
        column_names=_INSERT_COLUMN_NAMES,
    )
    return len(insert_rows)


def persist_normalized_records_to_clickhouse(
    *,
    records: list[NormalizedMetricRecord],
    auth_token: str | None = None,
) -> int:
    if len(records) == 0:
        return 0

    source_ids = {record.source_id for record in records}
    etf_source_ids = {
        source_id for source_id in source_ids if _is_etf_daily_source_id(source_id)
    }
    if etf_source_ids and len(etf_source_ids) != len(source_ids):
        raise RuntimeError(
            'Mixed ETF and non-ETF source ids are not allowed in one ClickHouse '
            f'insert batch. source_ids={sorted(source_ids)}'
        )
    is_etf_batch = len(source_ids) > 0 and len(etf_source_ids) == len(source_ids)
    if is_etf_batch:
        _validate_etf_daily_records(records)

    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )

    rows = normalized_records_to_json_rows(records=records)
    ingested_at_utc = datetime.now(UTC)

    try:
        if is_etf_batch:
            return _insert_rows(
                client=client,
                table=f'{settings.database}.{_ETF_DAILY_TABLE}',
                rows=rows,
                ingested_at_utc=ingested_at_utc,
            )

        _ensure_staging_table(client=client, database=settings.database)
        return _insert_rows(
            client=client,
            table=f'{settings.database}.{_STAGING_TABLE}',
            rows=rows,
            ingested_at_utc=ingested_at_utc,
        )
    finally:
        client.close()
