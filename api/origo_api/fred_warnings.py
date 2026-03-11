from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

from clickhouse_connect import get_client as _raw_get_client

from origo.fred.registry import load_fred_series_registry
from origo.query.native_core import resolve_clickhouse_http_settings

from .schemas import RawQueryWarning

logger = logging.getLogger(__name__)
get_client = cast(Any, _raw_get_client)

_FRED_TABLE = 'canonical_fred_series_metrics_native_v1'


@dataclass(frozen=True)
class FREDPublishFreshnessSnapshot:
    expected_source_ids: frozenset[str]
    latest_publish_by_source: dict[str, datetime]


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _source_id_list_sql(source_ids: Sequence[str]) -> str:
    return ', '.join(_sql_quote(source_id) for source_id in source_ids)


def _coerce_datetime_utc(value: Any, *, label: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        normalized = value.replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise RuntimeError(f'{label} must be ISO datetime, got: {value}') from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise RuntimeError(f'{label} has unsupported type: {type(value)}')


def _load_latest_publish_by_source(
    *, client: Any, database: str, expected_source_ids: tuple[str, ...]
) -> dict[str, datetime]:
    query = (
        'SELECT source_id, '
        "max(parseDateTimeBestEffortOrNull(JSONExtractString(provenance_json, 'last_updated_utc'))) "
        'AS latest_publish_utc '
        f'FROM {database}.{_FRED_TABLE} '
        f'WHERE source_id IN ({_source_id_list_sql(expected_source_ids)}) '
        'GROUP BY source_id'
    )

    rows = client.query(query).result_rows
    latest_publish_by_source: dict[str, datetime] = {}
    for row in rows:
        if len(row) != 2:
            raise RuntimeError(
                f'Expected two columns from FRED publish query, got {len(row)} columns'
            )

        source_id_raw = row[0]
        if not isinstance(source_id_raw, str) or source_id_raw.strip() == '':
            raise RuntimeError(
                f'FRED publish query returned invalid source_id: {source_id_raw}'
            )
        source_id = source_id_raw

        latest_publish = _coerce_datetime_utc(
            row[1], label=f'latest_publish_utc[{source_id}]'
        )
        if latest_publish is None:
            raise RuntimeError(
                'FRED publish query returned null latest_publish_utc for '
                f'source_id={source_id}'
            )

        latest_publish_by_source[source_id] = latest_publish

    return latest_publish_by_source


def build_warnings_from_snapshot(
    *,
    snapshot: FREDPublishFreshnessSnapshot,
    stale_max_age_days: int,
    today_utc: date | None = None,
) -> list[RawQueryWarning]:
    if stale_max_age_days <= 0:
        raise RuntimeError('stale_max_age_days must be > 0')

    today = today_utc if today_utc is not None else datetime.now(UTC).date()
    warnings: list[RawQueryWarning] = []

    missing_sources = tuple(
        sorted(snapshot.expected_source_ids.difference(snapshot.latest_publish_by_source))
    )
    if len(missing_sources) > 0:
        warnings.append(
            RawQueryWarning(
                code='FRED_SOURCE_PUBLISH_MISSING',
                message=(
                    'FRED publish timestamps are missing for expected sources: '
                    f'{list(missing_sources)}'
                ),
            )
        )

    stale_segments: list[str] = []
    for source_id in sorted(snapshot.latest_publish_by_source):
        latest_publish_utc = snapshot.latest_publish_by_source[source_id]
        publish_age_days = (today - latest_publish_utc.date()).days
        if publish_age_days > stale_max_age_days:
            stale_segments.append(
                f'{source_id}:latest_publish_utc={latest_publish_utc.isoformat()}:age_days={publish_age_days}'
            )

    if len(stale_segments) > 0:
        warnings.append(
            RawQueryWarning(
                code='FRED_SOURCE_PUBLISH_STALE',
                message=(
                    'FRED publish timestamps exceeded max age threshold: '
                    f'max_age_days={stale_max_age_days}, stale_sources={stale_segments}'
                ),
            )
        )

    return warnings


def build_fred_publish_freshness_warnings(
    auth_token: str | None, stale_max_age_days: int
) -> list[RawQueryWarning]:
    _, registry_entries = load_fred_series_registry()
    expected_source_ids = tuple(sorted(entry.source_id for entry in registry_entries))

    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
    try:
        snapshot = FREDPublishFreshnessSnapshot(
            expected_source_ids=frozenset(expected_source_ids),
            latest_publish_by_source=_load_latest_publish_by_source(
                client=client,
                database=settings.database,
                expected_source_ids=expected_source_ids,
            ),
        )
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    return build_warnings_from_snapshot(
        snapshot=snapshot,
        stale_max_age_days=stale_max_age_days,
    )
