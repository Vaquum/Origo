from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

from clickhouse_connect import get_client as _raw_get_client

from origo.query.native_core import resolve_clickhouse_http_settings

from .schemas import RawQueryWarning

logger = logging.getLogger(__name__)
get_client = cast(Any, _raw_get_client)

_ETF_DAILY_TABLE = 'etf_daily_metrics_long'
_ETF_EXPECTED_SOURCE_IDS: tuple[str, ...] = (
    'etf_ishares_ibit_daily',
    'etf_invesco_btco_daily',
    'etf_bitwise_bitb_daily',
    'etf_ark_arkb_daily',
    'etf_vaneck_hodl_daily',
    'etf_franklin_ezbc_daily',
    'etf_grayscale_gbtc_daily',
    'etf_fidelity_fbtc_daily',
    'etf_coinshares_brrr_daily',
    'etf_hashdex_defi_daily',
)
_ETF_REQUIRED_METRICS: tuple[str, ...] = (
    'issuer',
    'ticker',
    'as_of_date',
    'btc_units',
    'btc_market_value_usd',
    'total_net_assets_usd',
    'holdings_row_count',
)


@dataclass(frozen=True)
class ETFQualitySnapshot:
    latest_observed_day: date | None
    present_source_ids: frozenset[str]
    missing_required_metrics_by_source: dict[str, tuple[str, ...]]


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _source_id_list_sql() -> str:
    return ', '.join(_sql_quote(source_id) for source_id in _ETF_EXPECTED_SOURCE_IDS)


def _coerce_day(value: Any, *, label: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise RuntimeError(f'{label} must be ISO date, got: {value}') from exc
    raise RuntimeError(f'{label} has unsupported type: {type(value)}')


def _load_latest_observed_day(*, client: Any, database: str) -> date | None:
    query = (
        f'SELECT max(toDate(observed_at_utc)) AS latest_day '
        f'FROM {database}.{_ETF_DAILY_TABLE} '
        f'WHERE source_id IN ({_source_id_list_sql()})'
    )
    rows = client.query(query).result_rows
    if len(rows) != 1:
        raise RuntimeError(f'Expected exactly one row from latest-day query, got {len(rows)}')
    row = rows[0]
    if len(row) != 1:
        raise RuntimeError(
            f'Expected one column from latest-day query, got {len(row)} columns'
        )
    return _coerce_day(row[0], label='latest_day')


def _load_present_sources_for_day(
    *, client: Any, database: str, latest_day: date
) -> frozenset[str]:
    query = (
        f'SELECT source_id '
        f'FROM {database}.{_ETF_DAILY_TABLE} '
        f"WHERE toDate(observed_at_utc) = toDate('{latest_day.isoformat()}') "
        f'AND source_id IN ({_source_id_list_sql()}) '
        f'GROUP BY source_id'
    )
    rows = client.query(query).result_rows
    source_ids: set[str] = set()
    for row in rows:
        if len(row) != 1:
            raise RuntimeError(
                f'Expected one column from present-source query, got {len(row)} columns'
            )
        source_id = row[0]
        if not isinstance(source_id, str) or source_id.strip() == '':
            raise RuntimeError(
                f'present-source query returned invalid source_id value: {source_id}'
            )
        source_ids.add(source_id)
    return frozenset(source_ids)


def _load_missing_required_metrics_by_source(
    *, client: Any, database: str, latest_day: date
) -> dict[str, tuple[str, ...]]:
    query = (
        f'SELECT source_id, groupArrayDistinct(metric_name) AS metric_names '
        f'FROM {database}.{_ETF_DAILY_TABLE} '
        f"WHERE toDate(observed_at_utc) = toDate('{latest_day.isoformat()}') "
        f'AND source_id IN ({_source_id_list_sql()}) '
        f'GROUP BY source_id'
    )
    rows = client.query(query).result_rows
    required = set(_ETF_REQUIRED_METRICS)
    missing_by_source: dict[str, tuple[str, ...]] = {}
    for row in rows:
        if len(row) != 2:
            raise RuntimeError(
                f'Expected two columns from metric-coverage query, got {len(row)} columns'
            )
        source_id_raw = row[0]
        metric_names_raw = row[1]
        if not isinstance(source_id_raw, str) or source_id_raw.strip() == '':
            raise RuntimeError(f'metric-coverage query returned invalid source_id: {source_id_raw}')

        source_id = source_id_raw
        if metric_names_raw is None:
            observed_metrics: set[str] = set()
        elif isinstance(metric_names_raw, (list, tuple)):
            observed_metrics = set()
            metric_name_values = cast(list[Any] | tuple[Any, ...], metric_names_raw)
            for metric_name_raw in metric_name_values:
                if (
                    not isinstance(metric_name_raw, str)
                    or metric_name_raw.strip() == ''
                ):
                    raise RuntimeError(
                        f'metric-coverage query returned invalid metric_name: {metric_name_raw}'
                    )
                observed_metrics.add(metric_name_raw)
        else:
            raise RuntimeError(
                'metric-coverage query returned unsupported metric_names payload type: '
                f'{type(metric_names_raw)}'
            )

        missing_metrics = tuple(sorted(required.difference(observed_metrics)))
        if len(missing_metrics) > 0:
            missing_by_source[source_id] = missing_metrics
    return missing_by_source


def build_warnings_from_snapshot(
    *,
    snapshot: ETFQualitySnapshot,
    stale_max_age_days: int,
    today_utc: date | None = None,
) -> list[RawQueryWarning]:
    if stale_max_age_days <= 0:
        raise RuntimeError('stale_max_age_days must be > 0')

    today = today_utc if today_utc is not None else datetime.now(UTC).date()
    warnings: list[RawQueryWarning] = []

    latest_day = snapshot.latest_observed_day
    if latest_day is None:
        warnings.append(
            RawQueryWarning(
                code='ETF_DAILY_MISSING_RECORDS',
                message='No ETF daily records found for the expected issuer source set',
            )
        )
        return warnings

    age_days = (today - latest_day).days
    if age_days > stale_max_age_days:
        warnings.append(
            RawQueryWarning(
                code='ETF_DAILY_STALE_RECORDS',
                message=(
                    'ETF daily records are stale: '
                    f'latest_observed_day={latest_day.isoformat()}, '
                    f'age_days={age_days}, '
                    f'max_age_days={stale_max_age_days}'
                ),
            )
        )

    missing_sources = tuple(
        sorted(set(_ETF_EXPECTED_SOURCE_IDS).difference(snapshot.present_source_ids))
    )
    if len(missing_sources) > 0:
        warnings.append(
            RawQueryWarning(
                code='ETF_DAILY_MISSING_RECORDS',
                message=(
                    'ETF daily records are missing expected sources for latest_observed_day='
                    f'{latest_day.isoformat()}: missing_sources={list(missing_sources)}'
                ),
            )
        )

    if len(snapshot.missing_required_metrics_by_source) > 0:
        segments: list[str] = []
        for source_id in sorted(snapshot.missing_required_metrics_by_source):
            missing_metrics = list(snapshot.missing_required_metrics_by_source[source_id])
            segments.append(f'{source_id}:{missing_metrics}')
        warnings.append(
            RawQueryWarning(
                code='ETF_DAILY_INCOMPLETE_RECORDS',
                message=(
                    'ETF daily records are incomplete for latest_observed_day='
                    f'{latest_day.isoformat()}: missing_required_metrics='
                    f'{segments}'
                ),
            )
        )

    return warnings


def build_etf_daily_quality_warnings(
    auth_token: str | None, stale_max_age_days: int
) -> list[RawQueryWarning]:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
    try:
        latest_day = _load_latest_observed_day(client=client, database=settings.database)
        if latest_day is None:
            snapshot = ETFQualitySnapshot(
                latest_observed_day=None,
                present_source_ids=frozenset(),
                missing_required_metrics_by_source={},
            )
        else:
            snapshot = ETFQualitySnapshot(
                latest_observed_day=latest_day,
                present_source_ids=_load_present_sources_for_day(
                    client=client,
                    database=settings.database,
                    latest_day=latest_day,
                ),
                missing_required_metrics_by_source=_load_missing_required_metrics_by_source(
                    client=client,
                    database=settings.database,
                    latest_day=latest_day,
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
