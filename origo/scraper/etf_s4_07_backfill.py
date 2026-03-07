from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, date, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from clickhouse_connect import get_client

from origo.query.native_core import resolve_clickhouse_http_settings

from .clickhouse_staging import persist_normalized_records_to_clickhouse
from .contracts import NormalizedMetricRecord, ScrapeRunContext, SourceDescriptor
from .etf_adapters import ISharesIBITAdapter, build_s4_03_issuer_adapters
from .retry import RetryPolicy, retry_with_backoff

_ETF_DAILY_TABLE = 'etf_daily_metrics_long'
_ISHARES_NO_DATA_PATTERN = re.compile(r'^Fund Holdings as of,\s*"-"', re.MULTILINE)
_ISHARES_NO_DATA_PARSE_ERRORS = {
    'iShares IBIT holdings CSV missing holdings table header',
    'iShares IBIT holdings CSV returned zero data rows',
    'iShares IBIT holdings CSV missing bitcoin row',
}
_BACKFILL_FETCH_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    initial_backoff_seconds=0.5,
    backoff_multiplier=2.0,
)


def _parse_iso_date(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid {label} date: {value}. Expected YYYY-MM-DD') from exc


def _iter_days_inclusive(*, start_day: date, end_day: date) -> list[date]:
    if start_day > end_day:
        raise RuntimeError(
            f'Backfill start_day must be <= end_day, got {start_day} > {end_day}'
        )
    days: list[date] = []
    current = start_day
    while current <= end_day:
        days.append(current)
        current += timedelta(days=1)
    return days


def _set_query_param(*, uri: str, key: str, value: str) -> str:
    parsed = urlparse(uri)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params[key] = value
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(params),
            parsed.fragment,
        )
    )


def _extract_as_of_date_from_normalized(
    *,
    records: list[NormalizedMetricRecord],
    adapter_name: str,
) -> str:
    as_of_values = sorted(
        {
            str(record.metric_value)
            for record in records
            if record.metric_name == 'as_of_date'
        }
    )
    if len(as_of_values) != 1:
        raise RuntimeError(
            f'{adapter_name} normalized records must contain exactly one as_of_date, '
            f'got {len(as_of_values)}'
        )
    return as_of_values[0]


def _load_existing_as_of_days(
    *,
    source_ids: list[str],
) -> dict[str, set[str]]:
    settings = resolve_clickhouse_http_settings()
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    try:
        existing: dict[str, set[str]] = {source_id: set() for source_id in source_ids}
        if len(source_ids) == 0:
            return existing

        quoted_sources = ','.join(f"'{source_id}'" for source_id in sorted(source_ids))
        sql = (
            f'SELECT source_id, metric_value_string '
            f'FROM {settings.database}.{_ETF_DAILY_TABLE} '
            "WHERE metric_name = 'as_of_date' "
            f'AND source_id IN ({quoted_sources})'
        )
        rows = client.query(sql).result_rows
        for source_id_raw, as_of_raw in rows:
            source_id = str(source_id_raw)
            if source_id not in existing:
                continue
            if as_of_raw is None:
                continue
            as_of_date = str(as_of_raw).strip()
            if as_of_date == '':
                continue
            existing[source_id].add(as_of_date)
        return existing
    finally:
        client.close()


def _load_source_coverage(*, source_ids: list[str]) -> list[dict[str, int | str | None]]:
    settings = resolve_clickhouse_http_settings()
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    try:
        if len(source_ids) == 0:
            return []

        quoted_sources = ','.join(f"'{source_id}'" for source_id in sorted(source_ids))
        sql = (
            f'SELECT source_id, '
            f'min(metric_value_string) AS earliest_as_of_date, '
            f'max(metric_value_string) AS latest_as_of_date, '
            f'uniqExact(metric_value_string) AS distinct_as_of_days, '
            f'count() AS total_as_of_rows '
            f'FROM {settings.database}.{_ETF_DAILY_TABLE} '
            "WHERE metric_name = 'as_of_date' "
            f'AND source_id IN ({quoted_sources}) '
            'GROUP BY source_id '
            'ORDER BY source_id'
        )
        rows = client.query(sql).result_rows
        return [
            {
                'source_id': str(source_id),
                'earliest_as_of_date': (
                    str(earliest_as_of_date)
                    if earliest_as_of_date is not None
                    else None
                ),
                'latest_as_of_date': (
                    str(latest_as_of_date) if latest_as_of_date is not None else None
                ),
                'distinct_as_of_days': int(distinct_as_of_days),
                'total_as_of_rows': int(total_as_of_rows),
            }
            for (
                source_id,
                earliest_as_of_date,
                latest_as_of_date,
                distinct_as_of_days,
                total_as_of_rows,
            ) in rows
        ]
    finally:
        client.close()


def _build_ishares_backfill_source(
    *,
    template_source: SourceDescriptor,
    request_day: date,
    run_context: ScrapeRunContext,
) -> SourceDescriptor:
    request_day_compact = request_day.strftime('%Y%m%d')
    source_uri = _set_query_param(
        uri=template_source.source_uri,
        key='asOfDate',
        value=request_day_compact,
    )
    metadata = dict(template_source.metadata)
    metadata['request_as_of_date'] = request_day.isoformat()
    metadata['request_as_of_date_compact'] = request_day_compact

    return SourceDescriptor(
        source_id=template_source.source_id,
        source_name=template_source.source_name,
        source_uri=source_uri,
        discovered_at_utc=run_context.started_at_utc,
        metadata=metadata,
    )


def _is_ishares_no_data_day(*, csv_text: str) -> bool:
    return _ISHARES_NO_DATA_PATTERN.search(csv_text) is not None


def run_s4_07_backfill(
    *,
    ishares_start_date: date,
    end_date: date,
) -> dict[str, object]:
    run_started_at = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f's4-07-backfill-{run_started_at.strftime("%Y%m%dT%H%M%SZ")}',
        started_at_utc=run_started_at,
    )

    adapters = list(build_s4_03_issuer_adapters())
    source_ids = sorted(
        {
            source.source_id
            for adapter in adapters
            for source in adapter.discover_sources(run_context=run_context)
        }
    )
    existing_days = _load_existing_as_of_days(source_ids=source_ids)

    per_source_results: list[dict[str, object]] = []

    ishares_adapter = ISharesIBITAdapter()
    ishares_template_source = next(
        iter(ishares_adapter.discover_sources(run_context=run_context))
    )
    ishares_calendar_days = _iter_days_inclusive(
        start_day=ishares_start_date,
        end_day=end_date,
    )
    ishares_days = [
        request_day
        for request_day in ishares_calendar_days
        if request_day.weekday() < 5
    ]

    ishares_inserted_days: list[str] = []
    ishares_skipped_existing = 0
    ishares_skipped_no_data = 0

    for request_day in ishares_days:
        source = _build_ishares_backfill_source(
            template_source=ishares_template_source,
            request_day=request_day,
            run_context=run_context,
        )
        artifact = retry_with_backoff(
            operation_name=f'{ishares_adapter.adapter_name}.fetch',
            operation=lambda: ishares_adapter.fetch(
                source=source,
                run_context=run_context,
            ),
            policy=_BACKFILL_FETCH_RETRY_POLICY,
        )

        csv_text = artifact.content.decode('utf-8-sig', errors='replace')
        if _is_ishares_no_data_day(csv_text=csv_text):
            ishares_skipped_no_data += 1
            continue

        try:
            parsed_records = list(
                ishares_adapter.parse(
                    artifact=artifact,
                    source=source,
                    run_context=run_context,
                )
            )
        except RuntimeError as exc:
            if str(exc) in _ISHARES_NO_DATA_PARSE_ERRORS:
                ishares_skipped_no_data += 1
                continue
            raise
        normalized_records = list(
            ishares_adapter.normalize(
                parsed_records=parsed_records,
                source=source,
                run_context=run_context,
            )
        )
        as_of_date = _extract_as_of_date_from_normalized(
            records=normalized_records,
            adapter_name=ishares_adapter.adapter_name,
        )

        ishares_existing = existing_days.setdefault(source.source_id, set())
        if as_of_date in ishares_existing:
            ishares_skipped_existing += 1
            continue

        persist_normalized_records_to_clickhouse(records=normalized_records)
        ishares_existing.add(as_of_date)
        ishares_inserted_days.append(as_of_date)

    per_source_results.append(
        {
            'adapter_name': ishares_adapter.adapter_name,
            'source_id': ishares_template_source.source_id,
            'mode': 'date_parameter_backfill',
            'requested_start_date': ishares_start_date.isoformat(),
            'requested_end_date': end_date.isoformat(),
            'requested_calendar_days': len(ishares_calendar_days),
            'requested_weekdays': len(ishares_days),
            'inserted_days_count': len(ishares_inserted_days),
            'inserted_days_first': min(ishares_inserted_days)
            if len(ishares_inserted_days) > 0
            else None,
            'inserted_days_last': max(ishares_inserted_days)
            if len(ishares_inserted_days) > 0
            else None,
            'skipped_existing_days': ishares_skipped_existing,
            'skipped_no_data_days': ishares_skipped_no_data,
        }
    )

    for adapter in adapters:
        if adapter.adapter_name == ishares_adapter.adapter_name:
            continue

        discovered_sources = list(adapter.discover_sources(run_context=run_context))
        if len(discovered_sources) != 1:
            raise RuntimeError(
                f'{adapter.adapter_name} expected exactly one source for S4-C7 snapshot '
                f'path, got {len(discovered_sources)}'
            )

        source = discovered_sources[0]
        artifact = retry_with_backoff(
            operation_name=f'{adapter.adapter_name}.fetch',
            operation=lambda: adapter.fetch(
                source=source,
                run_context=run_context,
            ),
            policy=_BACKFILL_FETCH_RETRY_POLICY,
        )
        parsed_records = list(
            adapter.parse(
                artifact=artifact,
                source=source,
                run_context=run_context,
            )
        )
        normalized_records = list(
            adapter.normalize(
                parsed_records=parsed_records,
                source=source,
                run_context=run_context,
            )
        )
        as_of_date = _extract_as_of_date_from_normalized(
            records=normalized_records,
            adapter_name=adapter.adapter_name,
        )

        source_existing = existing_days.setdefault(source.source_id, set())
        inserted_rows = 0
        skipped_existing_days = 0
        if as_of_date in source_existing:
            skipped_existing_days = 1
        else:
            inserted_rows = persist_normalized_records_to_clickhouse(
                records=normalized_records
            )
            source_existing.add(as_of_date)

        per_source_results.append(
            {
                'adapter_name': adapter.adapter_name,
                'source_id': source.source_id,
                'mode': 'snapshot_backfill',
                'as_of_date': as_of_date,
                'inserted_rows': inserted_rows,
                'skipped_existing_days': skipped_existing_days,
            }
        )

    coverage = _load_source_coverage(source_ids=source_ids)

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 4 S4-C7 full available-history backfill per issuer',
        'run_id': run_context.run_id,
        'ishares_backfill_start_date': ishares_start_date.isoformat(),
        'backfill_end_date': end_date.isoformat(),
        'per_source_results': per_source_results,
        'source_coverage': coverage,
    }


def write_s4_07_backfill_proof(
    *,
    output_path: str,
    ishares_start_date: date,
    end_date: date,
) -> dict[str, object]:
    payload = run_s4_07_backfill(
        ishares_start_date=ishares_start_date,
        end_date=end_date,
    )
    output_file = json.dumps(payload, indent=2, sort_keys=True) + '\n'
    with open(output_path, 'w', encoding='utf-8') as handle:
        handle.write(output_file)
    return payload


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m origo.scraper.etf_s4_07_backfill',
        description='Run S4-C7 ETF full available-history backfill.',
    )
    parser.add_argument(
        '--ishares-start-date',
        default='2024-01-01',
        help='Start date for iShares asOfDate backfill (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--end-date',
        default=date.today().isoformat(),
        help='Inclusive backfill end date (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--output-path',
        default='spec/slices/slice-4-etf-use-case/capability-proof-s4-c7-backfill.json',
        help='Path for backfill proof artifact JSON.',
    )
    return parser


def main() -> None:
    args = _build_argument_parser().parse_args()
    ishares_start_date = _parse_iso_date(
        args.ishares_start_date,
        label='--ishares-start-date',
    )
    end_date = _parse_iso_date(args.end_date, label='--end-date')
    payload = write_s4_07_backfill_proof(
        output_path=args.output_path,
        ishares_start_date=ishares_start_date,
        end_date=end_date,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
