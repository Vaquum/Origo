from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_connect import get_client

from origo.query.native_core import resolve_clickhouse_http_settings

from .clickhouse_staging import persist_normalized_records_to_clickhouse
from .contracts import ScrapeRunContext
from .etf_adapters import build_s4_03_issuer_adapters

_ETF_DAILY_TABLE = 'etf_daily_metrics_long'


def _utc_datetime_literal(value: datetime) -> str:
    utc_value = value.astimezone(UTC)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def run_s4_06_capability_proof() -> dict[str, Any]:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f's4-06-proof-{started_at_utc.strftime("%Y%m%dT%H%M%SZ")}',
        started_at_utc=started_at_utc,
    )

    source_results: list[dict[str, Any]] = []
    source_ids: set[str] = set()
    total_inserted_rows = 0

    for adapter in build_s4_03_issuer_adapters():
        sources = list(adapter.discover_sources(run_context=run_context))
        if len(sources) != 1:
            raise RuntimeError(
                f'{adapter.adapter_name} expected exactly one source, got {len(sources)}'
            )

        source = sources[0]
        artifact = adapter.fetch(source=source, run_context=run_context)
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
        inserted_rows = persist_normalized_records_to_clickhouse(
            records=normalized_records
        )

        as_of_dates = sorted(
            {
                str(record.metric_value)
                for record in normalized_records
                if record.metric_name == 'as_of_date'
            }
        )
        metric_names = sorted({record.metric_name for record in normalized_records})

        source_results.append(
            {
                'adapter_name': adapter.adapter_name,
                'source_id': source.source_id,
                'as_of_dates': as_of_dates,
                'metric_names': metric_names,
                'normalized_record_count': len(normalized_records),
                'inserted_row_count': inserted_rows,
            }
        )

        source_ids.add(source.source_id)
        total_inserted_rows += inserted_rows

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
        start_literal = _utc_datetime_literal(started_at_utc)
        quoted_sources = ','.join(f"'{source_id}'" for source_id in sorted(source_ids))

        count_sql = (
            f'SELECT count() FROM {settings.database}.{_ETF_DAILY_TABLE} '
            f"WHERE ingested_at_utc >= toDateTime64('{start_literal}', 3, 'UTC') "
            f'AND source_id IN ({quoted_sources})'
        )
        db_total_inserted_rows = int(client.query(count_sql).first_row[0])

        by_source_sql = (
            f'SELECT source_id, count() FROM {settings.database}.{_ETF_DAILY_TABLE} '
            f"WHERE ingested_at_utc >= toDateTime64('{start_literal}', 3, 'UTC') "
            f'AND source_id IN ({quoted_sources}) '
            'GROUP BY source_id ORDER BY source_id'
        )
        by_source_rows = client.query(by_source_sql).result_rows
    finally:
        client.close()

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 4 S4-C6 ETF canonical daily ClickHouse load',
        'run_id': run_context.run_id,
        'database': settings.database,
        'canonical_table': _ETF_DAILY_TABLE,
        'total_inserted_rows': total_inserted_rows,
        'db_total_inserted_rows': db_total_inserted_rows,
        'row_count_match': db_total_inserted_rows == total_inserted_rows,
        'db_rows_by_source': [
            {
                'source_id': str(row[0]),
                'row_count': int(row[1]),
            }
            for row in by_source_rows
        ],
        'source_results': source_results,
    }


def write_s4_06_capability_proof(*, output_path: Path) -> dict[str, Any]:
    proof = run_s4_06_capability_proof()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(proof, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    return proof


if __name__ == '__main__':
    default_path = Path(
        'spec/slices/slice-4-etf-use-case/capability-proof-s4-c6-canonical-load.json'
    )
    payload = write_s4_06_capability_proof(output_path=default_path)
    print(json.dumps(payload, indent=2, sort_keys=True))
