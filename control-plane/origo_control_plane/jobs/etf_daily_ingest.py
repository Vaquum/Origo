import json
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import OpExecutionContext

from origo.scraper.contracts import ScrapeRunContext
from origo.scraper.etf_adapters import build_s4_03_issuer_adapters
from origo.scraper.pipeline import run_scraper_pipeline

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')


@op(name='origo_etf_daily_ingest_step')
def origo_etf_daily_ingest_step(context: OpExecutionContext) -> None:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )

    total_sources = 0
    total_parsed_records = 0
    total_normalized_records = 0
    total_inserted_rows = 0
    per_adapter_summary: list[dict[str, Any]] = []

    for adapter in build_s4_03_issuer_adapters():
        pipeline_result = run_scraper_pipeline(
            adapter=adapter,
            run_context=run_context,
        )
        total_sources += pipeline_result.total_sources
        total_parsed_records += pipeline_result.total_parsed_records
        total_normalized_records += pipeline_result.total_normalized_records
        total_inserted_rows += pipeline_result.total_inserted_rows
        per_adapter_summary.append(
            {
                'adapter_name': adapter.adapter_name,
                'total_sources': pipeline_result.total_sources,
                'total_parsed_records': pipeline_result.total_parsed_records,
                'total_normalized_records': pipeline_result.total_normalized_records,
                'total_inserted_rows': pipeline_result.total_inserted_rows,
                'source_ids': sorted(
                    {
                        source_result.source.source_id
                        for source_result in pipeline_result.source_results
                    }
                ),
            }
        )

    context.add_output_metadata(
        {
            'run_id': run_context.run_id,
            'started_at_utc': started_at_utc.isoformat(),
            'total_sources': total_sources,
            'total_parsed_records': total_parsed_records,
            'total_normalized_records': total_normalized_records,
            'total_inserted_rows': total_inserted_rows,
            'per_adapter_summary_json': json.dumps(
                per_adapter_summary,
                ensure_ascii=True,
                sort_keys=True,
            ),
        }
    )


@job(name='origo_etf_daily_ingest_job')
def origo_etf_daily_ingest_job() -> None:
    origo_etf_daily_ingest_step()
