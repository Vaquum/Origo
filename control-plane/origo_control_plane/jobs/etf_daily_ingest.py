import json
import sys
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from clickhouse_driver import Client as ClickhouseClient
from dagster import OpExecutionContext

from origo.scraper.contracts import ScrapeRunContext
from origo.scraper.etf_adapters import build_s4_03_issuer_adapters
from origo.scraper.pipeline import run_scraper_pipeline
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.etf_aligned_projector import (
    project_etf_daily_metrics_aligned,
)
from origo_control_plane.utils.etf_native_projector import (
    project_etf_daily_metrics_native,
)

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')


@op(name='origo_etf_daily_ingest_step')
def origo_etf_daily_ingest_step(context: OpExecutionContext) -> None:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )
    native_settings = resolve_clickhouse_native_settings()
    native_client: ClickhouseClient | None = None

    total_sources = 0
    total_parsed_records = 0
    total_normalized_records = 0
    total_inserted_rows = 0
    total_native_projected_rows = 0
    total_aligned_projected_rows = 0
    per_adapter_summary: list[dict[str, Any]] = []

    try:
        native_client = ClickhouseClient(
            host=native_settings.host,
            port=native_settings.port,
            user=native_settings.user,
            password=native_settings.password,
            database=native_settings.database,
            compression=True,
            send_receive_timeout=900,
        )
        for adapter in build_s4_03_issuer_adapters():
            pipeline_result = run_scraper_pipeline(
                adapter=adapter,
                run_context=run_context,
            )
            total_sources += pipeline_result.total_sources
            total_parsed_records += pipeline_result.total_parsed_records
            total_normalized_records += pipeline_result.total_normalized_records
            total_inserted_rows += pipeline_result.total_inserted_rows

            partition_ids = sorted(
                {
                    normalized_record.observed_at_utc.astimezone(UTC).date().isoformat()
                    for source_result in pipeline_result.source_results
                    for normalized_record in source_result.normalized_records
                }
            )
            if partition_ids == []:
                raise RuntimeError(
                    f'ETF pipeline adapter={adapter.adapter_name} produced zero partition ids'
                )

            native_projection_summary = project_etf_daily_metrics_native(
                client=native_client,
                database=native_settings.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )
            aligned_projection_summary = project_etf_daily_metrics_aligned(
                client=native_client,
                database=native_settings.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )
            total_native_projected_rows += native_projection_summary.rows_written
            total_aligned_projected_rows += aligned_projection_summary.rows_written
            per_adapter_summary.append(
                {
                    'adapter_name': adapter.adapter_name,
                    'total_sources': pipeline_result.total_sources,
                    'total_parsed_records': pipeline_result.total_parsed_records,
                    'total_normalized_records': pipeline_result.total_normalized_records,
                    'total_inserted_rows': pipeline_result.total_inserted_rows,
                    'native_projection_summary': native_projection_summary.to_dict(),
                    'aligned_projection_summary': aligned_projection_summary.to_dict(),
                    'source_ids': sorted(
                        {
                            source_result.source.source_id
                            for source_result in pipeline_result.source_results
                        }
                    ),
                    'partition_ids': partition_ids,
                }
            )
    finally:
        if native_client is not None:
            try:
                native_client.disconnect()
            except Exception as exc:
                active_exception = sys.exc_info()[1]
                if active_exception is not None:
                    active_exception.add_note(
                        f'ClickHouse disconnect failed during cleanup: {exc}'
                    )
                    context.log.warning(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    )
                else:
                    raise RuntimeError(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    ) from exc

    context.add_output_metadata(
        {
            'run_id': run_context.run_id,
            'started_at_utc': started_at_utc.isoformat(),
            'total_sources': total_sources,
            'total_parsed_records': total_parsed_records,
            'total_normalized_records': total_normalized_records,
            'total_inserted_rows': total_inserted_rows,
            'total_native_projected_rows': total_native_projected_rows,
            'total_aligned_projected_rows': total_aligned_projected_rows,
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
