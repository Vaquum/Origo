from __future__ import annotations

# pyright: reportUnknownParameterType=false, reportMissingParameterType=false
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import dagster as dg
from clickhouse_driver import Client as ClickhouseClient
from dagster import OpExecutionContext

from origo.events import (
    CanonicalBackfillStateStore,
    CanonicalStreamKey,
    SourceIdentityMaterial,
    build_partition_source_proof,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo.events.backfill_state import canonical_proof_matches_source_proof
from origo.events.errors import ReconciliationError
from origo.scraper.contracts import NormalizedMetricRecord, ScrapeRunContext
from origo.scraper.etf_adapters import build_s4_03_issuer_adapters
from origo.scraper.etf_canonical_event_ingest import (
    build_etf_canonical_payload,
    write_etf_normalized_records_to_canonical,
)
from origo.scraper.pipeline import PipelineSourceResult, run_scraper_pipeline
from origo_control_plane.backfill import apply_runtime_audit_mode_or_raise
from origo_control_plane.backfill.runtime_contract import (
    BackfillRuntimeContract,
    load_backfill_runtime_contract_from_tags_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.etf_aligned_projector import (
    project_etf_daily_metrics_aligned,
)
from origo_control_plane.utils.etf_native_projector import (
    project_etf_daily_metrics_native,
)

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')

_CANONICAL_SOURCE_ID = 'etf'
_CANONICAL_STREAM_ID = 'etf_daily_metrics'
_PAYLOAD_ENCODING = 'utf-8'


@dataclass(frozen=True)
class _PartitionSourceBatch:
    source_result: PipelineSourceResult
    records: list[NormalizedMetricRecord]


@dataclass(frozen=True)
class _PartitionBackfillResult:
    partition_id: str
    rows_processed: int
    rows_inserted: int
    rows_duplicate: int
    write_path: str
    partition_proof_state: str
    partition_proof_digest_sha256: str
    native_projection_summary: dict[str, int]
    aligned_projection_summary: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            'partition_id': self.partition_id,
            'rows_processed': self.rows_processed,
            'rows_inserted': self.rows_inserted,
            'rows_duplicate': self.rows_duplicate,
            'write_path': self.write_path,
            'partition_proof_state': self.partition_proof_state,
            'partition_proof_digest_sha256': self.partition_proof_digest_sha256,
            'native_projection_summary': self.native_projection_summary,
            'aligned_projection_summary': self.aligned_projection_summary,
        }


@dataclass(frozen=True)
class _BackfillRunSummary:
    total_sources: int
    total_parsed_records: int
    total_normalized_records: int
    total_inserted_rows: int
    total_native_projected_rows: int
    total_aligned_projected_rows: int
    partition_results: list[_PartitionBackfillResult]


def _build_clickhouse_client_or_raise() -> tuple[ClickhouseClient, str]:
    native_settings = resolve_clickhouse_native_settings()
    return (
        ClickhouseClient(
            host=native_settings.host,
            port=native_settings.port,
            user=native_settings.user,
            password=native_settings.password,
            database=native_settings.database,
            compression=True,
            send_receive_timeout=900,
        ),
        native_settings.database,
    )


def _partition_id_for_record_or_raise(record: NormalizedMetricRecord) -> str:
    return record.observed_at_utc.astimezone(UTC).date().isoformat()


def _build_legacy_etf_daily_ingest_summary(
    *,
    context: OpExecutionContext,
) -> dict[str, Any]:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )
    native_client, database = _build_clickhouse_client_or_raise()

    total_sources = 0
    total_parsed_records = 0
    total_normalized_records = 0
    total_inserted_rows = 0
    total_native_projected_rows = 0
    total_aligned_projected_rows = 0
    per_adapter_summary: list[dict[str, Any]] = []

    try:
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
                    _partition_id_for_record_or_raise(normalized_record)
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
                database=database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )
            aligned_projection_summary = project_etf_daily_metrics_aligned(
                client=native_client,
                database=database,
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
        _disconnect_clickhouse_client_or_raise(
            client=native_client,
            context=context,
        )

    return {
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


def _disconnect_clickhouse_client_or_raise(
    *,
    client: ClickhouseClient,
    context: OpExecutionContext,
) -> None:
    try:
        client.disconnect()
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


def _build_partition_batches_or_raise(
    source_results: list[PipelineSourceResult],
) -> dict[str, list[_PartitionSourceBatch]]:
    grouped: dict[str, list[_PartitionSourceBatch]] = {}
    for source_result in source_results:
        records_by_partition: dict[str, list[NormalizedMetricRecord]] = {}
        for record in source_result.normalized_records:
            partition_id = _partition_id_for_record_or_raise(record)
            records_by_partition.setdefault(partition_id, []).append(record)
        for partition_id, partition_records in records_by_partition.items():
            grouped.setdefault(partition_id, []).append(
                _PartitionSourceBatch(
                    source_result=source_result,
                    records=sorted(
                        partition_records,
                        key=lambda item: item.metric_id,
                    ),
                )
            )
    if grouped == {}:
        raise RuntimeError('ETF backfill prepared zero partition batches')
    return grouped


def _canonical_payload_sha256(record: NormalizedMetricRecord) -> str:
    payload_raw = json.dumps(
        build_etf_canonical_payload(record=record),
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode(_PAYLOAD_ENCODING)
    return hashlib.sha256(payload_raw).hexdigest()


def _build_etf_partition_source_proof_or_raise(
    *,
    partition_id: str,
    batches: list[_PartitionSourceBatch],
) -> Any:
    if batches == []:
        raise RuntimeError(
            f'ETF source proof requires non-empty batches for partition_id={partition_id}'
        )
    artifacts: list[dict[str, Any]] = []
    materials: list[SourceIdentityMaterial] = []
    for batch in sorted(
        batches,
        key=lambda item: (
            item.source_result.source.source_id,
            item.source_result.artifact.artifact_id,
        ),
    ):
        source_result = batch.source_result
        artifacts.append(
            {
                'source_id': source_result.source.source_id,
                'source_uri': source_result.source.source_uri,
                'source_name': source_result.source.source_name,
                'artifact_id': source_result.artifact.artifact_id,
                'artifact_sha256': source_result.artifact.content_sha256,
                'artifact_format': source_result.artifact.artifact_format,
                'fetch_method': source_result.artifact.fetch_method,
                'storage_uri': source_result.persisted_artifact.storage_uri,
                'manifest_uri': source_result.persisted_artifact.manifest_uri,
                'fetched_at_utc': source_result.artifact.fetched_at_utc.isoformat(),
                'persisted_at_utc': source_result.persisted_artifact.persisted_at_utc.isoformat(),
                'record_count': len(batch.records),
            }
        )
        for record in batch.records:
            idempotency_key = canonical_event_idempotency_key(
                source_id=_CANONICAL_SOURCE_ID,
                stream_id=_CANONICAL_STREAM_ID,
                partition_id=partition_id,
                source_offset_or_equivalent=record.metric_id,
            )
            materials.append(
                SourceIdentityMaterial(
                    source_offset_or_equivalent=record.metric_id,
                    event_id=str(canonical_event_id_from_key(idempotency_key)),
                    payload_sha256_raw=_canonical_payload_sha256(record),
                )
            )
    return build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id=_CANONICAL_SOURCE_ID,
            stream_id=_CANONICAL_STREAM_ID,
            partition_id=partition_id,
        ),
        offset_ordering='lexicographic',
        source_artifact_identity={
            'partition_id': partition_id,
            'artifact_count': len(artifacts),
            'artifacts': artifacts,
        },
        materials=materials,
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
    )


def _execute_etf_partition_backfill_or_raise(
    *,
    context: OpExecutionContext,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    batches: list[_PartitionSourceBatch],
    runtime_contract: BackfillRuntimeContract,
) -> _PartitionBackfillResult:
    records = [record for batch in batches for record in batch.records]
    if records == []:
        raise RuntimeError(
            f'ETF partition batch cannot be empty for partition_id={partition_id}'
        )

    source_proof = _build_etf_partition_source_proof_or_raise(
        partition_id=partition_id,
        batches=batches,
    )
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    try:
        state_store.assert_partition_can_execute_or_raise(
            stream_key=source_proof.stream_key,
            execution_mode=runtime_contract.execution_mode,
        )
    except ReconciliationError as exc:
        if runtime_contract.execution_mode == 'backfill' and exc.code == 'RECONCILE_REQUIRED':
            state_store.record_partition_state(
                source_proof=source_proof,
                state='reconcile_required',
                reason='backfill_execution_requires_reconcile',
                run_id=context.run_id,
                recorded_at_utc=datetime.now(UTC),
                proof_details={'trigger_message': exc.message},
            )
        raise

    execution_assessment = state_store.assess_partition_execution(
        stream_key=source_proof.stream_key
    )
    reconcile_existing_canonical_rows = (
        runtime_contract.execution_mode == 'reconcile'
        and execution_assessment.canonical_row_count > 0
    )

    source_manifested_at_utc = datetime.now(UTC)
    state_store.record_source_manifest(
        source_proof=source_proof,
        run_id=context.run_id,
        manifested_at_utc=source_manifested_at_utc,
    )
    state_store.record_partition_state(
        source_proof=source_proof,
        state='source_manifested',
        reason='source_manifest_recorded',
        run_id=context.run_id,
        recorded_at_utc=source_manifested_at_utc,
    )

    current_canonical_matches_source = False
    current_canonical_proof = None
    if reconcile_existing_canonical_rows:
        current_canonical_proof = state_store.compute_canonical_partition_proof_or_raise(
            source_proof=source_proof
        )
        current_canonical_matches_source = canonical_proof_matches_source_proof(
            source_proof=source_proof,
            canonical_proof=current_canonical_proof,
        )

    if reconcile_existing_canonical_rows and current_canonical_matches_source:
        write_summary = {
            'rows_processed': source_proof.source_row_count,
            'rows_inserted': 0,
            'rows_duplicate': source_proof.source_row_count,
        }
        write_path = 'reconcile_proof_only'
        proof_reason = 'reconcile_existing_canonical_rows_detected'
    else:
        write_summary = write_etf_normalized_records_to_canonical(
            client=client,
            database=database,
            records=records,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        ).to_dict()
        if reconcile_existing_canonical_rows:
            write_path = 'reconcile_writer_repair'
            proof_reason = 'reconcile_writer_repair_completed'
        else:
            write_path = 'writer'
            proof_reason = 'canonical_write_completed'

    rows_processed = int(write_summary['rows_processed'])
    rows_inserted = int(write_summary['rows_inserted'])
    rows_duplicate = int(write_summary['rows_duplicate'])
    if rows_processed != len(records):
        raise RuntimeError(
            'ETF canonical writer summary mismatch: '
            f'rows_processed={rows_processed} expected={len(records)} '
            f'partition_id={partition_id}'
        )
    if rows_inserted + rows_duplicate != rows_processed:
        raise RuntimeError(
            'ETF canonical writer summary mismatch: '
            f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
            f'rows_processed={rows_processed} partition_id={partition_id}'
        )

    proof_recorded_at_utc = datetime.now(UTC)
    state_store.record_partition_state(
        source_proof=source_proof,
        state='canonical_written_unproved',
        reason=proof_reason,
        run_id=context.run_id,
        recorded_at_utc=proof_recorded_at_utc,
    )
    proof_input = current_canonical_proof if write_path == 'reconcile_proof_only' else None
    partition_proof = state_store.prove_partition_or_quarantine(
        source_proof=source_proof,
        run_id=context.run_id,
        recorded_at_utc=proof_recorded_at_utc,
        canonical_proof=proof_input,
    )

    projected_at_utc = datetime.now(UTC)
    if runtime_contract.projection_mode == 'inline':
        native_projection_summary = project_etf_daily_metrics_native(
            client=client,
            database=database,
            partition_ids=[partition_id],
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        ).to_dict()
        aligned_projection_summary = project_etf_daily_metrics_aligned(
            client=client,
            database=database,
            partition_ids=[partition_id],
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        ).to_dict()
    else:
        native_projection_summary = {
            'partitions_processed': 0,
            'batches_processed': 0,
            'events_processed': 0,
            'rows_written': 0,
        }
        aligned_projection_summary = {
            'partitions_processed': 0,
            'policies_recorded': 0,
            'policies_duplicate': 0,
            'batches_processed': 0,
            'events_processed': 0,
            'rows_written': 0,
        }

    return _PartitionBackfillResult(
        partition_id=partition_id,
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        rows_duplicate=rows_duplicate,
        write_path=write_path,
        partition_proof_state=partition_proof.state,
        partition_proof_digest_sha256=partition_proof.proof_digest_sha256,
        native_projection_summary=native_projection_summary,
        aligned_projection_summary=aligned_projection_summary,
    )


def _run_etf_backfill_or_raise(*, context: OpExecutionContext) -> _BackfillRunSummary:
    runtime_contract = load_backfill_runtime_contract_from_tags_or_raise(
        context.run.tags,
    )
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )

    total_sources = 0
    total_parsed_records = 0
    total_normalized_records = 0
    collected_source_results: list[PipelineSourceResult] = []
    for adapter in build_s4_03_issuer_adapters():
        pipeline_result = run_scraper_pipeline(
            adapter=adapter,
            run_context=run_context,
            persist_normalized_records=False,
        )
        total_sources += pipeline_result.total_sources
        total_parsed_records += pipeline_result.total_parsed_records
        total_normalized_records += pipeline_result.total_normalized_records
        collected_source_results.extend(pipeline_result.source_results)

    partition_batches = _build_partition_batches_or_raise(collected_source_results)
    native_client, database = _build_clickhouse_client_or_raise()
    try:
        partition_results = [
            _execute_etf_partition_backfill_or_raise(
                context=context,
                client=native_client,
                database=database,
                partition_id=partition_id,
                batches=partition_batches[partition_id],
                runtime_contract=runtime_contract,
            )
            for partition_id in sorted(partition_batches)
        ]
    finally:
        _disconnect_clickhouse_client_or_raise(
            client=native_client,
            context=context,
        )

    return _BackfillRunSummary(
        total_sources=total_sources,
        total_parsed_records=total_parsed_records,
        total_normalized_records=total_normalized_records,
        total_inserted_rows=sum(result.rows_inserted for result in partition_results),
        total_native_projected_rows=sum(
            result.native_projection_summary['rows_written']
            for result in partition_results
        ),
        total_aligned_projected_rows=sum(
            result.aligned_projection_summary['rows_written']
            for result in partition_results
        ),
        partition_results=partition_results,
    )


@op(name='origo_etf_daily_ingest_step')
def origo_etf_daily_ingest_step(context) -> None:
    _run_etf_daily_ingest_step_or_raise(context=cast(OpExecutionContext, context))


def _run_etf_daily_ingest_step_or_raise(*, context: OpExecutionContext) -> None:
    context.add_output_metadata(_build_legacy_etf_daily_ingest_summary(context=context))


@op(name='origo_etf_daily_backfill_step')
def origo_etf_daily_backfill_step(context) -> None:
    _run_etf_daily_backfill_step_or_raise(context=cast(OpExecutionContext, context))


def _run_etf_daily_backfill_step_or_raise(*, context: OpExecutionContext) -> None:
    started_at_utc = datetime.now(UTC)
    summary = _run_etf_backfill_or_raise(context=context)
    context.add_output_metadata(
        {
            'run_id': f'etf-daily-{context.run_id}',
            'started_at_utc': started_at_utc.isoformat(),
            'total_sources': summary.total_sources,
            'total_parsed_records': summary.total_parsed_records,
            'total_normalized_records': summary.total_normalized_records,
            'total_inserted_rows': summary.total_inserted_rows,
            'total_native_projected_rows': summary.total_native_projected_rows,
            'total_aligned_projected_rows': summary.total_aligned_projected_rows,
            'partition_results_json': json.dumps(
                [result.to_dict() for result in summary.partition_results],
                ensure_ascii=True,
                sort_keys=True,
            ),
        }
    )


@job(name='origo_etf_daily_ingest_job')
def origo_etf_daily_ingest_job() -> None:
    origo_etf_daily_ingest_step()


@job(name='origo_etf_daily_backfill_job')
def origo_etf_daily_backfill_job() -> None:
    origo_etf_daily_backfill_step()
