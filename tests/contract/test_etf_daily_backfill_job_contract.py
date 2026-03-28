from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import origo_control_plane.jobs.etf_daily_ingest as etf_job
import pytest
from origo_control_plane.backfill.runtime_contract import BackfillRuntimeContract

from origo.scraper.contracts import (
    NormalizedMetricRecord,
    ParsedRecord,
    PersistedRawArtifact,
    ProvenanceMetadata,
    RawArtifact,
    ScrapeRunContext,
    SourceDescriptor,
)
from origo.scraper.pipeline import PipelineRunResult, PipelineSourceResult


class _FakeLog:
    def warning(self, _message: str) -> None:
        return None


class _FakeContext:
    def __init__(self, *, run_id: str, tags: dict[str, str]) -> None:
        self.run_id = run_id
        self.run = SimpleNamespace(tags=tags)
        self.log = _FakeLog()


def _build_source_result(*, partition_id: str) -> PipelineSourceResult:
    observed_at_utc = datetime.fromisoformat(f'{partition_id}T00:00:00+00:00')
    source = SourceDescriptor(
        source_id='etf_ishares_ibit_daily',
        source_name='iShares IBIT',
        source_uri='https://example.com/ibit.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
    )
    artifact = RawArtifact(
        artifact_id='artifact-1',
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-sha',
        content=b'csv',
    )
    parsed_record = ParsedRecord(
        record_id='record-1',
        artifact_id=artifact.artifact_id,
        payload={'as_of_date': partition_id, 'btc_units': 1.25},
        parser_name='parser',
        parser_version='1.0.0',
        parsed_at_utc=observed_at_utc,
    )
    provenance = ProvenanceMetadata(
        source_id=source.source_id,
        source_uri=source.source_uri,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.content_sha256,
        fetch_method='http',
        parser_name='parser',
        parser_version='1.0.0',
        fetched_at_utc=observed_at_utc,
        parsed_at_utc=observed_at_utc,
        normalized_at_utc=observed_at_utc,
    )
    normalized_record = NormalizedMetricRecord(
        metric_id='metric-1',
        source_id=source.source_id,
        metric_name='btc_units',
        metric_value=1.25,
        metric_unit='BTC',
        observed_at_utc=observed_at_utc,
        dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
        provenance=provenance,
    )
    return PipelineSourceResult(
        source=source,
        artifact=artifact,
        persisted_artifact=PersistedRawArtifact(
            artifact_id=artifact.artifact_id,
            storage_uri='s3://bucket/raw/artifact.csv',
            manifest_uri='s3://bucket/raw/manifest.json',
            persisted_at_utc=observed_at_utc,
        ),
        parsed_records=[parsed_record],
        normalized_records=[normalized_record],
        inserted_row_count=0,
    )


def test_run_etf_backfill_uses_deferred_pipeline_and_skips_projection(
    monkeypatch: Any,
) -> None:
    def _unexpected_run_scraper_pipeline(**_: Any) -> PipelineRunResult:
        raise AssertionError('live scraper pipeline must not run in ETF archive backfill mode')

    monkeypatch.setattr(
        etf_job,
        'run_scraper_pipeline',
        _unexpected_run_scraper_pipeline,
    )

    class _FakeClient:
        def disconnect(self) -> None:
            return None

    monkeypatch.setattr(
        etf_job,
        '_build_clickhouse_client_or_raise',
        lambda: (_FakeClient(), 'origo'),
    )
    monkeypatch.setattr(
        etf_job,
        '_load_existing_etf_partition_ids_or_raise',
        lambda **_: ('2026-03-26',),
    )
    monkeypatch.setattr(
        etf_job,
        '_load_etf_archived_source_results_or_raise',
        lambda **_: [_build_source_result(partition_id='2026-03-26')],
    )

    class _FakeStateStore:
        def __init__(self, **_: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_: Any) -> None:
            return None

        def assess_partition_execution(self, **_: Any) -> Any:
            return SimpleNamespace(
                latest_proof_state=None,
                canonical_row_count=0,
                active_quarantine=False,
            )

        def record_source_manifest(self, **_: Any) -> None:
            return None

        def record_partition_state(self, **_: Any) -> None:
            return None

        def prove_partition_or_quarantine(self, **_: Any) -> Any:
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='proof-sha',
            )

    monkeypatch.setattr(etf_job, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        etf_job,
        'write_etf_normalized_records_to_canonical',
        lambda **kwargs: SimpleNamespace(
            to_dict=lambda: {
                'rows_processed': len(kwargs['records']),
                'rows_inserted': len(kwargs['records']),
                'rows_duplicate': 0,
            }
        ),
    )

    native_called = False
    aligned_called = False

    def _native(**_: Any) -> Any:
        nonlocal native_called
        native_called = True
        return SimpleNamespace(to_dict=lambda: {'rows_written': 0})

    def _aligned(**_: Any) -> Any:
        nonlocal aligned_called
        aligned_called = True
        return SimpleNamespace(to_dict=lambda: {'rows_written': 0})

    monkeypatch.setattr(etf_job, 'project_etf_daily_metrics_native', _native)
    monkeypatch.setattr(etf_job, 'project_etf_daily_metrics_aligned', _aligned)

    summary = etf_job._run_etf_backfill_or_raise(
        context=_FakeContext(
            run_id='run-1',
            tags={
                'origo.backfill.projection_mode': 'deferred',
                'origo.backfill.execution_mode': 'backfill',
                'origo.backfill.runtime_audit_mode': 'summary',
            },
        )
    )

    assert summary.total_sources == 1
    assert summary.total_parsed_records == 1
    assert summary.total_normalized_records == 1
    assert summary.total_inserted_rows == 1
    assert summary.total_native_projected_rows == 0
    assert summary.total_aligned_projected_rows == 0
    assert native_called is False
    assert aligned_called is False
    assert [item.partition_proof_state for item in summary.partition_results] == [
        'proved_complete'
    ]


def test_execute_etf_partition_backfill_projects_only_after_proof(
    monkeypatch: Any,
) -> None:
    order: list[str] = []

    class _FakeStateStore:
        def __init__(self, **_: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_: Any) -> None:
            order.append('assert')

        def assess_partition_execution(self, **_: Any) -> Any:
            order.append('assess')
            return SimpleNamespace(
                latest_proof_state=None,
                canonical_row_count=0,
                active_quarantine=False,
            )

        def record_source_manifest(self, **_: Any) -> None:
            order.append('manifest')

        def record_partition_state(self, **kwargs: Any) -> None:
            order.append(f"state:{kwargs['state']}")

        def prove_partition_or_quarantine(self, **_: Any) -> Any:
            order.append('prove')
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='proof-sha',
            )

    monkeypatch.setattr(etf_job, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        etf_job,
        'write_etf_normalized_records_to_canonical',
        lambda **kwargs: SimpleNamespace(
            to_dict=lambda: {
                'rows_processed': len(kwargs['records']),
                'rows_inserted': len(kwargs['records']),
                'rows_duplicate': 0,
            }
        ),
    )

    def _native(**_: Any) -> Any:
        order.append('project_native')
        return SimpleNamespace(
            to_dict=lambda: {
                'partitions_processed': 1,
                'batches_processed': 1,
                'events_processed': 1,
                'rows_written': 1,
            }
        )

    def _aligned(**_: Any) -> Any:
        order.append('project_aligned')
        return SimpleNamespace(
            to_dict=lambda: {
                'partitions_processed': 1,
                'policies_recorded': 1,
                'policies_duplicate': 0,
                'batches_processed': 1,
                'events_processed': 1,
                'rows_written': 1,
            }
        )

    monkeypatch.setattr(etf_job, 'project_etf_daily_metrics_native', _native)
    monkeypatch.setattr(etf_job, 'project_etf_daily_metrics_aligned', _aligned)

    source_result = _build_source_result(partition_id='2026-03-26')
    batches = etf_job._build_partition_batches_or_raise([source_result])['2026-03-26']

    result = etf_job._execute_etf_partition_backfill_or_raise(
        context=_FakeContext(
            run_id='run-2',
            tags={},
        ),
        client=SimpleNamespace(),
        database='origo',
        partition_id='2026-03-26',
        batches=batches,
        runtime_contract=BackfillRuntimeContract(
            projection_mode='inline',
            execution_mode='backfill',
            runtime_audit_mode='summary',
        ),
    )

    assert result.partition_proof_state == 'proved_complete'
    assert order == [
        'assert',
        'assess',
        'manifest',
        'state:source_manifested',
        'state:canonical_written_unproved',
        'prove',
        'project_native',
        'project_aligned',
    ]


def test_load_etf_archived_source_results_fails_on_missing_partition_coverage(
    monkeypatch: Any,
) -> None:
    observed_at_utc = datetime(2026, 3, 26, 0, 0, tzinfo=UTC)
    source = SourceDescriptor(
        source_id='etf_ishares_ibit_daily',
        source_name='iShares IBIT',
        source_uri='https://example.com/ibit.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
    )
    artifact = RawArtifact(
        artifact_id='artifact-1',
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-sha',
        content=b'csv',
    )
    persisted = PersistedRawArtifact(
        artifact_id='artifact-1',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
    )
    parsed_record = ParsedRecord(
        record_id='record-1',
        artifact_id='artifact-1',
        payload={'as_of_date': '2026-03-26', 'btc_units': 1.25},
        parser_name='parser',
        parser_version='1.0.0',
        parsed_at_utc=observed_at_utc,
    )
    provenance = ProvenanceMetadata(
        source_id=source.source_id,
        source_uri=source.source_uri,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.content_sha256,
        fetch_method='http',
        parser_name='parser',
        parser_version='1.0.0',
        fetched_at_utc=observed_at_utc,
        parsed_at_utc=observed_at_utc,
        normalized_at_utc=observed_at_utc,
    )
    normalized_record = NormalizedMetricRecord(
        metric_id='metric-1',
        source_id=source.source_id,
        metric_name='btc_units',
        metric_value=1.25,
        metric_unit='BTC',
        observed_at_utc=observed_at_utc,
        dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
        provenance=provenance,
    )

    class _FakeAdapter:
        adapter_name = 'fake_etf'

        def parse(
            self,
            *,
            artifact: RawArtifact,
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[ParsedRecord]:
            del artifact, source, run_context
            return [parsed_record]

        def normalize(
            self,
            *,
            parsed_records: list[ParsedRecord],
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[NormalizedMetricRecord]:
            del parsed_records, source, run_context
            return [normalized_record]

    monkeypatch.setattr(
        etf_job,
        '_build_etf_adapter_source_index_or_raise',
        lambda **_: {source.source_id: (_FakeAdapter(), source)},
    )
    monkeypatch.setattr(
        etf_job,
        '_build_object_store_client_and_bucket_or_raise',
        lambda: ('client', 'bucket'),
    )
    monkeypatch.setattr(
        etf_job,
        '_load_all_raw_artifact_manifest_payloads_or_raise',
        lambda **_: [
            {
                'source_id': source.source_id,
                'artifact_id': artifact.artifact_id,
                'manifest_uri': persisted.manifest_uri,
                'fetched_at_utc': observed_at_utc.isoformat(),
            }
        ],
    )
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **_: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=artifact,
            persisted_artifact=persisted,
        ),
    )

    with pytest.raises(RuntimeError, match='ETF historical archive coverage is incomplete'):
        etf_job._load_etf_archived_source_results_or_raise(
            run_context=ScrapeRunContext(
                run_id='etf-daily-run',
                started_at_utc=observed_at_utc,
            ),
            required_partition_ids=('2026-03-25', '2026-03-26'),
        )


def test_load_etf_archived_source_results_deduplicates_exact_duplicate_artifacts(
    monkeypatch: Any,
) -> None:
    observed_at_utc = datetime(2026, 3, 26, 0, 0, tzinfo=UTC)
    source = SourceDescriptor(
        source_id='etf_ishares_ibit_daily',
        source_name='iShares IBIT',
        source_uri='https://example.com/ibit.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
    )
    artifact = RawArtifact(
        artifact_id='artifact-1',
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-sha',
        content=b'csv',
    )
    persisted = PersistedRawArtifact(
        artifact_id='artifact-1',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
    )
    parsed_record = ParsedRecord(
        record_id='record-1',
        artifact_id='artifact-1',
        payload={'as_of_date': '2026-03-26', 'btc_units': 1.25},
        parser_name='parser',
        parser_version='1.0.0',
        parsed_at_utc=observed_at_utc,
    )
    provenance = ProvenanceMetadata(
        source_id=source.source_id,
        source_uri=source.source_uri,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.content_sha256,
        fetch_method='http',
        parser_name='parser',
        parser_version='1.0.0',
        fetched_at_utc=observed_at_utc,
        parsed_at_utc=observed_at_utc,
        normalized_at_utc=observed_at_utc,
    )
    normalized_record = NormalizedMetricRecord(
        metric_id='metric-1',
        source_id=source.source_id,
        metric_name='btc_units',
        metric_value=1.25,
        metric_unit='BTC',
        observed_at_utc=observed_at_utc,
        dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
        provenance=provenance,
    )

    class _FakeAdapter:
        adapter_name = 'fake_etf'

        def parse(
            self,
            *,
            artifact: RawArtifact,
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[ParsedRecord]:
            del artifact, source, run_context
            return [parsed_record]

        def normalize(
            self,
            *,
            parsed_records: list[ParsedRecord],
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[NormalizedMetricRecord]:
            del parsed_records, source, run_context
            return [normalized_record]

    monkeypatch.setattr(
        etf_job,
        '_build_etf_adapter_source_index_or_raise',
        lambda **_: {source.source_id: (_FakeAdapter(), source)},
    )
    monkeypatch.setattr(
        etf_job,
        '_build_object_store_client_and_bucket_or_raise',
        lambda: ('client', 'bucket'),
    )
    monkeypatch.setattr(
        etf_job,
        '_load_all_raw_artifact_manifest_payloads_or_raise',
        lambda **_: [
            {
                'source_id': source.source_id,
                'artifact_id': artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-1.json',
                'fetched_at_utc': observed_at_utc.isoformat(),
            },
            {
                'source_id': source.source_id,
                'artifact_id': artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-2.json',
                'fetched_at_utc': observed_at_utc.isoformat(),
            },
        ],
    )
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **_: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=artifact,
            persisted_artifact=persisted,
        ),
    )

    results = etf_job._load_etf_archived_source_results_or_raise(
        run_context=ScrapeRunContext(
            run_id='etf-daily-run',
            started_at_utc=observed_at_utc,
        ),
        required_partition_ids=('2026-03-26',),
    )

    assert len(results) == 1
    assert results[0].source.source_id == source.source_id
