from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import origo_control_plane.jobs.etf_daily_ingest as etf_job
import pytest
from origo_control_plane.backfill.runtime_contract import BackfillRuntimeContract

from origo.events.errors import EventWriterError
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
    def info(self, _message: str) -> None:
        return None

    def warning(self, _message: str) -> None:
        return None


class _FakeContext:
    def __init__(
        self,
        *,
        run_id: str,
        tags: dict[str, str],
        op_config: dict[str, object] | None = None,
    ) -> None:
        self.run_id = run_id
        self.run = SimpleNamespace(tags=tags)
        self.log = _FakeLog()
        if op_config is not None:
            self.op_config = op_config


def _patch_single_source_history_contract(
    monkeypatch: Any,
    *,
    source_id: str,
    history_mode: str,
    first_partition_id: str | None = None,
) -> None:
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=source_id,
                history_mode=history_mode,
                first_partition_id=first_partition_id,
            )
        },
    )


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
    monkeypatch.setattr(etf_job, '_load_etf_partition_workers_or_raise', lambda: 10)

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


def test_build_clickhouse_client_uses_native_timeout_contract(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_client(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(
        etf_job,
        'resolve_clickhouse_native_settings',
        lambda: SimpleNamespace(
            host='clickhouse',
            port=9000,
            user='default',
            password='secret',
            database='origo',
            send_receive_timeout_seconds=3600,
        ),
    )
    monkeypatch.setattr(etf_job, 'ClickhouseClient', _fake_client)

    client, database = etf_job._build_clickhouse_client_or_raise()

    assert client is not None
    assert database == 'origo'
    assert captured['send_receive_timeout'] == 3600


def test_load_required_partition_ids_from_context_reads_config_date_window() -> None:
    context = _FakeContext(
        run_id='run-1',
        tags={},
        op_config={
            'projection_mode': 'deferred',
            'execution_mode': 'backfill',
            'runtime_audit_mode': 'summary',
            'start_date': '2024-01-11',
            'end_date': '2024-01-15',
        },
    )

    partition_ids = etf_job._load_required_partition_ids_from_context_or_none(
        context=context
    )

    assert partition_ids == ('2024-01-11', '2024-01-12', '2024-01-15')


def test_run_etf_backfill_honors_required_partition_ids_tag(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    class _FakeClient:
        def disconnect(self) -> None:
            return None

    monkeypatch.setattr(
        etf_job,
        '_build_clickhouse_client_or_raise',
        lambda: (_FakeClient(), 'origo'),
    )

    def _load_results(**kwargs: Any) -> list[PipelineSourceResult]:
        captured['required_partition_ids'] = kwargs['required_partition_ids']
        return [_build_source_result(partition_id='2026-03-26')]

    monkeypatch.setattr(
        etf_job,
        '_load_etf_archived_source_results_or_raise',
        _load_results,
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
    monkeypatch.setattr(
        etf_job,
        'project_etf_daily_metrics_native',
        lambda **_: SimpleNamespace(to_dict=lambda: {'rows_written': 0}),
    )
    monkeypatch.setattr(
        etf_job,
        'project_etf_daily_metrics_aligned',
        lambda **_: SimpleNamespace(to_dict=lambda: {'rows_written': 0}),
    )
    monkeypatch.setattr(etf_job, '_load_etf_partition_workers_or_raise', lambda: 10)

    etf_job._run_etf_backfill_or_raise(
        context=_FakeContext(
            run_id='run-1',
            tags={
                'origo.backfill.projection_mode': 'deferred',
                'origo.backfill.execution_mode': 'reconcile',
                'origo.backfill.runtime_audit_mode': 'summary',
                'origo.backfill.partition_ids': '2026-03-26',
            },
        )
    )

    assert captured['required_partition_ids'] == ('2026-03-26',)


def test_run_etf_backfill_skips_terminal_partitions_in_backfill_mode(
    monkeypatch: Any,
) -> None:
    executed: list[str] = []

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
        '_load_etf_archived_source_results_or_raise',
        lambda **_: [
            _build_source_result(partition_id='2026-03-25'),
            _build_source_result(partition_id='2026-03-26'),
        ],
    )

    class _FakeStateStore:
        def __init__(self, **_: Any) -> None:
            return None

        def assess_partition_execution(self, *, stream_key: Any) -> Any:
            if stream_key.partition_id == '2026-03-25':
                return SimpleNamespace(
                    latest_proof_state='proved_complete',
                    canonical_row_count=1,
                    active_quarantine=False,
                )
            return SimpleNamespace(
                latest_proof_state=None,
                canonical_row_count=0,
                active_quarantine=False,
            )

    monkeypatch.setattr(etf_job, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        etf_job,
        '_load_etf_partition_workers_or_raise',
        lambda: 10,
    )
    monkeypatch.setattr(
        etf_job,
        '_execute_etf_partition_backfill_in_worker_or_raise',
        lambda **kwargs: (
            executed.append(kwargs['partition_id']),
            SimpleNamespace(
                rows_inserted=1,
                native_projection_summary={'rows_written': 0},
                aligned_projection_summary={'rows_written': 0},
            ),
        )[1],
    )

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

    assert executed == ['2026-03-26']
    assert summary.total_inserted_rows == 1


def test_load_etf_partition_workers_requires_integer_ge_10(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('ORIGO_S34_ETF_PARTITION_WORKERS', '9')
    with pytest.raises(RuntimeError, match='must be >= 10'):
        etf_job._load_etf_partition_workers_or_raise()

    monkeypatch.setenv('ORIGO_S34_ETF_PARTITION_WORKERS', '10')
    assert etf_job._load_etf_partition_workers_or_raise() == 10


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


def test_reset_etf_partition_for_reconcile_deletes_existing_partition_state(
    monkeypatch: Any,
) -> None:
    recorded_states: list[dict[str, Any]] = []
    recorded_reset_boundaries: list[dict[str, Any]] = []
    executed_queries: list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]] = []
    count_calls = 0

    class _FakeClient:
        def execute(
            self,
            query: str,
            params: dict[str, Any] | None = None,
            settings: dict[str, Any] | None = None,
        ) -> list[tuple[int, ...]]:
            nonlocal count_calls
            normalized_query = ' '.join(query.split())
            executed_queries.append((normalized_query, params, settings))
            if normalized_query.startswith('SELECT ('):
                count_calls += 1
                if count_calls == 1:
                    return [(11, 7, 3, 2, 2)]
                return [(0, 0, 0, 0, 0)]
            return []

    class _FakeStateStore:
        def record_partition_state(self, **kwargs: Any) -> None:
            recorded_states.append(kwargs)

        def record_partition_reset_boundary(self, **kwargs: Any) -> None:
            recorded_reset_boundaries.append(kwargs)

    summary = etf_job._reset_etf_partition_for_reconcile_or_raise(
        context=_FakeContext(run_id='run-1', tags={}),
        client=_FakeClient(),
        database='origo',
        partition_id='2024-01-11',
        source_proof=SimpleNamespace(
            stream_key=SimpleNamespace(
                source_id='etf',
                stream_id='etf_daily_metrics',
                partition_id='2024-01-11',
            )
        ),
        state_store=_FakeStateStore(),
        writer_error=EventWriterError(
            code='WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
            message='Canonical identity conflict: payload hash mismatch for existing source-event identity',
        ),
    )

    assert summary == {
        'canonical_event_rows': 11,
        'native_projection_rows': 7,
        'aligned_projection_rows': 3,
        'projector_checkpoint_rows': 2,
        'projector_watermark_rows': 2,
    }
    assert [item['state'] for item in recorded_states] == ['reconcile_required']
    assert recorded_states[0]['reason'] == 'legacy_canonical_payload_reset_required'
    assert len(recorded_reset_boundaries) == 1
    assert recorded_reset_boundaries[0]['reason'] == 'legacy_canonical_payload_reset_required'
    alter_queries = [query for query, _params, _settings in executed_queries if query.startswith('ALTER TABLE')]
    assert len(alter_queries) == 4
    assert all('canonical_event_log' not in query for query in alter_queries)
    assert any('canonical_etf_daily_metrics_native_v1' in query for query in alter_queries)
    assert any('canonical_aligned_1s_aggregates' in query for query in alter_queries)
    assert any('canonical_projector_checkpoints' in query for query in alter_queries)
    assert any('canonical_projector_watermarks' in query for query in alter_queries)
    for query, _params, settings in executed_queries:
        if query.startswith('ALTER TABLE'):
            assert settings == {'mutations_sync': 2}


def test_execute_etf_partition_backfill_resets_legacy_conflict_once_on_reconcile(
    monkeypatch: Any,
) -> None:
    write_attempts: list[int] = []
    reset_calls: list[dict[str, Any]] = []

    class _FakeStateStore:
        def __init__(self, **_: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_: Any) -> None:
            return None

        def assess_partition_execution(self, **_: Any) -> Any:
            return SimpleNamespace(
                latest_proof_state='reconcile_required',
                canonical_row_count=11,
                active_quarantine=False,
            )

        def record_source_manifest(self, **_: Any) -> None:
            return None

        def record_partition_state(self, **_: Any) -> None:
            return None

        def record_partition_reset_boundary(self, **_: Any) -> None:
            return None

        def compute_canonical_partition_proof_or_raise(self, **_: Any) -> Any:
            return SimpleNamespace()

        def prove_partition_or_quarantine(self, **_: Any) -> Any:
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='proof-sha',
            )

    monkeypatch.setattr(etf_job, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        etf_job,
        'canonical_proof_matches_source_proof',
        lambda **_: False,
    )

    def _write(**kwargs: Any) -> Any:
        write_attempts.append(len(kwargs['records']))
        if len(write_attempts) == 1:
            raise EventWriterError(
                code='WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
                message='Canonical identity conflict: payload hash mismatch for existing source-event identity',
            )
        return SimpleNamespace(
            to_dict=lambda: {
                'rows_processed': len(kwargs['records']),
                'rows_inserted': len(kwargs['records']),
                'rows_duplicate': 0,
            }
        )

    monkeypatch.setattr(etf_job, 'write_etf_normalized_records_to_canonical', _write)
    monkeypatch.setattr(
        etf_job,
        '_reset_etf_partition_for_reconcile_or_raise',
        lambda **kwargs: (
            reset_calls.append(kwargs),
            {
                'canonical_event_rows': 11,
                'native_projection_rows': 0,
                'aligned_projection_rows': 0,
                'projector_checkpoint_rows': 0,
                'projector_watermark_rows': 0,
            },
        )[1],
    )

    source_result = _build_source_result(partition_id='2026-03-26')
    batches = etf_job._build_partition_batches_or_raise([source_result])['2026-03-26']

    result = etf_job._execute_etf_partition_backfill_or_raise(
        context=_FakeContext(run_id='run-2', tags={}),
        client=SimpleNamespace(),
        database='origo',
        partition_id='2026-03-26',
        batches=batches,
        runtime_contract=BackfillRuntimeContract(
            projection_mode='deferred',
            execution_mode='reconcile',
            runtime_audit_mode='summary',
        ),
    )

    assert write_attempts == [1, 1]
    assert len(reset_calls) == 1
    assert reset_calls[0]['partition_id'] == '2026-03-26'
    assert result.write_path == 'reconcile_partition_reset_rewrite'
    assert result.partition_proof_state == 'proved_complete'


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
    _patch_single_source_history_contract(
        monkeypatch,
        source_id=source.source_id,
        history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2026-03-25',
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
    _patch_single_source_history_contract(
        monkeypatch,
        source_id=source.source_id,
        history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2026-03-26',
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


def test_load_etf_archived_source_results_prefers_latest_valid_artifact_revision(
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
    persisted = PersistedRawArtifact(
        artifact_id='artifact-new',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
    )

    def _build_artifact(*, artifact_id: str, fetched_at_utc: datetime, payload_value: float) -> RawArtifact:
        del payload_value
        return RawArtifact(
            artifact_id=artifact_id,
            source_id=source.source_id,
            source_uri=source.source_uri,
            fetched_at_utc=fetched_at_utc,
            fetch_method='http',
            artifact_format='csv',
            content_sha256=f'sha-{artifact_id}',
            content=artifact_id.encode('utf-8'),
        )

    old_artifact = _build_artifact(
        artifact_id='artifact-old',
        fetched_at_utc=observed_at_utc,
        payload_value=1.25,
    )
    new_artifact = _build_artifact(
        artifact_id='artifact-new',
        fetched_at_utc=observed_at_utc.replace(hour=1),
        payload_value=2.50,
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
            del source, run_context
            return [
                ParsedRecord(
                    record_id=f'record-{artifact.artifact_id}',
                    artifact_id=artifact.artifact_id,
                    payload={
                        'as_of_date': '2026-03-26',
                        'btc_units': 1.25 if artifact.artifact_id == 'artifact-old' else 2.50,
                    },
                    parser_name='parser',
                    parser_version='1.0.0',
                    parsed_at_utc=artifact.fetched_at_utc,
                )
            ]

        def normalize(
            self,
            *,
            parsed_records: list[ParsedRecord],
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[NormalizedMetricRecord]:
            del source, run_context
            parsed = parsed_records[0]
            observed_at = datetime.fromisoformat(
                f"{parsed.payload['as_of_date']}T00:00:00+00:00"
            )
            return [
                NormalizedMetricRecord(
                    metric_id=f"metric-{parsed.artifact_id}",
                    source_id='etf_ishares_ibit_daily',
                    metric_name='btc_units',
                    metric_value=parsed.payload['btc_units'],
                    metric_unit='BTC',
                    observed_at_utc=observed_at,
                    dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
                    provenance=ProvenanceMetadata(
                        source_id='etf_ishares_ibit_daily',
                        source_uri='https://example.com/ibit.csv',
                        artifact_id=parsed.artifact_id,
                        artifact_sha256=f'sha-{parsed.artifact_id}',
                        fetch_method='http',
                        parser_name='parser',
                        parser_version='1.0.0',
                        fetched_at_utc=observed_at_utc,
                        parsed_at_utc=observed_at_utc,
                        normalized_at_utc=observed_at_utc,
                    ),
                )
            ]

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
                'artifact_id': old_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-old.json',
                'fetched_at_utc': old_artifact.fetched_at_utc.isoformat(),
            },
            {
                'source_id': source.source_id,
                'artifact_id': new_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-new.json',
                'fetched_at_utc': new_artifact.fetched_at_utc.isoformat(),
            },
        ],
    )
    artifact_map = {
        old_artifact.artifact_id: old_artifact,
        new_artifact.artifact_id: new_artifact,
    }
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **kwargs: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=artifact_map[kwargs['manifest_payload']['artifact_id']],
            persisted_artifact=persisted,
        ),
    )
    _patch_single_source_history_contract(
        monkeypatch,
        source_id=source.source_id,
        history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2026-03-26',
    )

    results = etf_job._load_etf_archived_source_results_or_raise(
        run_context=ScrapeRunContext(
            run_id='etf-daily-run',
            started_at_utc=observed_at_utc,
        ),
        required_partition_ids=('2026-03-26',),
    )

    assert len(results) == 1
    assert results[0].artifact.artifact_id == 'artifact-new'


def test_load_etf_archived_source_results_ignores_invalid_artifacts_when_coverage_is_satisfied(
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
    valid_artifact = RawArtifact(
        artifact_id='artifact-valid',
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-valid-sha',
        content=b'valid',
    )
    invalid_artifact = RawArtifact(
        artifact_id='artifact-invalid',
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=observed_at_utc.replace(day=27),
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-invalid-sha',
        content=b'invalid',
    )
    persisted = PersistedRawArtifact(
        artifact_id='artifact-valid',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
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
            del source, run_context
            if artifact.artifact_id == 'artifact-invalid':
                raise RuntimeError('invalid archived payload')
            return [
                ParsedRecord(
                    record_id='record-valid',
                    artifact_id=artifact.artifact_id,
                    payload={'as_of_date': '2026-03-26', 'btc_units': 1.25},
                    parser_name='parser',
                    parser_version='1.0.0',
                    parsed_at_utc=artifact.fetched_at_utc,
                )
            ]

        def normalize(
            self,
            *,
            parsed_records: list[ParsedRecord],
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[NormalizedMetricRecord]:
            del source, run_context
            parsed = parsed_records[0]
            return [
                NormalizedMetricRecord(
                    metric_id='metric-valid',
                    source_id='etf_ishares_ibit_daily',
                    metric_name='btc_units',
                    metric_value=1.25,
                    metric_unit='BTC',
                    observed_at_utc=datetime(2026, 3, 26, 0, 0, tzinfo=UTC),
                    dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
                    provenance=ProvenanceMetadata(
                        source_id='etf_ishares_ibit_daily',
                        source_uri='https://example.com/ibit.csv',
                        artifact_id=parsed.artifact_id,
                        artifact_sha256='artifact-valid-sha',
                        fetch_method='http',
                        parser_name='parser',
                        parser_version='1.0.0',
                        fetched_at_utc=observed_at_utc,
                        parsed_at_utc=observed_at_utc,
                        normalized_at_utc=observed_at_utc,
                    ),
                )
            ]

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
                'artifact_id': valid_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-valid.json',
                'fetched_at_utc': valid_artifact.fetched_at_utc.isoformat(),
            },
            {
                'source_id': source.source_id,
                'artifact_id': invalid_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-invalid.json',
                'fetched_at_utc': invalid_artifact.fetched_at_utc.isoformat(),
            },
        ],
    )
    artifact_map = {
        valid_artifact.artifact_id: valid_artifact,
        invalid_artifact.artifact_id: invalid_artifact,
    }
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **kwargs: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=artifact_map[kwargs['manifest_payload']['artifact_id']],
            persisted_artifact=persisted,
        ),
    )
    _patch_single_source_history_contract(
        monkeypatch,
        source_id=source.source_id,
        history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2026-03-26',
    )

    results = etf_job._load_etf_archived_source_results_or_raise(
        run_context=ScrapeRunContext(
            run_id='etf-daily-run',
            started_at_utc=observed_at_utc,
        ),
        required_partition_ids=('2026-03-26',),
    )

    assert len(results) == 1
    assert results[0].artifact.artifact_id == 'artifact-valid'


def test_build_expected_etf_archive_partitions_by_source_expands_snapshot_capture_forward(
    monkeypatch: Any,
) -> None:
    source_id = 'etf_test_snapshot_daily'
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
            )
        },
    )

    expected, unavailable, zero_history = (
        etf_job._build_expected_etf_archive_partitions_by_source_or_raise(
        valid_partition_ids_by_source={source_id: {'2026-03-26', '2026-03-30'}},
        no_data_partition_ids_by_source={},
        required_partition_filter=set(),
        )
    )

    assert unavailable == []
    assert zero_history == []
    assert expected == {
        source_id: ('2026-03-26', '2026-03-27', '2026-03-30')
    }


def test_build_expected_etf_archive_partitions_by_source_reports_zero_history_snapshot_source(
    monkeypatch: Any,
) -> None:
    source_id = 'etf_test_snapshot_daily'
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
            )
        },
    )

    expected, unavailable, zero_history = (
        etf_job._build_expected_etf_archive_partitions_by_source_or_raise(
        valid_partition_ids_by_source={},
        no_data_partition_ids_by_source={},
        required_partition_filter=set(),
        )
    )

    assert expected == {}
    assert unavailable == []
    assert zero_history == [
        {
            'source_id': source_id,
            'history_mode': etf_job._ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
            'reason': 'zero_history_before_first_valid_archive',
        }
    ]


def test_build_expected_etf_archive_partitions_by_source_still_reports_unavailable_official_source(
    monkeypatch: Any,
) -> None:
    source_id = 'etf_ishares_ibit_daily'
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
                first_partition_id='2024-01-11',
            )
        },
    )

    expected, unavailable, zero_history = (
        etf_job._build_expected_etf_archive_partitions_by_source_or_raise(
            valid_partition_ids_by_source={},
            no_data_partition_ids_by_source={},
            required_partition_filter=set(),
        )
    )

    assert expected == {}
    assert zero_history == []
    assert unavailable == [
        {
            'source_id': source_id,
            'history_mode': etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
            'reason': 'no_valid_archived_artifacts',
        }
    ]


def test_build_expected_etf_archive_partitions_by_source_excludes_official_no_data_days(
    monkeypatch: Any,
) -> None:
    source_id = 'etf_ishares_ibit_daily'
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
                first_partition_id='2024-01-11',
            )
        },
    )

    expected, unavailable, zero_history = (
        etf_job._build_expected_etf_archive_partitions_by_source_or_raise(
        valid_partition_ids_by_source={source_id: {'2024-01-11', '2024-01-12', '2024-01-16'}},
        no_data_partition_ids_by_source={source_id: {'2024-01-15'}},
        required_partition_filter=set(),
        )
    )

    assert unavailable == []
    assert zero_history == []
    assert expected == {
        source_id: ('2024-01-11', '2024-01-12', '2024-01-16')
    }


def test_load_etf_archived_source_results_honors_ishares_no_data_artifacts(
    monkeypatch: Any,
) -> None:
    observed_at_utc = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
    source = SourceDescriptor(
        source_id='etf_ishares_ibit_daily',
        source_name='iShares IBIT',
        source_uri='https://example.com/ibit.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
    )
    valid_artifact = RawArtifact(
        artifact_id='artifact-valid',
        source_id=source.source_id,
        source_uri='https://example.com/ibit.csv?asOfDate=20240116',
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-valid-sha',
        content=b'valid',
    )
    no_data_artifact = RawArtifact(
        artifact_id='artifact-no-data',
        source_id=source.source_id,
        source_uri='https://example.com/ibit.csv?asOfDate=20240115',
        fetched_at_utc=observed_at_utc.replace(day=15),
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-no-data-sha',
        content=b'Fund Holdings as of,\"-\"',
    )
    persisted = PersistedRawArtifact(
        artifact_id='artifact-valid',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
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
            del source, run_context
            return [
                ParsedRecord(
                    record_id='record-valid',
                    artifact_id=artifact.artifact_id,
                    payload={'as_of_date': '2024-01-16', 'btc_units': 1.25},
                    parser_name='parser',
                    parser_version='1.0.0',
                    parsed_at_utc=artifact.fetched_at_utc,
                )
            ]

        def normalize(
            self,
            *,
            parsed_records: list[ParsedRecord],
            source: SourceDescriptor,
            run_context: ScrapeRunContext,
        ) -> list[NormalizedMetricRecord]:
            del source, run_context
            parsed = parsed_records[0]
            return [
                NormalizedMetricRecord(
                    metric_id='metric-valid',
                    source_id='etf_ishares_ibit_daily',
                    metric_name='btc_units',
                    metric_value=1.25,
                    metric_unit='BTC',
                    observed_at_utc=datetime(2024, 1, 16, 0, 0, tzinfo=UTC),
                    dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
                    provenance=ProvenanceMetadata(
                        source_id='etf_ishares_ibit_daily',
                        source_uri='https://example.com/ibit.csv',
                        artifact_id=parsed.artifact_id,
                        artifact_sha256='artifact-valid-sha',
                        fetch_method='http',
                        parser_name='parser',
                        parser_version='1.0.0',
                        fetched_at_utc=observed_at_utc,
                        parsed_at_utc=observed_at_utc,
                        normalized_at_utc=observed_at_utc,
                    ),
                )
            ]

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
                'artifact_id': no_data_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-no-data.json',
                'fetched_at_utc': no_data_artifact.fetched_at_utc.isoformat(),
            },
            {
                'source_id': source.source_id,
                'artifact_id': valid_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-valid.json',
                'fetched_at_utc': valid_artifact.fetched_at_utc.isoformat(),
            },
        ],
    )
    artifact_map = {
        valid_artifact.artifact_id: valid_artifact,
        no_data_artifact.artifact_id: no_data_artifact,
    }
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **kwargs: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=artifact_map[kwargs['manifest_payload']['artifact_id']],
            persisted_artifact=persisted,
        ),
    )
    _patch_single_source_history_contract(
        monkeypatch,
        source_id=source.source_id,
        history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2024-01-15',
    )

    results = etf_job._load_etf_archived_source_results_or_raise(
        run_context=ScrapeRunContext(
            run_id='etf-daily-run',
            started_at_utc=observed_at_utc,
        ),
        required_partition_ids=('2024-01-15', '2024-01-16'),
    )

    assert len(results) == 1
    assert results[0].artifact.artifact_id == 'artifact-valid'


def test_load_etf_archived_source_results_allows_zero_history_snapshot_only_sources(
    monkeypatch: Any,
) -> None:
    observed_at_utc = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
    ishares_source = SourceDescriptor(
        source_id='etf_ishares_ibit_daily',
        source_name='iShares IBIT',
        source_uri='https://example.com/ibit.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
    )
    snapshot_source = SourceDescriptor(
        source_id='etf_grayscale_gbtc_daily',
        source_name='Grayscale GBTC',
        source_uri='https://example.com/gbtc.csv',
        discovered_at_utc=observed_at_utc,
        metadata={'issuer': 'grayscale', 'ticker': 'GBTC', 'rights_source': 'grayscale'},
    )
    valid_artifact = RawArtifact(
        artifact_id='artifact-valid',
        source_id=ishares_source.source_id,
        source_uri='https://example.com/ibit.csv?asOfDate=20240116',
        fetched_at_utc=observed_at_utc,
        fetch_method='http',
        artifact_format='csv',
        content_sha256='artifact-valid-sha',
        content=b'valid',
    )
    persisted = PersistedRawArtifact(
        artifact_id='artifact-valid',
        storage_uri='s3://bucket/raw/artifact.csv',
        manifest_uri='s3://bucket/raw/manifest.json',
        persisted_at_utc=observed_at_utc,
    )

    class _FakeAdapter:
        def parse(self, **kwargs: Any) -> list[ParsedRecord]:
            artifact = kwargs['artifact']
            return [
                ParsedRecord(
                    record_id='record-valid',
                    artifact_id=artifact.artifact_id,
                    payload={'as_of_date': '2024-01-16', 'btc_units': 1.25},
                    parser_name='parser',
                    parser_version='1.0.0',
                    parsed_at_utc=observed_at_utc,
                )
            ]

        def normalize(self, **kwargs: Any) -> list[NormalizedMetricRecord]:
            parsed = kwargs['parsed_records'][0]
            return [
                NormalizedMetricRecord(
                    metric_id='metric-valid',
                    source_id='etf_ishares_ibit_daily',
                    metric_name='btc_units',
                    metric_value=1.25,
                    metric_unit='BTC',
                    observed_at_utc=observed_at_utc,
                    dimensions={'issuer': 'iShares', 'ticker': 'IBIT'},
                    provenance=ProvenanceMetadata(
                        source_id='etf_ishares_ibit_daily',
                        source_uri='https://example.com/ibit.csv',
                        artifact_id=parsed.artifact_id,
                        artifact_sha256='artifact-valid-sha',
                        fetch_method='http',
                        parser_name='parser',
                        parser_version='1.0.0',
                        fetched_at_utc=observed_at_utc,
                        parsed_at_utc=observed_at_utc,
                        normalized_at_utc=observed_at_utc,
                    ),
                )
            ]

    monkeypatch.setattr(
        etf_job,
        '_build_etf_adapter_source_index_or_raise',
        lambda **_: {
            ishares_source.source_id: (_FakeAdapter(), ishares_source),
            snapshot_source.source_id: (_FakeAdapter(), snapshot_source),
        },
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
                'source_id': ishares_source.source_id,
                'artifact_id': valid_artifact.artifact_id,
                'manifest_uri': 's3://bucket/raw/manifest-valid.json',
                'fetched_at_utc': valid_artifact.fetched_at_utc.isoformat(),
            },
        ],
    )
    monkeypatch.setattr(
        etf_job,
        '_load_archived_raw_artifact_from_manifest_or_raise',
        lambda **kwargs: etf_job._ArchivedArtifactLoad(
            manifest_payload={},
            raw_artifact=valid_artifact,
            persisted_artifact=persisted,
        ),
    )
    monkeypatch.setattr(
        etf_job,
        '_ETF_HISTORICAL_AVAILABILITY_CONTRACTS',
        {
            ishares_source.source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=ishares_source.source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
                first_partition_id='2024-01-16',
            ),
            snapshot_source.source_id: etf_job._ETFHistoricalAvailabilityContract(
                source_id=snapshot_source.source_id,
                history_mode=etf_job._ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
            ),
        },
    )

    results = etf_job._load_etf_archived_source_results_or_raise(
        run_context=ScrapeRunContext(
            run_id='etf-daily-run',
            started_at_utc=observed_at_utc,
        ),
        required_partition_ids=('2024-01-16',),
        log=_FakeLog(),
    )

    assert len(results) == 1
    assert results[0].artifact.artifact_id == 'artifact-valid'
