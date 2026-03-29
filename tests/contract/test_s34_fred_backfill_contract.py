from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import origo_control_plane.jobs.fred_daily_ingest as fred_job
import origo_control_plane.s34_fred_backfill_runner as fred_runner
import pytest
from origo_control_plane.backfill.runtime_contract import BackfillRuntimeContract

from origo.events.errors import ReconciliationError
from origo.fred import FREDLongMetricRow, FREDRawSeriesBundle
from origo.scraper.contracts import PersistedRawArtifact


class _FakeClickHouseClient:
    pass


class _ProofQueryClickHouseClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def execute(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeDagsterContext:
    def __init__(self) -> None:
        self.run = SimpleNamespace(tags={}, run_id='dagster-run-id')
        self.run_id = 'dagster-run-id'
        self.log = SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
        )


def test_fred_job_reconcile_without_explicit_partitions_uses_authoritative_ambiguity(
    monkeypatch: Any,
) -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN', '2')
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS', '31')
    monkeypatch.setattr(
        fred_job,
        '_load_ambiguous_partition_ids_or_raise',
        lambda **_: ('2026-02-01', '2026-02-15', '2026-03-01'),
    )

    result = fred_job._resolve_source_partition_scope_ids_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
        runtime_contract=runtime_contract,
        explicit_partition_ids=None,
    )

    assert result == ('2026-02-01', '2026-02-15')


def test_fred_job_reconcile_processes_bounded_scope_ids_without_requerying_ambiguity(
    monkeypatch: Any,
) -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )

    result = fred_job._resolve_partition_ids_to_process_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
        runtime_contract=runtime_contract,
        source_partition_rows={
            '2026-02-01': [object()],
            '2026-02-15': [object()],
        },
        source_partition_scope_ids=('2026-02-01', '2026-02-15'),
        explicit_partition_ids=None,
    )

    assert result == ('2026-02-01', '2026-02-15')


def test_fred_job_explicit_partition_ids_must_exist_in_source_history() -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )

    with pytest.raises(
        RuntimeError,
        match='Requested FRED partition ids were not present in source history',
    ):
        fred_job._resolve_partition_ids_to_process_or_raise(
            client=_FakeClickHouseClient(),
            database='origo',
            runtime_contract=runtime_contract,
            source_partition_rows={'2026-02-01': [object()]},
            source_partition_scope_ids=None,
            explicit_partition_ids=('2026-02-01', '2026-03-01'),
        )


def test_s34_fred_backfill_runner_prefers_reconcile_when_ambiguity_exists(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN', '2')
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS', '31')
    monkeypatch.setattr(
        fred_runner,
        '_load_ambiguous_partition_ids_or_raise',
        lambda **_: ('1947-01-01', '1947-02-01', '1947-03-01'),
    )

    plan = fred_runner._plan_next_fred_run_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
    )

    assert plan.execution_mode == 'reconcile'
    assert plan.partition_ids == ('1947-01-01',)
    assert plan.ambiguous_partition_count == 3
    assert plan.source_window_start == '1947-01-01'
    assert plan.source_window_end == '1947-01-01'
    assert plan.source_window_days == 1


def test_s34_fred_backfill_runner_requires_reconcile_tranche_env(
    monkeypatch: Any,
) -> None:
    monkeypatch.delenv(
        'ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN',
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match='ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN must be set and non-empty',
    ):
        fred_runner.load_fred_reconcile_max_partitions_per_run_or_raise()


def test_s34_fred_backfill_runner_requires_reconcile_source_window_env(
    monkeypatch: Any,
) -> None:
    monkeypatch.delenv(
        'ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS',
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match='ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS must be set and non-empty',
    ):
        fred_runner.load_fred_reconcile_max_source_window_days_or_raise()


def test_s34_fred_backfill_runner_selects_prefix_by_source_window_and_count() -> None:
    result = fred_runner.select_fred_reconcile_partition_ids_or_raise(
        ambiguous_partition_ids=(
            '1947-01-01',
            '1947-02-01',
            '1947-12-31',
            '1948-01-02',
        ),
        max_partitions_per_run=3,
        max_source_window_days=365,
    )

    assert result == (
        '1947-01-01',
        '1947-02-01',
        '1947-12-31',
    )


def test_s34_fred_backfill_runner_reconcile_batch_summary_requires_terminal_proofs() -> None:
    client = _ProofQueryClickHouseClient(
        [
            (
                '1947-01-01',
                'reconcile_required',
                'source_manifest_recorded',
                'proof-1',
                1,
                0,
            )
        ]
    )

    with pytest.raises(
        RuntimeError,
        match='targeted partitions remain non-terminal',
    ):
        fred_runner._load_reconcile_partition_batch_summary_or_raise(
            client=client,
            database='origo',
            partition_ids=('1947-01-01',),
        )


def test_s34_fred_backfill_runner_builds_required_run_tags() -> None:
    tags = fred_runner._build_run_tags(
        control_run_id='s34-fred-test',
        execution_mode='reconcile',
        partition_ids=('2026-02-01', '2026-03-01'),
    )

    assert tags == {
        'origo.backfill.dataset': 'fred_series_metrics',
        'origo.backfill.control_run_id': 's34-fred-test',
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'reconcile',
        'origo.backfill.runtime_audit_mode': 'summary',
        'origo.backfill.partition_ids': '2026-02-01,2026-03-01',
    }


def test_fred_job_builds_revision_history_source_bundles(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        fred_job,
        'load_fred_series_registry',
        lambda: (
            '2026-03-06-s6-c1',
            [
                SimpleNamespace(
                    series_id='CPIAUCSL',
                    source_id='fred_cpiaucsl',
                    metric_name='consumer_price_index_all_items',
                )
            ],
        ),
    )
    monkeypatch.setattr(fred_job, 'build_fred_client_from_env', lambda: object())

    bundle = FREDRawSeriesBundle(
        source_id='fred_cpiaucsl',
        series_id='CPIAUCSL',
        source_uri='fred://series/CPIAUCSL',
        fetched_at_utc=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        registry_version='2026-03-06-s6-c1',
        metadata_payload={'seriess': [{'id': 'CPIAUCSL'}]},
        observations_payload={'observations': [{'date': '1947-01-01'}]},
    )
    monkeypatch.setattr(
        fred_job,
        'build_fred_raw_bundles',
        lambda **kwargs: (
            captured.setdefault('kwargs', kwargs),
            [bundle],
        )[1],
    )
    monkeypatch.setattr(
        fred_job,
        'persist_fred_raw_bundles_to_object_store',
        lambda **_: [
            PersistedRawArtifact(
                artifact_id='artifact-1',
                storage_uri='s3://bucket/fred/1.json',
                manifest_uri='s3://bucket/fred/1.manifest.json',
                persisted_at_utc=datetime(2026, 3, 28, 12, 1, tzinfo=UTC),
            )
        ],
    )

    result = fred_job._build_persisted_bundles_or_raise(
        context=_FakeDagsterContext(),
        source_partition_ids=None,
    )

    assert len(result) == 1
    assert captured['kwargs']['observations_mode'] == 'revision_history'


def test_fred_job_builds_revision_history_source_bundles_for_bounded_window(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        fred_job,
        'load_fred_series_registry',
        lambda: (
            '2026-03-06-s6-c1',
            [
                SimpleNamespace(
                    series_id='CPIAUCSL',
                    source_id='fred_cpiaucsl',
                    metric_name='consumer_price_index_all_items',
                )
            ],
        ),
    )
    monkeypatch.setattr(fred_job, 'build_fred_client_from_env', lambda: object())

    bundle = FREDRawSeriesBundle(
        source_id='fred_cpiaucsl',
        series_id='CPIAUCSL',
        source_uri='fred://series/CPIAUCSL',
        fetched_at_utc=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        registry_version='2026-03-06-s6-c1',
        metadata_payload={'seriess': [{'id': 'CPIAUCSL'}]},
        observations_payload={'observations': [{'date': '1947-01-01'}]},
    )
    monkeypatch.setattr(
        fred_job,
        'build_fred_raw_bundles',
        lambda **kwargs: (
            captured.setdefault('kwargs', kwargs),
            [bundle],
        )[1],
    )
    monkeypatch.setattr(
        fred_job,
        'persist_fred_raw_bundles_to_object_store',
        lambda **_: [
            PersistedRawArtifact(
                artifact_id='artifact-1',
                storage_uri='s3://bucket/fred/1.json',
                manifest_uri='s3://bucket/fred/1.manifest.json',
                persisted_at_utc=datetime(2026, 3, 28, 12, 1, tzinfo=UTC),
            )
        ],
    )

    result = fred_job._build_persisted_bundles_or_raise(
        context=_FakeDagsterContext(),
        source_partition_ids=('1947-01-01', '1947-01-03'),
    )

    assert len(result) == 1
    assert captured['kwargs']['observation_start'].isoformat() == '1947-01-01'
    assert captured['kwargs']['observation_end'].isoformat() == '1947-01-03'


def test_fred_job_reconcile_source_scope_uses_authoritative_ambiguity(
    monkeypatch: Any,
) -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN', '2')
    monkeypatch.setenv('ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS', '31')
    monkeypatch.setattr(
        fred_job,
        '_load_ambiguous_partition_ids_or_raise',
        lambda **_: ('1947-01-01', '1947-01-02', '1947-03-01'),
    )

    result = fred_job._resolve_source_partition_scope_ids_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
        runtime_contract=runtime_contract,
        explicit_partition_ids=None,
    )

    assert result == ('1947-01-01', '1947-01-02')


def test_fred_reconcile_resets_partition_after_proof_mismatch(
    monkeypatch: Any,
) -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )
    row = FREDLongMetricRow(
        metric_id='metric-1',
        source_id='fred_cpiaucsl',
        metric_name='consumer_price_index_all_items',
        metric_unit='Index 1982-1984=100',
        metric_value_string='21.48',
        metric_value_int=None,
        metric_value_float=21.48,
        metric_value_bool=None,
        observed_at_utc=datetime(1947, 1, 1, tzinfo=UTC),
        dimensions_json='{"series_id":"CPIAUCSL"}',
        provenance_json='{"realtime_start":"2026-03-11","realtime_end":"2026-03-11"}',
    )
    source_proof = SimpleNamespace(
        stream_key=SimpleNamespace(
            source_id='fred',
            stream_id='fred_series_metrics',
            partition_id='1947-01-01',
        ),
        source_row_count=1,
    )
    canonical_assessment = SimpleNamespace(canonical_row_count=1)
    initial_quarantine = SimpleNamespace(
        state='quarantined',
        reason='row_count_mismatch,identity_digest_mismatch',
        proof_digest_sha256='proof-1',
    )
    final_proof = SimpleNamespace(
        state='proved_complete',
        reason='source_and_canonical_match',
        proof_digest_sha256='proof-2',
    )

    class _FakeStateStore:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.prove_calls = 0
            self.latest_partition_proof: Any | None = None

        def assert_partition_can_execute_or_raise(self, **_: Any) -> None:
            return None

        def assess_partition_execution(self, **_: Any) -> Any:
            return canonical_assessment

        def record_source_manifest(self, **_: Any) -> None:
            return None

        def record_partition_state(self, **_: Any) -> None:
            return None

        def record_partition_reset_boundary(self, **_: Any) -> None:
            return None

        def compute_canonical_partition_proof_or_raise(self, **_: Any) -> Any:
            return SimpleNamespace(
                canonical_row_count=1,
                canonical_unique_offset_count=1,
                first_offset_or_equivalent='old-offset',
                last_offset_or_equivalent='old-offset',
                canonical_identity_digest_sha256='old-digest',
                gap_count=0,
                duplicate_count=0,
            )

        def prove_partition_or_quarantine(self, **_: Any) -> Any:
            self.prove_calls += 1
            if self.prove_calls == 1:
                self.latest_partition_proof = initial_quarantine
                raise ReconciliationError(
                    code='BACKFILL_PARTITION_PROOF_FAILED',
                    message=(
                        'Partition proof failed and stream was quarantined '
                        'for fred/fred_series_metrics/1947-01-01: '
                        'row_count_mismatch,identity_digest_mismatch'
                    ),
                    context={
                        'mismatch_reasons': [
                            'row_count_mismatch',
                            'identity_digest_mismatch',
                        ]
                    },
                )
            self.latest_partition_proof = final_proof
            return final_proof

        def fetch_latest_partition_proof(self, **_: Any) -> Any:
            return self.latest_partition_proof

    write_calls: list[str] = []
    reset_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(fred_job, '_build_partition_source_proof_or_raise', lambda **_: source_proof)
    monkeypatch.setattr(fred_job, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        fred_job,
        'canonical_proof_matches_source_proof',
        lambda **_: False,
    )
    monkeypatch.setattr(
        fred_job,
        'write_fred_long_metrics_to_canonical',
        lambda **_: (
            write_calls.append('write'),
            SimpleNamespace(
                to_dict=lambda: {
                    'rows_processed': 1,
                    'rows_inserted': 1,
                    'rows_duplicate': 0,
                }
            ),
        )[1],
    )
    monkeypatch.setattr(
        fred_job,
        '_reset_fred_partition_for_reconcile_or_raise',
        lambda **kwargs: (
            reset_calls.append(kwargs),
            {
                'canonical_event_rows': 1,
                'native_projection_rows': 0,
                'aligned_projection_rows': 0,
                'projector_checkpoint_rows': 0,
                'projector_watermark_rows': 0,
            },
        )[1],
    )

    result = fred_job._execute_partition_backfill_or_raise(
        context=_FakeDagsterContext(),
        client=_FakeClickHouseClient(),
        database='origo',
        partition_id='1947-01-01',
        rows=[row],
        persisted_bundles_by_source_id={'fred_cpiaucsl': object()},
        runtime_contract=runtime_contract,
    )

    assert result.write_path == 'reconcile_partition_reset_rewrite'
    assert result.partition_proof_state == 'proved_complete'
    assert len(write_calls) == 2
    assert len(reset_calls) == 1


def test_reset_fred_partition_for_reconcile_records_reset_boundary_without_event_log_delete() -> None:
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

    source_proof = SimpleNamespace(
        stream_key=SimpleNamespace(
            source_id='fred',
            stream_id='fred_series_metrics',
            partition_id='1947-02-01',
        )
    )

    summary = fred_job._reset_fred_partition_for_reconcile_or_raise(
        context=_FakeDagsterContext(),
        client=_FakeClient(),
        database='origo',
        partition_id='1947-02-01',
        source_proof=source_proof,
        state_store=_FakeStateStore(),
        reset_reason='legacy_request_time_canonical_reset_required',
        reset_details={'proof_reason': 'row_count_mismatch,identity_digest_mismatch'},
    )

    assert summary == {
        'canonical_event_rows': 11,
        'native_projection_rows': 7,
        'aligned_projection_rows': 3,
        'projector_checkpoint_rows': 2,
        'projector_watermark_rows': 2,
    }
    assert [item['state'] for item in recorded_states] == ['reconcile_required']
    assert recorded_states[0]['reason'] == 'legacy_request_time_canonical_reset_required'
    assert len(recorded_reset_boundaries) == 1
    assert (
        recorded_reset_boundaries[0]['reason']
        == 'legacy_request_time_canonical_reset_required'
    )
    alter_queries = [query for query, _params, _settings in executed_queries if query.startswith('ALTER TABLE')]
    assert len(alter_queries) == 4
    assert all('canonical_event_log' not in query for query in alter_queries)
    assert any('canonical_fred_series_metrics_native_v1' in query for query in alter_queries)
    assert any('canonical_aligned_1s_aggregates' in query for query in alter_queries)
    assert any('canonical_projector_checkpoints' in query for query in alter_queries)
    assert any('canonical_projector_watermarks' in query for query in alter_queries)
