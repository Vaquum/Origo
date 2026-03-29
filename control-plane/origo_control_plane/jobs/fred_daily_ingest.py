from __future__ import annotations

# pyright: reportUnknownParameterType=false, reportMissingParameterType=false
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

import dagster as dg
from clickhouse_driver import Client as ClickhouseClient
from dagster import OpExecutionContext

from origo.events import (
    CANONICAL_EVENT_LOG_READ_TABLE,
    CanonicalBackfillStateStore,
    CanonicalStreamKey,
    SourceIdentityMaterial,
    build_partition_source_proof,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo.events.backfill_state import canonical_proof_matches_source_proof
from origo.events.errors import EventWriterError, ReconciliationError
from origo.fred import (
    FREDLongMetricRow,
    FREDRawSeriesBundle,
    build_fred_canonical_payload,
    build_fred_client_from_env,
    build_fred_raw_bundles,
    fred_raw_bundle_content_sha256,
    load_fred_series_registry,
    normalize_fred_raw_bundles_to_long_metrics_or_raise,
    persist_fred_raw_bundles_to_object_store,
    write_fred_long_metrics_to_canonical,
)
from origo.scraper.contracts import PersistedRawArtifact
from origo_control_plane.backfill import apply_runtime_audit_mode_or_raise
from origo_control_plane.backfill.runtime_contract import (
    BACKFILL_PARTITION_IDS_TAG,
    BackfillRuntimeContract,
    load_backfill_runtime_contract_from_tags_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.s34_fred_reconcile_planning import (
    select_fred_reconcile_partition_ids_from_env_or_raise,
)
from origo_control_plane.s34_partition_authority import (
    load_nonterminal_partition_ids_for_stream_or_raise,
)
from origo_control_plane.utils.fred_aligned_projector import (
    project_fred_series_metrics_aligned,
)
from origo_control_plane.utils.fred_native_projector import (
    project_fred_series_metrics_native,
)

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')

_CANONICAL_SOURCE_ID = 'fred'
_CANONICAL_STREAM_ID = 'fred_series_metrics'
_PAYLOAD_ENCODING = 'utf-8'
_TERMINAL_PARTITION_STATES = ('proved_complete', 'empty_proved')
_FRED_NATIVE_PROJECTOR_ID = 'fred_series_metrics_native_v1'
_FRED_ALIGNED_PROJECTOR_ID = 'fred_series_metrics_aligned_1s_v1'
_WRITER_RESETTABLE_CONFLICT_CODES = frozenset(
    {
        'WRITER_IDENTITY_EVENT_ID_CONFLICT',
        'WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT',
    }
)


@dataclass(frozen=True)
class _PersistedFREDBundle:
    bundle: FREDRawSeriesBundle
    persisted_artifact: PersistedRawArtifact
    content_sha256: str


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
    total_series: int
    total_source_partitions: int
    total_normalized_records: int
    total_inserted_rows: int
    total_native_projected_rows: int
    total_aligned_projected_rows: int
    partition_results: list[_PartitionBackfillResult]


@dataclass(frozen=True)
class _SourceWindow:
    observation_start: date | None
    observation_end: date | None


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
            send_receive_timeout=native_settings.send_receive_timeout_seconds,
        ),
        native_settings.database,
    )


def _load_required_partition_ids_from_tags_or_none(
    tags: Mapping[str, str],
) -> tuple[str, ...] | None:
    raw = tags.get(BACKFILL_PARTITION_IDS_TAG)
    if raw is None:
        return None
    normalized = raw.strip()
    if normalized == '':
        raise RuntimeError(
            f'{BACKFILL_PARTITION_IDS_TAG} must be non-empty when provided'
        )
    partition_ids: list[str] = []
    seen: set[str] = set()
    for item in normalized.split(','):
        partition_id = item.strip()
        if partition_id == '':
            raise RuntimeError(
                f'{BACKFILL_PARTITION_IDS_TAG} must not contain empty partition ids'
            )
        try:
            normalized_partition_id = date.fromisoformat(partition_id).isoformat()
        except ValueError as exc:
            raise RuntimeError(
                f'{BACKFILL_PARTITION_IDS_TAG} must contain ISO dates, got {partition_id!r}'
            ) from exc
        if normalized_partition_id in seen:
            continue
        seen.add(normalized_partition_id)
        partition_ids.append(normalized_partition_id)
    if partition_ids == []:
        raise RuntimeError(
            f'{BACKFILL_PARTITION_IDS_TAG} must contain at least one partition id'
        )
    return tuple(sorted(partition_ids))


def _resolve_source_window_for_partition_ids(
    partition_ids: tuple[str, ...] | None,
) -> _SourceWindow:
    if partition_ids is None or partition_ids == ():
        return _SourceWindow(observation_start=None, observation_end=None)
    parsed = [date.fromisoformat(partition_id) for partition_id in partition_ids]
    return _SourceWindow(
        observation_start=min(parsed),
        observation_end=max(parsed),
    )


def _resolve_source_partition_scope_ids_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    runtime_contract: BackfillRuntimeContract,
    explicit_partition_ids: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    if explicit_partition_ids is not None:
        return explicit_partition_ids
    if runtime_contract.execution_mode != 'reconcile':
        return None
    ambiguous_partition_ids = _load_ambiguous_partition_ids_or_raise(
        client=client,
        database=database,
    )
    if ambiguous_partition_ids == ():
        raise RuntimeError('FRED reconcile requested but no ambiguous partitions exist')
    return select_fred_reconcile_partition_ids_from_env_or_raise(
        ambiguous_partition_ids=ambiguous_partition_ids
    )


def _count_partition_rows_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
) -> dict[str, int]:
    rows = client.execute(
        f'''
        SELECT
            (
                SELECT count()
                FROM {database}.{CANONICAL_EVENT_LOG_READ_TABLE}
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
            ) AS canonical_event_rows,
            (
                SELECT count()
                FROM {database}.canonical_fred_series_metrics_native_v1
                WHERE toDate(observed_at_utc) = toDate(%(partition_id)s)
            ) AS native_projection_rows,
            (
                SELECT count()
                FROM {database}.canonical_aligned_1s_aggregates
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
            ) AS aligned_projection_rows,
            (
                SELECT count()
                FROM {database}.canonical_projector_checkpoints
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
                  AND projector_id IN %(projector_ids)s
            ) AS projector_checkpoint_rows,
            (
                SELECT count()
                FROM {database}.canonical_projector_watermarks
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
                  AND projector_id IN %(projector_ids)s
            ) AS projector_watermark_rows
        ''',
        {
            'source_id': _CANONICAL_SOURCE_ID,
            'stream_id': _CANONICAL_STREAM_ID,
            'partition_id': partition_id,
            'projector_ids': (
                _FRED_NATIVE_PROJECTOR_ID,
                _FRED_ALIGNED_PROJECTOR_ID,
            ),
        },
    )
    if len(rows) != 1:
        raise RuntimeError(
            'FRED partition reset count query returned unexpected row count: '
            f'partition_id={partition_id} rows={len(rows)}'
        )
    row = rows[0]
    return {
        'canonical_event_rows': int(row[0]),
        'native_projection_rows': int(row[1]),
        'aligned_projection_rows': int(row[2]),
        'projector_checkpoint_rows': int(row[3]),
        'projector_watermark_rows': int(row[4]),
    }


def _delete_partition_rows_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
) -> None:
    delete_settings = {'mutations_sync': 2}
    params = {
        'source_id': _CANONICAL_SOURCE_ID,
        'stream_id': _CANONICAL_STREAM_ID,
        'partition_id': partition_id,
        'projector_ids': (
            _FRED_NATIVE_PROJECTOR_ID,
            _FRED_ALIGNED_PROJECTOR_ID,
        ),
    }
    client.execute(
        f'''
        ALTER TABLE {database}.canonical_fred_series_metrics_native_v1
        DELETE WHERE toDate(observed_at_utc) = toDate(%(partition_id)s)
        ''',
        params,
        settings=delete_settings,
    )
    client.execute(
        f'''
        ALTER TABLE {database}.canonical_aligned_1s_aggregates
        DELETE WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
        ''',
        params,
        settings=delete_settings,
    )
    client.execute(
        f'''
        ALTER TABLE {database}.canonical_projector_checkpoints
        DELETE WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
          AND projector_id IN %(projector_ids)s
        ''',
        params,
        settings=delete_settings,
    )
    client.execute(
        f'''
        ALTER TABLE {database}.canonical_projector_watermarks
        DELETE WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
          AND projector_id IN %(projector_ids)s
        ''',
        params,
        settings=delete_settings,
    )


def _reset_fred_partition_for_reconcile_or_raise(
    *,
    context: OpExecutionContext,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    source_proof: Any,
    state_store: CanonicalBackfillStateStore,
    reset_reason: str,
    reset_details: dict[str, Any],
) -> dict[str, int]:
    recorded_at_utc = datetime.now(UTC)
    counts_before_reset = _count_partition_rows_or_raise(
        client=client,
        database=database,
        partition_id=partition_id,
    )
    state_store.record_partition_state(
        source_proof=source_proof,
        state='reconcile_required',
        reason=reset_reason,
        run_id=context.run_id,
        recorded_at_utc=recorded_at_utc,
        proof_details=reset_details,
    )
    state_store.record_partition_reset_boundary(
        stream_key=source_proof.stream_key,
        reason=reset_reason,
        details=reset_details,
        run_id=context.run_id,
        recorded_at_utc=recorded_at_utc,
    )
    _delete_partition_rows_or_raise(
        client=client,
        database=database,
        partition_id=partition_id,
    )
    counts_after_reset = _count_partition_rows_or_raise(
        client=client,
        database=database,
        partition_id=partition_id,
    )
    if any(count != 0 for count in counts_after_reset.values()):
        raise RuntimeError(
            'FRED reconcile partition reset did not clear partition state fully: '
            + json.dumps(
                {
                    'partition_id': partition_id,
                    'counts_after_reset': counts_after_reset,
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    return counts_before_reset


def _build_persisted_bundles_or_raise(
    *,
    context: OpExecutionContext,
    source_partition_ids: tuple[str, ...] | None,
) -> list[_PersistedFREDBundle]:
    registry_version, registry_entries = load_fred_series_registry()
    source_window = _resolve_source_window_for_partition_ids(source_partition_ids)
    context.log.info(
        'FRED raw bundle source window '
        + json.dumps(
            {
                'execution_mode': context.run.tags.get('origo.backfill.execution_mode'),
                'partition_scope_count': (
                    0 if source_partition_ids is None else len(source_partition_ids)
                ),
                'observation_start': (
                    None
                    if source_window.observation_start is None
                    else source_window.observation_start.isoformat()
                ),
                'observation_end': (
                    None
                    if source_window.observation_end is None
                    else source_window.observation_end.isoformat()
                ),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    fred_client = build_fred_client_from_env()
    bundles = build_fred_raw_bundles(
        client=fred_client,
        registry_entries=registry_entries,
        registry_version=registry_version,
        observation_start=source_window.observation_start,
        observation_end=source_window.observation_end,
        observations_mode='revision_history',
    )
    persisted_artifacts = persist_fred_raw_bundles_to_object_store(
        bundles=bundles,
        run_id=f'fred-daily-{context.run_id}',
        run_started_at_utc=datetime.now(UTC),
    )
    if len(bundles) != len(persisted_artifacts):
        raise RuntimeError(
            'FRED raw bundle persistence count mismatch: '
            f'bundles={len(bundles)} persisted={len(persisted_artifacts)}'
        )
    return [
        _PersistedFREDBundle(
            bundle=bundle,
            persisted_artifact=persisted_artifact,
            content_sha256=fred_raw_bundle_content_sha256(bundle),
        )
        for bundle, persisted_artifact in zip(
            bundles,
            persisted_artifacts,
            strict=True,
        )
    ]


def _normalize_partition_rows_or_raise(
    *, persisted_bundles: list[_PersistedFREDBundle]
) -> dict[str, list[FREDLongMetricRow]]:
    if persisted_bundles == []:
        raise RuntimeError('persisted_bundles must be non-empty')
    registry_version, registry_entries = load_fred_series_registry()
    rows = normalize_fred_raw_bundles_to_long_metrics_or_raise(
        bundles=[item.bundle for item in persisted_bundles],
        registry_entries=registry_entries,
        registry_version=registry_version,
    )
    if rows == []:
        raise RuntimeError('FRED raw bundles normalized to zero rows')
    partition_rows: dict[str, list[FREDLongMetricRow]] = defaultdict(list)
    for row in rows:
        partition_rows[row.observed_at_utc.date().isoformat()].append(row)
    return dict(partition_rows)


def _load_ambiguous_partition_ids_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
) -> tuple[str, ...]:
    return load_nonterminal_partition_ids_for_stream_or_raise(
        client=client,
        database=database,
        source_id=_CANONICAL_SOURCE_ID,
        stream_id=_CANONICAL_STREAM_ID,
        terminal_states=_TERMINAL_PARTITION_STATES,
    )


def _resolve_partition_ids_to_process_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    runtime_contract: BackfillRuntimeContract,
    source_partition_rows: dict[str, list[FREDLongMetricRow]],
    source_partition_scope_ids: tuple[str, ...] | None,
    explicit_partition_ids: tuple[str, ...] | None,
) -> tuple[str, ...]:
    available_partition_ids = tuple(sorted(source_partition_rows))
    if available_partition_ids == ():
        raise RuntimeError('FRED source history produced zero available partitions')

    if explicit_partition_ids is not None:
        missing = [
            partition_id
            for partition_id in explicit_partition_ids
            if partition_id not in source_partition_rows
        ]
        if missing != []:
            raise RuntimeError(
                'Requested FRED partition ids were not present in source history: '
                f'count={len(missing)} preview={missing[:10]}'
            )
        return explicit_partition_ids

    if runtime_contract.execution_mode == 'reconcile':
        if source_partition_scope_ids is None or source_partition_scope_ids == ():
            raise RuntimeError(
                'FRED reconcile requested but no bounded partition scope was resolved'
            )
        missing = [
            partition_id
            for partition_id in source_partition_scope_ids
            if partition_id not in source_partition_rows
        ]
        if missing != []:
            raise RuntimeError(
                'Bounded FRED reconcile partitions are missing from source history: '
                f'count={len(missing)} preview={missing[:10]}'
            )
        return source_partition_scope_ids

    return available_partition_ids


def _filter_terminal_partitions_for_backfill_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: tuple[str, ...],
) -> tuple[str, ...]:
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    filtered: list[str] = []
    for partition_id in partition_ids:
        assessment = state_store.assess_partition_execution(
            stream_key=CanonicalStreamKey(
                source_id=_CANONICAL_SOURCE_ID,
                stream_id=_CANONICAL_STREAM_ID,
                partition_id=partition_id,
            )
        )
        if assessment.latest_proof_state in _TERMINAL_PARTITION_STATES:
            continue
        filtered.append(partition_id)
    return tuple(filtered)


def _canonical_payload_sha256(row: FREDLongMetricRow) -> str:
    payload_raw = json.dumps(
        build_fred_canonical_payload(row=row),
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode(_PAYLOAD_ENCODING)
    return hashlib.sha256(payload_raw).hexdigest()


def _build_partition_source_proof_or_raise(
    *,
    partition_id: str,
    rows: list[FREDLongMetricRow],
    persisted_bundles_by_source_id: dict[str, _PersistedFREDBundle],
) -> Any:
    if rows == []:
        raise RuntimeError(
            f'FRED source proof requires non-empty rows for partition_id={partition_id}'
        )
    source_row_counts: dict[str, int] = defaultdict(int)
    materials: list[SourceIdentityMaterial] = []
    for row in sorted(
        rows,
        key=lambda item: (
            item.source_id,
            item.metric_name,
            item.metric_id,
        ),
    ):
        source_row_counts[row.source_id] += 1
        idempotency_key = canonical_event_idempotency_key(
            source_id=_CANONICAL_SOURCE_ID,
            stream_id=_CANONICAL_STREAM_ID,
            partition_id=partition_id,
            source_offset_or_equivalent=row.metric_id,
        )
        materials.append(
            SourceIdentityMaterial(
                source_offset_or_equivalent=row.metric_id,
                event_id=str(canonical_event_id_from_key(idempotency_key)),
                payload_sha256_raw=_canonical_payload_sha256(row),
            )
        )

    artifacts: list[dict[str, Any]] = []
    for source_id in sorted(source_row_counts):
        persisted_bundle = persisted_bundles_by_source_id.get(source_id)
        if persisted_bundle is None:
            raise RuntimeError(
                'FRED partition references source with no persisted raw bundle: '
                f'partition_id={partition_id} source_id={source_id}'
            )
        artifacts.append(
            {
                'source_id': persisted_bundle.bundle.source_id,
                'series_id': persisted_bundle.bundle.series_id,
                'source_uri': persisted_bundle.bundle.source_uri,
                'registry_version': persisted_bundle.bundle.registry_version,
                'artifact_id': persisted_bundle.persisted_artifact.artifact_id,
                'artifact_sha256': persisted_bundle.content_sha256,
                'storage_uri': persisted_bundle.persisted_artifact.storage_uri,
                'manifest_uri': persisted_bundle.persisted_artifact.manifest_uri,
                'fetched_at_utc': persisted_bundle.bundle.fetched_at_utc.isoformat(),
                'persisted_at_utc': persisted_bundle.persisted_artifact.persisted_at_utc.isoformat(),
                'partition_row_count': source_row_counts[source_id],
            }
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


def _execute_partition_backfill_or_raise(
    *,
    context: OpExecutionContext,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    rows: list[FREDLongMetricRow],
    persisted_bundles_by_source_id: dict[str, _PersistedFREDBundle],
    runtime_contract: BackfillRuntimeContract,
) -> _PartitionBackfillResult:
    source_proof = _build_partition_source_proof_or_raise(
        partition_id=partition_id,
        rows=rows,
        persisted_bundles_by_source_id=persisted_bundles_by_source_id,
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
    except Exception as exc:
        if (
            runtime_contract.execution_mode == 'backfill'
            and getattr(exc, 'code', None) == 'RECONCILE_REQUIRED'
        ):
            state_store.record_partition_state(
                source_proof=source_proof,
                state='reconcile_required',
                reason='backfill_execution_requires_reconcile',
                run_id=context.run_id,
                recorded_at_utc=datetime.now(UTC),
                proof_details={'trigger_message': str(exc)},
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
            source_proof=source_proof,
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
        try:
            write_summary = write_fred_long_metrics_to_canonical(
                client=client,
                database=database,
                rows=rows,
                run_id=context.run_id,
                ingested_at_utc=datetime.now(UTC),
            ).to_dict()
            if reconcile_existing_canonical_rows:
                write_path = 'reconcile_writer_repair'
                proof_reason = 'reconcile_writer_repair_completed'
            else:
                write_path = 'writer'
                proof_reason = 'canonical_write_completed'
        except EventWriterError as exc:
            if (
                not reconcile_existing_canonical_rows
                or runtime_contract.execution_mode != 'reconcile'
                or exc.code not in _WRITER_RESETTABLE_CONFLICT_CODES
            ):
                raise
            reset_summary = _reset_fred_partition_for_reconcile_or_raise(
                context=context,
                client=client,
                database=database,
                partition_id=partition_id,
                source_proof=source_proof,
                state_store=state_store,
                reset_reason='legacy_request_time_canonical_reset_required',
                reset_details={
                    'writer_error_code': exc.code,
                    'writer_error_message': exc.message,
                },
            )
            context.log.warning(
                'FRED reconcile reset legacy canonical partition rows before rewrite: '
                + json.dumps(
                    {
                        'partition_id': partition_id,
                        'writer_error_code': exc.code,
                        'writer_error_message': exc.message,
                        'counts_before_reset': reset_summary,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
            write_summary = write_fred_long_metrics_to_canonical(
                client=client,
                database=database,
                rows=rows,
                run_id=context.run_id,
                ingested_at_utc=datetime.now(UTC),
            ).to_dict()
            write_path = 'reconcile_partition_reset_rewrite'
            proof_reason = 'reconcile_partition_reset_rewrite_completed'

    rows_processed = int(write_summary['rows_processed'])
    rows_inserted = int(write_summary['rows_inserted'])
    rows_duplicate = int(write_summary['rows_duplicate'])
    if rows_processed != len(rows):
        raise RuntimeError(
            'FRED canonical writer summary mismatch: '
            f'rows_processed={rows_processed} expected={len(rows)} '
            f'partition_id={partition_id}'
        )
    if rows_inserted + rows_duplicate != rows_processed:
        raise RuntimeError(
            'FRED canonical writer summary mismatch: '
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
    proof_input = (
        current_canonical_proof if write_path == 'reconcile_proof_only' else None
    )
    try:
        partition_proof = state_store.prove_partition_or_quarantine(
            source_proof=source_proof,
            run_id=context.run_id,
            recorded_at_utc=proof_recorded_at_utc,
            canonical_proof=proof_input,
        )
    except ReconciliationError as exc:
        if (
            not reconcile_existing_canonical_rows
            or runtime_contract.execution_mode != 'reconcile'
            or write_path == 'reconcile_proof_only'
            or exc.code != 'BACKFILL_PARTITION_PROOF_FAILED'
        ):
            raise
        latest_partition_proof = state_store.fetch_latest_partition_proof(
            stream_key=source_proof.stream_key
        )
        if latest_partition_proof is None:
            raise RuntimeError(
                'FRED reconcile proof failure did not leave a latest proof row: '
                f'partition_id={partition_id}'
            ) from exc
        if latest_partition_proof.state != 'quarantined':
            raise RuntimeError(
                'FRED reconcile proof failure expected quarantined latest proof state, '
                f'got state={latest_partition_proof.state} partition_id={partition_id}'
            ) from exc
        partition_proof = latest_partition_proof
    if (
        reconcile_existing_canonical_rows
        and runtime_contract.execution_mode == 'reconcile'
        and write_path != 'reconcile_proof_only'
        and partition_proof.state != 'proved_complete'
    ):
        reset_summary = _reset_fred_partition_for_reconcile_or_raise(
            context=context,
            client=client,
            database=database,
            partition_id=partition_id,
            source_proof=source_proof,
            state_store=state_store,
            reset_reason='legacy_request_time_canonical_reset_required',
            reset_details={
                'quarantine_reason': partition_proof.reason,
                'quarantine_state': partition_proof.state,
                'quarantine_proof_digest_sha256': partition_proof.proof_digest_sha256,
            },
        )
        context.log.warning(
            'FRED reconcile reset stale canonical partition rows after proof mismatch: '
            + json.dumps(
                {
                    'partition_id': partition_id,
                    'initial_write_path': write_path,
                    'quarantine_reason': partition_proof.reason,
                    'counts_before_reset': reset_summary,
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
        write_summary = write_fred_long_metrics_to_canonical(
            client=client,
            database=database,
            rows=rows,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        ).to_dict()
        write_path = 'reconcile_partition_reset_rewrite'
        proof_reason = 'reconcile_partition_reset_rewrite_completed'
        rows_processed = int(write_summary['rows_processed'])
        rows_inserted = int(write_summary['rows_inserted'])
        rows_duplicate = int(write_summary['rows_duplicate'])
        if rows_processed != len(rows):
            raise RuntimeError(
                'FRED canonical writer summary mismatch after partition reset rewrite: '
                f'rows_processed={rows_processed} expected={len(rows)} '
                f'partition_id={partition_id}'
            )
        if rows_inserted + rows_duplicate != rows_processed:
            raise RuntimeError(
                'FRED canonical writer summary mismatch after partition reset rewrite: '
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
        partition_proof = state_store.prove_partition_or_quarantine(
            source_proof=source_proof,
            run_id=context.run_id,
            recorded_at_utc=proof_recorded_at_utc,
        )

    projected_at_utc = datetime.now(UTC)
    if runtime_contract.projection_mode == 'inline':
        native_projection_summary = project_fred_series_metrics_native(
            client=client,
            database=database,
            partition_ids=[partition_id],
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        ).to_dict()
        aligned_projection_summary = project_fred_series_metrics_aligned(
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


def _run_fred_backfill_or_raise(*, context: OpExecutionContext) -> _BackfillRunSummary:
    runtime_contract = load_backfill_runtime_contract_from_tags_or_raise(
        context.run.tags,
    )
    explicit_partition_ids = _load_required_partition_ids_from_tags_or_none(
        context.run.tags
    )
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    native_client, database = _build_clickhouse_client_or_raise()
    try:
        source_partition_scope_ids = _resolve_source_partition_scope_ids_or_raise(
            client=native_client,
            database=database,
            runtime_contract=runtime_contract,
            explicit_partition_ids=explicit_partition_ids,
        )
        persisted_bundles = _build_persisted_bundles_or_raise(
            context=context,
            source_partition_ids=source_partition_scope_ids,
        )
        persisted_bundles_by_source_id = {
            item.bundle.source_id: item for item in persisted_bundles
        }
        if len(persisted_bundles_by_source_id) != len(persisted_bundles):
            raise RuntimeError('FRED raw bundles must have unique source_id values')
        partition_rows = _normalize_partition_rows_or_raise(
            persisted_bundles=persisted_bundles
        )
        partition_ids_to_process = _resolve_partition_ids_to_process_or_raise(
            client=native_client,
            database=database,
            runtime_contract=runtime_contract,
            source_partition_rows=partition_rows,
            source_partition_scope_ids=source_partition_scope_ids,
            explicit_partition_ids=explicit_partition_ids,
        )
        if runtime_contract.execution_mode == 'backfill':
            partition_ids_to_process = _filter_terminal_partitions_for_backfill_or_raise(
                client=native_client,
                database=database,
                partition_ids=partition_ids_to_process,
            )

        partition_results = [
            _execute_partition_backfill_or_raise(
                context=context,
                client=native_client,
                database=database,
                partition_id=partition_id,
                rows=partition_rows[partition_id],
                persisted_bundles_by_source_id=persisted_bundles_by_source_id,
                runtime_contract=runtime_contract,
            )
            for partition_id in partition_ids_to_process
        ]
    finally:
        native_client.disconnect()

    return _BackfillRunSummary(
        total_series=len(persisted_bundles),
        total_source_partitions=len(partition_rows),
        total_normalized_records=sum(
            len(rows) for rows in partition_rows.values()
        ),
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


@op(name='origo_fred_daily_backfill_step')
def origo_fred_daily_backfill_step(context) -> None:
    _run_fred_daily_backfill_step_or_raise(
        context=cast(OpExecutionContext, context)
    )


def _run_fred_daily_backfill_step_or_raise(*, context: OpExecutionContext) -> None:
    started_at_utc = datetime.now(UTC)
    summary = _run_fred_backfill_or_raise(context=context)
    context.add_output_metadata(
        {
            'run_id': f'fred-daily-{context.run_id}',
            'started_at_utc': started_at_utc.isoformat(),
            'total_series': summary.total_series,
            'total_source_partitions': summary.total_source_partitions,
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


@job(name='origo_fred_daily_backfill_job')
def origo_fred_daily_backfill_job() -> None:
    origo_fred_daily_backfill_step()
