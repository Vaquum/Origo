from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events import (
    CanonicalStreamKey,
    PartitionProofState,
    QuarantineState,
    RangeProofState,
    SourceManifestState,
)
from origo.events.backfill_state import BackfillPartitionState, QuarantineStatus
from origo.events.ingest_state import OffsetOrdering
from origo_control_plane.backfill import (
    assert_s34_backfill_contract_consistency_or_raise,
    list_s34_dataset_contracts,
    load_backfill_manifest_log_path,
)
from origo_control_plane.backfill.s34_contract import S34DatasetBackfillContract
from origo_control_plane.config import resolve_clickhouse_native_settings

_TERMINAL_PROOF_STATES = {'proved_complete', 'empty_proved'}
_SLICE_DIR = Path('spec/slices/slice-34-full-canonical-backfill')
_OUTPUT_PATH = _SLICE_DIR / 'proof-s34-g1-g2-closeout-prep.json'


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_clickhouse_client_or_raise() -> tuple[ClickHouseClient, str]:
    settings = resolve_clickhouse_native_settings()
    return (
        ClickHouseClient(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            database=settings.database,
            compression=True,
            send_receive_timeout=900,
        ),
        settings.database,
    )


def _load_json_object_or_raise(*, payload: str, label: str) -> dict[str, Any]:
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'{label} must be valid JSON object text') from exc
    if not isinstance(loaded, dict):
        raise RuntimeError(f'{label} must decode to JSON object, got {type(loaded).__name__}')
    return cast(dict[str, Any], loaded)


def _partition_key(source_id: str, stream_id: str, partition_id: str) -> tuple[str, str, str]:
    return (source_id, stream_id, partition_id)


def _stream_key(source_id: str, stream_id: str) -> tuple[str, str]:
    return (source_id, stream_id)


def _sql_string_literal_or_raise(value: str) -> str:
    if value == '':
        raise RuntimeError('S34 stream filter cannot contain empty SQL literal values')
    return "'" + value.replace("'", "''") + "'"


def _render_s34_stream_pair_in_clause_or_raise(
    *,
    source_field: str,
    stream_field: str,
) -> str:
    stream_pairs = sorted(
        {
            (contract.source_id, contract.stream_id)
            for contract in list_s34_dataset_contracts()
        }
    )
    if stream_pairs == []:
        raise RuntimeError('S34 closeout prep requires at least one configured stream pair')
    rendered_pairs = ', '.join(
        (
            '('
            f'{_sql_string_literal_or_raise(source_id)}, '
            f'{_sql_string_literal_or_raise(stream_id)}'
            ')'
        )
        for source_id, stream_id in stream_pairs
    )
    return (
        f'({source_field}, {stream_field}) IN ({rendered_pairs})'
    )


def _sort_partition_ids_or_raise(
    contract: S34DatasetBackfillContract,
    partition_ids: list[str],
) -> list[str]:
    if contract.partition_scheme == 'daily':
        try:
            return [
                value.isoformat()
                for value in sorted(date.fromisoformat(partition_id) for partition_id in partition_ids)
            ]
        except ValueError as exc:
            raise RuntimeError(
                f'S34 closeout prep expected YYYY-MM-DD partition ids for dataset={contract.dataset}'
            ) from exc
    return sorted(partition_ids)


def _load_manifest_event_summary_or_raise(
    *,
    path: Path,
    known_datasets: set[str],
) -> dict[str, Any]:
    empty: dict[str, Any] = {
        'path': str(path),
        'exists': path.is_file(),
        'event_counts_by_dataset': {dataset: {} for dataset in sorted(known_datasets)},
        'latest_partition_event_by_dataset': {},
        'latest_range_event_by_dataset': {},
    }
    if not path.is_file():
        return empty

    event_counts: dict[str, defaultdict[str, int]] = {
        dataset: defaultdict(int) for dataset in sorted(known_datasets)
    }
    latest_partition_event_by_dataset: dict[str, dict[str, Any]] = {}
    latest_range_event_by_dataset: dict[str, dict[str, Any]] = {}

    with path.open('r', encoding='utf-8') as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if line == '':
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f'Backfill manifest log line {line_number} is not valid JSON'
                ) from exc
            if not isinstance(event, dict):
                raise RuntimeError(
                    f'Backfill manifest log line {line_number} must decode to JSON object'
                )
            event_object = cast(dict[str, object], event)
            dataset = event_object.get('dataset')
            event_type = event_object.get('event_type')
            if not isinstance(dataset, str) or dataset.strip() == '':
                raise RuntimeError(
                    f'Backfill manifest log line {line_number} is missing non-empty dataset'
                )
            if dataset not in known_datasets:
                raise RuntimeError(
                    f'Backfill manifest log line {line_number} references unknown dataset={dataset!r}'
                )
            if not isinstance(event_type, str) or event_type.strip() == '':
                raise RuntimeError(
                    f'Backfill manifest log line {line_number} is missing non-empty event_type'
                )
            event_counts[dataset][event_type] += 1
            if event_type == 's34_backfill_partition_completed':
                latest_partition_event_by_dataset[dataset] = cast(dict[str, Any], event)
            elif event_type == 's34_backfill_range_proved':
                latest_range_event_by_dataset[dataset] = cast(dict[str, Any], event)

    return {
        'path': str(path),
        'exists': True,
        'event_counts_by_dataset': {
            dataset: dict(sorted(event_counts[dataset].items()))
            for dataset in sorted(known_datasets)
        },
        'latest_partition_event_by_dataset': latest_partition_event_by_dataset,
        'latest_range_event_by_dataset': latest_range_event_by_dataset,
    }


def _fetch_latest_source_manifests_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[tuple[str, str, str], SourceManifestState]:
    stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='manifests.source_id',
        stream_field='manifests.stream_id',
    )
    latest_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT
            manifests.source_id,
            manifests.stream_id,
            manifests.partition_id,
            manifests.manifest_revision,
            manifests.manifest_id,
            manifests.offset_ordering,
            manifests.source_artifact_identity_json,
            manifests.source_row_count,
            manifests.first_offset_or_equivalent,
            manifests.last_offset_or_equivalent,
            manifests.source_offset_digest_sha256,
            manifests.source_identity_digest_sha256,
            manifests.allow_empty_partition,
            manifests.manifested_by_run_id,
            manifests.manifested_at_utc
        FROM {database}.canonical_backfill_source_manifests AS manifests
        INNER JOIN
        (
            SELECT
                source_id,
                stream_id,
                partition_id,
                max(manifest_revision) AS manifest_revision
            FROM {database}.canonical_backfill_source_manifests
            WHERE {latest_stream_filter}
            GROUP BY source_id, stream_id, partition_id
        ) AS latest
        ON manifests.source_id = latest.source_id
        AND manifests.stream_id = latest.stream_id
        AND manifests.partition_id = latest.partition_id
        AND manifests.manifest_revision = latest.manifest_revision
        WHERE {stream_filter}
        ORDER BY manifests.source_id, manifests.stream_id, manifests.partition_id
        '''
        ),
    )
    manifests: dict[tuple[str, str, str], SourceManifestState] = {}
    for row in rows:
        state = SourceManifestState(
            stream_key=CanonicalStreamKey(
                source_id=str(row[0]),
                stream_id=str(row[1]),
                partition_id=str(row[2]),
            ),
            manifest_revision=int(row[3]),
            manifest_id=row[4],
            offset_ordering=cast(OffsetOrdering, row[5]),
            source_artifact_identity_json=str(row[6]),
            source_row_count=int(row[7]),
            first_offset_or_equivalent=(None if row[8] is None else str(row[8])),
            last_offset_or_equivalent=(None if row[9] is None else str(row[9])),
            source_offset_digest_sha256=str(row[10]),
            source_identity_digest_sha256=str(row[11]),
            allow_empty_partition=bool(row[12]),
            manifested_by_run_id=str(row[13]),
            manifested_at_utc=row[14],
        )
        manifests[_partition_key(str(row[0]), str(row[1]), str(row[2]))] = state
    return manifests


def _fetch_latest_partition_proofs_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[tuple[str, str, str], PartitionProofState]:
    stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='proofs.source_id',
        stream_field='proofs.stream_id',
    )
    latest_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT
            proofs.source_id,
            proofs.stream_id,
            proofs.partition_id,
            proofs.proof_revision,
            proofs.proof_id,
            proofs.state,
            proofs.reason,
            proofs.offset_ordering,
            proofs.source_row_count,
            proofs.canonical_row_count,
            proofs.canonical_unique_offset_count,
            proofs.first_offset_or_equivalent,
            proofs.last_offset_or_equivalent,
            proofs.source_offset_digest_sha256,
            proofs.source_identity_digest_sha256,
            proofs.canonical_offset_digest_sha256,
            proofs.canonical_identity_digest_sha256,
            proofs.gap_count,
            proofs.duplicate_count,
            proofs.proof_digest_sha256,
            proofs.proof_details_json,
            proofs.recorded_by_run_id,
            proofs.recorded_at_utc
        FROM {database}.canonical_backfill_partition_proofs AS proofs
        INNER JOIN
        (
            SELECT
                source_id,
                stream_id,
                partition_id,
                max(proof_revision) AS proof_revision
            FROM {database}.canonical_backfill_partition_proofs
            WHERE {latest_stream_filter}
            GROUP BY source_id, stream_id, partition_id
        ) AS latest
        ON proofs.source_id = latest.source_id
        AND proofs.stream_id = latest.stream_id
        AND proofs.partition_id = latest.partition_id
        AND proofs.proof_revision = latest.proof_revision
        WHERE {stream_filter}
        ORDER BY proofs.source_id, proofs.stream_id, proofs.partition_id
        '''
        ),
    )
    proofs: dict[tuple[str, str, str], PartitionProofState] = {}
    for row in rows:
        state = PartitionProofState(
            stream_key=CanonicalStreamKey(
                source_id=str(row[0]),
                stream_id=str(row[1]),
                partition_id=str(row[2]),
            ),
            proof_revision=int(row[3]),
            proof_id=row[4],
            state=cast(BackfillPartitionState, row[5]),
            reason=str(row[6]),
            offset_ordering=cast(OffsetOrdering, row[7]),
            source_row_count=int(row[8]),
            canonical_row_count=int(row[9]),
            canonical_unique_offset_count=int(row[10]),
            first_offset_or_equivalent=(None if row[11] is None else str(row[11])),
            last_offset_or_equivalent=(None if row[12] is None else str(row[12])),
            source_offset_digest_sha256=str(row[13]),
            source_identity_digest_sha256=str(row[14]),
            canonical_offset_digest_sha256=str(row[15]),
            canonical_identity_digest_sha256=str(row[16]),
            gap_count=int(row[17]),
            duplicate_count=int(row[18]),
            proof_digest_sha256=str(row[19]),
            proof_details_json=str(row[20]),
            recorded_by_run_id=str(row[21]),
            recorded_at_utc=row[22],
        )
        proofs[_partition_key(str(row[0]), str(row[1]), str(row[2]))] = state
    return proofs


def _fetch_latest_quarantines_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[tuple[str, str, str], QuarantineState]:
    stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='quarantines.source_id',
        stream_field='quarantines.stream_id',
    )
    latest_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT
            quarantines.source_id,
            quarantines.stream_id,
            quarantines.partition_id,
            quarantines.quarantine_revision,
            quarantines.status,
            quarantines.reason,
            quarantines.details_json,
            quarantines.recorded_by_run_id,
            quarantines.recorded_at_utc
        FROM {database}.canonical_quarantined_streams AS quarantines
        INNER JOIN
        (
            SELECT
                source_id,
                stream_id,
                partition_id,
                max(quarantine_revision) AS quarantine_revision
            FROM {database}.canonical_quarantined_streams
            WHERE {latest_stream_filter}
            GROUP BY source_id, stream_id, partition_id
        ) AS latest
        ON quarantines.source_id = latest.source_id
        AND quarantines.stream_id = latest.stream_id
        AND quarantines.partition_id = latest.partition_id
        AND quarantines.quarantine_revision = latest.quarantine_revision
        WHERE {stream_filter}
        ORDER BY quarantines.source_id, quarantines.stream_id, quarantines.partition_id
        '''
        ),
    )
    quarantines: dict[tuple[str, str, str], QuarantineState] = {}
    for row in rows:
        state = QuarantineState(
            stream_key=CanonicalStreamKey(
                source_id=str(row[0]),
                stream_id=str(row[1]),
                partition_id=str(row[2]),
            ),
            quarantine_revision=int(row[3]),
            status=cast(QuarantineStatus, row[4]),
            reason=str(row[5]),
            details_json=str(row[6]),
            recorded_by_run_id=str(row[7]),
            recorded_at_utc=row[8],
        )
        quarantines[_partition_key(str(row[0]), str(row[1]), str(row[2]))] = state
    return quarantines


def _fetch_latest_range_proofs_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[tuple[str, str], list[RangeProofState]]:
    stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='proofs.source_id',
        stream_field='proofs.stream_id',
    )
    latest_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT
            proofs.source_id,
            proofs.stream_id,
            proofs.range_start_partition_id,
            proofs.range_end_partition_id,
            proofs.range_revision,
            proofs.range_proof_id,
            proofs.partition_count,
            proofs.range_digest_sha256,
            proofs.range_details_json,
            proofs.recorded_by_run_id,
            proofs.recorded_at_utc
        FROM {database}.canonical_backfill_range_proofs AS proofs
        INNER JOIN
        (
            SELECT
                source_id,
                stream_id,
                range_start_partition_id,
                range_end_partition_id,
                max(range_revision) AS range_revision
            FROM {database}.canonical_backfill_range_proofs
            WHERE {latest_stream_filter}
            GROUP BY source_id, stream_id, range_start_partition_id, range_end_partition_id
        ) AS latest
        ON proofs.source_id = latest.source_id
        AND proofs.stream_id = latest.stream_id
        AND proofs.range_start_partition_id = latest.range_start_partition_id
        AND proofs.range_end_partition_id = latest.range_end_partition_id
        AND proofs.range_revision = latest.range_revision
        WHERE {stream_filter}
        ORDER BY proofs.source_id, proofs.stream_id, proofs.range_start_partition_id, proofs.range_end_partition_id
        '''
        ),
    )
    grouped: dict[tuple[str, str], list[RangeProofState]] = defaultdict(list)
    for row in rows:
        grouped[_stream_key(str(row[0]), str(row[1]))].append(
            RangeProofState(
                source_id=str(row[0]),
                stream_id=str(row[1]),
                range_start_partition_id=str(row[2]),
                range_end_partition_id=str(row[3]),
                range_revision=int(row[4]),
                range_proof_id=row[5],
                partition_count=int(row[6]),
                range_digest_sha256=str(row[7]),
                range_details_json=str(row[8]),
                recorded_by_run_id=str(row[9]),
                recorded_at_utc=row[10],
            )
        )
    return {key: value for key, value in grouped.items()}


def _fetch_ambiguous_canonical_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[tuple[str, str], list[str]]:
    event_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    proof_stream_filter = _render_s34_stream_pair_in_clause_or_raise(
        source_field='source_id',
        stream_field='stream_id',
    )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT
            event_partitions.source_id,
            event_partitions.stream_id,
            event_partitions.partition_id
        FROM
        (
            SELECT DISTINCT source_id, stream_id, partition_id
            FROM {database}.canonical_event_log
            WHERE {event_stream_filter}
        ) AS event_partitions
        LEFT JOIN
        (
            SELECT
                source_id,
                stream_id,
                partition_id,
                argMax(state, proof_revision) AS state
            FROM {database}.canonical_backfill_partition_proofs
            WHERE {proof_stream_filter}
            GROUP BY source_id, stream_id, partition_id
        ) AS proofs
        ON event_partitions.source_id = proofs.source_id
        AND event_partitions.stream_id = proofs.stream_id
        AND event_partitions.partition_id = proofs.partition_id
        WHERE proofs.state IS NULL OR proofs.state NOT IN %(terminal_states)s
        ORDER BY event_partitions.source_id, event_partitions.stream_id, event_partitions.partition_id
        ''',
        {'terminal_states': tuple(sorted(_TERMINAL_PROOF_STATES))},
        ),
    )
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in rows:
        grouped[_stream_key(str(row[0]), str(row[1]))].append(str(row[2]))
    return {key: value for key, value in grouped.items()}


def _serialize_source_manifest_or_raise(source_manifest: SourceManifestState) -> dict[str, Any]:
    return {
        'manifest_revision': source_manifest.manifest_revision,
        'manifest_id': str(source_manifest.manifest_id),
        'offset_ordering': source_manifest.offset_ordering,
        'source_artifact_identity': _load_json_object_or_raise(
            payload=source_manifest.source_artifact_identity_json,
            label='source_artifact_identity_json',
        ),
        'source_row_count': source_manifest.source_row_count,
        'first_offset_or_equivalent': source_manifest.first_offset_or_equivalent,
        'last_offset_or_equivalent': source_manifest.last_offset_or_equivalent,
        'source_offset_digest_sha256': source_manifest.source_offset_digest_sha256,
        'source_identity_digest_sha256': source_manifest.source_identity_digest_sha256,
        'allow_empty_partition': source_manifest.allow_empty_partition,
        'manifested_by_run_id': source_manifest.manifested_by_run_id,
        'manifested_at_utc': source_manifest.manifested_at_utc.isoformat(),
    }


def _serialize_partition_proof_or_raise(proof: PartitionProofState) -> dict[str, Any]:
    return {
        'proof_revision': proof.proof_revision,
        'proof_id': str(proof.proof_id),
        'state': proof.state,
        'reason': proof.reason,
        'offset_ordering': proof.offset_ordering,
        'source_row_count': proof.source_row_count,
        'canonical_row_count': proof.canonical_row_count,
        'canonical_unique_offset_count': proof.canonical_unique_offset_count,
        'first_offset_or_equivalent': proof.first_offset_or_equivalent,
        'last_offset_or_equivalent': proof.last_offset_or_equivalent,
        'source_offset_digest_sha256': proof.source_offset_digest_sha256,
        'source_identity_digest_sha256': proof.source_identity_digest_sha256,
        'canonical_offset_digest_sha256': proof.canonical_offset_digest_sha256,
        'canonical_identity_digest_sha256': proof.canonical_identity_digest_sha256,
        'gap_count': proof.gap_count,
        'duplicate_count': proof.duplicate_count,
        'proof_digest_sha256': proof.proof_digest_sha256,
        'proof_details': _load_json_object_or_raise(
            payload=proof.proof_details_json,
            label='proof_details_json',
        ),
        'recorded_by_run_id': proof.recorded_by_run_id,
        'recorded_at_utc': proof.recorded_at_utc.isoformat(),
    }


def _serialize_range_proof_or_raise(proof: RangeProofState) -> dict[str, Any]:
    return {
        'range_revision': proof.range_revision,
        'range_proof_id': str(proof.range_proof_id),
        'range_start_partition_id': proof.range_start_partition_id,
        'range_end_partition_id': proof.range_end_partition_id,
        'partition_count': proof.partition_count,
        'range_digest_sha256': proof.range_digest_sha256,
        'range_details': _load_json_object_or_raise(
            payload=proof.range_details_json,
            label='range_details_json',
        ),
        'recorded_by_run_id': proof.recorded_by_run_id,
        'recorded_at_utc': proof.recorded_at_utc.isoformat(),
    }


def _build_daily_boundary_gap_summary_or_raise(
    *,
    contract: S34DatasetBackfillContract,
    terminal_partition_ids: list[str],
    plan_end_date: date | None,
) -> tuple[dict[str, Any], list[str]]:
    if contract.partition_scheme != 'daily' or contract.earliest_partition_date is None:
        return (
            {
                'status': 'not_applicable',
                'plan_end_date': None,
                'missing_partition_count': None,
                'missing_partition_preview': [],
            },
            [],
        )
    if plan_end_date is None:
        return (
            {
                'status': 'plan_end_date_missing',
                'plan_end_date': None,
                'missing_partition_count': None,
                'missing_partition_preview': [],
            },
            [
                'plan_end_date_missing_for_daily_completeness_assessment',
            ],
        )
    if plan_end_date < contract.earliest_partition_date:
        raise RuntimeError(
            f'plan_end_date must be >= earliest_partition_date for dataset={contract.dataset}'
        )
    expected_partition_ids = [
        current.isoformat()
        for current in (
            contract.earliest_partition_date
            + (date.resolution * offset)
            for offset in range((plan_end_date - contract.earliest_partition_date).days + 1)
        )
    ]
    observed = set(terminal_partition_ids)
    missing = [partition_id for partition_id in expected_partition_ids if partition_id not in observed]
    return (
        {
            'status': 'complete' if missing == [] else 'incomplete',
            'plan_end_date': plan_end_date.isoformat(),
            'missing_partition_count': len(missing),
            'missing_partition_preview': missing[:20],
        },
        ([] if missing == [] else ['daily_terminal_partition_set_incomplete_to_plan_end_date']),
    )


def build_s34_closeout_prep_summary(
    *,
    contracts: tuple[S34DatasetBackfillContract, ...],
    manifest_summary: dict[str, Any],
    latest_source_manifests: dict[tuple[str, str, str], SourceManifestState],
    latest_partition_proofs: dict[tuple[str, str, str], PartitionProofState],
    latest_quarantines: dict[tuple[str, str, str], QuarantineState],
    latest_range_proofs: dict[tuple[str, str], list[RangeProofState]],
    ambiguous_partition_ids: dict[tuple[str, str], list[str]],
    plan_end_date: date | None,
) -> dict[str, Any]:
    dataset_summaries: list[dict[str, Any]] = []
    overall_gaps: list[dict[str, Any]] = []

    for contract in contracts:
        stream = _stream_key(contract.source_id, contract.stream_id)
        partition_proofs_for_dataset = {
            key: proof
            for key, proof in latest_partition_proofs.items()
            if key[:2] == stream
        }
        source_manifests_for_dataset = {
            key: manifest
            for key, manifest in latest_source_manifests.items()
            if key[:2] == stream
        }
        quarantines_for_dataset = {
            key: quarantine
            for key, quarantine in latest_quarantines.items()
            if key[:2] == stream
        }
        ambiguous_for_dataset = sorted(ambiguous_partition_ids.get(stream, []))
        active_quarantines = {
            key: quarantine
            for key, quarantine in quarantines_for_dataset.items()
            if quarantine.status == 'active'
        }
        proof_state_counts: dict[str, int] = defaultdict(int)
        for proof in partition_proofs_for_dataset.values():
            proof_state_counts[proof.state] += 1

        terminal_partition_ids = _sort_partition_ids_or_raise(
            contract,
            [
                key[2]
                for key, proof in partition_proofs_for_dataset.items()
                if proof.state in _TERMINAL_PROOF_STATES
            ],
        )
        latest_terminal_partition_id = (
            None if terminal_partition_ids == [] else terminal_partition_ids[-1]
        )

        latest_terminal_evidence = None
        if latest_terminal_partition_id is not None:
            partition_key = _partition_key(
                contract.source_id,
                contract.stream_id,
                latest_terminal_partition_id,
            )
            latest_terminal_manifest = source_manifests_for_dataset.get(partition_key)
            latest_terminal_proof = partition_proofs_for_dataset.get(partition_key)
            if latest_terminal_manifest is None or latest_terminal_proof is None:
                raise RuntimeError(
                    'Latest terminal partition evidence is incomplete for '
                    f'dataset={contract.dataset} partition_id={latest_terminal_partition_id}'
                )
            latest_terminal_evidence = {
                'partition_id': latest_terminal_partition_id,
                'source_manifest': _serialize_source_manifest_or_raise(
                    latest_terminal_manifest
                ),
                'partition_proof': _serialize_partition_proof_or_raise(
                    latest_terminal_proof
                ),
            }

        latest_range_proof_for_dataset = None
        if latest_range_proofs.get(stream):
            ordered_range_proofs = sorted(
                latest_range_proofs[stream],
                key=lambda item: (
                    item.recorded_at_utc,
                    item.range_start_partition_id,
                    item.range_end_partition_id,
                ),
            )
            latest_range_proof_for_dataset = _serialize_range_proof_or_raise(
                ordered_range_proofs[-1]
            )

        non_terminal_partition_ids = _sort_partition_ids_or_raise(
            contract,
            [
                key[2]
                for key, proof in partition_proofs_for_dataset.items()
                if proof.state not in _TERMINAL_PROOF_STATES
            ],
        )
        first_unresolved_partition = None
        if ambiguous_for_dataset != []:
            first_unresolved_partition = {
                'partition_id': ambiguous_for_dataset[0],
                'unresolved_kind': 'ambiguous_canonical_rows_without_terminal_proof',
            }
        elif non_terminal_partition_ids != []:
            first_partition_id = non_terminal_partition_ids[0]
            first_proof = partition_proofs_for_dataset[
                _partition_key(contract.source_id, contract.stream_id, first_partition_id)
            ]
            first_unresolved_partition = {
                'partition_id': first_partition_id,
                'unresolved_kind': 'non_terminal_partition_proof',
                'partition_proof_state': first_proof.state,
                'partition_proof_reason': first_proof.reason,
            }
        elif active_quarantines != {}:
            first_partition_id = _sort_partition_ids_or_raise(
                contract,
                [key[2] for key in active_quarantines],
            )[0]
            first_quarantine = active_quarantines[
                _partition_key(contract.source_id, contract.stream_id, first_partition_id)
            ]
            first_unresolved_partition = {
                'partition_id': first_partition_id,
                'unresolved_kind': 'active_quarantine',
                'quarantine_reason': first_quarantine.reason,
            }

        dataset_gaps: list[str] = []
        if manifest_summary['exists'] is False:
            dataset_gaps.append('manifest_log_missing')
        manifest_event_counts = cast(
            dict[str, int],
            manifest_summary['event_counts_by_dataset'].get(contract.dataset, {}),
        )
        if manifest_event_counts.get('s34_backfill_partition_completed', 0) == 0:
            dataset_gaps.append('partition_completed_manifest_events_missing')
        if manifest_event_counts.get('s34_backfill_range_proved', 0) == 0:
            dataset_gaps.append('range_proved_manifest_events_missing')
        if active_quarantines != {}:
            dataset_gaps.append('active_quarantine_present')
        if ambiguous_for_dataset != []:
            dataset_gaps.append('ambiguous_canonical_partitions_present')
        if non_terminal_partition_ids != []:
            dataset_gaps.append('non_terminal_partition_proofs_present')

        boundary_summary, boundary_gaps = _build_daily_boundary_gap_summary_or_raise(
            contract=contract,
            terminal_partition_ids=terminal_partition_ids,
            plan_end_date=plan_end_date,
        )
        dataset_gaps.extend(boundary_gaps)

        closeout_ready = dataset_gaps == []
        dataset_summary = {
            'dataset': contract.dataset,
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'phase': contract.phase,
            'partition_scheme': contract.partition_scheme,
            'aligned_capable': contract.aligned_capable,
            'proof_state_counts': dict(sorted(proof_state_counts.items())),
            'source_manifest_partition_count': len(source_manifests_for_dataset),
            'range_proof_count': len(latest_range_proofs.get(stream, [])),
            'active_quarantine_count': len(active_quarantines),
            'ambiguous_canonical_partition_count': len(ambiguous_for_dataset),
            'latest_terminal_partition_id': latest_terminal_partition_id,
            'latest_terminal_partition_evidence': latest_terminal_evidence,
            'latest_range_proof': latest_range_proof_for_dataset,
            'manifest_event_counts': manifest_event_counts,
            'latest_manifest_partition_event': manifest_summary[
                'latest_partition_event_by_dataset'
            ].get(contract.dataset),
            'latest_manifest_range_event': manifest_summary[
                'latest_range_event_by_dataset'
            ].get(contract.dataset),
            'first_unresolved_partition': first_unresolved_partition,
            'daily_boundary_assessment': boundary_summary,
            'closeout_ready': closeout_ready,
            'remaining_closeout_gaps': dataset_gaps,
        }
        dataset_summaries.append(dataset_summary)
        if dataset_gaps != []:
            overall_gaps.append(
                {
                    'dataset': contract.dataset,
                    'remaining_closeout_gaps': dataset_gaps,
                }
            )

    return {
        'proof_scope': 'Slice 34 S34-G1/S34-G2 closeout-prep summary',
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'plan_end_date': None if plan_end_date is None else plan_end_date.isoformat(),
        'manifest_log': {
            'path': manifest_summary['path'],
            'exists': manifest_summary['exists'],
        },
        'dataset_summaries': dataset_summaries,
        'remaining_closeout_gaps': overall_gaps,
    }


def run_s34_g1_g2_closeout_prep(
    *,
    plan_end_date: date | None = None,
) -> dict[str, Any]:
    assert_s34_backfill_contract_consistency_or_raise()
    contracts = list_s34_dataset_contracts()
    manifest_path = load_backfill_manifest_log_path()
    known_datasets = {contract.dataset for contract in contracts}
    manifest_summary = _load_manifest_event_summary_or_raise(
        path=manifest_path,
        known_datasets=known_datasets,
    )
    client, database = _build_clickhouse_client_or_raise()
    try:
        return build_s34_closeout_prep_summary(
            contracts=contracts,
            manifest_summary=manifest_summary,
            latest_source_manifests=_fetch_latest_source_manifests_or_raise(
                client=client,
                database=database,
            ),
            latest_partition_proofs=_fetch_latest_partition_proofs_or_raise(
                client=client,
                database=database,
            ),
            latest_quarantines=_fetch_latest_quarantines_or_raise(
                client=client,
                database=database,
            ),
            latest_range_proofs=_fetch_latest_range_proofs_or_raise(
                client=client,
                database=database,
            ),
            ambiguous_partition_ids=_fetch_ambiguous_canonical_partition_ids_or_raise(
                client=client,
                database=database,
            ),
            plan_end_date=plan_end_date,
        )
    finally:
        client.disconnect()


def main() -> None:
    payload = run_s34_g1_g2_closeout_prep()
    output_path = (_repo_root() / _OUTPUT_PATH).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
