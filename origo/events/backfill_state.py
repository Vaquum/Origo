from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal
from uuid import UUID, uuid5

from clickhouse_driver import Client as ClickHouseClient

from .errors import ReconciliationError
from .ingest_state import CanonicalStreamKey, OffsetOrdering

BackfillPartitionState = Literal[
    'source_manifested',
    'canonical_written_unproved',
    'proved_complete',
    'empty_proved',
    'quarantined',
    'reconcile_required',
]
BackfillExecutionMode = Literal['backfill', 'reconcile']
QuarantineStatus = Literal['active', 'cleared']

_EMPTY_SHA256: Final[str] = hashlib.sha256(b'').hexdigest()
_MANIFEST_NAMESPACE: Final[UUID] = UUID('19a9ec6d-7b53-4e5f-b33c-6afcb283c3e8')
_PARTITION_PROOF_NAMESPACE: Final[UUID] = UUID('67f9bd64-9e12-4a31-8d04-9ea64c76cb69')
_RANGE_PROOF_NAMESPACE: Final[UUID] = UUID('53d2631f-5522-4cb4-a2bc-d8a5858db925')
_TERMINAL_PARTITION_STATES: Final[frozenset[str]] = frozenset(
    {'proved_complete', 'empty_proved'}
)
_BLOCKING_PARTITION_STATES: Final[frozenset[str]] = frozenset(
    {
        'source_manifested',
        'canonical_written_unproved',
        'quarantined',
        'reconcile_required',
    }
)


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise ReconciliationError(
            code='BACKFILL_INVALID_INPUT',
            message=f'{field_name} must be non-empty',
        )
    return normalized


def _ensure_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ReconciliationError(
            code='BACKFILL_INVALID_INPUT',
            message=f'{field_name} must be timezone-aware UTC datetime',
        )
    return value.astimezone(UTC)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(',', ':'), sort_keys=True)


def _sha256_lines(lines: list[str]) -> str:
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line.encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


def _ordered_identity_materials_or_raise(
    *,
    offset_ordering: OffsetOrdering,
    materials: list[tuple[str, str, str]],
) -> list[tuple[str, str, str]]:
    if offset_ordering == 'numeric':
        numeric_materials: list[tuple[int, str, str, str]] = []
        for offset, event_id, payload_sha256_raw in materials:
            try:
                numeric_offset = int(offset)
            except ValueError as exc:
                raise ReconciliationError(
                    code='BACKFILL_INVALID_NUMERIC_OFFSET',
                    message=(
                        'Numeric offset ordering requires integer offsets, '
                        f'got {offset!r}'
                    ),
                ) from exc
            numeric_materials.append(
                (
                    numeric_offset,
                    offset,
                    event_id,
                    payload_sha256_raw,
                )
            )
        numeric_materials.sort(key=lambda item: (item[0], item[2], item[3]))
        return [
            (offset, event_id, payload_sha256_raw)
            for _, offset, event_id, payload_sha256_raw in numeric_materials
        ]
    return sorted(materials)


def _source_manifest_id(*, source_proof: PartitionSourceProof) -> UUID:
    return uuid5(
        _MANIFEST_NAMESPACE,
        '|'.join(
            (
                source_proof.stream_key.source_id,
                source_proof.stream_key.stream_id,
                source_proof.stream_key.partition_id,
                source_proof.offset_ordering,
                source_proof.source_artifact_identity_json,
                str(source_proof.source_row_count),
                '' if source_proof.first_offset_or_equivalent is None else source_proof.first_offset_or_equivalent,
                '' if source_proof.last_offset_or_equivalent is None else source_proof.last_offset_or_equivalent,
                source_proof.source_offset_digest_sha256,
                source_proof.source_identity_digest_sha256,
                'true' if source_proof.allow_empty_partition else 'false',
            )
        ),
    )


def _partition_proof_id(
    *,
    stream_key: CanonicalStreamKey,
    state: BackfillPartitionState,
    reason: str,
    offset_ordering: OffsetOrdering,
    source_row_count: int,
    canonical_row_count: int,
    canonical_unique_offset_count: int,
    first_offset_or_equivalent: str | None,
    last_offset_or_equivalent: str | None,
    source_offset_digest_sha256: str,
    source_identity_digest_sha256: str,
    canonical_offset_digest_sha256: str,
    canonical_identity_digest_sha256: str,
    gap_count: int,
    duplicate_count: int,
    proof_details_json: str,
) -> UUID:
    return uuid5(
        _PARTITION_PROOF_NAMESPACE,
        '|'.join(
            (
                stream_key.source_id,
                stream_key.stream_id,
                stream_key.partition_id,
                state,
                reason,
                offset_ordering,
                str(source_row_count),
                str(canonical_row_count),
                str(canonical_unique_offset_count),
                '' if first_offset_or_equivalent is None else first_offset_or_equivalent,
                '' if last_offset_or_equivalent is None else last_offset_or_equivalent,
                source_offset_digest_sha256,
                source_identity_digest_sha256,
                canonical_offset_digest_sha256,
                canonical_identity_digest_sha256,
                str(gap_count),
                str(duplicate_count),
                proof_details_json,
            )
        ),
    )


def _partition_proof_digest(
    *,
    state: BackfillPartitionState,
    reason: str,
    source_row_count: int,
    canonical_row_count: int,
    canonical_unique_offset_count: int,
    first_offset_or_equivalent: str | None,
    last_offset_or_equivalent: str | None,
    source_identity_digest_sha256: str,
    canonical_identity_digest_sha256: str,
    gap_count: int,
    duplicate_count: int,
) -> str:
    return hashlib.sha256(
        '|'.join(
            (
                state,
                reason,
                str(source_row_count),
                str(canonical_row_count),
                str(canonical_unique_offset_count),
                '' if first_offset_or_equivalent is None else first_offset_or_equivalent,
                '' if last_offset_or_equivalent is None else last_offset_or_equivalent,
                source_identity_digest_sha256,
                canonical_identity_digest_sha256,
                str(gap_count),
                str(duplicate_count),
            )
        ).encode('utf-8')
    ).hexdigest()


def _range_proof_id(
    *,
    source_id: str,
    stream_id: str,
    start_partition_id: str,
    end_partition_id: str,
    partition_count: int,
    range_digest_sha256: str,
) -> UUID:
    return uuid5(
        _RANGE_PROOF_NAMESPACE,
        '|'.join(
            (
                source_id,
                stream_id,
                start_partition_id,
                end_partition_id,
                str(partition_count),
                range_digest_sha256,
            )
        ),
    )


@dataclass(frozen=True)
class PartitionSourceProof:
    stream_key: CanonicalStreamKey
    offset_ordering: OffsetOrdering
    source_artifact_identity_json: str
    source_row_count: int
    first_offset_or_equivalent: str | None
    last_offset_or_equivalent: str | None
    source_offset_digest_sha256: str
    source_identity_digest_sha256: str
    allow_empty_partition: bool


@dataclass(frozen=True)
class PartitionCanonicalProof:
    canonical_row_count: int
    canonical_unique_offset_count: int
    first_offset_or_equivalent: str | None
    last_offset_or_equivalent: str | None
    canonical_offset_digest_sha256: str
    canonical_identity_digest_sha256: str
    gap_count: int
    duplicate_count: int


@dataclass(frozen=True)
class PartitionProofState:
    stream_key: CanonicalStreamKey
    proof_revision: int
    proof_id: UUID
    state: BackfillPartitionState
    reason: str
    offset_ordering: OffsetOrdering
    source_row_count: int
    canonical_row_count: int
    canonical_unique_offset_count: int
    first_offset_or_equivalent: str | None
    last_offset_or_equivalent: str | None
    source_offset_digest_sha256: str
    source_identity_digest_sha256: str
    canonical_offset_digest_sha256: str
    canonical_identity_digest_sha256: str
    gap_count: int
    duplicate_count: int
    proof_digest_sha256: str
    proof_details_json: str
    recorded_by_run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class SourceManifestState:
    stream_key: CanonicalStreamKey
    manifest_revision: int
    manifest_id: UUID
    offset_ordering: OffsetOrdering
    source_artifact_identity_json: str
    source_row_count: int
    first_offset_or_equivalent: str | None
    last_offset_or_equivalent: str | None
    source_offset_digest_sha256: str
    source_identity_digest_sha256: str
    allow_empty_partition: bool
    manifested_by_run_id: str
    manifested_at_utc: datetime


@dataclass(frozen=True)
class QuarantineState:
    stream_key: CanonicalStreamKey
    quarantine_revision: int
    status: QuarantineStatus
    reason: str
    details_json: str
    recorded_by_run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class PartitionExecutionAssessment:
    stream_key: CanonicalStreamKey
    latest_proof_state: BackfillPartitionState | None
    canonical_row_count: int
    active_quarantine: bool


@dataclass(frozen=True)
class RangeProofState:
    source_id: str
    stream_id: str
    range_start_partition_id: str
    range_end_partition_id: str
    range_revision: int
    range_proof_id: UUID
    partition_count: int
    range_digest_sha256: str
    range_details_json: str
    recorded_by_run_id: str
    recorded_at_utc: datetime


@dataclass(frozen=True)
class DailyProjectorWatermarkCoverage:
    projector_id: str
    last_contiguous_partition_id: str | None


@dataclass(frozen=True)
class SourceIdentityMaterial:
    source_offset_or_equivalent: str
    event_id: str
    payload_sha256_raw: str


def build_partition_source_proof_from_precomputed(
    *,
    stream_key: CanonicalStreamKey,
    offset_ordering: OffsetOrdering,
    source_artifact_identity: dict[str, Any],
    source_row_count: int,
    first_offset_or_equivalent: str | None,
    last_offset_or_equivalent: str | None,
    source_offset_digest_sha256: str,
    source_identity_digest_sha256: str,
    allow_empty_partition: bool,
) -> PartitionSourceProof:
    artifact_identity_json = _canonical_json(source_artifact_identity)
    if source_row_count < 0:
        raise ReconciliationError(
            code='BACKFILL_INVALID_INPUT',
            message='source_row_count must be >= 0',
        )
    if source_row_count == 0:
        if not allow_empty_partition:
            raise ReconciliationError(
                code='BACKFILL_EMPTY_SOURCE_DISALLOWED',
                message=(
                    'Source proof cannot be empty when allow_empty_partition=false '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        return PartitionSourceProof(
            stream_key=stream_key,
            offset_ordering=offset_ordering,
            source_artifact_identity_json=artifact_identity_json,
            source_row_count=0,
            first_offset_or_equivalent=None,
            last_offset_or_equivalent=None,
            source_offset_digest_sha256=_EMPTY_SHA256,
            source_identity_digest_sha256=_EMPTY_SHA256,
            allow_empty_partition=True,
        )
    normalized_first_offset = _require_non_empty(
        first_offset_or_equivalent or '',
        field_name='first_offset_or_equivalent',
    )
    normalized_last_offset = _require_non_empty(
        last_offset_or_equivalent or '',
        field_name='last_offset_or_equivalent',
    )
    normalized_source_offset_digest = _require_non_empty(
        source_offset_digest_sha256,
        field_name='source_offset_digest_sha256',
    )
    normalized_source_identity_digest = _require_non_empty(
        source_identity_digest_sha256,
        field_name='source_identity_digest_sha256',
    )
    return PartitionSourceProof(
        stream_key=stream_key,
        offset_ordering=offset_ordering,
        source_artifact_identity_json=artifact_identity_json,
        source_row_count=source_row_count,
        first_offset_or_equivalent=normalized_first_offset,
        last_offset_or_equivalent=normalized_last_offset,
        source_offset_digest_sha256=normalized_source_offset_digest,
        source_identity_digest_sha256=normalized_source_identity_digest,
        allow_empty_partition=allow_empty_partition,
    )


def build_partition_source_proof(
    *,
    stream_key: CanonicalStreamKey,
    offset_ordering: OffsetOrdering,
    source_artifact_identity: dict[str, Any],
    materials: list[SourceIdentityMaterial],
    allow_empty_partition: bool,
) -> PartitionSourceProof:
    if materials == []:
        return build_partition_source_proof_from_precomputed(
            stream_key=stream_key,
            offset_ordering=offset_ordering,
            source_artifact_identity=source_artifact_identity,
            source_row_count=0,
            first_offset_or_equivalent=None,
            last_offset_or_equivalent=None,
            source_offset_digest_sha256=_EMPTY_SHA256,
            source_identity_digest_sha256=_EMPTY_SHA256,
            allow_empty_partition=allow_empty_partition,
        )

    normalized_materials = _ordered_identity_materials_or_raise(
        offset_ordering=offset_ordering,
        materials=[
            (
            _require_non_empty(item.source_offset_or_equivalent, field_name='source_offset_or_equivalent'),
            _require_non_empty(item.event_id, field_name='event_id'),
            _require_non_empty(item.payload_sha256_raw, field_name='payload_sha256_raw'),
        )
            for item in materials
        ],
    )
    offsets = [item[0] for item in normalized_materials]
    identity_lines = [f'{item[0]}|{item[1]}|{item[2]}' for item in normalized_materials]
    return build_partition_source_proof_from_precomputed(
        stream_key=stream_key,
        offset_ordering=offset_ordering,
        source_artifact_identity=source_artifact_identity,
        source_row_count=len(normalized_materials),
        first_offset_or_equivalent=offsets[0],
        last_offset_or_equivalent=offsets[-1],
        source_offset_digest_sha256=_sha256_lines(offsets),
        source_identity_digest_sha256=_sha256_lines(identity_lines),
        allow_empty_partition=allow_empty_partition,
    )


class CanonicalBackfillStateStore:
    def __init__(self, *, client: ClickHouseClient, database: str) -> None:
        self._client = client
        self._database = _require_non_empty(database, field_name='database')

    def record_source_manifest(
        self,
        *,
        source_proof: PartitionSourceProof,
        run_id: str,
        manifested_at_utc: datetime,
    ) -> SourceManifestState:
        stream_key = source_proof.stream_key
        normalized_run_id = _require_non_empty(run_id, field_name='run_id')
        normalized_manifested_at_utc = _ensure_utc(
            manifested_at_utc, field_name='manifested_at_utc'
        )
        latest = self.fetch_latest_source_manifest(stream_key=stream_key)
        manifest_id = _source_manifest_id(source_proof=source_proof)
        if latest is not None and latest.manifest_id == manifest_id:
            return latest
        next_revision = 1 if latest is None else latest.manifest_revision + 1
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_backfill_source_manifests
            (
                source_id,
                stream_id,
                partition_id,
                manifest_revision,
                manifest_id,
                offset_ordering,
                source_artifact_identity_json,
                source_row_count,
                first_offset_or_equivalent,
                last_offset_or_equivalent,
                source_offset_digest_sha256,
                source_identity_digest_sha256,
                allow_empty_partition,
                manifested_by_run_id,
                manifested_at_utc
            )
            VALUES
            ''' ,
            [
                (
                    stream_key.source_id,
                    stream_key.stream_id,
                    stream_key.partition_id,
                    next_revision,
                    manifest_id,
                    source_proof.offset_ordering,
                    source_proof.source_artifact_identity_json,
                    source_proof.source_row_count,
                    source_proof.first_offset_or_equivalent,
                    source_proof.last_offset_or_equivalent,
                    source_proof.source_offset_digest_sha256,
                    source_proof.source_identity_digest_sha256,
                    source_proof.allow_empty_partition,
                    normalized_run_id,
                    normalized_manifested_at_utc,
                )
            ],
        )
        latest_after_insert = self.fetch_latest_source_manifest(stream_key=stream_key)
        if latest_after_insert is None:
            raise ReconciliationError(
                code='BACKFILL_MANIFEST_MISSING_AFTER_INSERT',
                message='Source manifest insert succeeded but latest manifest could not be loaded',
            )
        return latest_after_insert

    def fetch_latest_source_manifest(
        self, *, stream_key: CanonicalStreamKey
    ) -> SourceManifestState | None:
        rows = self._client.execute(
            f'''
            SELECT
                manifest_revision,
                manifest_id,
                offset_ordering,
                source_artifact_identity_json,
                source_row_count,
                first_offset_or_equivalent,
                last_offset_or_equivalent,
                source_offset_digest_sha256,
                source_identity_digest_sha256,
                allow_empty_partition,
                manifested_by_run_id,
                manifested_at_utc
            FROM {self._database}.canonical_backfill_source_manifests
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            ORDER BY manifest_revision DESC
            LIMIT 1
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        row = rows[0]
        return SourceManifestState(
            stream_key=stream_key,
            manifest_revision=int(row[0]),
            manifest_id=row[1],
            offset_ordering=row[2],
            source_artifact_identity_json=str(row[3]),
            source_row_count=int(row[4]),
            first_offset_or_equivalent=(None if row[5] is None else str(row[5])),
            last_offset_or_equivalent=(None if row[6] is None else str(row[6])),
            source_offset_digest_sha256=str(row[7]),
            source_identity_digest_sha256=str(row[8]),
            allow_empty_partition=bool(row[9]),
            manifested_by_run_id=str(row[10]),
            manifested_at_utc=row[11],
        )

    def record_partition_state(
        self,
        *,
        source_proof: PartitionSourceProof,
        state: BackfillPartitionState,
        reason: str,
        run_id: str,
        recorded_at_utc: datetime,
        canonical_proof: PartitionCanonicalProof | None = None,
        proof_details: dict[str, Any] | None = None,
    ) -> PartitionProofState:
        stream_key = source_proof.stream_key
        normalized_reason = _require_non_empty(reason, field_name='reason')
        normalized_run_id = _require_non_empty(run_id, field_name='run_id')
        normalized_recorded_at_utc = _ensure_utc(
            recorded_at_utc, field_name='recorded_at_utc'
        )
        canonical = canonical_proof or PartitionCanonicalProof(
            canonical_row_count=0,
            canonical_unique_offset_count=0,
            first_offset_or_equivalent=None,
            last_offset_or_equivalent=None,
            canonical_offset_digest_sha256=_EMPTY_SHA256,
            canonical_identity_digest_sha256=_EMPTY_SHA256,
            gap_count=0,
            duplicate_count=0,
        )
        proof_details_json = _canonical_json(proof_details or {})
        proof_id = _partition_proof_id(
            stream_key=stream_key,
            state=state,
            reason=normalized_reason,
            offset_ordering=source_proof.offset_ordering,
            source_row_count=source_proof.source_row_count,
            canonical_row_count=canonical.canonical_row_count,
            canonical_unique_offset_count=canonical.canonical_unique_offset_count,
            first_offset_or_equivalent=(
                canonical.first_offset_or_equivalent
                if canonical.first_offset_or_equivalent is not None
                else source_proof.first_offset_or_equivalent
            ),
            last_offset_or_equivalent=(
                canonical.last_offset_or_equivalent
                if canonical.last_offset_or_equivalent is not None
                else source_proof.last_offset_or_equivalent
            ),
            source_offset_digest_sha256=source_proof.source_offset_digest_sha256,
            source_identity_digest_sha256=source_proof.source_identity_digest_sha256,
            canonical_offset_digest_sha256=canonical.canonical_offset_digest_sha256,
            canonical_identity_digest_sha256=canonical.canonical_identity_digest_sha256,
            gap_count=canonical.gap_count,
            duplicate_count=canonical.duplicate_count,
            proof_details_json=proof_details_json,
        )
        latest = self.fetch_latest_partition_proof(stream_key=stream_key)
        if latest is not None and latest.proof_id == proof_id:
            return latest
        next_revision = 1 if latest is None else latest.proof_revision + 1
        first_offset_or_equivalent = (
            canonical.first_offset_or_equivalent
            if canonical.first_offset_or_equivalent is not None
            else source_proof.first_offset_or_equivalent
        )
        last_offset_or_equivalent = (
            canonical.last_offset_or_equivalent
            if canonical.last_offset_or_equivalent is not None
            else source_proof.last_offset_or_equivalent
        )
        proof_digest_sha256 = _partition_proof_digest(
            state=state,
            reason=normalized_reason,
            source_row_count=source_proof.source_row_count,
            canonical_row_count=canonical.canonical_row_count,
            canonical_unique_offset_count=canonical.canonical_unique_offset_count,
            first_offset_or_equivalent=first_offset_or_equivalent,
            last_offset_or_equivalent=last_offset_or_equivalent,
            source_identity_digest_sha256=source_proof.source_identity_digest_sha256,
            canonical_identity_digest_sha256=canonical.canonical_identity_digest_sha256,
            gap_count=canonical.gap_count,
            duplicate_count=canonical.duplicate_count,
        )
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_backfill_partition_proofs
            (
                source_id,
                stream_id,
                partition_id,
                proof_revision,
                proof_id,
                state,
                reason,
                offset_ordering,
                source_row_count,
                canonical_row_count,
                canonical_unique_offset_count,
                first_offset_or_equivalent,
                last_offset_or_equivalent,
                source_offset_digest_sha256,
                source_identity_digest_sha256,
                canonical_offset_digest_sha256,
                canonical_identity_digest_sha256,
                gap_count,
                duplicate_count,
                proof_digest_sha256,
                proof_details_json,
                recorded_by_run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    stream_key.source_id,
                    stream_key.stream_id,
                    stream_key.partition_id,
                    next_revision,
                    proof_id,
                    state,
                    normalized_reason,
                    source_proof.offset_ordering,
                    source_proof.source_row_count,
                    canonical.canonical_row_count,
                    canonical.canonical_unique_offset_count,
                    first_offset_or_equivalent,
                    last_offset_or_equivalent,
                    source_proof.source_offset_digest_sha256,
                    source_proof.source_identity_digest_sha256,
                    canonical.canonical_offset_digest_sha256,
                    canonical.canonical_identity_digest_sha256,
                    canonical.gap_count,
                    canonical.duplicate_count,
                    proof_digest_sha256,
                    proof_details_json,
                    normalized_run_id,
                    normalized_recorded_at_utc,
                )
            ],
        )
        latest_after_insert = self.fetch_latest_partition_proof(stream_key=stream_key)
        if latest_after_insert is None:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_PROOF_MISSING_AFTER_INSERT',
                message='Partition proof insert succeeded but latest proof could not be loaded',
            )
        return latest_after_insert

    def fetch_latest_partition_proof(
        self, *, stream_key: CanonicalStreamKey
    ) -> PartitionProofState | None:
        rows = self._client.execute(
            f'''
            SELECT
                proof_revision,
                proof_id,
                state,
                reason,
                offset_ordering,
                source_row_count,
                canonical_row_count,
                canonical_unique_offset_count,
                first_offset_or_equivalent,
                last_offset_or_equivalent,
                source_offset_digest_sha256,
                source_identity_digest_sha256,
                canonical_offset_digest_sha256,
                canonical_identity_digest_sha256,
                gap_count,
                duplicate_count,
                proof_digest_sha256,
                proof_details_json,
                recorded_by_run_id,
                recorded_at_utc
            FROM {self._database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            ORDER BY proof_revision DESC
            LIMIT 1
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        row = rows[0]
        return PartitionProofState(
            stream_key=stream_key,
            proof_revision=int(row[0]),
            proof_id=row[1],
            state=row[2],
            reason=str(row[3]),
            offset_ordering=row[4],
            source_row_count=int(row[5]),
            canonical_row_count=int(row[6]),
            canonical_unique_offset_count=int(row[7]),
            first_offset_or_equivalent=(None if row[8] is None else str(row[8])),
            last_offset_or_equivalent=(None if row[9] is None else str(row[9])),
            source_offset_digest_sha256=str(row[10]),
            source_identity_digest_sha256=str(row[11]),
            canonical_offset_digest_sha256=str(row[12]),
            canonical_identity_digest_sha256=str(row[13]),
            gap_count=int(row[14]),
            duplicate_count=int(row[15]),
            proof_digest_sha256=str(row[16]),
            proof_details_json=str(row[17]),
            recorded_by_run_id=str(row[18]),
            recorded_at_utc=row[19],
        )

    def fetch_latest_quarantine(
        self, *, stream_key: CanonicalStreamKey
    ) -> QuarantineState | None:
        rows = self._client.execute(
            f'''
            SELECT
                quarantine_revision,
                status,
                reason,
                details_json,
                recorded_by_run_id,
                recorded_at_utc
            FROM {self._database}.canonical_quarantined_streams
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            ORDER BY quarantine_revision DESC
            LIMIT 1
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        if rows == []:
            return None
        row = rows[0]
        return QuarantineState(
            stream_key=stream_key,
            quarantine_revision=int(row[0]),
            status=row[1],
            reason=str(row[2]),
            details_json=str(row[3]),
            recorded_by_run_id=str(row[4]),
            recorded_at_utc=row[5],
        )

    def record_quarantine(
        self,
        *,
        stream_key: CanonicalStreamKey,
        status: QuarantineStatus,
        reason: str,
        details: dict[str, Any],
        run_id: str,
        recorded_at_utc: datetime,
    ) -> QuarantineState:
        latest = self.fetch_latest_quarantine(stream_key=stream_key)
        normalized_reason = _require_non_empty(reason, field_name='reason')
        normalized_run_id = _require_non_empty(run_id, field_name='run_id')
        normalized_recorded_at_utc = _ensure_utc(
            recorded_at_utc, field_name='recorded_at_utc'
        )
        details_json = _canonical_json(details)
        if (
            latest is not None
            and latest.status == status
            and latest.reason == normalized_reason
            and latest.details_json == details_json
        ):
            return latest
        next_revision = 1 if latest is None else latest.quarantine_revision + 1
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_quarantined_streams
            (
                source_id,
                stream_id,
                partition_id,
                quarantine_revision,
                status,
                reason,
                details_json,
                recorded_by_run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    stream_key.source_id,
                    stream_key.stream_id,
                    stream_key.partition_id,
                    next_revision,
                    status,
                    normalized_reason,
                    details_json,
                    normalized_run_id,
                    normalized_recorded_at_utc,
                )
            ],
        )
        latest_after_insert = self.fetch_latest_quarantine(stream_key=stream_key)
        if latest_after_insert is None:
            raise ReconciliationError(
                code='BACKFILL_QUARANTINE_MISSING_AFTER_INSERT',
                message='Quarantine insert succeeded but latest quarantine could not be loaded',
            )
        return latest_after_insert

    def clear_quarantine(
        self,
        *,
        stream_key: CanonicalStreamKey,
        run_id: str,
        recorded_at_utc: datetime,
    ) -> QuarantineState:
        return self.record_quarantine(
            stream_key=stream_key,
            status='cleared',
            reason='reconcile_cleared',
            details={},
            run_id=run_id,
            recorded_at_utc=recorded_at_utc,
        )

    def count_canonical_rows(self, *, stream_key: CanonicalStreamKey) -> int:
        rows = self._client.execute(
            f'''
            SELECT count()
            FROM {self._database}.canonical_event_log
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            ''',
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
        )
        return int(rows[0][0]) if rows != [] else 0

    def assert_partition_terminally_proved_or_raise(
        self,
        *,
        stream_key: CanonicalStreamKey,
    ) -> PartitionProofState:
        latest_proof = self.fetch_latest_partition_proof(stream_key=stream_key)
        if latest_proof is None:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_PROOF_MISSING',
                message=(
                    'Projection requires terminal partition proof state '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        if latest_proof.state not in _TERMINAL_PARTITION_STATES:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_NOT_TERMINAL_PROVED',
                message=(
                    'Projection requires terminal partition proof state '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}, '
                    f'got state={latest_proof.state}'
                ),
            )
        return latest_proof

    def assert_partitions_terminally_proved_or_raise(
        self,
        *,
        source_id: str,
        stream_id: str,
        partition_ids: list[str] | set[str] | tuple[str, ...],
    ) -> None:
        normalized_partition_ids = sorted(
            {
                _require_non_empty(partition_id, field_name='partition_id')
                for partition_id in partition_ids
            }
        )
        if normalized_partition_ids == []:
            raise ReconciliationError(
                code='BACKFILL_INVALID_INPUT',
                message='partition_ids must be non-empty',
            )
        rows = self._client.execute(
            f'''
            SELECT
                partition_id,
                argMax(state, proof_revision) AS state
            FROM {self._database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id IN %(partition_ids)s
            GROUP BY partition_id
            ORDER BY partition_id ASC
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'partition_ids': tuple(normalized_partition_ids),
            },
        )
        observed_states = {
            str(row[0]): str(row[1])
            for row in rows
        }
        missing = [
            partition_id
            for partition_id in normalized_partition_ids
            if partition_id not in observed_states
        ]
        if missing != []:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_PROOF_MISSING',
                message=(
                    'Projection requires terminal partition proof state for every partition '
                    f'for {source_id}/{stream_id}; missing={missing}'
                ),
            )
        non_terminal = {
            partition_id: state
            for partition_id, state in observed_states.items()
            if state not in _TERMINAL_PARTITION_STATES
        }
        if non_terminal != {}:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_NOT_TERMINAL_PROVED',
                message=(
                    'Projection requires terminal partition proof state for every partition '
                    f'for {source_id}/{stream_id}; non_terminal={non_terminal}'
                ),
                context={'non_terminal_partitions': non_terminal},
            )

    def list_terminal_partition_ids(
        self,
        *,
        source_id: str,
        stream_id: str,
    ) -> tuple[str, ...]:
        normalized_source_id = _require_non_empty(
            source_id,
            field_name='source_id',
        )
        normalized_stream_id = _require_non_empty(
            stream_id,
            field_name='stream_id',
        )
        rows = self._client.execute(
            f'''
            SELECT
                partition_id,
                argMax(state, proof_revision) AS state
            FROM {self._database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
            GROUP BY partition_id
            ORDER BY partition_id ASC
            ''',
            {
                'source_id': normalized_source_id,
                'stream_id': normalized_stream_id,
            },
        )
        terminal_partition_ids: list[str] = []
        for row in rows:
            partition_id = str(row[0])
            state = str(row[1])
            if state in _TERMINAL_PARTITION_STATES:
                terminal_partition_ids.append(partition_id)
        return tuple(terminal_partition_ids)

    def list_projector_watermark_partition_ids(
        self,
        *,
        projector_id: str,
        source_id: str,
        stream_id: str,
    ) -> tuple[str, ...]:
        normalized_projector_id = _require_non_empty(
            projector_id,
            field_name='projector_id',
        )
        normalized_source_id = _require_non_empty(
            source_id,
            field_name='source_id',
        )
        normalized_stream_id = _require_non_empty(
            stream_id,
            field_name='stream_id',
        )
        rows = self._client.execute(
            f'''
            SELECT partition_id
            FROM
            (
                SELECT partition_id, max(watermark_revision) AS watermark_revision
                FROM {self._database}.canonical_projector_watermarks
                WHERE projector_id = %(projector_id)s
                  AND source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            )
            ORDER BY partition_id ASC
            ''',
            {
                'projector_id': normalized_projector_id,
                'source_id': normalized_source_id,
                'stream_id': normalized_stream_id,
            },
        )
        return tuple(str(row[0]) for row in rows)

    def assert_projector_partition_coverage_matches_terminal_proof_or_raise(
        self,
        *,
        projector_id: str,
        source_id: str,
        stream_id: str,
    ) -> tuple[str, ...]:
        terminal_partition_ids = self.list_terminal_partition_ids(
            source_id=source_id,
            stream_id=stream_id,
        )
        if terminal_partition_ids == ():
            raise ReconciliationError(
                code='BACKFILL_TERMINAL_PARTITION_PROOFS_MISSING',
                message=(
                    'Serving promotion requires at least one terminal partition proof '
                    f'for {source_id}/{stream_id}'
                ),
            )
        watermark_partition_ids = self.list_projector_watermark_partition_ids(
            projector_id=projector_id,
            source_id=source_id,
            stream_id=stream_id,
        )
        if watermark_partition_ids != terminal_partition_ids:
            raise ReconciliationError(
                code='BACKFILL_PROJECTOR_WATERMARK_PARTITION_SET_MISMATCH',
                message=(
                    'Serving promotion requires projector watermark partition coverage '
                    'to exactly match terminal proof partition coverage '
                    f'for projector_id={projector_id} {source_id}/{stream_id}'
                ),
                context={
                    'projector_id': projector_id,
                    'expected_partition_ids': list(terminal_partition_ids),
                    'observed_partition_ids': list(watermark_partition_ids),
                },
            )
        return terminal_partition_ids

    def assess_partition_execution(
        self,
        *,
        stream_key: CanonicalStreamKey,
    ) -> PartitionExecutionAssessment:
        latest_proof = self.fetch_latest_partition_proof(stream_key=stream_key)
        latest_quarantine = self.fetch_latest_quarantine(stream_key=stream_key)
        canonical_row_count = self.count_canonical_rows(stream_key=stream_key)
        return PartitionExecutionAssessment(
            stream_key=stream_key,
            latest_proof_state=None if latest_proof is None else latest_proof.state,
            canonical_row_count=canonical_row_count,
            active_quarantine=(
                latest_quarantine is not None and latest_quarantine.status == 'active'
            ),
        )

    def assert_partition_can_execute_or_raise(
        self,
        *,
        stream_key: CanonicalStreamKey,
        execution_mode: BackfillExecutionMode,
    ) -> None:
        assessment = self.assess_partition_execution(stream_key=stream_key)
        if execution_mode == 'reconcile':
            if assessment.latest_proof_state in _TERMINAL_PARTITION_STATES:
                raise ReconciliationError(
                    code='BACKFILL_PARTITION_ALREADY_COMPLETE',
                    message=(
                        'Reconcile refused because partition is already terminal-complete '
                        f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                    ),
                )
            if (
                assessment.latest_proof_state is None
                and assessment.canonical_row_count == 0
                and not assessment.active_quarantine
            ):
                raise ReconciliationError(
                    code='BACKFILL_RECONCILE_NOT_REQUIRED',
                    message=(
                        'Reconcile refused because partition has no ambiguous or partial state '
                        f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                    ),
                )
            return
        if assessment.latest_proof_state in _TERMINAL_PARTITION_STATES:
            raise ReconciliationError(
                code='BACKFILL_PARTITION_ALREADY_COMPLETE',
                message=(
                    'Partition already has terminal proof state '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        if assessment.latest_proof_state in _BLOCKING_PARTITION_STATES:
            raise ReconciliationError(
                code='RECONCILE_REQUIRED',
                message=(
                    'Partition has blocking non-terminal proof state and requires explicit reconcile '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        if assessment.active_quarantine:
            raise ReconciliationError(
                code='RECONCILE_REQUIRED',
                message=(
                    'Partition is quarantined and requires explicit reconcile '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        if assessment.latest_proof_state is None and assessment.canonical_row_count > 0:
            raise ReconciliationError(
                code='RECONCILE_REQUIRED',
                message=(
                    'Partition already has canonical rows without terminal proof state '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )

    def compute_canonical_partition_proof_or_raise(
        self,
        *,
        source_proof: PartitionSourceProof,
    ) -> PartitionCanonicalProof:
        stream_key = source_proof.stream_key
        if source_proof.offset_ordering == 'numeric':
            digest_query = f'''
            SELECT
                row_count,
                unique_offset_count,
                if(
                    row_count = 0,
                    CAST(NULL AS Nullable(String)),
                    toString(tupleElement(materials[1], 2))
                ) AS first_offset_or_equivalent,
                if(
                    row_count = 0,
                    CAST(NULL AS Nullable(String)),
                    toString(tupleElement(materials[length(materials)], 2))
                ) AS last_offset_or_equivalent,
                if(
                    row_count = 0,
                    %(empty_sha256)s,
                    lower(hex(SHA256(concat(
                        arrayStringConcat(
                            arrayMap(item -> tupleElement(item, 2), materials),
                            '\\n'
                        ),
                        '\\n'
                    ))))
                ) AS canonical_offset_digest_sha256,
                if(
                    row_count = 0,
                    %(empty_sha256)s,
                    lower(hex(SHA256(concat(
                        arrayStringConcat(
                            arrayMap(
                                item -> concat(
                                    tupleElement(item, 2),
                                    '|',
                                    tupleElement(item, 3),
                                    '|',
                                    tupleElement(item, 4)
                                ),
                                materials
                            ),
                            '\\n'
                        ),
                        '\\n'
                    ))))
                ) AS canonical_identity_digest_sha256,
                if(
                    row_count = 0,
                    0,
                    greatest(max_offset_int - min_offset_int + 1 - unique_offset_count, 0)
                ) AS gap_count,
                row_count - unique_offset_count AS duplicate_count
            FROM
            (
                SELECT
                    count() AS row_count,
                    uniqExact(source_offset_or_equivalent) AS unique_offset_count,
                    min(toInt64(source_offset_or_equivalent)) AS min_offset_int,
                    max(toInt64(source_offset_or_equivalent)) AS max_offset_int,
                    arraySort(
                        groupArray(
                            (
                                toInt64(source_offset_or_equivalent),
                                source_offset_or_equivalent,
                                toString(event_id),
                                payload_sha256_raw
                            )
                        )
                    ) AS materials
                FROM {self._database}.canonical_event_log
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
            )
            '''
        else:
            digest_query = f'''
            SELECT
                row_count,
                unique_offset_count,
                if(
                    row_count = 0,
                    CAST(NULL AS Nullable(String)),
                    toString(tupleElement(materials[1], 1))
                ) AS first_offset_or_equivalent,
                if(
                    row_count = 0,
                    CAST(NULL AS Nullable(String)),
                    toString(tupleElement(materials[length(materials)], 1))
                ) AS last_offset_or_equivalent,
                if(
                    row_count = 0,
                    %(empty_sha256)s,
                    lower(hex(SHA256(concat(
                        arrayStringConcat(
                            arrayMap(item -> tupleElement(item, 1), materials),
                            '\\n'
                        ),
                        '\\n'
                    ))))
                ) AS canonical_offset_digest_sha256,
                if(
                    row_count = 0,
                    %(empty_sha256)s,
                    lower(hex(SHA256(concat(
                        arrayStringConcat(
                            arrayMap(
                                item -> concat(
                                    tupleElement(item, 1),
                                    '|',
                                    tupleElement(item, 2),
                                    '|',
                                    tupleElement(item, 3)
                                ),
                                materials
                            ),
                            '\\n'
                        ),
                        '\\n'
                    ))))
                ) AS canonical_identity_digest_sha256,
                0 AS gap_count,
                row_count - unique_offset_count AS duplicate_count
            FROM
            (
                SELECT
                    count() AS row_count,
                    uniqExact(source_offset_or_equivalent) AS unique_offset_count,
                    arraySort(
                        groupArray(
                            (
                                source_offset_or_equivalent,
                                toString(event_id),
                                payload_sha256_raw
                            )
                        )
                    ) AS materials
                FROM {self._database}.canonical_event_log
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                  AND partition_id = %(partition_id)s
            )
            '''
        rows = self._client.execute(
            digest_query,
            {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
                'empty_sha256': _EMPTY_SHA256,
            },
        )
        if rows == []:
            raise ReconciliationError(
                code='BACKFILL_CANONICAL_PROOF_QUERY_EMPTY',
                message=(
                    'Canonical proof query returned no rows '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        row = rows[0]
        canonical_row_count = int(row[0])
        if canonical_row_count == 0:
            if source_proof.source_row_count == 0 and source_proof.allow_empty_partition:
                return PartitionCanonicalProof(
                    canonical_row_count=0,
                    canonical_unique_offset_count=0,
                    first_offset_or_equivalent=None,
                    last_offset_or_equivalent=None,
                    canonical_offset_digest_sha256=_EMPTY_SHA256,
                    canonical_identity_digest_sha256=_EMPTY_SHA256,
                    gap_count=0,
                    duplicate_count=0,
                )
            raise ReconciliationError(
                code='BACKFILL_CANONICAL_ROWS_MISSING',
                message=(
                    'Canonical proof expected rows but found none '
                    f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
                ),
            )
        return PartitionCanonicalProof(
            canonical_row_count=canonical_row_count,
            canonical_unique_offset_count=int(row[1]),
            first_offset_or_equivalent=(None if row[2] is None else str(row[2])),
            last_offset_or_equivalent=(None if row[3] is None else str(row[3])),
            canonical_offset_digest_sha256=str(row[4]),
            canonical_identity_digest_sha256=str(row[5]),
            gap_count=int(row[6]),
            duplicate_count=int(row[7]),
        )

    def prove_partition_or_quarantine(
        self,
        *,
        source_proof: PartitionSourceProof,
        run_id: str,
        recorded_at_utc: datetime,
    ) -> PartitionProofState:
        canonical_proof = self.compute_canonical_partition_proof_or_raise(
            source_proof=source_proof
        )
        details: dict[str, Any] = {}
        state: BackfillPartitionState
        reason: str
        if source_proof.source_row_count == 0 and canonical_proof.canonical_row_count == 0:
            state = 'empty_proved'
            reason = 'source_and_canonical_empty_proved'
        else:
            mismatch_reasons: list[str] = []
            if source_proof.source_row_count != canonical_proof.canonical_row_count:
                mismatch_reasons.append('row_count_mismatch')
            if (
                source_proof.source_identity_digest_sha256
                != canonical_proof.canonical_identity_digest_sha256
            ):
                mismatch_reasons.append('identity_digest_mismatch')
            if (
                source_proof.first_offset_or_equivalent
                != canonical_proof.first_offset_or_equivalent
            ):
                mismatch_reasons.append('first_offset_mismatch')
            if (
                source_proof.last_offset_or_equivalent
                != canonical_proof.last_offset_or_equivalent
            ):
                mismatch_reasons.append('last_offset_mismatch')
            if canonical_proof.gap_count > 0:
                mismatch_reasons.append('gap_detected')
            if canonical_proof.duplicate_count > 0:
                mismatch_reasons.append('duplicate_offsets_detected')
            if mismatch_reasons == []:
                state = 'proved_complete'
                reason = 'source_and_canonical_match'
            else:
                state = 'quarantined'
                reason = ','.join(mismatch_reasons)
                details = {
                    'mismatch_reasons': mismatch_reasons,
                    'source_row_count': source_proof.source_row_count,
                    'canonical_row_count': canonical_proof.canonical_row_count,
                    'source_identity_digest_sha256': source_proof.source_identity_digest_sha256,
                    'canonical_identity_digest_sha256': canonical_proof.canonical_identity_digest_sha256,
                    'gap_count': canonical_proof.gap_count,
                    'duplicate_count': canonical_proof.duplicate_count,
                }
        proof_state = self.record_partition_state(
            source_proof=source_proof,
            state=state,
            reason=reason,
            run_id=run_id,
            recorded_at_utc=recorded_at_utc,
            canonical_proof=canonical_proof,
            proof_details=details,
        )
        if state == 'quarantined':
            self.record_quarantine(
                stream_key=source_proof.stream_key,
                status='active',
                reason=reason,
                details=details,
                run_id=run_id,
                recorded_at_utc=recorded_at_utc,
            )
            raise ReconciliationError(
                code='BACKFILL_PARTITION_PROOF_FAILED',
                message=(
                    'Partition proof failed and stream was quarantined '
                    f'for {source_proof.stream_key.source_id}/'
                    f'{source_proof.stream_key.stream_id}/'
                    f'{source_proof.stream_key.partition_id}: {reason}'
                ),
                context=details,
            )
        latest_quarantine = self.fetch_latest_quarantine(
            stream_key=source_proof.stream_key
        )
        if latest_quarantine is not None and latest_quarantine.status == 'active':
            self.clear_quarantine(
                stream_key=source_proof.stream_key,
                run_id=run_id,
                recorded_at_utc=recorded_at_utc,
            )
        return proof_state

    def load_last_completed_daily_partition_or_raise(
        self,
        *,
        source_id: str,
        stream_id: str,
        earliest_partition_date: date,
    ) -> str | None:
        blocking_rows = self._client.execute(
            f'''
            SELECT partition_id, state
            FROM
            (
                SELECT
                    partition_id,
                    argMax(state, proof_revision) AS state
                FROM {self._database}.canonical_backfill_partition_proofs
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            )
            WHERE state IN %(blocking_states)s
            ORDER BY partition_id ASC
            LIMIT 1
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'blocking_states': tuple(_BLOCKING_PARTITION_STATES),
            },
        )
        if blocking_rows != []:
            row = blocking_rows[0]
            raise ReconciliationError(
                code='RECONCILE_REQUIRED',
                message=(
                    'Dataset resume blocked by non-terminal partition proof state '
                    f'for {source_id}/{stream_id}/{row[0]} state={row[1]}'
                ),
            )
        ambiguous_rows = self._client.execute(
            f'''
            SELECT partition_id
            FROM
            (
                SELECT DISTINCT partition_id
                FROM {self._database}.canonical_event_log
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
            ) AS event_partitions
            LEFT JOIN
            (
                SELECT partition_id, argMax(state, proof_revision) AS state
                FROM {self._database}.canonical_backfill_partition_proofs
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            ) AS proofs
            ON event_partitions.partition_id = proofs.partition_id
            WHERE proofs.state IS NULL OR proofs.state NOT IN %(terminal_states)s
            ORDER BY partition_id ASC
            LIMIT 1
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'terminal_states': tuple(_TERMINAL_PARTITION_STATES),
            },
        )
        if ambiguous_rows != []:
            raise ReconciliationError(
                code='RECONCILE_REQUIRED',
                message=(
                    'Dataset resume blocked by canonical rows without terminal proof state '
                    f'for {source_id}/{stream_id}/{ambiguous_rows[0][0]}'
                ),
            )
        terminal_rows = self._client.execute(
            f'''
            SELECT partition_id
            FROM
            (
                SELECT partition_id, argMax(state, proof_revision) AS state
                FROM {self._database}.canonical_backfill_partition_proofs
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            )
            WHERE state IN %(terminal_states)s
            ORDER BY partition_id ASC
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'terminal_states': tuple(_TERMINAL_PARTITION_STATES),
            },
        )
        if terminal_rows == []:
            return None
        terminal_partition_ids = [str(row[0]) for row in terminal_rows]
        expected_day = earliest_partition_date
        for partition_id in terminal_partition_ids:
            expected_partition_id = expected_day.isoformat()
            if partition_id != expected_partition_id:
                raise ReconciliationError(
                    code='RECONCILE_REQUIRED',
                    message=(
                        'Dataset resume blocked by non-contiguous terminal proof coverage '
                        f'for {source_id}/{stream_id}: expected={expected_partition_id} '
                        f'actual={partition_id}'
                    ),
                )
            expected_day += timedelta(days=1)
        return terminal_partition_ids[-1]

    def record_range_proof(
        self,
        *,
        source_id: str,
        stream_id: str,
        partition_ids: list[str],
        run_id: str,
        recorded_at_utc: datetime,
    ) -> RangeProofState:
        if partition_ids == []:
            raise ReconciliationError(
                code='BACKFILL_INVALID_INPUT',
                message='partition_ids must be non-empty for range proof',
            )
        normalized_run_id = _require_non_empty(run_id, field_name='run_id')
        normalized_recorded_at_utc = _ensure_utc(
            recorded_at_utc, field_name='recorded_at_utc'
        )
        rows = self._client.execute(
            f'''
            SELECT
                partition_id,
                argMax(proof_digest_sha256, proof_revision) AS proof_digest_sha256,
                argMax(state, proof_revision) AS state
            FROM {self._database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id IN %(partition_ids)s
            GROUP BY partition_id
            ORDER BY partition_id ASC
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'partition_ids': tuple(partition_ids),
            },
        )
        observed_partition_ids = [str(row[0]) for row in rows]
        if observed_partition_ids != sorted(partition_ids):
            raise ReconciliationError(
                code='BACKFILL_RANGE_PROOF_PARTITION_SET_MISMATCH',
                message=(
                    'Range proof requires one terminal proof per partition '
                    f'for {source_id}/{stream_id}'
                ),
                context={
                    'expected_partition_ids': sorted(partition_ids),
                    'observed_partition_ids': observed_partition_ids,
                },
            )
        for row in rows:
            if str(row[2]) not in _TERMINAL_PARTITION_STATES:
                raise ReconciliationError(
                    code='BACKFILL_RANGE_PROOF_NON_TERMINAL_PARTITION',
                    message=(
                        'Range proof requires terminal partition proofs '
                        f'for {source_id}/{stream_id}/{row[0]}'
                    ),
                )
        digest_lines = [f'{row[0]}|{row[1]}' for row in rows]
        range_digest_sha256 = _sha256_lines(digest_lines)
        details_json = _canonical_json(
            {
                'partition_ids': observed_partition_ids,
                'partition_proof_digests': [str(row[1]) for row in rows],
            }
        )
        latest_rows = self._client.execute(
            f'''
            SELECT maxOrNull(range_revision)
            FROM {self._database}.canonical_backfill_range_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND range_start_partition_id = %(range_start_partition_id)s
              AND range_end_partition_id = %(range_end_partition_id)s
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'range_start_partition_id': observed_partition_ids[0],
                'range_end_partition_id': observed_partition_ids[-1],
            },
        )
        next_revision = 1 if latest_rows[0][0] is None else int(latest_rows[0][0]) + 1
        range_proof_id = _range_proof_id(
            source_id=source_id,
            stream_id=stream_id,
            start_partition_id=observed_partition_ids[0],
            end_partition_id=observed_partition_ids[-1],
            partition_count=len(observed_partition_ids),
            range_digest_sha256=range_digest_sha256,
        )
        self._client.execute(
            f'''
            INSERT INTO {self._database}.canonical_backfill_range_proofs
            (
                source_id,
                stream_id,
                range_start_partition_id,
                range_end_partition_id,
                range_revision,
                range_proof_id,
                partition_count,
                range_digest_sha256,
                range_details_json,
                recorded_by_run_id,
                recorded_at_utc
            )
            VALUES
            ''',
            [
                (
                    source_id,
                    stream_id,
                    observed_partition_ids[0],
                    observed_partition_ids[-1],
                    next_revision,
                    range_proof_id,
                    len(observed_partition_ids),
                    range_digest_sha256,
                    details_json,
                    normalized_run_id,
                    normalized_recorded_at_utc,
                )
            ],
        )
        return RangeProofState(
            source_id=source_id,
            stream_id=stream_id,
            range_start_partition_id=observed_partition_ids[0],
            range_end_partition_id=observed_partition_ids[-1],
            range_revision=next_revision,
            range_proof_id=range_proof_id,
            partition_count=len(observed_partition_ids),
            range_digest_sha256=range_digest_sha256,
            range_details_json=details_json,
            recorded_by_run_id=normalized_run_id,
            recorded_at_utc=normalized_recorded_at_utc,
        )

    def load_last_daily_projector_watermark_partition_or_raise(
        self,
        *,
        projector_id: str,
        source_id: str,
        stream_id: str,
        earliest_partition_date: date,
    ) -> str | None:
        normalized_projector_id = _require_non_empty(
            projector_id,
            field_name='projector_id',
        )
        rows = self._client.execute(
            f'''
            SELECT partition_id
            FROM
            (
                SELECT partition_id, max(watermark_revision) AS watermark_revision
                FROM {self._database}.canonical_projector_watermarks
                WHERE projector_id = %(projector_id)s
                  AND source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            )
            ORDER BY partition_id ASC
            ''',
            {
                'projector_id': normalized_projector_id,
                'source_id': source_id,
                'stream_id': stream_id,
            },
        )
        if rows == []:
            return None
        expected_day = earliest_partition_date
        last_contiguous_partition_id: str | None = None
        for row in rows:
            partition_id = str(row[0])
            expected_partition_id = expected_day.isoformat()
            if partition_id != expected_partition_id:
                break
            last_contiguous_partition_id = partition_id
            expected_day += timedelta(days=1)
        return last_contiguous_partition_id
