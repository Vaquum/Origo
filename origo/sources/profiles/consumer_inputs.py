"""Bar publication inputs, independent of unrelated projection activations."""

from __future__ import annotations

from ..contracts import Row, Snapshot, StateRecord
from ..hashing import content_hash, state_token
from ..storage import SourceStore

CONSUMED_COMPONENTS = frozenset(('time', 'dollar', 'time_latest', 'raw_latest'))


def input_token(source: str, snapshot: Snapshot) -> str:
    rows: list[Row] = [('origo-bar-publication-input-v1', source)]
    for record in sorted(
        snapshot.records, key=lambda item: (item.partition.provisional, item.partition.key)
    ):
        rows.append(
            (
                record.partition.key,
                record.partition.start,
                record.partition.end,
                record.partition.provisional,
                record.revision,
                record.build_id,
            )
        )
        rows.extend(
            (name, digest)
            for name, digest in sorted(record.component_hashes)
            if name in CONSUMED_COMPONENTS
        )
    return content_hash(rows, schema_version=1)


def month_input_tokens(
    source: str, snapshot: Snapshot, *, export_start_date: str
) -> dict[str, str]:
    grouped: dict[str, list[StateRecord]] = {}
    for record in snapshot.records:
        month = record.partition.start.strftime('%Y-%m')
        if month >= export_start_date[:7]:
            grouped.setdefault(month, []).append(record)
    return {
        month: input_token(source, Snapshot('', tuple(grouped[month]))) for month in sorted(grouped)
    }


def verified_snapshot(store: SourceStore, pinned: Snapshot) -> Snapshot:
    """Accept canonical addon drift while retaining exactly the provisional rows read."""
    canonical = store.snapshot(canonical_only=True)
    original = Snapshot(
        '', tuple(record for record in pinned.records if not record.partition.provisional)
    )
    if input_token(store.spec.key, canonical) != input_token(store.spec.key, original):
        raise RuntimeError(
            'Canonical state changed while rendering; staged output remains unpublished.'
        )
    records = canonical.records + tuple(
        record for record in pinned.records if record.partition.provisional
    )
    return Snapshot(state_token(store.spec.key, records), records)
