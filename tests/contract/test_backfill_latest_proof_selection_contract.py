from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from origo.events.backfill_state import CanonicalBackfillStateStore
from origo.events.ingest_state import CanonicalStreamKey


class _QueryCapturingClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.query: str | None = None
        self.params: dict[str, Any] | None = None

    def execute(
        self,
        query: str,
        params: dict[str, Any],
    ) -> list[tuple[Any, ...]]:
        self.query = query
        self.params = params
        return self.rows


def test_fetch_latest_partition_proof_orders_by_revision_recorded_at_and_proof_id() -> None:
    client = _QueryCapturingClient(
        rows=[
            (
                7,
                'proof-uuid',
                'proved_complete',
                'source_and_canonical_match',
                'lexicographic',
                10,
                10,
                10,
                'first-offset',
                'last-offset',
                'source-offset-digest',
                'source-identity-digest',
                'canonical-offset-digest',
                'canonical-identity-digest',
                0,
                0,
                'proof-digest',
                '{}',
                'run-1',
                datetime(2026, 3, 29, 9, 0, tzinfo=UTC),
            )
        ]
    )
    store = CanonicalBackfillStateStore(client=client, database='origo')

    proof = store.fetch_latest_partition_proof(
        stream_key=CanonicalStreamKey(
            source_id='fred',
            stream_id='fred_series_metrics',
            partition_id='2011-01-01',
        )
    )

    assert proof is not None
    assert proof.proof_revision == 7
    assert client.query is not None
    assert (
        'ORDER BY proof_revision DESC, recorded_at_utc DESC, proof_id DESC'
        in client.query
    )
    assert client.params == {
        'source_id': 'fred',
        'stream_id': 'fred_series_metrics',
        'partition_id': '2011-01-01',
    }
