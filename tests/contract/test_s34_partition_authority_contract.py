from __future__ import annotations

from typing import Any

from origo_control_plane.s34_partition_authority import (
    load_grouped_nonterminal_partition_ids_or_raise,
    load_nonterminal_partition_ids_for_stream_or_raise,
)


class _FakeClickHouseClient:
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


def test_load_nonterminal_partition_ids_for_stream_uses_event_manifest_and_proof_candidates() -> None:
    client = _FakeClickHouseClient(rows=[('2024-01-11',), ('2024-01-12',)])

    result = load_nonterminal_partition_ids_for_stream_or_raise(
        client=client,
        database='origo',
        source_id='etf',
        stream_id='etf_daily_metrics',
        terminal_states=('proved_complete', 'empty_proved'),
    )

    assert result == ('2024-01-11', '2024-01-12')
    assert client.query is not None
    assert 'canonical_event_log_active_v1' in client.query
    assert 'canonical_backfill_source_manifests' in client.query
    assert 'canonical_backfill_partition_proofs' in client.query
    assert (
        'argMax(state, tuple(proof_revision, recorded_at_utc, proof_id)) AS state'
        in client.query
    )
    assert client.params == {
        'source_id': 'etf',
        'stream_id': 'etf_daily_metrics',
        'terminal_states': ('proved_complete', 'empty_proved'),
    }


def test_load_grouped_nonterminal_partition_ids_uses_all_slice34_candidate_tables() -> None:
    client = _FakeClickHouseClient(
        rows=[
            ('etf', 'etf_daily_metrics', '2024-01-11'),
            ('bitcoin_core', 'bitcoin_block_headers', '000000840288-000000840431'),
        ]
    )

    result = load_grouped_nonterminal_partition_ids_or_raise(
        client=client,
        database='origo',
        stream_pair_filter_sql="(source_id, stream_id) IN (('etf', 'etf_daily_metrics'))",
        terminal_states=('proved_complete', 'empty_proved'),
    )

    assert result == {
        ('etf', 'etf_daily_metrics'): ['2024-01-11'],
        ('bitcoin_core', 'bitcoin_block_headers'): ['000000840288-000000840431'],
    }
    assert client.query is not None
    assert 'canonical_event_log_active_v1' in client.query
    assert 'canonical_backfill_source_manifests' in client.query
    assert 'canonical_backfill_partition_proofs' in client.query
    assert (
        'argMax(state, tuple(proof_revision, recorded_at_utc, proof_id)) AS state'
        in client.query
    )
    assert client.params == {
        'terminal_states': ('proved_complete', 'empty_proved')
    }
