from __future__ import annotations

from datetime import UTC, datetime

from origo_control_plane.bitcoin_core import (
    format_bitcoin_height_range_partition_id_or_raise,
)
from origo_control_plane.s20_bitcoin_proof_common import (
    build_fixture_canonical_events,
    build_fixture_rows_by_dataset,
)


def test_bitcoin_chain_fixture_events_use_height_range_partition_ids() -> None:
    rows_by_dataset = build_fixture_rows_by_dataset()
    events = build_fixture_canonical_events(rows_by_dataset=rows_by_dataset)
    expected_partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=840000,
        end_height=840001,
    )

    for stream_id in (
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ):
        stream_partition_ids = {
            event.partition_id for event in events if event.stream_id == stream_id
        }
        assert stream_partition_ids == {expected_partition_id}


def test_bitcoin_mempool_fixture_events_remain_daily_snapshot_partitioned() -> None:
    rows_by_dataset = build_fixture_rows_by_dataset()
    events = build_fixture_canonical_events(rows_by_dataset=rows_by_dataset)
    mempool_partition_ids = {
        event.partition_id for event in events if event.stream_id == 'bitcoin_mempool_state'
    }

    assert mempool_partition_ids == {
        datetime(2024, 4, 20, 0, 0, 2, tzinfo=UTC).date().isoformat()
    }
