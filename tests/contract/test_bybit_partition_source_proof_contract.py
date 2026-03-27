from __future__ import annotations

from datetime import UTC, datetime

from origo_control_plane.utils.bybit_canonical_event_ingest import (
    BybitSpotTradeEvent,
    build_bybit_partition_source_proof,
)


def test_bybit_partition_source_proof_uses_trd_match_id_lexicographic_identity() -> None:
    partition_id = '2026-03-25'
    events = [
        BybitSpotTradeEvent(
            symbol='BTCUSDT',
            trade_id=1,
            trd_match_id='ffffffff-ffff-ffff-ffff-ffffffffffff',
            side='Buy',
            price_text='100000.0',
            size_text='0.1',
            quote_quantity_text='10000.0',
            timestamp=1_742_860_800_000,
            event_time_utc=datetime(2026, 3, 25, 0, 0, tzinfo=UTC),
            tick_direction='PlusTick',
            gross_value_text='10000.0',
            home_notional_text='0.1',
            foreign_notional_text='10000.0',
        ),
        BybitSpotTradeEvent(
            symbol='BTCUSDT',
            trade_id=2,
            trd_match_id='00000000-0000-0000-0000-000000000001',
            side='Sell',
            price_text='99999.0',
            size_text='0.2',
            quote_quantity_text='19999.8',
            timestamp=1_742_860_801_000,
            event_time_utc=datetime(2026, 3, 25, 0, 0, 1, tzinfo=UTC),
            tick_direction='MinusTick',
            gross_value_text='19999.8',
            home_notional_text='0.2',
            foreign_notional_text='19999.8',
        ),
    ]

    proof = build_bybit_partition_source_proof(
        partition_id=partition_id,
        events=events,
        source_file_url='https://public.bybit.com/trading/BTCUSDT/BTCUSDT20260325.csv.gz',
        source_filename='BTCUSDT20260325.csv.gz',
        gzip_sha256='1' * 64,
        csv_sha256='2' * 64,
        source_etag='etag-1',
    )

    assert proof.offset_ordering == 'lexicographic'
    assert proof.allow_duplicate_offsets is True
    assert proof.first_offset_or_equivalent == '00000000-0000-0000-0000-000000000001'
    assert proof.last_offset_or_equivalent == 'ffffffff-ffff-ffff-ffff-ffffffffffff'
