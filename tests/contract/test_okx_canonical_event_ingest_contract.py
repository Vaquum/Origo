from __future__ import annotations

from datetime import UTC, datetime

import pytest
from origo_control_plane.utils.okx_canonical_event_ingest import (
    OKXSpotTradeEvent,
    deduplicate_okx_exact_duplicate_events_or_raise,
    deduplicate_okx_exact_duplicate_frame_or_raise,
    parse_okx_spot_trade_csv,
    parse_okx_spot_trade_csv_frame,
)


def _event(
    *,
    trade_id: int,
    side: str = 'buy',
    price_text: str = '42457.6',
    size_text: str = '0.00118219',
    timestamp: int = 1704038400149,
) -> OKXSpotTradeEvent:
    return OKXSpotTradeEvent(
        instrument_name='BTC-USDT',
        trade_id=trade_id,
        side=side,
        price_text=price_text,
        size_text=size_text,
        quote_quantity_text='50.19075974',
        timestamp=timestamp,
        event_time_utc=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
    )


def test_deduplicate_okx_exact_duplicate_events_collapses_exact_duplicates() -> None:
    events = [
        _event(trade_id=465953984),
        _event(trade_id=465953984),
        _event(trade_id=465953985, side='sell', timestamp=1704038401149),
    ]

    result = deduplicate_okx_exact_duplicate_events_or_raise(events)

    assert [event.trade_id for event in result.events] == [465953984, 465953985]
    assert result.raw_row_count == 3
    assert result.exact_duplicate_row_count == 1


def test_deduplicate_okx_exact_duplicate_events_fails_on_conflicting_duplicate_payload() -> None:
    events = [
        _event(trade_id=465953984, side='buy'),
        _event(trade_id=465953984, side='sell'),
    ]

    with pytest.raises(
        RuntimeError,
        match='OKX source contains conflicting duplicate trade_id payloads',
    ):
        deduplicate_okx_exact_duplicate_events_or_raise(events)


def test_parse_okx_spot_trade_csv_preserves_high_precision_size_text() -> None:
    events = parse_okx_spot_trade_csv(
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,465953984,buy,104933.5,0.00044782999999999997,1737331200000\n'
    )

    assert len(events) == 1
    assert events[0].price_text == '104933.5'
    assert events[0].size_text == '0.00044782999999999997'


def test_parse_okx_spot_trade_csv_preserves_scientific_notation_size_text() -> None:
    events = parse_okx_spot_trade_csv(
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,465953985,sell,104933.5,1e-05,1737331201000\n'
    )

    assert len(events) == 1
    assert events[0].price_text == '104933.5'
    assert events[0].size_text == '1e-05'


def test_parse_okx_spot_trade_csv_frame_and_dedup_preserve_raw_count() -> None:
    frame = parse_okx_spot_trade_csv_frame(
        b'instrument_name,trade_id,side,price,size,created_time\n'
        b'BTC-USDT,465953984,buy,104933.5,0.001,1737331200000\n'
        b'BTC-USDT,465953984,buy,104933.5,0.001,1737331200000\n'
        b'BTC-USDT,465953985,sell,104934.0,0.002,1737331201000\n'
    )

    deduplicated = deduplicate_okx_exact_duplicate_frame_or_raise(frame)

    assert deduplicated.raw_row_count == 3
    assert deduplicated.exact_duplicate_row_count == 1
    assert deduplicated.frame.get_column('trade_id').to_list() == [465953984, 465953985]
