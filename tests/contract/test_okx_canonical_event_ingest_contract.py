from __future__ import annotations

from datetime import UTC, datetime

import pytest
from origo_control_plane.utils.okx_canonical_event_ingest import (
    OKXSpotTradeEvent,
    deduplicate_okx_exact_duplicate_events_or_raise,
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
