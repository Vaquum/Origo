from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from origo_control_plane.utils.binance_canonical_event_ingest import (
    BinanceSpotTradeEvent,
    write_binance_spot_trades_to_canonical,
)
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    BybitSpotTradeEvent,
    write_bybit_spot_trades_to_canonical,
)
from origo_control_plane.utils.okx_canonical_event_ingest import (
    OKXSpotTradeEvent,
    write_okx_spot_trades_to_canonical,
)

_WriteFn = Callable[..., dict[str, int]]
_EventFactory = Callable[[datetime, int], Any]


class _RecordingClickHouseClient:
    def __init__(self, *, partition_has_rows: bool) -> None:
        self._partition_has_rows = partition_has_rows
        self.select_calls = 0
        self.insert_calls = 0

    def execute(
        self, query: str, params: dict[str, Any] | list[tuple[object, ...]]
    ) -> list[tuple[object, ...]]:
        normalized = query.strip().upper()
        if normalized.startswith('SELECT'):
            self.select_calls += 1
            return [(1,)] if self._partition_has_rows else []
        if normalized.startswith('INSERT'):
            self.insert_calls += 1
            return []
        raise RuntimeError('Unexpected query shape')


def _binance_event(at_utc: datetime, trade_id: int) -> BinanceSpotTradeEvent:
    return BinanceSpotTradeEvent(
        partition_id=at_utc.strftime('%Y-%m-%d'),
        trade_id=trade_id,
        price_text='41000.10000000',
        quantity_text='0.01000000',
        quote_quantity_text='410.00100000',
        timestamp_ms=int(at_utc.timestamp() * 1000),
        is_buyer_maker=False,
        is_best_match=True,
    )


def _okx_event(at_utc: datetime, trade_id: int) -> OKXSpotTradeEvent:
    return OKXSpotTradeEvent(
        instrument_name='BTC-USDT',
        trade_id=trade_id,
        side='buy',
        price_text='41000.10000000',
        size_text='0.01000000',
        quote_quantity_text='410.00100000',
        timestamp=int(at_utc.timestamp() * 1000),
        event_time_utc=at_utc,
    )


def _bybit_event(at_utc: datetime, trade_id: int) -> BybitSpotTradeEvent:
    return BybitSpotTradeEvent(
        symbol='BTCUSDT',
        trade_id=trade_id,
        trd_match_id=f'm-{trade_id}',
        side='buy',
        price_text='41000.10000000',
        size_text='0.01000000',
        quote_quantity_text='410.00100000',
        timestamp=int(at_utc.timestamp() * 1000),
        event_time_utc=at_utc,
        tick_direction='PlusTick',
        gross_value_text='410.00100000',
        home_notional_text='0.01000000',
        foreign_notional_text='410.00100000',
    )


_CASES: list[tuple[str, _WriteFn, _EventFactory]] = [
    (
        'binance',
        cast(_WriteFn, write_binance_spot_trades_to_canonical),
        _binance_event,
    ),
    ('okx', cast(_WriteFn, write_okx_spot_trades_to_canonical), _okx_event),
    ('bybit', cast(_WriteFn, write_bybit_spot_trades_to_canonical), _bybit_event),
]


@pytest.mark.parametrize(('exchange_name', 'write_fn', 'event_factory'), _CASES)
def test_fast_insert_rejects_multi_partition_batch(
    exchange_name: str,
    write_fn: _WriteFn,
    event_factory: _EventFactory,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    import origo.events.runtime_audit as runtime_audit

    runtime_audit._runtime_audit_singleton = None
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / f'{exchange_name}-runtime-audit.jsonl'),
    )

    client = _RecordingClickHouseClient(partition_has_rows=False)
    start = datetime(2021, 5, 19, 0, 0, 1, tzinfo=UTC)
    events = [
        event_factory(start, 1),
        event_factory(start + timedelta(days=1), 2),
    ]

    with pytest.raises(RuntimeError, match='exactly one partition_id'):
        write_fn(
            client=client,
            database='origo',
            events=events,
            run_id='run-fast-insert',
            ingested_at_utc=datetime(2026, 3, 12, 0, 0, 1, tzinfo=UTC),
            fast_insert_mode='assume_new_partition',
        )

    assert client.select_calls == 0
    assert client.insert_calls == 0


@pytest.mark.parametrize(('exchange_name', 'write_fn', 'event_factory'), _CASES)
def test_fast_insert_rejects_non_empty_partition(
    exchange_name: str,
    write_fn: _WriteFn,
    event_factory: _EventFactory,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    import origo.events.runtime_audit as runtime_audit

    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / f'{exchange_name}-runtime-audit.jsonl'),
    )
    runtime_audit._runtime_audit_singleton = None

    client = _RecordingClickHouseClient(partition_has_rows=True)
    start = datetime(2021, 5, 19, 0, 0, 1, tzinfo=UTC)
    events = [event_factory(start, 1)]

    with pytest.raises(RuntimeError, match='requires empty target partition'):
        write_fn(
            client=client,
            database='origo',
            events=events,
            run_id='run-fast-insert',
            ingested_at_utc=datetime(2026, 3, 12, 0, 0, 1, tzinfo=UTC),
            fast_insert_mode='assume_new_partition',
        )

    assert client.select_calls == 1
    assert client.insert_calls == 0


def test_okx_fast_insert_allows_multi_day_events_with_partition_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    import origo.events.runtime_audit as runtime_audit

    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'okx-runtime-audit.jsonl'),
    )
    runtime_audit._runtime_audit_singleton = None

    client = _RecordingClickHouseClient(partition_has_rows=False)
    start = datetime(2021, 5, 19, 0, 0, 1, tzinfo=UTC)
    events = [
        _okx_event(start, 1),
        _okx_event(start + timedelta(days=1), 2),
    ]

    summary = write_okx_spot_trades_to_canonical(
        client=client,
        database='origo',
        events=events,
        run_id='run-fast-insert-override',
        ingested_at_utc=datetime(2026, 3, 12, 0, 0, 1, tzinfo=UTC),
        canonical_partition_id='2021-05-19',
        fast_insert_mode='assume_new_partition',
    )

    assert summary['rows_processed'] == 2
    assert summary['rows_inserted'] == 2
    assert summary['rows_duplicate'] == 0
    assert client.select_calls == 1
    assert client.insert_calls > 0
