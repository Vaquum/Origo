from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from origo_control_plane.utils.bybit_canonical_event_ingest import (
    BybitSpotTradeEvent,
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)

_HEADER = (
    'timestamp,symbol,side,size,price,tickDirection,trdMatchID,'
    'grossValue,homeNotional,foreignNotional\n'
)
_HEADER_WITH_RPI = (
    'timestamp,symbol,side,size,price,tickDirection,trdMatchID,'
    'grossValue,homeNotional,foreignNotional,RPI\n'
)


def _csv_payload(
    *,
    trd_match_id: str,
    timestamp_seconds: str,
    size: str = '0.001',
    price: str = '42000.0',
    gross_value: str = '42000000.0',
    home_notional: str = '0.001',
    foreign_notional: str = '42.0',
    side: str = 'buy',
    tick_direction: str = 'PlusTick',
    rpi_text: str | None = None,
) -> bytes:
    header = _HEADER_WITH_RPI if rpi_text is not None else _HEADER
    rpi_suffix = f',{rpi_text}' if rpi_text is not None else ''
    return (
        header
        + f'{timestamp_seconds},BTCUSDT,{side},{size},{price},{tick_direction},'
        + f'{trd_match_id},{gross_value},{home_notional},{foreign_notional}{rpi_suffix}\n'
    ).encode('utf-8')


def test_bybit_parser_accepts_m_prefixed_trade_id() -> None:
    events = parse_bybit_spot_trade_csv(
        csv_content=_csv_payload(
            trd_match_id='m-998877',
            timestamp_seconds='1704067200.100',
        ),
        date_str='2024-01-01',
        day_start_ts_utc_ms=1704067200000,
        day_end_ts_utc_ms=1704153600000,
    )

    assert len(events) == 1
    assert events[0].trade_id == 998877
    assert events[0].trd_match_id == 'm-998877'


def test_bybit_parser_accepts_uuid_trade_id_and_assigns_deterministic_sequence_id() -> None:
    events = parse_bybit_spot_trade_csv(
        csv_content=_csv_payload(
            trd_match_id='08ff9568-cb50-55d6-b497-13727eec09dc',
            timestamp_seconds='1704067200.200',
        ),
        date_str='2024-01-01',
        day_start_ts_utc_ms=1704067200000,
        day_end_ts_utc_ms=1704153600000,
    )

    assert len(events) == 1
    assert events[0].trade_id == 1
    assert events[0].trd_match_id == '08ff9568-cb50-55d6-b497-13727eec09dc'


def test_bybit_parser_rejects_unsupported_trd_match_id_format() -> None:
    import pytest

    with pytest.raises(RuntimeError, match='must be m-<digits> or canonical UUID'):
        parse_bybit_spot_trade_csv(
            csv_content=_csv_payload(
                trd_match_id='not-a-valid-id',
                timestamp_seconds='1704067200.300',
            ),
            date_str='2024-01-01',
            day_start_ts_utc_ms=1704067200000,
            day_end_ts_utc_ms=1704153600000,
        )


def test_bybit_parser_accepts_zero_size_and_zero_home_notional_from_official_source() -> None:
    events = parse_bybit_spot_trade_csv(
        csv_content=_csv_payload(
            trd_match_id='82c4f9a7-00e7-5c4b-b4bb-1159ae58b81f',
            timestamp_seconds='1594115944.4363',
            size='0.0',
            price='9221.0',
            gross_value='10000.0',
            home_notional='0.0',
            foreign_notional='0.0001',
            side='Sell',
            tick_direction='ZeroMinusTick',
        ),
        date_str='2020-07-07',
        day_start_ts_utc_ms=1594080000000,
        day_end_ts_utc_ms=1594166400000,
    )

    assert len(events) == 1
    assert events[0].size_text == '0.0'
    assert events[0].home_notional_text == '0.0'
    assert events[0].quote_quantity_text == '0.0001'


def test_bybit_parser_accepts_trailing_rpi_column_and_preserves_field() -> None:
    events = parse_bybit_spot_trade_csv(
        csv_content=_csv_payload(
            trd_match_id='08ff9568-cb50-55d6-b497-13727eec09dc',
            timestamp_seconds='1774656000.1446',
            size='0.001',
            price='66374.60',
            gross_value='6.63746e+09',
            home_notional='0.001',
            foreign_notional='66.3746',
            side='Sell',
            tick_direction='ZeroMinusTick',
            rpi_text='0',
        ),
        date_str='2026-03-28',
        day_start_ts_utc_ms=1774656000000,
        day_end_ts_utc_ms=1774742400000,
    )

    assert len(events) == 1
    assert events[0].rpi_text == '0'
    assert events[0].to_payload()['rpi'] == '0'


class _RecordingClient:
    def __init__(self) -> None:
        self.insert_rows: list[tuple[object, ...]] = []

    def execute(
        self, query: str, params: dict[str, object] | list[tuple[object, ...]]
    ) -> list[tuple[object, ...]]:
        normalized = query.strip().upper()
        if normalized.startswith('SELECT'):
            return []
        if normalized.startswith('INSERT'):
            if not isinstance(params, list):
                raise RuntimeError('INSERT parameters must be row tuples')
            self.insert_rows.extend(params)
            return []
        raise RuntimeError('Unexpected query shape in recording client')


def test_bybit_canonical_write_uses_trd_match_id_as_source_offset(
    monkeypatch: Any, tmp_path: Any
) -> None:
    import origo.events.runtime_audit as runtime_audit

    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'bybit-runtime-audit.jsonl'),
    )
    runtime_audit._runtime_audit_singleton = None

    event_time_utc = datetime(2024, 1, 1, 0, 0, 0, 250000, tzinfo=UTC)
    events = [
        BybitSpotTradeEvent(
            symbol='BTCUSDT',
            trade_id=1,
            trd_match_id='m-1',
            side='buy',
            price_text='42000.0',
            size_text='0.001',
            quote_quantity_text='42.0',
            timestamp=1704067200250,
            event_time_utc=event_time_utc,
            tick_direction='PlusTick',
            gross_value_text='42000000.0',
            home_notional_text='0.001',
            foreign_notional_text='42.0',
        ),
        BybitSpotTradeEvent(
            symbol='BTCUSDT',
            trade_id=1,
            trd_match_id='08ff9568-cb50-55d6-b497-13727eec09dc',
            side='sell',
            price_text='42001.0',
            size_text='0.002',
            quote_quantity_text='84.002',
            timestamp=1704067200300,
            event_time_utc=datetime(2024, 1, 1, 0, 0, 0, 300000, tzinfo=UTC),
            tick_direction='MinusTick',
            gross_value_text='84002000.0',
            home_notional_text='0.002',
            foreign_notional_text='84.002',
        ),
    ]
    client = _RecordingClient()

    summary = write_bybit_spot_trades_to_canonical(
        client=client,  # type: ignore[arg-type]
        database='origo',
        events=events,
        run_id='contract-bybit-offset',
        ingested_at_utc=datetime(2026, 3, 13, 0, 0, 1, tzinfo=UTC),
        fast_insert_mode='assume_new_partition',
    )

    assert summary['rows_processed'] == 2
    assert summary['rows_inserted'] == 2
    assert summary['rows_duplicate'] == 0
    offsets = [str(row[5]) for row in client.insert_rows]
    assert offsets == ['m-1', '08ff9568-cb50-55d6-b497-13727eec09dc']
    event_ids = [str(row[1]) for row in client.insert_rows]
    assert len(set(event_ids)) == 2
