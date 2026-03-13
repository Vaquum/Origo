from __future__ import annotations

from origo_control_plane.utils.bybit_canonical_event_ingest import (
    parse_bybit_spot_trade_csv,
)

_HEADER = (
    'timestamp,symbol,side,size,price,tickDirection,trdMatchID,'
    'grossValue,homeNotional,foreignNotional\n'
)


def _csv_payload(*, trd_match_id: str, timestamp_seconds: str) -> bytes:
    return (
        _HEADER
        + f'{timestamp_seconds},BTCUSDT,buy,0.001,42000.0,PlusTick,'
        + f'{trd_match_id},42000000.0,0.001,42.0\n'
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
