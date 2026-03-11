from __future__ import annotations

from datetime import UTC, datetime

import pytest

from origo.events import (
    CanonicalEventWriteInput,
    EventWriterError,
    build_canonical_event_row,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)


def _event_input(*, offset: str = '100') -> CanonicalEventWriteInput:
    return CanonicalEventWriteInput(
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
        source_offset_or_equivalent=offset,
        source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
        ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
        payload_content_type='application/json',
        payload_encoding='utf-8',
        payload_raw=(
            b'{"qty":"0.01000000","price":"41000.12345678",'
            b'"quote_qty":"410.00123456","trade_id":"1"}'
        ),
    )


def test_build_canonical_row_is_deterministic_for_same_input() -> None:
    event_input = _event_input()

    row_1 = build_canonical_event_row(event_input)
    row_2 = build_canonical_event_row(event_input)

    assert row_1.event_id == row_2.event_id
    assert row_1.payload_sha256_raw == row_2.payload_sha256_raw
    assert row_1.payload_json == row_2.payload_json == (
        '{"price":"41000.12345678","qty":"0.01000000",'
        '"quote_qty":"410.00123456","trade_id":1}'
    )


def test_event_id_changes_when_identity_changes() -> None:
    row_1 = build_canonical_event_row(_event_input(offset='100'))
    row_2 = build_canonical_event_row(_event_input(offset='101'))
    assert row_1.event_id != row_2.event_id


def test_build_canonical_row_rejects_naive_ingested_time() -> None:
    with pytest.raises(EventWriterError, match='ingested_at_utc must be timezone-aware UTC') as exc_info:
        build_canonical_event_row(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='spot_trades',
                partition_id='btcusdt',
                source_offset_or_equivalent='100',
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2),
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=b'{"a":1}',
            )
        )
    assert exc_info.value.code == 'WRITER_INVALID_TIMESTAMP'


def test_build_canonical_row_rejects_non_json_payload_type() -> None:
    with pytest.raises(RuntimeError, match='Only application/json payloads are currently supported'):
        build_canonical_event_row(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='spot_trades',
                partition_id='btcusdt',
                source_offset_or_equivalent='100',
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
                payload_content_type='text/csv',
                payload_encoding='utf-8',
                payload_raw=b'price,qty\n1,2',
            )
        )


def test_build_canonical_row_rejects_unmapped_numeric_field() -> None:
    with pytest.raises(RuntimeError, match='missing precision rule'):
        build_canonical_event_row(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='spot_trades',
                partition_id='btcusdt',
                source_offset_or_equivalent='100',
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=b'{"trade_id":"1","unexpected_numeric":2}',
            )
        )


def test_build_canonical_row_rejects_decimal_scale_overflow() -> None:
    with pytest.raises(RuntimeError, match='exceeds configured scale'):
        build_canonical_event_row(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='spot_trades',
                partition_id='btcusdt',
                source_offset_or_equivalent='100',
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=(
                    b'{"trade_id":"1","price":"41000.12345678",'
                    b'"qty":"0.010000001","quote_qty":"410.00123456"}'
                ),
            )
        )


def test_idempotency_key_and_event_id_are_deterministic() -> None:
    key = canonical_event_idempotency_key(
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
        source_offset_or_equivalent='100',
    )
    assert key == '1|binance|spot_trades|btcusdt|100'
    event_id_1 = canonical_event_id_from_key(key)
    event_id_2 = canonical_event_id_from_key(key)
    assert event_id_1 == event_id_2
