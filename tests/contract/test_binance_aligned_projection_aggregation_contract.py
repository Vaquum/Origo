from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from origo.query.binance_aligned_1s import _shape_aligned_frame


def _bucket_payload(*, trade_id: str, price: str, qty: str, quote_qty: str) -> str:
    return json.dumps(
        {
            'rows': [
                {
                    'source_offset_or_equivalent': trade_id,
                    'payload_json': json.dumps(
                        {
                            'price': price,
                            'qty': qty,
                            'quote_qty': quote_qty,
                        },
                        sort_keys=True,
                        separators=(',', ':'),
                    ),
                }
            ]
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def test_binance_aligned_shape_aggregates_split_buckets() -> None:
    rows = [
        (
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            _bucket_payload(
                trade_id='101',
                price='101.0',
                qty='2.0',
                quote_qty='202.0',
            ),
            '101',
            'bucket-a',
        ),
        (
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            _bucket_payload(
                trade_id='100',
                price='100.0',
                qty='1.0',
                quote_qty='100.0',
            ),
            '100',
            'bucket-b',
        ),
        (
            datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            _bucket_payload(
                trade_id='102',
                price='102.0',
                qty='0.5',
                quote_qty='51.0',
            ),
            '102',
            'bucket-c',
        ),
    ]

    frame = _shape_aligned_frame(rows=rows, datetime_iso_output=False)
    payload = frame.to_dicts()

    assert len(payload) == 2

    first = payload[0]
    assert int(first['trade_count']) == 2
    assert float(first['open_price']) == pytest.approx(100.0)
    assert float(first['high_price']) == pytest.approx(101.0)
    assert float(first['low_price']) == pytest.approx(100.0)
    assert float(first['close_price']) == pytest.approx(101.0)
    assert float(first['quantity_sum']) == pytest.approx(3.0)
    assert float(first['quote_volume_sum']) == pytest.approx(302.0)

    second = payload[1]
    assert int(second['trade_count']) == 1
    assert float(second['open_price']) == pytest.approx(102.0)
    assert float(second['close_price']) == pytest.approx(102.0)
    assert float(second['quantity_sum']) == pytest.approx(0.5)
    assert float(second['quote_volume_sum']) == pytest.approx(51.0)


def test_binance_aligned_shape_supports_iso_datetime_output() -> None:
    rows = [
        (
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            _bucket_payload(
                trade_id='100',
                price='100.0',
                qty='1.0',
                quote_qty='100.0',
            ),
            '100',
            'bucket-a',
        )
    ]

    frame = _shape_aligned_frame(rows=rows, datetime_iso_output=True)
    payload = frame.to_dicts()

    assert payload == [
        {
            'aligned_at_utc': '2024-01-01T00:00:00.000Z',
            'open_price': 100.0,
            'high_price': 100.0,
            'low_price': 100.0,
            'close_price': 100.0,
            'quantity_sum': 1.0,
            'quote_volume_sum': 100.0,
            'trade_count': 1,
        }
    ]
