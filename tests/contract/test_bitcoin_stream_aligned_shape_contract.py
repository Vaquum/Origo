from __future__ import annotations

import json
from datetime import UTC, datetime

from origo.query.bitcoin_stream_aligned_1s import (
    _shape_header_frame,
    _shape_mempool_frame,
    _shape_transaction_frame,
)


def _payload_rows_json(*, rows: list[dict[str, object]]) -> str:
    return json.dumps({'rows': rows}, sort_keys=True, separators=(',', ':'))


def test_shape_header_frame_is_deterministic_and_uses_latest_offset() -> None:
    aligned_at = datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC)
    ingested_at = datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC)
    payload_rows_json = _payload_rows_json(
        rows=[
            {
                'source_offset_or_equivalent': '100',
                'payload_json': json.dumps(
                    {
                        'height': 100,
                        'block_hash': 'a',
                        'prev_hash': 'z',
                        'merkle_root': 'm1',
                        'version': 1,
                        'nonce': 10,
                        'difficulty': '1000.0',
                        'timestamp_ms': 1704067200000,
                        'source_chain': 'main',
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
            {
                'source_offset_or_equivalent': '101',
                'payload_json': json.dumps(
                    {
                        'height': 101,
                        'block_hash': 'b',
                        'prev_hash': 'a',
                        'merkle_root': 'm2',
                        'version': 2,
                        'nonce': 20,
                        'difficulty': '2000.0',
                        'timestamp_ms': 1704067201000,
                        'source_chain': 'main',
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
        ]
    )
    rows = [
        (aligned_at, payload_rows_json, '100', '101', ingested_at, 'x' * 64),
    ]
    frame = _shape_header_frame(rows=rows, datetime_iso_output=False)
    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row['records_in_bucket'] == 2
    assert row['latest_height'] == 101
    assert row['latest_block_hash'] == 'b'


def test_shape_transaction_frame_aggregates_values_and_coinbase_count() -> None:
    aligned_at = datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC)
    ingested_at = datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC)
    payload_rows_json = _payload_rows_json(
        rows=[
            {
                'source_offset_or_equivalent': '840000:0:tx0',
                'payload_json': json.dumps(
                    {
                        'block_height': 840000,
                        'block_timestamp_ms': 1704067200000,
                        'txid': 'tx0',
                        'coinbase': True,
                        'source_chain': 'main',
                        'values_json': json.dumps(
                            {
                                'input_value_btc_sum': 0,
                                'output_value_btc_sum': 6.25,
                            },
                            sort_keys=True,
                            separators=(',', ':'),
                        ),
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
            {
                'source_offset_or_equivalent': '840000:1:tx1',
                'payload_json': json.dumps(
                    {
                        'block_height': 840000,
                        'block_timestamp_ms': 1704067200000,
                        'txid': 'tx1',
                        'coinbase': False,
                        'source_chain': 'main',
                        'values_json': json.dumps(
                            {
                                'input_value_btc_sum': 1.5,
                                'output_value_btc_sum': 1.4,
                            },
                            sort_keys=True,
                            separators=(',', ':'),
                        ),
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
        ]
    )
    rows = [
        (aligned_at, payload_rows_json, '840000:0:tx0', '840000:1:tx1', ingested_at, 'y' * 64),
    ]
    frame = _shape_transaction_frame(rows=rows, datetime_iso_output=False)
    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row['records_in_bucket'] == 2
    assert row['coinbase_tx_count'] == 1
    assert row['tx_count'] == 2
    assert row['total_input_value_btc'] == 1.5
    assert row['total_output_value_btc'] == 7.65


def test_shape_mempool_frame_aggregates_fee_and_vsize_stats() -> None:
    aligned_at = datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC)
    ingested_at = datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC)
    payload_rows_json = _payload_rows_json(
        rows=[
            {
                'source_offset_or_equivalent': '1704067201000:tx0',
                'payload_json': json.dumps(
                    {
                        'snapshot_at_unix_ms': 1704067201000,
                        'txid': 'tx0',
                        'fee_rate_sat_vb': '10.0',
                        'vsize': 100,
                        'first_seen_timestamp': 1704067100,
                        'rbf_flag': False,
                        'source_chain': 'main',
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
            {
                'source_offset_or_equivalent': '1704067201000:tx1',
                'payload_json': json.dumps(
                    {
                        'snapshot_at_unix_ms': 1704067201000,
                        'txid': 'tx1',
                        'fee_rate_sat_vb': '20.0',
                        'vsize': 300,
                        'first_seen_timestamp': 1704067150,
                        'rbf_flag': True,
                        'source_chain': 'main',
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                ),
            },
        ]
    )
    rows = [
        (aligned_at, payload_rows_json, '1704067201000:tx0', '1704067201000:tx1', ingested_at, 'z' * 64),
    ]
    frame = _shape_mempool_frame(rows=rows, datetime_iso_output=False)
    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row['records_in_bucket'] == 2
    assert row['tx_count'] == 2
    assert row['fee_rate_sat_vb_min'] == 10.0
    assert row['fee_rate_sat_vb_max'] == 20.0
    assert row['fee_rate_sat_vb_avg'] == 15.0
    assert row['total_vsize'] == 400
    assert row['rbf_true_count'] == 1
