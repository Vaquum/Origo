from __future__ import annotations

import json
from pathlib import Path

import pytest

from origo.events import (
    assert_payload_json_has_no_float_values,
    canonical_source_precision_registry_contract,
    canonicalize_payload_json_with_precision,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT_PATH = _REPO_ROOT / 'contracts' / 'canonical-source-precision-v1.json'


def test_precision_registry_contract_json_matches_python_contract() -> None:
    payload = json.loads(_CONTRACT_PATH.read_text(encoding='utf-8'))
    assert payload == canonical_source_precision_registry_contract()


def test_canonicalize_payload_json_preserves_registry_numeric_types() -> None:
    payload_json = canonicalize_payload_json_with_precision(
        source_id='binance',
        stream_id='binance_spot_trades',
        payload_raw=(
            b'{"qty":"0.01000000","price":"41000.12345678",'
            b'"quote_qty":"410.00123456","trade_id":"1"}'
        ),
        payload_encoding='utf-8',
    )
    parsed = json.loads(payload_json)
    assert parsed == {
        'price': '41000.12345678',
        'qty': '0.01000000',
        'quote_qty': '410.00123456',
        'trade_id': 1,
    }
    assert_payload_json_has_no_float_values(
        source_id='binance',
        stream_id='binance_spot_trades',
        payload_json=payload_json,
    )


def test_assert_payload_json_has_no_float_values_rejects_float() -> None:
    with pytest.raises(RuntimeError, match='Float value found in canonical payload_json'):
        assert_payload_json_has_no_float_values(
            source_id='binance',
            stream_id='binance_spot_trades',
            payload_json='{"trade_id":1,"price":41000.12}',
        )


def test_canonicalize_payload_json_rejects_unknown_stream_registry() -> None:
    with pytest.raises(RuntimeError, match='No source precision rules found'):
        canonicalize_payload_json_with_precision(
            source_id='binance',
            stream_id='unknown_stream',
            payload_raw=b'{"trade_id":"1"}',
            payload_encoding='utf-8',
        )


def test_canonicalize_payload_json_rejects_non_finite_numbers() -> None:
    with pytest.raises(RuntimeError, match='Non-finite numeric constant'):
        canonicalize_payload_json_with_precision(
            source_id='binance',
            stream_id='binance_spot_trades',
            payload_raw=b'{"trade_id":NaN}',
            payload_encoding='utf-8',
        )


def test_canonicalize_payload_json_supports_okx_spot_trade_payload() -> None:
    payload_json = canonicalize_payload_json_with_precision(
        source_id='okx',
        stream_id='okx_spot_trades',
        payload_raw=(
            b'{"instrument_name":"BTC-USDT","trade_id":"12345","side":"buy",'
            b'"price":"42500.12340000","size":"0.01000000","timestamp":"1704067200000"}'
        ),
        payload_encoding='utf-8',
    )
    parsed = json.loads(payload_json)
    assert parsed == {
        'instrument_name': 'BTC-USDT',
        'price': '42500.12340000',
        'side': 'buy',
        'size': '0.01000000',
        'timestamp': 1704067200000,
        'trade_id': 12345,
    }
    assert_payload_json_has_no_float_values(
        source_id='okx',
        stream_id='okx_spot_trades',
        payload_json=payload_json,
    )


def test_canonicalize_payload_json_supports_bybit_spot_trade_payload() -> None:
    payload_json = canonicalize_payload_json_with_precision(
        source_id='bybit',
        stream_id='bybit_spot_trades',
        payload_raw=(
            b'{"symbol":"BTCUSDT","trade_id":"9001","trd_match_id":"abc","side":"buy",'
            b'"price":"44235.60","size":"0.142","quote_quantity":"6281.455199999999",'
            b'"timestamp":"1704153600360","tick_direction":"ZeroMinusTick",'
            b'"gross_value":"6.2814552e+11","home_notional":"0.142",'
            b'"foreign_notional":"6281.455199999999"}'
        ),
        payload_encoding='utf-8',
    )
    parsed = json.loads(payload_json)
    assert parsed == {
        'foreign_notional': '6281.455199999999000',
        'gross_value': '628145520000.000000000000000',
        'home_notional': '0.142000000000000',
        'price': '44235.600000000000000',
        'quote_quantity': '6281.455199999999000',
        'side': 'buy',
        'size': '0.142000000000000',
        'symbol': 'BTCUSDT',
        'tick_direction': 'ZeroMinusTick',
        'timestamp': 1704153600360,
        'trade_id': 9001,
        'trd_match_id': 'abc',
    }
    assert_payload_json_has_no_float_values(
        source_id='bybit',
        stream_id='bybit_spot_trades',
        payload_json=payload_json,
    )


def test_canonicalize_payload_json_supports_bitcoin_block_header_payload() -> None:
    payload_json = canonicalize_payload_json_with_precision(
        source_id='bitcoin_core',
        stream_id='bitcoin_block_headers',
        payload_raw=(
            b'{"height":"840000","block_hash":"00000000000000000000abc",'
            b'"prev_hash":"00000000000000000000def","merkle_root":"mroot",'
            b'"version":"536870912","nonce":"1305998796",'
            b'"difficulty":"860221984436.2223","timestamp_ms":"1713571765000",'
            b'"source_chain":"main"}'
        ),
        payload_encoding='utf-8',
    )
    parsed = json.loads(payload_json)
    assert parsed == {
        'block_hash': '00000000000000000000abc',
        'difficulty': '860221984436.222300000000000000',
        'height': 840000,
        'merkle_root': 'mroot',
        'nonce': 1305998796,
        'prev_hash': '00000000000000000000def',
        'source_chain': 'main',
        'timestamp_ms': 1713571765000,
        'version': 536870912,
    }
    assert_payload_json_has_no_float_values(
        source_id='bitcoin_core',
        stream_id='bitcoin_block_headers',
        payload_json=payload_json,
    )
