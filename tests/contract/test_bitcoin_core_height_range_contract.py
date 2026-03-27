from __future__ import annotations

import pytest
from origo_control_plane.bitcoin_core import (
    format_bitcoin_height_range_partition_id_or_raise,
    parse_bitcoin_height_range_partition_id_or_raise,
    resolve_bitcoin_core_node_settings_with_height_range_or_raise,
)


def test_resolve_bitcoin_core_node_settings_with_height_range_uses_explicit_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_URL', 'http://127.0.0.1:8332')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_USER', 'user')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_PASSWORD', 'pass')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_NETWORK', 'main')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS', '30')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT', '0')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT', '1')

    settings = resolve_bitcoin_core_node_settings_with_height_range_or_raise(
        headers_start_height=100,
        headers_end_height=200,
    )

    assert settings.headers_start_height == 100
    assert settings.headers_end_height == 200


def test_resolve_bitcoin_core_node_settings_with_height_range_rejects_invalid_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_URL', 'http://127.0.0.1:8332')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_USER', 'user')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_PASSWORD', 'pass')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_NETWORK', 'main')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS', '30')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT', '0')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT', '1')

    with pytest.raises(RuntimeError, match='headers_end_height must be >= headers_start_height'):
        resolve_bitcoin_core_node_settings_with_height_range_or_raise(
            headers_start_height=200,
            headers_end_height=100,
        )


def test_format_bitcoin_height_range_partition_id_is_zero_padded_and_sort_safe() -> None:
    first = format_bitcoin_height_range_partition_id_or_raise(
        start_height=1,
        end_height=9,
    )
    second = format_bitcoin_height_range_partition_id_or_raise(
        start_height=10,
        end_height=20,
    )

    assert first == '000000000001-000000000009'
    assert second == '000000000010-000000000020'
    assert sorted([second, first]) == [first, second]


def test_parse_bitcoin_height_range_partition_id_round_trips() -> None:
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=840000,
        end_height=840143,
    )

    assert parse_bitcoin_height_range_partition_id_or_raise(partition_id) == (
        840000,
        840143,
    )


def test_parse_bitcoin_height_range_partition_id_rejects_invalid_shape() -> None:
    with pytest.raises(RuntimeError, match='must match 12-digit start/end format'):
        parse_bitcoin_height_range_partition_id_or_raise('840000-840143')
