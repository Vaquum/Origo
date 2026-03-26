from __future__ import annotations

import pytest
from origo_control_plane.bitcoin_core import (
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
