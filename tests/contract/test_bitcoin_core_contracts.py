from __future__ import annotations

from typing import Any, cast

import pytest
from origo_control_plane.bitcoin_core.rpc import (
    BitcoinCoreNodeSettings,
    BitcoinCoreRpcClient,
    resolve_bitcoin_core_node_settings,
    validate_bitcoin_core_node_contract_or_raise,
)


class _FakeBitcoinCoreRpcClient:
    def __init__(self, *, blockchain_info: dict[str, Any], tip_height: int) -> None:
        self._blockchain_info = blockchain_info
        self._tip_height = tip_height

    def get_blockchain_info(self) -> dict[str, Any]:
        return dict(self._blockchain_info)

    def get_best_block_hash(self) -> str:
        return '0' * 64

    def get_block_count(self) -> int:
        return self._tip_height


def _set_valid_bitcoin_core_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_URL', 'http://localhost:8332')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_USER', 'rpc-user')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_PASSWORD', 'rpc-password')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_NETWORK', 'main')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS', '30')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT', '840000')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT', '840010')


def test_resolve_bitcoin_core_node_settings_accepts_valid_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_bitcoin_core_env(monkeypatch)

    settings = resolve_bitcoin_core_node_settings()

    assert settings.rpc_url == 'http://localhost:8332'
    assert settings.rpc_user == 'rpc-user'
    assert settings.rpc_password == 'rpc-password'
    assert settings.network == 'main'
    assert settings.rpc_timeout_seconds == 30
    assert settings.headers_start_height == 840000
    assert settings.headers_end_height == 840010


def test_resolve_bitcoin_core_node_settings_rejects_invalid_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_bitcoin_core_env(monkeypatch)
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_NETWORK', 'bitcoin-mainnet')

    with pytest.raises(
        RuntimeError, match='ORIGO_BITCOIN_CORE_NETWORK must be one of'
    ):
        resolve_bitcoin_core_node_settings()


def test_resolve_bitcoin_core_node_settings_rejects_invalid_height_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_bitcoin_core_env(monkeypatch)
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT', '900001')
    monkeypatch.setenv('ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT', '900000')

    with pytest.raises(
        RuntimeError,
        match='ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT must be >=',
    ):
        resolve_bitcoin_core_node_settings()


def test_validate_bitcoin_core_node_contract_accepts_synced_unpruned_node() -> None:
    settings = BitcoinCoreNodeSettings(
        rpc_url='http://localhost:8332',
        rpc_user='rpc-user',
        rpc_password='rpc-password',
        network='main',
        rpc_timeout_seconds=30,
        headers_start_height=840000,
        headers_end_height=840010,
    )
    fake_client = _FakeBitcoinCoreRpcClient(
        blockchain_info={
            'chain': 'main',
            'pruned': False,
            'initialblockdownload': False,
            'blocks': 840100,
        },
        tip_height=840100,
    )

    contract = validate_bitcoin_core_node_contract_or_raise(
        client=cast(BitcoinCoreRpcClient, fake_client),
        settings=settings,
    )

    assert contract.chain == 'main'
    assert contract.best_block_height == 840100
    assert contract.best_block_hash == '0' * 64


def test_validate_bitcoin_core_node_contract_rejects_pruned_node() -> None:
    settings = BitcoinCoreNodeSettings(
        rpc_url='http://localhost:8332',
        rpc_user='rpc-user',
        rpc_password='rpc-password',
        network='main',
        rpc_timeout_seconds=30,
        headers_start_height=840000,
        headers_end_height=840010,
    )
    fake_client = _FakeBitcoinCoreRpcClient(
        blockchain_info={
            'chain': 'main',
            'pruned': True,
            'initialblockdownload': False,
            'blocks': 840100,
        },
        tip_height=840100,
    )

    with pytest.raises(RuntimeError, match='must be unpruned'):
        validate_bitcoin_core_node_contract_or_raise(
            client=cast(BitcoinCoreRpcClient, fake_client),
            settings=settings,
        )
