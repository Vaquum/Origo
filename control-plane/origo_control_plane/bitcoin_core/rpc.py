from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

import requests

from origo_control_plane.config import require_env, require_int_env


@dataclass(frozen=True)
class BitcoinCoreNodeSettings:
    rpc_url: str
    rpc_user: str
    rpc_password: str
    network: str
    rpc_timeout_seconds: int
    headers_start_height: int
    headers_end_height: int


@dataclass(frozen=True)
class BitcoinCoreNodeContract:
    chain: str
    best_block_hash: str
    best_block_height: int


def _require_non_negative_int_env(name: str) -> int:
    value = require_int_env(name)
    if value < 0:
        raise RuntimeError(f'{name} must be >= 0')
    return value


def _resolve_rpc_timeout_seconds() -> int:
    raw = require_int_env('ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS')
    if raw <= 0:
        raise RuntimeError('ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS must be > 0')
    return raw


def _resolve_network() -> str:
    network = require_env('ORIGO_BITCOIN_CORE_NETWORK')
    if network not in {'main', 'test', 'signet', 'regtest'}:
        raise RuntimeError(
            'ORIGO_BITCOIN_CORE_NETWORK must be one of '
            "[main, test, signet, regtest]"
        )
    return network


def _validate_rpc_url_or_raise(rpc_url: str) -> None:
    parsed = urlparse(rpc_url)
    if parsed.scheme not in {'http', 'https'}:
        raise RuntimeError(
            'ORIGO_BITCOIN_CORE_RPC_URL must use http or https scheme'
        )
    if parsed.netloc.strip() == '':
        raise RuntimeError(
            'ORIGO_BITCOIN_CORE_RPC_URL must include host and optional port'
        )


def _build_bitcoin_core_node_settings_or_raise(
    *,
    headers_start_height: int,
    headers_end_height: int,
) -> BitcoinCoreNodeSettings:
    if headers_start_height < 0:
        raise RuntimeError('headers_start_height must be >= 0')
    if headers_end_height < 0:
        raise RuntimeError('headers_end_height must be >= 0')
    if headers_end_height < headers_start_height:
        raise RuntimeError(
            'headers_end_height must be >= headers_start_height'
        )
    rpc_url = require_env('ORIGO_BITCOIN_CORE_RPC_URL')
    _validate_rpc_url_or_raise(rpc_url)
    return BitcoinCoreNodeSettings(
        rpc_url=rpc_url,
        rpc_user=require_env('ORIGO_BITCOIN_CORE_RPC_USER'),
        rpc_password=require_env('ORIGO_BITCOIN_CORE_RPC_PASSWORD'),
        network=_resolve_network(),
        rpc_timeout_seconds=_resolve_rpc_timeout_seconds(),
        headers_start_height=headers_start_height,
        headers_end_height=headers_end_height,
    )


def resolve_bitcoin_core_node_settings() -> BitcoinCoreNodeSettings:
    headers_start_height = _require_non_negative_int_env(
        'ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT'
    )
    headers_end_height = _require_non_negative_int_env(
        'ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT'
    )
    return _build_bitcoin_core_node_settings_or_raise(
        headers_start_height=headers_start_height,
        headers_end_height=headers_end_height,
    )


def resolve_bitcoin_core_node_settings_with_height_range_or_raise(
    *,
    headers_start_height: int,
    headers_end_height: int,
) -> BitcoinCoreNodeSettings:
    return _build_bitcoin_core_node_settings_or_raise(
        headers_start_height=headers_start_height,
        headers_end_height=headers_end_height,
    )


class BitcoinCoreRpcClient:
    def __init__(self, *, settings: BitcoinCoreNodeSettings) -> None:
        self._settings = settings

    def _call(self, method: str, params: list[Any] | None = None) -> Any:
        payload = {
            'jsonrpc': '1.0',
            'id': 'origo-bitcoin-core',
            'method': method,
            'params': [] if params is None else params,
        }
        try:
            response = requests.post(
                self._settings.rpc_url,
                json=payload,
                auth=(self._settings.rpc_user, self._settings.rpc_password),
                timeout=self._settings.rpc_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f'Bitcoin Core RPC request failed for method={method}: {exc}'
            ) from exc

        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(
                'Bitcoin Core RPC returned non-success status '
                f'for method={method}: status_code={response.status_code}, '
                f'body={response.text}'
            )

        try:
            raw_payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f'Bitcoin Core RPC returned non-JSON payload for method={method}'
            ) from exc

        if not isinstance(raw_payload, dict):
            raise RuntimeError(
                f'Bitcoin Core RPC response must be an object for method={method}'
            )
        payload_obj = cast(dict[str, Any], raw_payload)
        if 'result' not in payload_obj:
            raise RuntimeError(
                f'Bitcoin Core RPC response is missing result field for method={method}'
            )
        rpc_error = payload_obj.get('error')
        if rpc_error is not None:
            raise RuntimeError(
                f'Bitcoin Core RPC error for method={method}: {rpc_error}'
            )
        return payload_obj['result']

    def get_block_count(self) -> int:
        result = self._call('getblockcount')
        if isinstance(result, bool) or not isinstance(result, int):
            raise RuntimeError('Bitcoin Core getblockcount must return an integer')
        if result < 0:
            raise RuntimeError('Bitcoin Core getblockcount returned negative height')
        return result

    def get_best_block_hash(self) -> str:
        result = self._call('getbestblockhash')
        if not isinstance(result, str) or result.strip() == '':
            raise RuntimeError('Bitcoin Core getbestblockhash must return non-empty str')
        return result

    def get_block_hash(self, height: int) -> str:
        if height < 0:
            raise RuntimeError(f'Bitcoin block height must be >= 0, got={height}')
        result = self._call('getblockhash', [height])
        if not isinstance(result, str) or result.strip() == '':
            raise RuntimeError(
                f'Bitcoin Core getblockhash must return non-empty str for height={height}'
            )
        return result

    def get_block_header(self, block_hash: str) -> dict[str, Any]:
        if block_hash.strip() == '':
            raise RuntimeError('Bitcoin block hash must be non-empty')
        result = self._call('getblockheader', [block_hash, True])
        if not isinstance(result, dict):
            raise RuntimeError(
                f'Bitcoin Core getblockheader must return object for hash={block_hash}'
            )
        result_map = cast(dict[Any, Any], result)
        normalized: dict[str, Any] = {}
        for key, value in result_map.items():
            if not isinstance(key, str):
                raise RuntimeError('Bitcoin Core getblockheader keys must be strings')
            normalized[key] = value
        return normalized

    def get_blockchain_info(self) -> dict[str, Any]:
        result = self._call('getblockchaininfo')
        if not isinstance(result, dict):
            raise RuntimeError('Bitcoin Core getblockchaininfo must return object')
        result_map = cast(dict[Any, Any], result)
        normalized: dict[str, Any] = {}
        for key, value in result_map.items():
            if not isinstance(key, str):
                raise RuntimeError(
                    'Bitcoin Core getblockchaininfo keys must be strings'
                )
            normalized[key] = value
        return normalized

    def get_raw_mempool(self, *, verbose: bool) -> Any:
        return self._call('getrawmempool', [verbose])

    def get_block(self, block_hash: str, verbosity: int) -> Any:
        return self._call('getblock', [block_hash, verbosity])


def _read_str_field_or_raise(*, payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'Bitcoin Core field {key} must be non-empty string')
    return value


def _read_int_field_or_raise(*, payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'Bitcoin Core field {key} must be int')
    return value


def validate_bitcoin_core_node_contract_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    settings: BitcoinCoreNodeSettings,
) -> BitcoinCoreNodeContract:
    info = client.get_blockchain_info()
    chain = _read_str_field_or_raise(payload=info, key='chain')
    if chain != settings.network:
        raise RuntimeError(
            'Bitcoin Core chain mismatch: '
            f'expected={settings.network} actual={chain}'
        )

    pruned = info.get('pruned')
    if not isinstance(pruned, bool):
        raise RuntimeError('Bitcoin Core field pruned must be bool')
    if pruned:
        raise RuntimeError('Bitcoin Core node must be unpruned for S13 V1')

    ibd = info.get('initialblockdownload')
    if not isinstance(ibd, bool):
        raise RuntimeError('Bitcoin Core field initialblockdownload must be bool')
    if ibd:
        raise RuntimeError(
            'Bitcoin Core node is in initial block download mode; deterministic '
            'S13 ingest is blocked until sync completes'
        )

    blocks = _read_int_field_or_raise(payload=info, key='blocks')
    if blocks < settings.headers_end_height:
        raise RuntimeError(
            'Bitcoin Core node is behind requested header range: '
            f'blocks={blocks}, requested_end_height={settings.headers_end_height}'
        )

    best_block_hash = client.get_best_block_hash()
    tip_height = client.get_block_count()
    if tip_height < settings.headers_end_height:
        raise RuntimeError(
            'Bitcoin Core tip height is behind requested header range: '
            f'tip_height={tip_height}, requested_end_height={settings.headers_end_height}'
        )

    return BitcoinCoreNodeContract(
        chain=chain,
        best_block_hash=best_block_hash,
        best_block_height=tip_height,
    )
