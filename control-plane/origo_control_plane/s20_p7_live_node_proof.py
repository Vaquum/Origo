from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from origo_control_plane.bitcoin_core import (
    BitcoinCoreRpcClient,
    resolve_bitcoin_core_node_settings,
    validate_bitcoin_core_node_contract_or_raise,
)

_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _require_str(payload: dict[str, Any], *, key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'Bitcoin RPC field {key} must be non-empty string')
    return value


def _require_int(payload: dict[str, Any], *, key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'Bitcoin RPC field {key} must be int')
    return value


def _require_bool(payload: dict[str, Any], *, key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise RuntimeError(f'Bitcoin RPC field {key} must be bool')
    return value


def run_s20_p7_live_node_proof() -> dict[str, Any]:
    settings = resolve_bitcoin_core_node_settings()
    client = BitcoinCoreRpcClient(settings=settings)

    contract = validate_bitcoin_core_node_contract_or_raise(
        client=client,
        settings=settings,
    )

    blockchain_info = client.get_blockchain_info()
    chain = _require_str(blockchain_info, key='chain')
    if chain != settings.network:
        raise RuntimeError(
            'Live-node chain mismatch: '
            f'expected={settings.network} observed={chain}'
        )

    initial_block_download = _require_bool(
        blockchain_info,
        key='initialblockdownload',
    )
    if initial_block_download:
        raise RuntimeError(
            'Live-node proof precondition failed: node is still in IBD'
        )

    blocks = _require_int(blockchain_info, key='blocks')
    headers = _require_int(blockchain_info, key='headers')
    if blocks <= 0:
        raise RuntimeError(f'Live-node proof requires positive block height, got={blocks}')
    if headers < blocks:
        raise RuntimeError(
            'Live-node proof expected headers >= blocks, '
            f'observed headers={headers} blocks={blocks}'
        )

    tip_height = contract.best_block_height
    tip_hash = client.get_block_hash(tip_height)
    tip_header = client.get_block_header(tip_hash)
    tip_header_hash = _require_str(tip_header, key='hash')
    if tip_header_hash != tip_hash:
        raise RuntimeError(
            'Live-node proof tip hash mismatch between getblockhash and getblockheader'
        )

    previous_height = tip_height - 1
    previous_hash = client.get_block_hash(previous_height)
    previous_header = client.get_block_header(previous_hash)
    previous_header_hash = _require_str(previous_header, key='hash')
    if previous_header_hash != previous_hash:
        raise RuntimeError('Live-node proof previous hash mismatch for parent block')

    tip_previous_block_hash = _require_str(tip_header, key='previousblockhash')
    if tip_previous_block_hash != previous_hash:
        raise RuntimeError(
            'Live-node linkage mismatch: tip.previousblockhash does not match parent hash'
        )

    return {
        'proof_scope': (
            'Slice 20 S20-P7 live-node gate for Bitcoin event-sourcing proof '
            '(non-IBD precondition and live header linkage checks)'
        ),
        'node_contract': {
            'chain': contract.chain,
            'best_block_height': contract.best_block_height,
            'best_block_hash': contract.best_block_hash,
        },
        'ibd': initial_block_download,
        'blocks': blocks,
        'headers': headers,
        'tip_height': tip_height,
        'tip_hash': tip_hash,
        'parent_height': previous_height,
        'parent_hash': previous_hash,
        'live_node_gate_verified': True,
    }


def main() -> None:
    payload = run_s20_p7_live_node_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-p7-live-node-gate.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
