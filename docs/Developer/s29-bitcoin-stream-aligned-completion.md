# S29 Bitcoin Stream Aligned Completion

## Metadata
- Owner: Origo API
- Last updated: 2026-03-12
- Slice reference: S29 (API v0.1.20, control-plane v1.2.64)

## Purpose and scope
- Complete `aligned_1s` coverage for remaining Bitcoin stream datasets.
- Scope is only:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`

## Capability delivered
- Added stream-aligned query module:
  - `origo/query/bitcoin_stream_aligned_1s.py`
- Added stream-aligned projector utility:
  - `control-plane/origo_control_plane/utils/bitcoin_stream_aligned_projector.py`
- Wired three Bitcoin stream assets to execute aligned projection writes into `canonical_aligned_1s_aggregates`.
- Added aligned planner path and strict selected-field allowlists for all three datasets.
- Added API/export aligned allowlist coverage for all three datasets.

## Contract behavior
- Aligned storage contract is mandatory and fail-loud (`canonical_aligned_1s_aggregates`).
- Query path enforces deterministic ordering by:
  - `aligned_at_utc`
  - stream offset tie-breakers
- Dataset-specific aligned outputs:
  - headers: latest block-state fields + bucket metadata
  - transactions: tx/coinbase counts + value sums + latest tx metadata
  - mempool: fee stats, vsize stats, first-seen bounds, RBF count + latest snapshot metadata

## Proof coverage
- Acceptance/shape contract:
  - `tests/contract/test_bitcoin_stream_aligned_shape_contract.py`
- Planner/field contract:
  - `tests/contract/test_bitcoin_aligned_planner.py`
- Canonical aligned SQL target proof:
  - `tests/contract/test_bitcoin_event_projection_query_contract.py`
- Canonical aligned storage negative proofs for stream path:
  - `tests/contract/test_bitcoin_stream_aligned_storage_contract.py`
- Replay determinism proof:
  - `tests/replay/test_native_query_replay.py`

## Guardrails
- Fail-loud contract checks are active for missing aligned table/column/type drift.
- Rights/auth/strict/error taxonomy remain shared through existing API guardrails (`200/404/409/503`).
- All currently onboarded datasets are aligned-capable after this slice.

## Key implementation files
- `origo/query/aligned_core.py`
- `origo/query/bitcoin_stream_aligned_1s.py`
- `api/origo_api/main.py`
- `control-plane/origo_control_plane/jobs/raw_export_native.py`
- `control-plane/origo_control_plane/assets/bitcoin_block_headers_to_origo.py`
- `control-plane/origo_control_plane/assets/bitcoin_block_transactions_to_origo.py`
- `control-plane/origo_control_plane/assets/bitcoin_mempool_state_to_origo.py`
- `control-plane/origo_control_plane/utils/bitcoin_stream_aligned_projector.py`
