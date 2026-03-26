# Slice 34 Developer: Bitcoin Height-Window Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-26
- Slice reference: S34 (`S34-C7a`, `S34-07a`)

## Purpose and scope
- Defines the explicit Dagster run-tag contract for height-based Bitcoin backfill runs in Slice 34.
- Scope covers height-based Bitcoin datasets only:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`

## Inputs and outputs with contract shape
- Required Dagster run tags:
  - `origo.backfill.height_start`
  - `origo.backfill.height_end`
- Contract loader:
  - `origo_control_plane.backfill.runtime_contract.load_backfill_height_window_or_raise`
- Node settings builder:
  - `origo_control_plane.bitcoin_core.resolve_bitcoin_core_node_settings_with_height_range_or_raise`

## Data definitions (field names, types, units, timezone, nullability)
- `origo.backfill.height_start`
  - integer
  - inclusive Bitcoin block height
  - required
- `origo.backfill.height_end`
  - integer
  - inclusive Bitcoin block height
  - required
- Range rule:
  - `height_end >= height_start`

## Source/provenance and freshness semantics
- Height-based Bitcoin assets no longer take per-run backfill boundaries from static env.
- Dagster run tags are now the per-run range authority for height-based Bitcoin backfill execution.
- RPC connection/auth/network still come from env contract.

## Failure modes, warnings, and error codes
- Missing height tags: fail loudly.
- Non-integer height tags: fail loudly.
- Negative height tags: fail loudly.
- `height_end < height_start`: fail loudly.

## Determinism/replay notes
- The same run tags must produce the same requested node height window.
- Contract coverage lives in:
  - `tests/contract/test_backfill_runtime_contract.py`
  - `tests/contract/test_bitcoin_core_height_range_contract.py`

## Environment variables and required config
- Still required for node access:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
- Not used for per-run Slice 34 height selection anymore in height-based Bitcoin assets:
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`

## Minimal examples
- Example Dagster tags for a Bitcoin height-window run:
  - `origo.backfill.height_start=840000`
  - `origo.backfill.height_end=840999`
