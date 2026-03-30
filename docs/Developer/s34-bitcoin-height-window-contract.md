# Slice 34 Developer: Bitcoin Height-Window Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Slice reference: S34 (`S34-C7a`, `S34-07a`, `S34-C7b`, `S34-07b`, `S34-C7c`, `S34-07c`)

## Purpose and scope
- Defines the explicit Dagster height-window contract for height-based Bitcoin backfill runs in Slice 34.
- Scope covers height-based Bitcoin datasets only:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`
- `bitcoin_mempool_state` is intentionally out of scope for this contract because it remains time-native and daily snapshot-partitioned.

## Inputs and outputs with contract shape
- Launchpad config fields:
  - `height_start`
  - `height_end`
- Dagster run tags:
  - `origo.backfill.height_start`
  - `origo.backfill.height_end`
- Contract loader:
  - `origo_control_plane.backfill.runtime_contract.load_backfill_height_window_or_raise`
- Node settings builder:
  - `origo_control_plane.bitcoin_core.resolve_bitcoin_core_node_settings_with_height_range_or_raise`

## Data definitions (field names, types, units, timezone, nullability)
- `height_start`
  - integer
  - inclusive Bitcoin block height
  - required on Dagit Launchpad runs
- `height_end`
  - integer
  - inclusive Bitcoin block height
  - required on Dagit Launchpad runs
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
- Dagster Launchpad config is the first-class manual control surface for height-based Bitcoin backfill execution, with run-tag fallback kept for repo-native controllers.
- Height-based Bitcoin canonical events now stamp one zero-padded `height_range` `partition_id` per Dagster run window:
  - format: `000000840000-000000840999`
  - ordering: lexicographically sortable in the same order as numeric height windows
- `bitcoin_mempool_state` remains time-native and keeps UTC-day snapshot partitions; it is not coerced into the height-range contract.
- All seven Bitcoin Slice 34 assets now enter the canonical proof state machine before a partition can be treated as complete:
  - record source manifest in ClickHouse
  - record explicit partition-state transitions
  - emit terminal proof or quarantine outcome
- Reconcile mode may prove an already-written Bitcoin partition directly from source-vs-canonical evidence; it must not silently bypass proof state.
- RPC connection/auth/network still come from env contract.

## Failure modes, warnings, and error codes
- Missing height window in both Launchpad config and run tags: fail loudly.
- Non-integer height window values in config/tags: fail loudly.
- Negative height window values: fail loudly.
- `height_end < height_start`: fail loudly.

## Determinism/replay notes
- The same run tags must produce the same requested node height window.
- The same height window must produce the same canonical `partition_id` for all six chain-derived Bitcoin datasets.
- Bitcoin assets must not write canonical rows without recording `source_manifested`, `canonical_written_unproved`, and terminal proof/quarantine state.
- Contract coverage lives in:
  - `tests/contract/test_backfill_runtime_contract.py`
  - `tests/contract/test_bitcoin_core_height_range_contract.py`
  - `tests/contract/test_bitcoin_partition_truth_contract.py`
  - `tests/contract/test_bitcoin_canonical_event_ingest_contract.py`

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
- Example Dagit Launchpad config for a Bitcoin height-window run:
  - `height_start: 840000`
  - `height_end: 840999`
- Example repo-native Dagster tags for a Bitcoin height-window run:
  - `origo.backfill.height_start=840000`
  - `origo.backfill.height_end=840999`
