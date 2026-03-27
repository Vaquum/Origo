# Slice 34 Developer: Bitcoin Backfill Runner

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-27
- Slice reference: S34 (`S34-C7d`, `S34-07d`, `S34-G2`)

## Purpose and scope
- Defines the repo-native Slice 34 Bitcoin backfill/controller path.
- Scope is explicitly split:
  - height-range chain datasets:
    - `bitcoin_block_headers`
    - `bitcoin_block_transactions`
    - `bitcoin_block_fee_totals`
    - `bitcoin_block_subsidy_schedule`
    - `bitcoin_network_hashrate_estimate`
    - `bitcoin_circulating_supply`
  - separate mempool daily path:
    - `bitcoin_mempool_state`
- The runner uses real Dagster jobs only. It does not invoke asset code directly.

## Inputs and outputs with contract shape
- Runner entrypoint:
  - `control-plane/origo_control_plane/s34_bitcoin_backfill_runner.py`
- Modes:
  - `chain`
  - `chain-sequence`
  - `mempool`
- Required inputs for `chain`:
  - `dataset`
  - `plan_end_height`
  - `batch_size_blocks`
  - `projection_mode`
  - `runtime_audit_mode`
- Required inputs for `chain-sequence`:
  - `plan_end_height`
  - `batch_size_blocks`
  - `projection_mode`
  - `runtime_audit_mode`
- Required inputs for `mempool`:
  - `projection_mode`
  - `runtime_audit_mode`
- Optional inputs:
  - `run_id_prefix`
  - `run_id`
  - `max_batches`
  - `requested_partition_id` for mempool path
- Output:
  - chain mode:
    - `dataset`
    - `job_name`
    - `plan_end_height`
    - `batch_size_blocks`
    - `completed_batch_count`
    - `completed_batches`
    - `controller_stopped_reason`
  - chain-sequence mode:
    - `datasets`
    - `plan_end_height`
    - `batch_size_blocks`
    - `completed_dataset_count`
    - `completed_datasets`
    - `controller_stopped_reason`
  - mempool mode:
    - `dataset`
    - `job_name`
    - `requested_partition_id`
    - `executed_partition_id`
    - `run_id`
    - `dagster_run_id`
    - `started_at_utc`
    - `finished_at_utc`
    - `proof_summary`

## Data definitions (field names, types, units, timezone, nullability)
- Chain batch plan fields:
  - `dataset`: S34 Bitcoin height-range dataset
  - `execution_mode`: `backfill | reconcile`
  - `start_height`: inclusive integer height
  - `end_height`: inclusive integer height
  - `partition_id`: zero-padded `height_range` id
  - `run_id`: deterministic controller run id
- Proof summary fields:
  - `partition_id`
  - `proof_state`
  - `proof_reason`
  - `proof_digest_sha256`
  - `source_row_count`
  - `canonical_row_count`
  - `gap_count`
  - `duplicate_count`

## Source/provenance and freshness semantics
- Chain planning derives resume truth from ClickHouse proof state, not env and not cursor max.
- Chain planning rules:
  - ambiguous existing canonical partitions are reconciled first
  - otherwise next batch starts from the contiguous terminal-proof frontier
- Height-range runs carry explicit Dagster tags:
  - `origo.backfill.projection_mode`
  - `origo.backfill.execution_mode`
  - `origo.backfill.runtime_audit_mode`
  - `origo.backfill.height_start`
  - `origo.backfill.height_end`
- Mempool is not treated as chain history. It has its own daily snapshot path.
- Historical mempool replay from first-party Bitcoin Core RPC is unsupported and must fail loudly.

## Failure modes, warnings, and error codes
- Non-Bitcoin or non-height-range dataset sent to chain planner: fail loudly.
- Height-range batch request below contract `earliest_height`: fail loudly.
- Overlapping or out-of-order terminal proof ranges: fail loudly.
- Dagster job resolution failure: fail loudly.
- Dagster run failure: fail loudly.
- Successful Dagster run without terminal proof state: fail loudly.
- Historical mempool date request: fail loudly.

## Determinism/replay notes
- For a fixed proof-state snapshot and fixed inputs, chain batch planning is deterministic.
- Reconcile takes precedence whenever ambiguous partitions exist.
- Contract coverage lives in:
  - `tests/contract/test_s34_bitcoin_backfill_runner_contract.py`
  - `tests/contract/test_s34_backfill_contract.py`
  - `tests/contract/test_bitcoin_canonical_event_ingest_contract.py`

## Environment variables and required config
- Dagster:
  - `DAGSTER_HOME`
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Bitcoin job runtime still inherits Bitcoin node env contract:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`

## Minimal examples
- Run one chain dataset forward in 144-block batches:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_bitcoin_backfill_runner --mode chain --dataset bitcoin_block_headers --plan-end-height 840999 --batch-size-blocks 144 --projection-mode deferred --runtime-audit-mode summary`
- Run the full chain-dataset sequence:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_bitcoin_backfill_runner --mode chain-sequence --plan-end-height 840999 --batch-size-blocks 144 --projection-mode deferred --runtime-audit-mode summary`
- Run the explicit mempool daily path for the current UTC day only:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_bitcoin_backfill_runner --mode mempool --projection-mode deferred --runtime-audit-mode summary`
