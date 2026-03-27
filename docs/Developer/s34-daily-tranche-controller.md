# Slice 34 Developer: Daily Tranche Controller

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-26
- Slice reference: S34 (`S34-04a`, `S34-G2`)

## Purpose and scope
- Defines the repo-native daily tranche controller for S34 exchange backfills.
- Scope covers daily dataset batch planning, reconcile-first behavior, and immediate batch chaining without idle gaps.
- This controller is the execution unit composed by `docs/Developer/s34-exchange-sequence-controller.md` for post-Binance exchange order.

## Inputs and outputs with contract shape
- Controller entrypoint:
  - `control-plane/origo_control_plane/s34_daily_dataset_tranche_controller.py`
- Required inputs:
  - `dataset`
  - `plan_end_date`
  - `batch_size_days`
  - `concurrency`
  - `projection_mode`
  - `runtime_audit_mode`
- Output:
  - completed batch list with `execution_mode`, partition window, run id, and runner result payload

## Data definitions (field names, types, units, timezone, nullability)
- Daily batch plan fields:
  - `dataset`
  - `execution_mode` (`reconcile | backfill`)
  - `partition_ids`
  - `batch_start_partition_id`
  - `batch_end_partition_id`
  - `end_date` (required only for `backfill`)
  - `run_id`
- Planning rules:
  - if ambiguous canonical daily partitions exist, next batch is always `reconcile`
  - otherwise next batch is `backfill` from terminal-proof frontier to `plan_end_date`
- Current daily dataset scope:
  - `binance_spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`

## Source/provenance and freshness semantics
- Batch planning uses authoritative ClickHouse state only.
- `run_exchange_backfill` remains the execution engine; the controller does not execute assets directly.
- The controller is deterministic for a fixed proof-state snapshot and fixed `plan_end_date`.

## Failure modes, warnings, and error codes
- Unknown dataset or non-daily dataset: fail loudly.
- `batch_size_days <= 0` or `max_batches <= 0`: fail loudly.
- Any `RECONCILE_REQUIRED` failure from resume truth is surfaced; the controller does not silently skip or downgrade.
- Runner failures are not swallowed; controller execution stops on the first failing tranche.

## Determinism/replay notes
- Reconcile-first and terminal-frontier planning rules are covered by:
  - `tests/contract/test_s34_daily_dataset_tranche_controller_contract.py`
- Run ids are deterministic from:
  - prefix
  - dataset
  - execution mode
  - batch start partition
  - batch end partition

## Environment variables and required config
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Runner/runtime:
  - `DAGSTER_HOME`
  - `ORIGO_BACKFILL_MANIFEST_LOG_PATH`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_S34_BACKFILL_CONCURRENCY` (used when controller delegates to runner without explicit override)

## Minimal examples
- Run one or more OKX tranches up to a boundary:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_daily_dataset_tranche_controller --dataset okx_spot_trades --plan-end-date 2021-12-31 --batch-size-days 100 --concurrency 20 --projection-mode deferred --runtime-audit-mode summary`
- Stop after one tranche for inspection:
  - add `--max-batches 1`
