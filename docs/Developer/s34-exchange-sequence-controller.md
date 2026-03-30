# Slice 34 Developer: Exchange Sequence Controller

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-26
- Slice reference: S34 (`S34-C4a`, `S34-04b`, `S34-G2`)

## Status
- Historical slice record only. Not a live runtime contract.
- Any CLI example or helper entrypoint in this document is provenance only.
- Canonical backfill/reconcile writes must follow [docs/Developer/dagster-authority-contract.md](./dagster-authority-contract.md).
- The historical helper entrypoint named here now fails closed for write execution.

## Purpose and scope
- Records the historical repo-native post-Binance exchange controller for S34.
- Scope covers deterministic execution order for `okx_spot_trades -> bybit_spot_trades` using the daily tranche controller as the execution unit.

## Inputs and outputs with contract shape
- Controller entrypoint:
  - `control-plane/origo_control_plane/s34_exchange_sequence_controller.py`
- Required inputs:
  - `plan_end_date`
  - `batch_size_days`
  - `concurrency`
  - `projection_mode`
  - `runtime_audit_mode`
- Optional inputs:
  - repeated `dataset` values, but only from exchange-parallel scope and only in S34 contract order
  - `run_id_prefix`
- Output:
  - completed dataset list with per-dataset tranche-controller result payloads

## Data definitions (field names, types, units, timezone, nullability)
- Default dataset order is derived from S34 contract rank:
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Execution rule:
  - the next dataset starts only after the current dataset controller returns `no_remaining_work`
- Sequence output fields:
  - `datasets`
  - `plan_end_date`
  - `batch_size_days`
  - `concurrency`
  - `completed_dataset_count`
  - `completed_datasets`
  - `controller_stopped_reason`

## Source/provenance and freshness semantics
- Dataset order comes from the S34 contract, not from ad-hoc shell ordering.
- The sequence controller delegates actual batch planning and execution to:
  - `control-plane/origo_control_plane/s34_daily_dataset_tranche_controller.py`
- No asset code is invoked directly.

## Failure modes, warnings, and error codes
- Non-exchange or non-daily dataset: fail loudly.
- Out-of-contract dataset order: fail loudly.
- Duplicate dataset in explicit input: fail loudly.
- If one dataset stops for any reason other than `no_remaining_work`, sequence execution raises immediately and does not advance.

## Determinism/replay notes
- Deterministic dataset order and stop-loud behavior are covered by:
  - `tests/contract/test_s34_exchange_sequence_controller_contract.py`
- For a fixed proof-state snapshot and fixed controller inputs, dataset order is deterministic.

## Environment variables and required config
- Inherits the same runtime requirements as the daily tranche controller:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
  - `DAGSTER_HOME`
  - `ORIGO_BACKFILL_MANIFEST_LOG_PATH`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_S34_BACKFILL_CONCURRENCY`

## Minimal examples
- Run the full post-Binance exchange sequence:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_exchange_sequence_controller --plan-end-date 2026-03-26 --batch-size-days 100 --concurrency 20 --projection-mode deferred --runtime-audit-mode summary`
- Run only OKX while keeping contract order:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_exchange_sequence_controller --dataset okx_spot_trades --plan-end-date 2026-03-26 --batch-size-days 100 --concurrency 20 --projection-mode deferred --runtime-audit-mode summary`
