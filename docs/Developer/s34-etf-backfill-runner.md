# Slice 34 Developer: ETF Backfill Runner

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-26
- Slice reference: S34 (`S34-C5a`, `S34-05a`, `S34-G2`)

## Purpose and scope
- Defines the repo-native Slice 34 ETF backfill runner.
- Scope covers launching the real Dagster ETF ingest job, waiting fail-loud for completion, and validating ETF terminal-proof state from ClickHouse before reporting success.

## Inputs and outputs with contract shape
- Runner entrypoint:
  - `control-plane/origo_control_plane/s34_etf_backfill_runner.py`
- Required inputs:
  - none beyond runtime env and Dagster workspace
- Optional inputs:
  - `run_id`
- Output:
  - `dataset`
  - `job_name`
  - `run_id`
  - `dagster_run_id`
  - `started_at_utc`
  - `finished_at_utc`
  - `proof_summary`

## Data definitions (field names, types, units, timezone, nullability)
- Fixed dataset:
  - `etf_daily_metrics`
- Fixed Dagster job:
  - `origo_etf_daily_ingest_job`
- Proof summary fields:
  - `dataset`
  - `proof_boundary_partition_id`
  - `terminal_partition_count`
  - `ambiguous_partition_count`

## Source/provenance and freshness semantics
- Execution happens through the real Dagster ETF job, not by calling ops directly.
- Post-run proof state is read from:
  - `canonical_backfill_partition_proofs`
  - `canonical_event_log`
- Success means:
  - Dagster job completed successfully
  - ETF terminal proof boundary exists
  - no ambiguous ETF partitions remain

## Failure modes, warnings, and error codes
- Missing Dagster workspace/job resolution: fail loudly.
- Dagster job failure: fail loudly.
- No ETF terminal proof boundary after success: fail loudly.
- Ambiguous ETF partitions after success: fail loudly.

## Determinism/replay notes
- Runner tags are deterministic from `run_id`.
- Contract coverage lives in:
  - `tests/contract/test_s34_etf_backfill_runner_contract.py`

## Environment variables and required config
- Dagster:
  - `DAGSTER_HOME`
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- ETF job runtime inherits existing ETF ingest env contract.

## Minimal examples
- Run ETF backfill with auto run id:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_backfill_runner`
- Run ETF backfill with explicit run id:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_backfill_runner --run-id s34-etf-20260326`
