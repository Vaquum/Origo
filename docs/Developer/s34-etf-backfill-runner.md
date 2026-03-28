# Slice 34 Developer: ETF Backfill Runner

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-28
- Slice reference: S34 (`S34-C5a`, `S34-C5d`, `S34-05a`, `S34-05d`, `S34-G2`)

## Purpose and scope
- Defines the repo-native Slice 34 ETF backfill runner.
- Scope covers launching the hardened Dagster ETF backfill job, waiting fail-loud for completion, and validating ETF terminal-proof state from ClickHouse before reporting success.
- Historical Slice-34 ETF backfill is proof-first and artifact-aware.
- The legacy ETF daily ingest job remains the live scrape path, not the historical backfill path.

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
  - `origo_etf_daily_backfill_job`
- Runner tags:
  - `origo.backfill.dataset=etf_daily_metrics`
  - `origo.backfill.execution_mode=backfill`
  - `origo.backfill.projection_mode=deferred`
  - `origo.backfill.runtime_audit_mode=summary`
  - `origo.backfill.control_run_id=<operator-facing id>`
- Proof summary fields:
  - `dataset`
  - `proof_boundary_partition_id`
  - `terminal_partition_count`
  - `ambiguous_partition_count`

## Source/provenance and freshness semantics
- Execution happens through the real Dagster ETF backfill job, not by calling ops directly.
- The backfill job records:
  - source manifests
  - `source_manifested`
  - `canonical_written_unproved`
  - terminal proof or quarantine
- Projectors run only after terminal proof and only when the runtime contract explicitly requests inline projection.
- The Slice-34 runner submits ETF backfill with deferred projection so projector promotion never outruns proof.
- Post-run proof state is read from:
  - `canonical_backfill_partition_proofs`
  - `canonical_event_log`
- Success means:
  - Dagster job completed successfully
  - ETF terminal proof boundary exists
  - no ambiguous ETF partitions remain
- Historical completeness still depends on archived issuer-source artifact coverage. Missing archived artifact coverage is a hard failure, not a fallback to the legacy live scrape job.

## Failure modes, warnings, and error codes
- Missing Dagster workspace/job resolution: fail loudly.
- Dagster job failure: fail loudly.
- No ETF terminal proof boundary after success: fail loudly.
- Ambiguous ETF partitions after success: fail loudly.
- Projector execution before terminal proof: fail loudly by contract.
- Missing historical issuer artifacts for claimed ETF backfill coverage: fail loudly.

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
- Object-store runtime must be present for raw artifact persistence:
  - `ORIGO_OBJECT_STORE_ENDPOINT_URL`
  - `ORIGO_OBJECT_STORE_REGION`
  - `ORIGO_OBJECT_STORE_ACCESS_KEY_ID`
  - `ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY`
  - `ORIGO_OBJECT_STORE_BUCKET`
- Browser-backed ETF adapters also require the deployed control-plane image to include:
  - Python `playwright`
  - Playwright-installed Chromium browser payload

## Minimal examples
- Run ETF backfill with auto run id:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_backfill_runner`
- Run ETF backfill with explicit run id:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_backfill_runner --run-id s34-etf-20260326`
