# Docker Local Platform Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice/version reference: S7 (API `v0.1.2`, control-plane `v1.2.52`)

## Purpose and scope
- User-facing guide for running Origo locally in Docker and validating raw query/export behavior end-to-end.

## Inputs and outputs with contract shape
- Inputs:
  - local `.env` matching `.env.example`
  - Docker runtime
- Outputs:
  - API endpoint serving `/v1/raw/query` and `/v1/raw/export`
  - self-hosted Bitcoin Core mainnet node service (`bitcoin-core`)
  - deterministic proof artifacts under `spec/slices/slice-7-docker-local-platform/`

## Data definitions (fields, types, units, timezone, nullability)
- Proof window dates: UTC day boundaries (`2017-08-17`, `2017-08-18`).
- Query and export responses follow existing contracts in:
  - `docs/raw-query-reference.md`
  - `docs/raw-export-reference.md`
- Seed dataset: `spot_trades` (Binance daily files).

## Source/provenance and freshness semantics
- Local proof reads exchange data from canonical Binance daily archives.
- Proof artifacts include source checksums to identify exact input files.
- Freshness semantics are unchanged from raw API source-specific contracts.

## Failure modes, warnings, and error codes
- Missing env var: startup/proof fails immediately.
- Service health timeout: proof fails with failing step in `smoke-result.json`.
- Export lifecycle failure: proof fails and captures status payload details.

## Determinism/replay notes
- Replay checks compare two query runs plus one post-restart run.
- Slice 7 proof is valid only when deterministic flags are true in proof artifacts.

## Environment variables and required config
- Required local contract is `.env.example`.
- Minimal must-set groups:
  - `CLICKHOUSE_*`
  - `ORIGO_DOCKER_*`
  - `ORIGO_BITCOIN_CORE_*`
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_DAGSTER_*`
  - `ORIGO_EXPORT_*`

## Minimal examples
- Bring services up:
  - `scripts/s7_docker_stack.sh up`
- Run deterministic proof:
  - `scripts/s7_docker_stack.sh proof`
- Query health endpoint:
  - `curl -sS http://localhost:18000/health`
