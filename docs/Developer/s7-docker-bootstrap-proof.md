# Slice 7 Developer: Docker Bootstrap and Proof Workflow

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S7 (`S7-C3`, `S7-C5`, `S7-P1`, `S7-P2`, `S7-P3`, `S7-P4`, `S7-G2`)
- Version reference: API `v0.1.2`, control-plane `v1.2.52`

## Purpose and scope
- Defines deterministic local bootstrap and proof execution for full Docker stack validation.
- Scope includes migrations, fixed-window seed ingest, query/export proof, replay and restart persistence checks.

## Inputs and outputs with contract shape
- Inputs:
  - `scripts/s7_docker_bootstrap.sh`
  - `scripts/s7_docker_local_proof.sh`
  - `.env` runtime contract
  - fixed proof days: `2017-08-17`, `2017-08-18`
- Outputs (artifact files under `spec/slices/slice-7-docker-local-platform/`):
  - `bootstrap.log`
  - `migrations.log`
  - `seed-ingest-results.json`
  - `proof-s7-local-docker.json`
  - `baseline-fixture-2017-08-17_2017-08-18.json`
  - `smoke-result.json`
  - `docker-compose-ps.txt`
  - `docker-compose-tail.log`

## Data definitions (field names, types, units, timezone, nullability)
- `seed-ingest-results.json`:
  - `ingest_results[]` with checksums, rows inserted, and per-day verification summary.
- `proof-s7-local-docker.json`:
  - export status/artifact metadata, aligned/native replay determinism, post-restart match flags.
- `baseline-fixture-*.json`:
  - per-day fingerprint rows (`row_count`, offsets in milliseconds, sums, row hash).

## Source/provenance and freshness semantics
- Seed data is Binance daily files fetched by existing control-plane ingest capability.
- Checksums in proof artifacts identify exact source files used for replay.
- Proof window is fixed to enforce replay comparability.

## Failure modes, warnings, and error codes
- Any missing env var causes immediate script failure.
- Any service health failure causes immediate proof failure.
- Any export lifecycle failure raises and marks smoke result failed with failing step name.

## Determinism/replay notes
- Two query runs per day are fingerprinted and compared.
- Post-restart run is compared against run-1 fingerprint.
- `deterministic_match` and `deterministic_match_full` must both be `true` for S7 proof completion.

## Environment variables and required config
- Additional proof-run vars:
  - `ORIGO_DOCKER_DAGSTER_GRAPHQL_URL`
  - `ORIGO_DOCKER_API_BASE_URL`
  - `ORIGO_INTERNAL_API_KEY`

## Minimal examples
- Clean bootstrap (down/build/up + migrations + seed ingest):
  - `scripts/s7_docker_stack.sh bootstrap`
- Full local proof runner:
  - `scripts/s7_docker_stack.sh proof`
