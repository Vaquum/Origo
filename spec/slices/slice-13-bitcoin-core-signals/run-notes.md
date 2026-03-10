# Slice 13 Run Notes

## Run metadata
- Dates (UTC):
  - 2026-03-08 (fixture proof tranche)
  - 2026-03-10 (live-node proof tranche)
- Scope: Slice 13 (`S13-C1..S13-C7`, `S13-P1..S13-P4`, `S13-G1..S13-G6`)
- Fixture window:
  - `2024-04-20T00:00:00Z` to `2024-04-22T00:00:00Z`
  - partition days: `2024-04-20`, `2024-04-21`
- Live-node window:
  - `2009-01-03T00:00:00Z` to `2009-01-12T00:00:00Z`
  - partition days: `2009-01-03` through `2009-01-11`
- Runtime environment:
  - Local repository runtime using `.ci-tests-venv` and `.venv-local`
  - `PYTHONPATH=.:control-plane`
  - fixture generator: `scripts/s13_generate_proof_artifacts.py`
  - live-node generator: `scripts/s13_generate_live_node_proof_artifacts.py`
  - SSH tunnel to remote node RPC for live-node run (`localhost:18332 -> server:127.0.0.1:8332`)

## System changes made as proof side effects
- Added migration files:
  - `control-plane/migrations/sql/0010__create_bitcoin_block_headers.sql`
  - `control-plane/migrations/sql/0011__create_bitcoin_block_transactions.sql`
  - `control-plane/migrations/sql/0012__create_bitcoin_mempool_state.sql`
  - `control-plane/migrations/sql/0013__create_bitcoin_block_fee_totals.sql`
  - `control-plane/migrations/sql/0014__create_bitcoin_block_subsidy_schedule.sql`
  - `control-plane/migrations/sql/0015__create_bitcoin_network_hashrate_estimate.sql`
  - `control-plane/migrations/sql/0016__create_bitcoin_circulating_supply.sql`
- Added/updated query and export integration for Slice-13 datasets (`native` + `aligned_1s` as scoped).
- Added Bitcoin stream and derived integrity suites and wired them into ingest assets.
- Added live-node proof generator:
  - `scripts/s13_generate_live_node_proof_artifacts.py`
- Added Docker-managed Bitcoin Core node runtime:
  - `docker-compose.yml` service `bitcoin-core`
  - `deploy/docker-compose.server.yml` service `bitcoin-core`
  - deploy workflow requires `ORIGO_BITCOIN_CORE_*` env contract values to already exist in `/opt/origo/deploy/.env` (fail-loud if missing)
- Generated Slice-13 artifacts:
  - `proof-s13-p1-acceptance.json`
  - `proof-s13-p2-derived-native-aligned-acceptance.json`
  - `proof-s13-p3-determinism.json`
  - `proof-s13-p4-parity.json`
  - `baseline-fixture-2024-04-20_2024-04-21.json`
- Generated Slice-13 live-node artifacts:
  - `proof-s13-live-node-p1-acceptance.json`
  - `proof-s13-live-node-p2-derived-native-aligned-acceptance.json`
  - `proof-s13-live-node-p3-determinism.json`
  - `proof-s13-live-node-p4-parity.json`
  - `baseline-fixture-live-node-2009-01-03_2009-01-11.json`
- Remote runtime validation completed after deploy:
  - mainnet node contract satisfied (`chain=main`, `pruned=false`, `initialblockdownload=false`)
  - S13 Dagster ingest jobs executed end-to-end on server (`headers`, `transactions`, `mempool`, `fees`, `subsidy`, `hashrate`, `supply`)
  - `/v1/raw/query` and `/v1/raw/export` validated for S13 datasets in `native` and `aligned_1s` (derived scope for aligned)

## Known warnings and disposition
- No unresolved quality-gate warnings.
- Explicit note:
  - initial live-node attempts failed loudly as designed (`missing env` and then `initialblockdownload=true`).
  - after node sync completion and env-contract alignment, live-node proofs were generated successfully.
  - deploy remains fail-loud on missing `ORIGO_BITCOIN_CORE_*` in `/opt/origo/deploy/.env` (no fallback generation).

## Deferred guardrails
- None deferred in Slice-13 code/doc/test scope.
- Operational live-node replay proof is completed for the configured header window.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md`, S13 capability/proof/guardrails marked complete).
- Version bumped: yes (`pyproject.toml`, `control-plane/pyproject.toml`).
- Changelog updated: yes (`CHANGELOG.md`, `control-plane/CHANGELOG.md`).
- `.env.example` updated/reviewed: yes (Bitcoin Core env contract present; no deployment-specific runtime hardcoding added in Slice-13 paths).
