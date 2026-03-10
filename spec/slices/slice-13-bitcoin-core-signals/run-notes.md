# Slice 13 Run Notes

## Run metadata
- Date (UTC): 2026-03-08
- Scope: Slice 13 (`S13-C1..S13-C7`, `S13-P1..S13-P4`, `S13-G1..S13-G6`)
- Fixture window:
  - `2024-04-20T00:00:00Z` to `2024-04-22T00:00:00Z`
  - partition days: `2024-04-20`, `2024-04-21`
- Runtime environment:
  - Local repository runtime using `.ci-tests-venv`
  - `PYTHONPATH=.:control-plane`
  - deterministic fixture/proof generator: `scripts/s13_generate_proof_artifacts.py`

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

## Known warnings and disposition
- No unresolved quality-gate warnings.
- Explicit note:
  - acceptance/proof artifacts are still fixture-based, not live-node runtime captures.
  - initial local live-node attempt failed due missing Bitcoin Core node contract vars:
    - `ORIGO_BITCOIN_CORE_RPC_URL`
    - `ORIGO_BITCOIN_CORE_RPC_USER`
    - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
    - `ORIGO_BITCOIN_CORE_NETWORK`
    - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
    - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
    - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`
  - after setting the env contract and starting a local mainnet node, live-node proof failed loudly with:
    - `RuntimeError: Bitcoin Core node is in initial block download mode; deterministic S13 ingest is blocked until sync completes`
  - deployment server check showed `ORIGO_BITCOIN_CORE_*` was initially missing in `/opt/origo/deploy/.env`; deploy now fails loudly until these vars are explicitly provided.
  - this is documented in `manifest.md` and should be followed by live-node proof when credentials are available.

## Deferred guardrails
- None deferred in Slice-13 code/doc/test scope.
- Operational live-node replay proof against a running unpruned non-IBD mainnet node is pending chain sync completion.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md`, S13 capability/proof/guardrails marked complete).
- Version bumped: yes (`pyproject.toml`, `control-plane/pyproject.toml`).
- Changelog updated: yes (`CHANGELOG.md`, `control-plane/CHANGELOG.md`).
- `.env.example` updated/reviewed: yes (Bitcoin Core env contract present; no deployment-specific runtime hardcoding added in Slice-13 paths).
