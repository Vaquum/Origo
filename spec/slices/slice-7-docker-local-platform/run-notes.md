# Slice 7 Run Notes

## Run metadata
- Date (UTC): 2026-03-07
- Scope: Slice 7 (`S7-C1..S7-C5`, `S7-P1..S7-P4`, `S7-G1..S7-G4`)
- Fixture window:
  - Seed/proof days: `2017-08-17`, `2017-08-18`
  - Query/export window: `2017-08-17T00:00:00Z` to `2017-08-19T00:00:00Z`
- Runtime environment:
  - Local Docker runtime with compose stack (`clickhouse`, `dagster-webserver`, `dagster-daemon`, `api`)
  - Proof execution exported vars from `.env.example` then overlaid values from `.env` for local secrets (`FRED_API_KEY`), because `.env` alone did not include full Docker contract keys.

## System changes made as proof side effects
- Docker images were built for API and control-plane services.
- Docker named volumes were created/updated:
  - `clickhouse-data`
  - `dagster-instance`
  - `origo-storage`
- ClickHouse migrations were applied inside Docker.
- Seed ingest inserted fixed Binance daily rows for two days.
- Export artifacts were written under `/workspace/storage/exports` inside the Docker storage volume.

## Known warnings and disposition
- No unresolved warnings remained after root-cause fixes.
- Previously observed issues (healthcheck command mismatch, trade-id zero integrity failure, Dagster run typename mismatch) were fixed in code and re-proven.
- Missing Docker contract vars in `.env` was treated as configuration debt, not tolerated at runtime. Proof now documents the explicit environment overlay used for this run.

## Deferred guardrails
- None in Slice 7. `S7-G1..S7-G4` are complete.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md` S7 capability/proof/guardrails checked).
- Version bumped: yes (`pyproject.toml` and `control-plane/pyproject.toml`).
- Changelog updated: yes (root `CHANGELOG.md` and `control-plane/CHANGELOG.md`).
- `.env.example` updated/reviewed: yes (Docker env contract + required runtime vars present).
