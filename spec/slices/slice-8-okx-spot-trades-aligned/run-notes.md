# Slice 8 Run Notes

## Run metadata
- Date (UTC): 2026-03-07
- Scope: Slice 8 (`S8-C1..S8-C4`, `S8-P1..S8-P4`, `S8-G1..S8-G5`)
- Fixture window:
  - Source days: `2024-01-01`, `2024-01-02`
  - UTC windows:
    - `2023-12-31T16:00:00Z` to `2024-01-01T16:00:00Z`
    - `2024-01-01T16:00:00Z` to `2024-01-02T16:00:00Z`
- Runtime environment:
  - Local Docker stack for ClickHouse + Dagster proof ingest execution.
  - Query proof execution in Docker network context (`host=clickhouse`) for deterministic access to loaded data.

## System changes made as proof side effects
- Applied ClickHouse migration `0008__create_okx_spot_trades.sql`.
- Inserted fixed-window OKX rows for two source days into `okx_spot_trades`.
- Generated and stored proof artifacts:
  - `proof-s8-p1-acceptance.json`
  - `proof-s8-p2-aligned-acceptance.json`
  - `proof-s8-p3-determinism.json`
  - `proof-s8-p4-source-checksums.json`
  - `baseline-fixture-2024-01-01_2024-01-02.json`

## Known warnings and disposition
- No unresolved warnings accepted in proof output.
- Root-cause correction applied during proof:
  - day-bound checks originally assumed UTC day windows
  - corrected to OKX source-day UTC+8 windows
  - post-fix ingest verification and proof fingerprints matched expected semantics.

## Deferred guardrails
- None in Slice 8. `S8-G1..S8-G5` are complete.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md` S8 capability/proof/guardrails checked).
- Version bumped: yes (`pyproject.toml`, `control-plane/pyproject.toml`).
- Changelog updated: yes (`CHANGELOG.md`, `control-plane/CHANGELOG.md`).
- `.env.example` updated/reviewed: yes (reviewed; no new S8 env vars introduced, no deployment-specific values hard-coded in new S8 paths).
