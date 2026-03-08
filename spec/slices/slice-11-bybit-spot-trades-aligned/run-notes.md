# Slice 11 Run Notes

## Run metadata
- Date (UTC): 2026-03-08
- Scope: Slice 11 (`S11-C1..S11-C4`, `S11-P1..S11-P4`, `S11-G1..S11-G5`)
- Fixture window:
  - Source days: `2024-01-01`, `2024-01-02`
  - UTC windows:
    - `2024-01-01T00:00:00Z` to `2024-01-02T00:00:00Z`
    - `2024-01-02T00:00:00Z` to `2024-01-03T00:00:00Z`
- Runtime environment:
  - Local Docker stack (`origo_default` network) for ClickHouse availability.
  - One-off container runs from local `origo/control-plane:local` and `origo/api:local` images with workspace bind-mount for branch code execution.

## System changes made as proof side effects
- Applied ClickHouse migration `0009__create_bybit_spot_trades.sql`.
- Inserted fixed-window Bybit rows for two source days into `bybit_spot_trades`.
- Generated and stored proof artifacts:
  - `proof-s11-p1-acceptance.json`
  - `proof-s11-p2-aligned-acceptance.json`
  - `proof-s11-p3-determinism.json`
  - `proof-s11-p4-source-checksums.json`
  - `baseline-fixture-2024-01-01_2024-01-02.json`
  - `ingest-results.json`

## Known warnings and disposition
- No unresolved warnings accepted in proof output.
- Root-cause correction applied during proof execution:
  - one-off container ingest command initially lacked `-i`, so heredoc Python did not execute.
  - fixed by requiring `docker run -i` for heredoc runs and rerunning proof flow.

## Deferred guardrails
- None in Slice 11. `S11-G1..S11-G5` are complete.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md` S11 capability/proof/guardrails checked).
- Version bumped: yes (`pyproject.toml`, `control-plane/pyproject.toml`).
- Changelog updated: yes (`CHANGELOG.md`, `control-plane/CHANGELOG.md`).
- `.env.example` updated/reviewed: yes (reviewed; no new S11 env vars introduced, no deployment-specific values hard-coded in new S11 paths).
