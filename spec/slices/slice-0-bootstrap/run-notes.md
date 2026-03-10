# Slice 0 Run Notes

## Run metadata
- Date (UTC): original proof window on 2026-03-04; run-notes format normalized retrospectively on 2026-03-10.
- Scope: Slice 0 (`S0-C1..S0-C6`, `S0-P1..S0-P3`, `S0-G1..S0-G10`) bootstrap migration and early guardrail setup.
- Fixture window:
  - ingest/replay days: `2017-08-17`, `2017-08-18`, `2017-08-19`
  - symbol: `BTCUSDT`
- Runtime environment:
  - local Docker Compose stack with ClickHouse + Dagster runtime services
  - control-plane migration and ingestion runners executed against local ClickHouse

## System changes made as proof side effects
- Imported and renamed control-plane code into monorepo runtime paths.
- Executed fixed-window Binance ingest and replay determinism runs.
- Added `uv.lock` workflow for deterministic dependency resolution in control-plane.
- Added SQL migration scaffold and ledger-enforced migration runner.
- Enforced root env contract (`.env.example`) and fail-loud runtime env checks.
- Generated/updated baseline fixture artifact:
  - `spec/slices/slice-0-bootstrap/baseline-fixture-2017-08-17_2017-08-19.json`

## Known warnings and disposition
- ClickHouse emitted `ALTER DATABASE ... MODIFY SETTING` warnings for unsupported Atomic-engine behavior in the proof environment; warnings were surfaced and not silently ignored.
- Historical note: this run-notes file was reformatted after-the-fact to the standardized section contract; no proof payload values were changed by the formatting pass.

## Deferred guardrails
- `S0-G2` TLS enforcement deferred (crossed over in planning; TLS to be handled at Cloudflare layer later).
- `S0-G3` immutable audit-log sink remains open.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`spec/2-itemized-work-plan.md` updated for Slice 0 status decisions).
- Version bumped: previously completed during Slice 0 implementation; no additional version bump in this retrospective formatting pass.
- Changelog updated: previously completed during Slice 0 implementation; no additional changelog entry in this retrospective formatting pass.
- `.env.example` updated/reviewed: yes (env contract exists; deployment-specific hardcoding removed from active runtime paths).
