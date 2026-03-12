## Run metadata
- Date: 2026-03-12
- Scope: S30 capability/proof/guardrails for historical Bitcoin operationalization across Python + HTTP surfaces.
- Fixture window: 2024-01-01 UTC day (contract-level deterministic fixtures).
- Runtime environment: local development run with `uv run` and `PYTHONPATH=.:control-plane`.

## System changes made as proof side effects
- Added Bitcoin historical request schema (`HistoricalBitcoinDatasetRequest`) and parser/handler wiring.
- Added seven Bitcoin historical HTTP routes in API main.
- Added shared historical Bitcoin query helper in endpoint layer.
- Added seven explicit Bitcoin `HistoricalData` methods.
- Expanded historical contract and replay test suites to include all seven Bitcoin datasets across both modes.

## Known warnings and disposition
- Mutable/sample window warnings remain expected (`WINDOW_LATEST_ROWS_MUTABLE`, `WINDOW_RANDOM_SAMPLE`) and escalate to `409` when `strict=true`.
- Local environment warning about stale uninstall metadata (`origo-0.1.17.dist-info`) was observed during `uv run` and treated as non-blocking.
- No runtime/backend regressions were observed in S30-focused contract/replay runs.

## Deferred guardrails
- None.

## Closeout confirmation
- S30 checkboxes were marked complete in `spec/2-itemized-work-plan.md`.
- API version was bumped to `0.1.21` and changelog entry appended.
- `.env.example` was reviewed against S30 changes; no new environment variables were introduced and no contract changes were required.
- Developer docs were updated in `docs/Developer/`.
- User docs and taxonomy references were updated in `docs/`.
