## Run metadata
- Date: 2026-03-11
- Scope: S25 capability/proof/guardrails for historical contract normalization and raw/historical optional-window behavior.
- Fixture window: contract-level fixed fixture (`2024-01-01T00:00:00Z` to `2024-01-02T00:00:00Z`) from historical endpoint mock proofs.
- Runtime environment: local development (`uv run`) with repo-local test gates.

## System changes made as proof side effects
- Added `AllRowsWindow` to native query core and compilation path.
- Updated raw/historical selector validators and query-window resolvers to optional-selector semantics.
- Added historical `fields`/`filters` plumbing and mode contract handling in API and query helpers.
- Updated historical Python interface signatures to shared contract and strict warning behavior.
- Updated contract tests for no-selector and multi-selector validation semantics.

## Known warnings and disposition
- Mutable/sample window warnings (`n_latest_rows`, `n_random_rows`) are expected by design and escalate with `strict=true`.
- Historical `aligned_1s` mode is intentionally fail-loud in this slice and tracked for S26 capability delivery.

## Deferred guardrails
- Live end-to-end historical `aligned_1s` execution parity is deferred to Slice 26.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for Slice 25.
- Version bumped and changelog updated for this slice.
- Root `.env.example` reviewed; no new or changed environment variables were required in this slice.
