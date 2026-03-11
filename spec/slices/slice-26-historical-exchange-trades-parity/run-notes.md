## Run metadata
- Date: 2026-03-11
- Scope: S26 capability/proof/guardrails for historical exchange spot-trades `native`/`aligned_1s` parity across Binance/OKX/Bybit.
- Fixture window: 2024-01-01 UTC day (contract-level deterministic fixture).
- Runtime environment: local development run (`uv run`, contract/replay/integrity gates).

## System changes made as proof side effects
- Added `aligned_1s` execution path for historical exchange trade queries in shared historical endpoint helpers.
- Added contract coverage for exchange trade aligned-mode behavior across all three historical trade routes.
- Added replay determinism coverage for historical exchange trade queries in both modes.
- Updated historical developer/user references and taxonomy for current mode availability.

## Known warnings and disposition
- `WINDOW_LATEST_ROWS_MUTABLE` and `WINDOW_RANDOM_SAMPLE` remain expected mutable-window warnings and are escalated by `strict=true`.
- Source archive checksum metadata in baseline fixture is retrospective and recorded as partial placeholders for this closeout.

## Deferred guardrails
- Historical `aligned_1s` support for exchange spot-kline convenience routes remains deferred and continues to fail loudly by contract in this tranche.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for Slice 26.
- Version bumped to `0.1.17` and changelog entry added.
- Root `.env.example` reviewed; no new or changed environment variables were required in this slice.
