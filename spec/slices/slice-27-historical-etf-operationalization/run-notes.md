## Run metadata
- Date: 2026-03-11
- Scope: S27 capability/proof/guardrails for historical ETF operationalization (`etf_daily_metrics`) across Python + HTTP surfaces.
- Fixture window: 2024-01-01 UTC day (contract-level deterministic fixture).
- Runtime environment: local development run (`uv run`, focused contract/replay gates).

## System changes made as proof side effects
- Added ETF historical query helper in shared endpoint layer with both `native` and `aligned_1s` execution.
- Extended historical window resolver to support dataset-specific datetime columns (`observed_at_utc` for ETF).
- Added ETF historical request schema and route handler.
- Added ETF historical method on `HistoricalData`.
- Added ETF historical contract and replay/parity tests.
- Updated historical ETF user and developer docs.

## Known warnings and disposition
- `WINDOW_LATEST_ROWS_MUTABLE` and `WINDOW_RANDOM_SAMPLE` remain expected mutable-window warnings and are escalated by `strict=true`.
- ETF quality warnings (`ETF_DAILY_STALE_RECORDS`, `ETF_DAILY_MISSING_RECORDS`, `ETF_DAILY_INCOMPLETE_RECORDS`) remain expected fail-loud warning channels and are escalated by `strict=true`.
- Full `tests/contract` and `tests/integrity` local runs still fail collection when `origo_control_plane` is not importable in local PYTHONPATH; S27-focused suites executed successfully.

## Deferred guardrails
- None.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for Slice 27.
- Version bumped to `0.1.18` and changelog entry added.
- Root `.env.example` reviewed; no new or changed environment variables were required in this slice.
