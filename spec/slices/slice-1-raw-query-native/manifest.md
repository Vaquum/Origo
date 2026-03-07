## What was done
- Implemented a new internal native query kernel (`ClickHouse SQL -> Arrow -> Polars`) in `origo/query/native_core.py`.
- Implemented a unified Binance planner for `spot_trades`, `spot_agg_trades`, and `futures_trades` with strict field allowlists.
- Added UTC millisecond datetime canonicalization and optional ISO datetime output in the query execution path.
- Added a wide-row envelope builder with schema metadata for API-ready responses.
- Rewired `origo.data.HistoricalData` native trade endpoints to use the new planner path.
- Removed eager package import side effects from `origo` package init modules to keep API/runtime imports decoupled from optional utility dependencies.
- Added FastAPI adapter scaffold in `api/origo_api` with `/v1/raw/query` and `/health`.
- Added Slice 1 guardrails in API adapter: static API key check, fixed status/error mapping (`200/404/409/503`), warnings envelope, `strict=true` fail behavior, and concurrency/queue controls.
- Replaced swallowed ClickHouse client-close exceptions with explicit warning logs.
- Added Slice 1 proof artifacts in `spec/slices/slice-1-raw-query-native/` (acceptance proof + baseline fixture).

## Current state
- `spec/2-itemized-work-plan.md` has all Slice 1 capability/proof/guardrail checkboxes marked complete.
- Native query capability exists as an internal reusable module independent of HTTP, and API route wiring now delegates into this core.
- Planner coverage includes all three Binance raw datasets in one contract.
- Query output supports deterministic UTC datetime handling and optional ISO datetime serialization.
- Wide-row envelope shape now exists (`mode`, `source`, `row_count`, `schema`, `warnings`, `rows`) for direct API use.
- Local validation environment has sample data loaded for `binance_agg_trades` and `binance_futures_trades` tables for replay/acceptance checks.

## Watch out
- API runtime dependencies (`fastapi`, `uvicorn`) are validated in ephemeral `uv run --with ...` execution but are not yet pinned in a dedicated API project lockfile.
- Current API queue/concurrency guardrails are in-process only; they are not cross-process/distributed.
- Binance agg-trades source files include an extra boolean column (`column_8`) that is intentionally dropped to match current table schema.
- Query warning semantics are intentionally minimal in Slice 1 (window-mode based) and should be expanded with richer freshness/integrity warnings in later slices.
