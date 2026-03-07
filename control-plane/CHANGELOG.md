# Changelog

## v1.2.6 on 4th of March, 2026
- Added Slice 1 native query core in `origo/query/native_core.py` (`ClickHouse SQL -> Arrow -> Polars`) with deterministic window semantics.
- Added unified Binance native planner in `origo/query/binance_native.py` covering `spot_trades`, `spot_agg_trades`, and `futures_trades` with strict field allowlists.
- Added UTC millisecond canonicalization and optional ISO datetime output for query results.
- Added wide-row envelope builder with schema metadata in `origo/query/response.py`.
- Rewired `origo.data.HistoricalData` native trade methods through the new query planner path.
- Removed eager package import side effects in `origo/__init__.py` and `origo/data/__init__.py` so API/runtime imports do not require optional utility dependencies.
- Replaced swallowed ClickHouse client-close exceptions with explicit warning logs in query execution paths.
- Replaced swallowed ClickHouse disconnect exceptions in Binance ingestion assets (`daily`, `monthly`, `monthly_agg`, `monthly_futures`) and preserved traceback semantics (`raise`).
- Added FastAPI adapter scaffold in `api/origo_api` with `/v1/raw/query` + request/response contracts and `/health`.
- Added Slice 1 guardrails in API adapter:
  - static internal API key middleware,
  - status/error mapping (`200/404/409/503`),
  - structured warnings + `strict=true` fail behavior,
  - in-process concurrency and queue controls.
- Added Slice 1 proof artifacts under `spec/slices/slice-1-raw-query-native/` and marked all Slice 1 work-plan checkboxes complete.

## v1.2.5 on 4th of March, 2026
- Added monorepo env contract at root `.env.example` for required ClickHouse runtime variables.
- Enforced fail-loud env loading in control-plane via `origo_control_plane.config.env` (missing or empty required vars now raise immediately).
- Removed deployment-specific ClickHouse defaults from control-plane asset/migration/runtime paths.
- Updated ClickHouse client wiring to require explicit native (`CLICKHOUSE_PORT`) and HTTP (`CLICKHOUSE_HTTP_PORT`) ports.
- Updated compose wiring to pass required ClickHouse env vars into Dagster services without hard-coded defaults.
- Marked Slice 0 guardrail `S0-G6` complete in the itemized work plan.

## v1.2.4 on 4th of March, 2026
- Added SQL migration scaffold under `control-plane/migrations/` with ordered version files.
- Added migration runner (`status` and `migrate`) in `origo_control_plane.migrations` with strict filename, contiguous version, and checksum enforcement.
- Added migration ledger management (`schema_migrations`) and `{{DATABASE}}` placeholder rendering support.
- Added first scaffold migration `0001__create_migration_probe.sql` and validated apply/status against ClickHouse.
- Marked Slice 0 guardrail `S0-G5` complete in the itemized work plan.

## v1.2.3 on 4th of March, 2026
- Added `control-plane/uv.lock` and validated deterministic environment creation with `uv sync --frozen`.
- Switched Docker dependency install path to lockfile-based `uv sync --frozen --no-dev`.
- Added runtime dependency `dagster-webserver` and switched compose command from `dagit` to `dagster-webserver`.
- Added `control-plane/.dockerignore` to prevent large/dirty host context from entering image builds.
- Added `uv` workflow instructions to `control-plane/README.md`.
- Marked Slice 0 guardrail `S0-G4` complete in the itemized work plan.

## v1.2.2 on 4th of March, 2026
- Retroactive Slice 0 closeout record (`slice-0-bootstrap`) added in monorepo `spec/`.
- `tdw-control-plane` imported to `control-plane` and major TDW -> Origo renaming applied.
- Control-plane metadata/config updated (`pyproject`, Docker image names, Dagster code location, README).
- Enforced fail-fast ClickHouse secret wiring in compose (`CLICKHOUSE_PASSWORD` is now required).
- Fixed `create_binance_trades_table_origo` table settings for ClickHouse compatibility.
- Fixed UTC conversion bug in `daily_trades_to_origo` timestamp handling.
- Added required runtime dependencies: `clickhouse-connect`, `pyarrow`, `polars`.
- Imported client module from Limen into `origo/data` (excluding `standard_bars.py`) and remapped imports.
- Verified deterministic fixture replay for Binance daily files (`2017-08-17` to `2017-08-19`) with matching checksums/fingerprints across two runs.
