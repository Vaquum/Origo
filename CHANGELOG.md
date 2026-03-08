# Changelog

## 2026-03-08
- Completed Slice 11 Bybit capability/proof/guardrails: added first-party Bybit daily ingest, migration-backed `bybit_spot_trades` table, native + aligned query integration, export support, and fixed-window deterministic proof artifacts.
- Added Bybit rights/legal artifacts (`Hosted Allowed`) and exchange integrity coverage for schema/type, sequence, monotonic-time, and anomaly checks.
- Added S11 developer/user documentation and full taxonomy updates for Bybit source/query/export contracts.
- Updated versions to `Origo API v0.1.4` and `control-plane v1.2.54`.

## 2026-03-07
- Completed Slice 8 OKX capability/proof/guardrails: added first-party OKX daily ingest, migration-backed `okx_spot_trades` table, native + aligned query integration, export support, and fixed-window deterministic proof artifacts.
- Added OKX rights/legal artifacts (`Hosted Allowed`) and integrity-suite coverage for schema/type, sequence-gap, and side anomaly checks.
- Updated user/developer references for OKX dataset taxonomy, query/export contracts, and aligned-mode semantics.
- Completed Slice 7 Docker local-platform capability/proof/guardrails: added root Docker stack, deterministic bootstrap/proof runners, replay+persistence artifacts, and S7 developer/user docs.
- Fixed Docker proof root causes: Binance integrity now allows `trade_id=0` on first day, Dagster run-status parsing supports current `Run` typename, and local healthchecks use runtime-supported commands.
- Enforced migration-first schema policy by disabling legacy Dagster `create_*` schema assets and adding Binance raw-table SQL migrations `0004`-`0007`.
- Enabled `fred_series_metrics` raw export end-to-end (rights gate, API tag parsing, Dagster export path, and legal scope note).
- Removed ClickHouse env alias fallbacks and standardized to one required `CLICKHOUSE_*` contract.
- Split CI test gates into independent `contract-gate`, `replay-gate`, and `integrity-gate` workflows.
- Updated docs/spec for status-map truth (`202` included), FRED export coverage, and migration/env policy consistency.
- Tightened fail-loud behavior in ingestion cleanup paths by raising disconnect errors.
