# Changelog

## 2026-03-07
- Completed Slice 7 Docker local-platform capability/proof/guardrails: added root Docker stack, deterministic bootstrap/proof runners, replay+persistence artifacts, and S7 developer/user docs.
- Fixed Docker proof root causes: Binance integrity now allows `trade_id=0` on first day, Dagster run-status parsing supports current `Run` typename, and local healthchecks use runtime-supported commands.
- Enforced migration-first schema policy by disabling legacy Dagster `create_*` schema assets and adding Binance raw-table SQL migrations `0004`-`0007`.
- Enabled `fred_series_metrics` raw export end-to-end (rights gate, API tag parsing, Dagster export path, and legal scope note).
- Removed ClickHouse env alias fallbacks and standardized to one required `CLICKHOUSE_*` contract.
- Split CI test gates into independent `contract-gate`, `replay-gate`, and `integrity-gate` workflows.
- Updated docs/spec for status-map truth (`202` included), FRED export coverage, and migration/env policy consistency.
- Tightened fail-loud behavior in ingestion cleanup paths by raising disconnect errors.
