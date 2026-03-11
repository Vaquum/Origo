# ETF Dataset Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-11
- Slice/version reference: S4, S5, S16, S27 (API v0.1.18)

## Purpose and scope
- User-facing reference for ETF data in Origo query/export surfaces.
- Scope covers source coverage, field taxonomy, event-driven serving semantics, and quality warning behavior.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
- Historical endpoint: `POST /v1/historical/etf/daily_metrics`
- Source key: `etf_daily_metrics`
- Raw query request shape:
  - `mode`: `native | aligned_1s`
  - `sources`: must include `["etf_daily_metrics"]`
  - one window selector: `time_range | n_rows | n_random`
  - optional `fields`, `filters`, `strict`
- Historical request shape:
  - `mode`: `native | aligned_1s`
  - one historical selector mode: `start_date/end_date | n_latest_rows | n_random_rows` (or no selector for full history)
  - optional `fields`, `filters`, `strict`
- Response shape:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `freshness`, `warnings`, `rows`
  - `rights_state`, `rights_provisional`

## Data definitions (fields, types, units, timezone, nullability)
- Core identity fields:
  - `metric_id` (string, non-null)
  - `source_id` (string, non-null)
  - `metric_name` (string, non-null)
  - `metric_unit` (string, nullable)
- Value channels:
  - `metric_value_string` (string, nullable)
  - `metric_value_int` (int, nullable)
  - `metric_value_float` (float, nullable)
  - `metric_value_bool` (int 0/1, nullable)
- Time/provenance fields:
  - `observed_at_utc` (UTC timestamp, non-null, native mode)
  - `aligned_at_utc` (UTC second, non-null, aligned mode)
  - `valid_from_utc` / `valid_to_utc_exclusive` (UTC interval bounds, aligned forward-fill)
  - `dimensions_json` (JSON string, non-null)
  - `provenance_json` (JSON string, nullable)
  - `ingested_at_utc` / `latest_ingested_at_utc` (UTC timestamp)
  - `records_in_bucket` (uint, aligned mode)

## Source/provenance and freshness semantics
- Canonical source hierarchy is issuer official pages/files.
- Coverage:
  - `IBIT`, `FBTC`, `GBTC`, `ARKB`, `BITB`, `HODL`, `BTCO`, `EZBC`, `BRRR`, `DEFI`
- ETF writes are eventized (`source_id='etf'`, `stream_id='etf_daily_metrics`) and served from projections:
  - native: `canonical_etf_daily_metrics_native_v1`
  - aligned: `canonical_aligned_1s_aggregates`
- Freshness/quality warnings are projection-driven and evaluated on requested windows.

## Failure modes, warnings, and error codes
- Warning codes:
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
- Historical window warning codes:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
- `strict=true` escalates warnings to `409` (`STRICT_MODE_WARNING_FAILURE`).
- Common errors:
  - `404` no data for window
  - `409` rights/contract/strict conflicts
  - `503` backend/runtime failures

## Determinism/replay notes
- ETF event-sourcing proof artifacts are maintained in:
  - `spec/slices/slice-16-etf-event-sourcing-port/`
- Baseline fixture includes deterministic native/aligned fingerprints:
  - `baseline-fixture-2026-03-08_2026-03-09.json`

## Environment variables and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- `ORIGO_INTERNAL_API_KEY`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- Native ETF query:
  - `{ "mode":"native", "sources":["etf_daily_metrics"], "time_range":["2026-03-08T00:00:00Z","2026-03-10T00:00:00Z"], "fields":["metric_id","source_id","metric_name","metric_value_float","observed_at_utc"], "strict":false }`
- Aligned ETF query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "time_range":["2026-03-08T00:00:00Z","2026-03-10T00:00:00Z"], "fields":["aligned_at_utc","source_id","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "strict":false }`
- Native ETF historical query:
  - `{ "mode":"native", "start_date":"2024-01-01", "end_date":"2024-01-02", "fields":["source_id","metric_name","metric_value_float","observed_at_utc"], "strict":false }`
- Aligned ETF historical query:
  - `{ "mode":"aligned_1s", "start_date":"2024-01-01", "end_date":"2024-01-02", "fields":["aligned_at_utc","source_id","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "strict":false }`
