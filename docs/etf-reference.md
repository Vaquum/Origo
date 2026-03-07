# ETF Dataset Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice/version reference: S4, S5 (API v0.1.0)

## Purpose and scope
- User-facing reference for ETF data in Origo query surfaces.
- Scope covers source coverage, field taxonomy, freshness/warnings, and serving constraints.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
- Source key: `etf_daily_metrics`
- Request shape (current public contract):
  - `mode`: `native | aligned_1s`
  - `sources`: must include `["etf_daily_metrics"]` for ETF queries
  - one window selector: `time_range | n_rows | n_random`
  - optional `fields`, `filters`, `strict`, `auth_token`
- Response shape:
  - `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`

## Data definitions (fields, types, units, timezone, nullability)
- Core identity fields (non-null):
  - `source_id` (string)
  - `metric_name` (string)
  - `metric_unit` (string)
- Value channel fields (nullable depending on metric type):
  - `metric_value_float` (float)
  - `metric_value_int` (int)
  - `metric_value_string` (string)
  - `metric_value_bool` (bool)
- Time/provenance fields:
  - `observed_at_utc` (UTC timestamp, non-null)
  - `dimensions_json` (JSON string)
  - `provenance_json` (JSON string, non-null)
  - `ingested_at_utc` (UTC timestamp, non-null)
- Units:
  - `BTC`, `USD`, `PCT`, `COUNT`

## Source/provenance and freshness semantics
- Canonical source hierarchy is issuer official pages/files.
- Coverage:
  - `IBIT`, `BTCO`, `BITB`, `ARKB`, `HODL`, `EZBC`, `GBTC`, `FBTC`, `BRRR`, `DEFI`
- Cadence is daily; normalized time semantics are UTC.
- Freshness and quality checks are evaluated from canonical ETF daily records.

## Failure modes, warnings, and error codes
- Warning codes:
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
- `strict=true` turns warnings into `409` failure.
- Common errors:
  - `404` no data for window
  - `409` rights/contract/strict conflict
  - `503` runtime/backend/warning-evaluation failures

## Determinism/replay notes
- ETF ingestion and query proofs are stored in:
  - `spec/slices/slice-4-etf-use-case/`
  - `spec/slices/slice-5-raw-query-aligned-1s/`
- Fixed replay windows are expected to produce deterministic fingerprints.

## Environment variables and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_ETF_QUERY_SERVING_STATE`
- `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- Native ETF query:
  - `{ "mode":"native", "sources":["etf_daily_metrics"], "time_range":["2026-03-01T00:00:00Z","2026-03-07T00:00:00Z"], "fields":["source_id","metric_name","metric_value_float","observed_at_utc"], "strict":false }`
- Aligned ETF query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "n_rows":100, "fields":["aligned_at_utc","source_id","metric_name","metric_value_float"], "strict":false }`
