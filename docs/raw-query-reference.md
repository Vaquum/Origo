# Raw Query API Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice/version reference: S1, S4, S5, S6 (API v0.1.0)

## Purpose and scope
- This is the user-facing reference for `POST /v1/raw/query`.
- Scope includes `native` and `aligned_1s` query modes over Binance, ETF, and FRED datasets.

## Inputs and outputs with contract shape
- Endpoint: `POST /v1/raw/query`
- Required header: `X-API-Key`
- Request contract:
  - `mode`: `native | aligned_1s` (default `native`)
  - `sources`: list of source keys (current capability requires exactly one item)
  - `fields`: optional list of projected fields
  - `time_range`: optional `[start_iso, end_iso]`
  - `n_rows`: optional integer > 0
  - `n_random`: optional integer > 0
  - `filters`: optional list of filter clauses:
    - `{ "field": "<column>", "op": "eq|ne|gt|gte|lt|lte|in|not_in", "value": <any> }`
  - `strict`: boolean (default `false`)
  - `auth_token`: optional source credential passthrough
- Window selection rule: exactly one of `time_range`, `n_rows`, `n_random` must be provided.
- Response contract:
  - `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`

## Data definitions (fields, types, units, timezone, nullability)
- Timestamp fields are UTC and returned as ISO-8601 strings.
- `schema` response entries are `{name, dtype}`.
- Dataset/source keys:
  - `spot_trades`
  - `spot_agg_trades`
  - `futures_trades`
  - `etf_daily_metrics`
  - `fred_series_metrics`
- Field-level definitions are maintained in:
  - `docs/data-taxonomy.md`
  - `docs/aligned-reference.md`
  - `docs/etf-reference.md`
  - `docs/fred-reference.md`

## Source/provenance and freshness semantics
- Query reads from ClickHouse canonical tables loaded from original sources.
- `aligned_1s` responses can include freshness payload:
  - `freshness.as_of_utc`
  - `freshness.lag_seconds`
- ETF and FRED warning paths include source-specific freshness/quality checks.

## Failure modes, warnings, and error codes
- Status map:
  - `200`: success
  - `404`: no rows for requested window
  - `409`: contract/auth/rights conflict or strict-warning failure
  - `503`: backend/runtime/queue unavailable
- Warning codes:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- `strict=true` fails with `409` when warnings exist.

## Determinism/replay notes
- Deterministic ordering is enforced for replayable windows.
- Proof artifacts live under:
  - `spec/slices/slice-1-raw-query-native/`
  - `spec/slices/slice-5-raw-query-aligned-1s/`
  - `spec/slices/slice-6-fred-integration/`

## Environment variables and required config
- `ORIGO_INTERNAL_API_KEY`
- `ORIGO_QUERY_MAX_CONCURRENCY`
- `ORIGO_QUERY_MAX_QUEUE`
- `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
- `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
- `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
- `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Native time-range query:
  - `{ "mode":"native", "sources":["spot_trades"], "fields":["trade_id","price","timestamp"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "filters":[{"field":"price","op":"gt","value":1000}], "strict":false }`
- Aligned latest rows query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "fields":["aligned_at_utc","metric_name","metric_value_float"], "n_rows":100, "strict":false }`
