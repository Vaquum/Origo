# Aligned Mode Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice/version reference: S5, S6, S8, S11 (API v0.1.4)

## Purpose and scope
- User-facing reference for `mode=aligned_1s` query and export behavior.
- Scope includes supported sources, field semantics, freshness/warnings, and deterministic behavior.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
  - `mode` must be `aligned_1s`
  - `sources` currently must contain exactly one key from:
    - `spot_trades`
    - `spot_agg_trades`
    - `futures_trades`
    - `okx_spot_trades`
    - `bybit_spot_trades`
    - `etf_daily_metrics`
    - `fred_series_metrics`
  - one window selector: `time_range | n_rows | n_random`
  - optional `fields`, `filters`, `strict`
- Export endpoint: `POST /v1/raw/export`
  - `mode=aligned_1s`
  - `format=parquet|csv`
  - current export contract remains dataset-based for dispatch.
- Response envelope:
  - `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`

## Data definitions (fields, types, units, timezone, nullability)
- Shared aligned fields:
  - `aligned_at_utc` (UTC second)
- Exchange aligned fields (Binance, OKX, and Bybit):
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`
- ETF/FRED aligned metric fields:
  - `source_id`, `metric_name`, `metric_unit`
  - value channels (`metric_value_*`)
  - `provenance_json`, `records_in_bucket`, `latest_ingested_at_utc`
- Forward-fill interval fields:
  - `valid_from_utc` (inclusive UTC timestamp)
  - `valid_to_utc_exclusive` (exclusive UTC timestamp)

## Source/provenance and freshness semantics
- Aligned data is derived from canonical native tables.
- Freshness payload:
  - `as_of_utc`
  - `lag_seconds`
- Provenance from source-native rows is preserved through aligned transforms.

## Failure modes, warnings, and error codes
- Warning codes:
  - `ALIGNED_FRESHNESS_STALE`
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - ETF and FRED source-specific warnings when relevant
- `strict=true` returns `409` if warnings exist.
- Common errors:
  - `404` no rows
  - `409` contract/rights/strict conflicts
  - `503` backend/runtime/queue failures

## Determinism/replay notes
- Fixed aligned windows are replay deterministic.
- Forward-fill boundary behavior is validated in slice proofs:
  - `spec/slices/slice-5-raw-query-aligned-1s/`
  - `spec/slices/slice-6-fred-integration/`
  - `spec/slices/slice-8-okx-spot-trades-aligned/`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/`

## Environment variables and required config
- `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
- `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
- `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
- `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- ClickHouse credentials (`CLICKHOUSE_*`)

## Minimal examples
- Binance aligned query:
  - `{ "mode":"aligned_1s", "sources":["spot_trades"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- OKX aligned query:
  - `{ "mode":"aligned_1s", "sources":["okx_spot_trades"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- Bybit aligned query:
  - `{ "mode":"aligned_1s", "sources":["bybit_spot_trades"], "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- ETF aligned query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "n_rows":100, "fields":["aligned_at_utc","source_id","metric_name","metric_value_float"], "strict":false }`
