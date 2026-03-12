# Aligned Mode Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S5, S6, S8, S11, S13, S15, S16, S17, S18, S19, S20, S21, S29 (API v0.1.20)

## Purpose and scope
- User-facing reference for `mode=aligned_1s` query and export behavior.
- Scope includes supported sources, field semantics, freshness/warnings, and deterministic behavior.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
  - `mode` must be `aligned_1s`
  - `sources` currently must contain exactly one key from:
    - `binance_spot_trades`
    - `binance_spot_trades`
    - `binance_spot_trades`
    - `okx_spot_trades`
    - `bybit_spot_trades`
    - `etf_daily_metrics`
    - `fred_series_metrics`
    - `bitcoin_block_headers`
    - `bitcoin_block_transactions`
    - `bitcoin_mempool_state`
    - `bitcoin_block_fee_totals`
    - `bitcoin_block_subsidy_schedule`
    - `bitcoin_network_hashrate_estimate`
    - `bitcoin_circulating_supply`
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
- ETF/FRED/Bitcoin-derived aligned metric fields:
  - `source_id`, `metric_name`, `metric_unit`
  - value channels (`metric_value_*`)
  - `provenance_json`, `records_in_bucket`, `latest_ingested_at_utc`
- Bitcoin stream aligned fields:
  - `records_in_bucket`
  - `first_source_offset_or_equivalent`, `last_source_offset_or_equivalent`
  - `bucket_sha256`
  - stream-specific latest/aggregate fields documented in `docs/bitcoin-core-reference.md`
- Forward-fill interval fields:
  - `valid_from_utc` (inclusive UTC timestamp)
  - `valid_to_utc_exclusive` (exclusive UTC timestamp)

## Source/provenance and freshness semantics
- Binance aligned data is served from canonical aligned aggregate projections built directly from canonical event log records.
- OKX aligned data is served from canonical aligned aggregate projections built directly from canonical OKX event rows.
- Bybit aligned data is served from canonical aligned aggregate projections built directly from canonical Bybit event rows.
- ETF aligned data is also served from canonical aligned aggregate projections built directly from canonical ETF event rows.
- FRED aligned data is also served from canonical aligned aggregate projections built directly from canonical FRED event rows.
- Bitcoin derived aligned data is served from canonical aligned aggregate projections built directly from canonical Bitcoin event rows (S20 cutover).
- Bitcoin stream aligned data is served from canonical aligned aggregate projections built directly from canonical Bitcoin event rows (S29 completion).
- Canonical aligned storage contract is fixed to `canonical_aligned_1s_aggregates`; no alternate aligned storage path is supported.
- Freshness payload:
  - `as_of_utc`
  - `lag_seconds`
- Provenance from source-native rows is preserved through aligned transforms.

## Failure modes, warnings, and error codes
- Canonical aligned storage contract violations fail loudly:
  - missing `canonical_aligned_1s_aggregates` table
  - missing required columns
  - required column type drift
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
- Slice 21 enforces storage-contract checks without changing fixed-window replay fingerprints.
- Forward-fill boundary behavior is validated in slice proofs:
  - `spec/slices/slice-5-raw-query-aligned-1s/`
  - `spec/slices/slice-15-binance-event-sourcing-port/`
  - `spec/slices/slice-16-etf-event-sourcing-port/`
  - `spec/slices/slice-17-fred-event-sourcing-port/`
  - `spec/slices/slice-18-okx-event-sourcing-port/`
  - `spec/slices/slice-19-bybit-event-sourcing-port/`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/`
  - `spec/slices/slice-8-okx-spot-trades-aligned/`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/`
  - `spec/slices/slice-13-bitcoin-core-signals/`
  - `spec/slices/slice-29-bitcoin-stream-aligned-completion/`

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
  - `{ "mode":"aligned_1s", "sources":["binance_spot_trades"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- OKX aligned query:
  - `{ "mode":"aligned_1s", "sources":["okx_spot_trades"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- Bybit aligned query:
  - `{ "mode":"aligned_1s", "sources":["bybit_spot_trades"], "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "strict":false }`
- ETF aligned query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "n_rows":100, "fields":["aligned_at_utc","source_id","metric_name","metric_value_float"], "strict":false }`
- Bitcoin derived aligned query:
  - `{ "mode":"aligned_1s", "sources":["bitcoin_circulating_supply"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "fields":["aligned_at_utc","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "strict":false }`
- Bitcoin stream aligned query:
  - `{ "mode":"aligned_1s", "sources":["bitcoin_mempool_state"], "time_range":["2024-04-20T00:00:00Z","2024-04-20T00:10:00Z"], "fields":["aligned_at_utc","tx_count","fee_rate_sat_vb_avg","rbf_true_count"], "strict":false }`
