# Binance Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S1, S5, S14, S15 (API v0.1.8)

## Purpose and scope
- User-facing reference for Binance datasets in Raw Query/Export after Slice 15 event-sourcing cutover.
- Scope includes `spot_trades`, `spot_agg_trades`, and `futures_trades` in both `native` and `aligned_1s` modes.

## Inputs and outputs with contract shape
- Query:
  - endpoint: `POST /v1/raw/query`
  - `sources=["spot_trades"]` or `sources=["spot_agg_trades"]` or `sources=["futures_trades"]`
  - `mode`: `native | aligned_1s`
  - one window selector: `time_range | n_rows | n_random`
  - optional: `fields`, `filters`, `strict`
- Export:
  - endpoint: `POST /v1/raw/export`
  - `dataset`: one of Binance datasets
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
- Output envelopes include:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `rows`
  - `freshness`, `warnings`
  - `rights_state`, `rights_provisional`

## Data definitions (field names, types, units, timezone, nullability)
- `spot_trades` native fields:
  - `trade_id` integer
  - `timestamp` integer epoch milliseconds UTC
  - `price` float
  - `quantity` float
  - `quote_quantity` float
  - `is_buyer_maker` integer/bool-like (`0|1`)
  - `is_best_match` integer/bool-like (`0|1`)
  - `datetime` UTC timestamp
- `spot_agg_trades` native fields:
  - `agg_trade_id` integer
  - `timestamp` integer epoch milliseconds UTC
  - `price` float
  - `quantity` float
  - `first_trade_id` integer
  - `last_trade_id` integer
  - `is_buyer_maker` integer/bool-like (`0|1`)
  - `datetime` UTC timestamp
- `futures_trades` native fields:
  - `futures_trade_id` integer
  - `timestamp` integer epoch milliseconds UTC
  - `price` float
  - `quantity` float
  - `quote_quantity` float
  - `is_buyer_maker` integer/bool-like (`0|1`)
  - `datetime` UTC timestamp
- Aligned fields for all three datasets (`aligned_1s`):
  - `aligned_at_utc` UTC second timestamp
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source truth is direct Binance first-party historical files (no third-party API source).
- Serving is event-driven:
  - canonical append-only event log stores raw payload bytes + hashes
  - native and aligned rows are projection outputs built from canonical events
- Aligned responses include freshness payload and can emit `ALIGNED_FRESHNESS_STALE` warnings.

## Failure modes, warnings, and error codes
- Query status map:
  - `200` success
  - `404` no rows for window
  - `409` strict-warning failure or rights/config conflict
  - `503` backend/query runtime failure
- Common warning/error behavior:
  - `ALIGNED_FRESHNESS_STALE` on stale aligned responses
  - `STRICT_MODE_WARNING_FAILURE` when `strict=true` and warnings are present
- Rights fields are always present in response contract (`rights_state`, `rights_provisional`).

## Determinism/replay notes
- Slice 15 proof artifacts:
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p1-acceptance.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p2-parity.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p3-determinism.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/baseline-fixture-2024-01-04_2024-01-04.json`

## Environment variables and required config
- API runtime:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- ClickHouse serving backend:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query (`spot_trades`):
  - `{ "mode":"native", "sources":["spot_trades"], "fields":["trade_id","timestamp","price","quantity"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
- Native query (`spot_agg_trades`):
  - `{ "mode":"native", "sources":["spot_agg_trades"], "fields":["agg_trade_id","timestamp","price","quantity"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
- Aligned query (`futures_trades`):
  - `{ "mode":"aligned_1s", "sources":["futures_trades"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
- Native export (`spot_trades`):
  - `{ "mode":"native", "format":"parquet", "dataset":"spot_trades", "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
