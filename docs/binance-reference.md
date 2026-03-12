# Binance Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S15, S33 (API v0.1.24)

## Purpose and scope
- User-facing reference for Binance dataset behavior in Raw Query/Export after contract cleanup.
- Scope includes only `binance_spot_trades` in both `native` and `aligned_1s` modes.

## Inputs and outputs with contract shape
- Query:
  - endpoint: `POST /v1/raw/query`
  - `sources=["binance_spot_trades"]`
  - `mode`: `native | aligned_1s`
  - window selector: `time_range | n_rows | n_random` (or none for full history)
  - optional: `fields`, `filters`, `strict`
- Export:
  - endpoint: `POST /v1/raw/export`
  - `dataset`: `binance_spot_trades`
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
- Output envelope fields:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `rows`
  - `freshness`, `warnings`
  - `rights_state`, `rights_provisional`

## Data definitions
- `binance_spot_trades` native fields:
  - `trade_id` integer
  - `timestamp` integer epoch milliseconds UTC
  - `price` float
  - `quantity` float
  - `quote_quantity` float
  - `is_buyer_maker` integer/bool-like (`0|1`)
  - `is_best_match` integer/bool-like (`0|1`)
  - `datetime` UTC timestamp
- `binance_spot_trades` aligned fields:
  - `aligned_at_utc` UTC second timestamp
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source truth is direct Binance first-party historical files.
- Serving is event-driven from canonical append-only events and projection tables.
- Aligned responses can emit `ALIGNED_FRESHNESS_STALE`.

## Failure modes and codes
- `200` success
- `404` no rows for window
- `409` strict-warning failure or rights/config conflict
- `503` backend/query runtime failure

## Determinism/replay notes
- Key proof artifacts:
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p1-acceptance.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p3-determinism.json`

## Environment variables
- `ORIGO_INTERNAL_API_KEY`
- `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query:
  - `{ "mode":"native", "sources":["binance_spot_trades"], "fields":["trade_id","timestamp","price","quantity"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
- Aligned query:
  - `{ "mode":"aligned_1s", "sources":["binance_spot_trades"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
- Native export:
  - `{ "mode":"native", "format":"parquet", "dataset":"binance_spot_trades", "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "strict":false }`
