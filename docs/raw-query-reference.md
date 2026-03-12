# Raw Query API Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S1, S4, S5, S6, S8, S11, S13, S14, S15, S16, S17, S18, S19, S20, S21, S25, S26, S29 (API v0.1.20)

## Purpose and scope
- This is the user-facing reference for `POST /v1/raw/query`.
- Scope includes `native` and `aligned_1s` query modes over Binance, OKX, Bybit, ETF, FRED, and Bitcoin Core datasets.

## Inputs and outputs with contract shape
- Endpoint: `POST /v1/raw/query`
- Required header: `X-API-Key`
- Optional header: `X-ClickHouse-Token` (required only for BYOK-gated sources)
- Request contract:
  - `mode`: `native | aligned_1s` (default `native`)
  - `sources`: list of source keys (current capability requires exactly one item)
  - `view_id`: optional view identifier (must be paired with `view_version`)
  - `view_version`: optional positive integer (must be paired with `view_id`)
  - `fields`: optional list of projected fields
  - `time_range`: optional `[start_iso, end_iso]`
  - `n_rows`: optional integer > 0
  - `n_random`: optional integer > 0
  - `filters`: optional list of filter clauses:
    - `{ "field": "<column>", "op": "eq|ne|gt|gte|lt|lte|in|not_in", "value": <any> }`
  - `strict`: boolean (default `false`)
- Window selection rule: at most one of `time_range`, `n_rows`, `n_random` can be provided.
- If no selector is provided, query window defaults to full available history (`earliest -> now`).
- Response contract:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `freshness`, `warnings`, `rows`
  - `view_id`, `view_version`
  - `rights_state`, `rights_provisional`

## Data definitions (fields, types, units, timezone, nullability)
- Timestamp fields are UTC and returned as ISO-8601 strings.
- `schema` response entries are `{name, dtype}`.
- Dataset/source keys:
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
- Field-level definitions are maintained in:
  - `docs/binance-reference.md`
  - `docs/data-taxonomy.md`
  - `docs/aligned-reference.md`
  - `docs/okx-reference.md`
  - `docs/bybit-reference.md`
  - `docs/etf-reference.md`
  - `docs/fred-reference.md`
  - `docs/bitcoin-core-reference.md`

## Source/provenance and freshness semantics
- Query reads from ClickHouse canonical tables loaded from original sources.
- `aligned_1s` responses can include freshness payload:
  - `freshness.as_of_utc`
  - `freshness.lag_seconds`
- ETF and FRED warning paths include source-specific freshness/quality checks.
- OKX native/aligned serving is event-driven from canonical OKX projections (S18 cutover).
- Bybit native/aligned serving is event-driven from canonical Bybit projections (S19 cutover).
- ETF native/aligned serving is event-driven from canonical ETF projections (S16 cutover).
- FRED native/aligned serving is event-driven from canonical FRED projections (S17 cutover).
- Bitcoin native/aligned serving is event-driven from canonical Bitcoin projections (S20 cutover, S29 aligned stream completion).

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
- Binance `aligned_1s` runtime enforces canonical aligned-storage contract and fails loudly on table/schema drift.
- OKX `aligned_1s` runtime enforces canonical aligned-storage contract and fails loudly on table/schema drift.
- Bybit `aligned_1s` runtime enforces canonical aligned-storage contract and fails loudly on table/schema drift.
- Bitcoin stream `aligned_1s` runtime enforces canonical aligned-storage contract and fails loudly on table/schema drift.
- Missing rights metadata in response contract is fail-loud and treated as runtime error.

## Determinism/replay notes
- Deterministic ordering is enforced for replayable windows.
- No-selector native windows are deterministically ordered by source event time and source identity key.
- Proof artifacts live under:
  - `spec/slices/slice-1-raw-query-native/`
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
  - `{ "mode":"native", "sources":["binance_spot_trades"], "fields":["trade_id","price","timestamp"], "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "filters":[{"field":"price","op":"gt","value":1000}], "strict":false }`
- Native OKX query:
  - `{ "mode":"native", "sources":["okx_spot_trades"], "fields":["trade_id","timestamp","price","size","side"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Native Bybit query:
  - `{ "mode":"native", "sources":["bybit_spot_trades"], "fields":["trade_id","timestamp","price","size","side"], "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
- Native Bitcoin headers query:
  - `{ "mode":"native", "sources":["bitcoin_block_headers"], "fields":["height","difficulty","timestamp"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Aligned Bitcoin derived query:
  - `{ "mode":"aligned_1s", "sources":["bitcoin_network_hashrate_estimate"], "fields":["aligned_at_utc","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Aligned latest rows query:
  - `{ "mode":"aligned_1s", "sources":["etf_daily_metrics"], "fields":["aligned_at_utc","metric_name","metric_value_float"], "n_rows":100, "strict":false }`
- Native full-history query (no selector):
  - `{ "mode":"native", "sources":["binance_spot_trades"], "fields":["trade_id","timestamp","price","quantity"], "strict":false }`
