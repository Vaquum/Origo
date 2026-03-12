# Historical Contract Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S25, S26, S27, S28, S30 (API v0.1.21)

## Purpose and scope
- User reference for shared historical request behavior on Origo historical dataset routes.
- Covers normalized parameter naming and selector semantics.

## Shared request contract
- `mode`
- `start_date`
- `end_date`
- `n_latest_rows`
- `n_random_rows`
- `fields`
- `filters`
- `strict`

Route-specific additions:
- trades routes: `include_datetime_col`
- klines routes: `kline_size`

Historical dataset routes in scope:
- `POST /v1/historical/binance/spot/trades`
- `POST /v1/historical/binance/spot/klines`
- `POST /v1/historical/okx/spot/trades`
- `POST /v1/historical/okx/spot/klines`
- `POST /v1/historical/bybit/spot/trades`
- `POST /v1/historical/bybit/spot/klines`
- `POST /v1/historical/etf/daily_metrics`
- `POST /v1/historical/fred/series_metrics`
- `POST /v1/historical/bitcoin/block_headers`
- `POST /v1/historical/bitcoin/block_transactions`
- `POST /v1/historical/bitcoin/mempool_state`
- `POST /v1/historical/bitcoin/block_fee_totals`
- `POST /v1/historical/bitcoin/block_subsidy_schedule`
- `POST /v1/historical/bitcoin/network_hashrate_estimate`
- `POST /v1/historical/bitcoin/circulating_supply`

## Window behavior
- At most one window selector can be supplied.
- Valid selectors:
  - `start_date` / `end_date` (date-window)
  - `n_latest_rows`
  - `n_random_rows`
- If no selector is supplied, query defaults to full available history (`earliest -> now`).
- Date format is strict UTC `YYYY-MM-DD`.

## Response shape
- `mode`
- `source`
- `sources`
- `row_count`
- `schema`
- `warnings`
- `rows`
- `rights_state`
- `rights_provisional`

## Error behavior
- `200` success
- `404` no rows found
- `409` contract/auth/strict failures
- `503` runtime/backend failure

## Current mode availability
- Historical route contracts accept `mode=native|aligned_1s`.
- Historical trades routes execute both `native` and `aligned_1s` for:
  - `spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Historical klines routes execute both `native` and `aligned_1s`.
- Historical ETF route executes both `native` and `aligned_1s` for:
  - `etf_daily_metrics`
- Historical FRED route executes both `native` and `aligned_1s` for:
  - `fred_series_metrics`
- Historical Bitcoin routes execute both `native` and `aligned_1s` for:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`
- Historical scope explicitly excludes `spot_agg_trades` and `futures_trades` in this tranche.
