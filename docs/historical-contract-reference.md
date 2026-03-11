# Historical Contract Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-11
- Slice/version reference: S25, S26, S27 (API v0.1.18)

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
- Historical scope explicitly excludes `spot_agg_trades` and `futures_trades` in this tranche.
