# Historical Spot API Reference

- Last updated: 2026-03-11
- Scope: Binance, OKX, Bybit spot historical trades and klines

## Endpoints

Trades:
- `POST /v1/historical/binance/spot/trades`
- `POST /v1/historical/okx/spot/trades`
- `POST /v1/historical/bybit/spot/trades`

Klines:
- `POST /v1/historical/binance/spot/klines`
- `POST /v1/historical/okx/spot/klines`
- `POST /v1/historical/bybit/spot/klines`

All endpoints require `X-API-Key`.

## Request contracts

Trades request:
- `start_date: YYYY-MM-DD | null`
- `end_date: YYYY-MM-DD | null`
- `n_latest_rows: int | null`
- `n_random_rows: int | null`
- `include_datetime_col: bool` (default `true`)
- `strict: bool` (default `false`)

Klines request:
- `start_date: YYYY-MM-DD | null`
- `end_date: YYYY-MM-DD | null`
- `n_latest_rows: int | null`
- `n_random_rows: int | null`
- `kline_size: int` (seconds, `> 0`)
- `strict: bool` (default `false`)

Exactly one window mode is allowed:
- date-window (`start_date` and/or `end_date`)
- `n_latest_rows`
- `n_random_rows`

Date semantics:
- start is inclusive at `00:00:00Z`
- end day is inclusive via next-day exclusive query bound
- open bounds are resolved from available source data range

## Response contract

Response fields:
- `mode`
- `source`
- `sources`
- `row_count`
- `schema`
- `warnings`
- `rows`
- `rights_state`
- `rights_provisional`

## Schema taxonomy

Trades rows (all exchanges):
- `trade_id`
- `timestamp`
- `price`
- `quantity`
- `is_buyer_maker`
- `datetime` (optional if `include_datetime_col=false`)

Klines rows (all exchanges):
- `datetime`
- `open`, `high`, `low`, `close`
- `mean`, `std`, `median`, `iqr`
- `volume`, `maker_ratio`, `no_of_trades`
- `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`
- `liquidity_sum`, `maker_volume`, `maker_liquidity`

Mapping for OKX/Bybit:
- source `side=buy` => `is_buyer_maker=0`
- source `side=sell` => `is_buyer_maker=1`

## Errors

- `200`: success
- `404`: no data in selected window
- `409`: contract/auth/strict-warning failures
- `503`: runtime/backend failure
