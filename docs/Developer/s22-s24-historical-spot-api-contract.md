# S22-S24 Historical Spot API Contract

- Owner: Origo API
- Last updated: 2026-03-11
- Slice references: S22, S23, S24, S25

## Purpose and scope
Operational historical spot access for Binance, OKX, and Bybit through six explicit Python methods and six explicit HTTP endpoints. S25 normalizes parameter naming and window semantics across this surface.

## Python interface contract
Exactly six methods are supported in `origo/data/historical_data.py`.

- `get_binance_spot_trades(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, include_datetime_col)`
- `get_okx_spot_trades(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, include_datetime_col)`
- `get_bybit_spot_trades(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, include_datetime_col)`
- `get_binance_spot_klines(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, kline_size)`
- `get_okx_spot_klines(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, kline_size)`
- `get_bybit_spot_klines(mode, start_date, end_date, n_latest_rows, n_random_rows, fields, filters, strict, kline_size)`

Dropped methods are hard-removed:
- `get_spot_trades`
- `get_spot_klines`
- `get_spot_agg_trades`
- `get_futures_trades`
- `get_futures_klines`

## HTTP contract
POST routes:
- `/v1/historical/binance/spot/trades`
- `/v1/historical/binance/spot/klines`
- `/v1/historical/okx/spot/trades`
- `/v1/historical/okx/spot/klines`
- `/v1/historical/bybit/spot/trades`
- `/v1/historical/bybit/spot/klines`

Auth:
- `X-API-Key` required (same internal auth as raw API).
- `X-ClickHouse-Token` optional pass-through for BYOK/CH auth path.

Response envelope:
- `mode`, `source`, `sources`, `row_count`, `schema`, `warnings`, `rows`
- rights metadata: `rights_state`, `rights_provisional`

## Window and date semantics
At most one mode is allowed per request:
- date-window: (`start_date` and/or `end_date`)
- `n_latest_rows`
- `n_random_rows`

No selector defaults to full available history (`earliest -> now`).

Date format is strict UTC `YYYY-MM-DD`.
- start bound = `start_date 00:00:00Z` inclusive
- end bound = day-end inclusive, implemented as next-day exclusive
- open bounds are resolved from source table min/max UTC day

Invalid dates fail loudly with contract error.

Mode semantics:
- request contract accepts `mode=native|aligned_1s`.
- current historical route execution supports `native` only.
- `aligned_1s` requests fail loudly with `409` and `HISTORICAL_CONTRACT_ERROR` until S26.

## Data schema and mapping
Trades schema (all exchanges):
- `trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`

Maker mapping for OKX/Bybit:
- `buy -> 0`
- `sell -> 1`
- any other value is fail-loud contract/runtime error

Klines schema:
- unchanged from existing kline contract (`datetime`, OHLC, summary stats, volume/liquidity fields)

## Error taxonomy
Historical routes follow:
- `200` success
- `404` no data in selected window
- `409` contract/auth/strict-warning failures
- `503` runtime/backend failures

## Environment contract
No new environment variables in S22-S24.
Existing required API/runtime vars remain authoritative in root `.env.example`.
