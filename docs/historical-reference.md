# Historical API Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S31 (API v0.1.22)

## Purpose and scope
- Canonical end-user reference for all historical dataset interfaces.
- Covers HTTP endpoints, Python method equivalents, mode support, and deferred scope.

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
- spot trades routes: `include_datetime_col`
- spot klines routes: `kline_size`

Modes:
- `native`
- `aligned_1s`

Window rules:
- at most one of date-window, `n_latest_rows`, `n_random_rows`
- no selector means full available history (`earliest -> now`)
- dates are strict UTC `YYYY-MM-DD`

## Historical dataset matrix

| Dataset | HTTP endpoint | Python method | Modes |
|---|---|---|---|
| `spot_trades` | `POST /v1/historical/binance/spot/trades` | `get_binance_spot_trades` | `native`, `aligned_1s` |
| `okx_spot_trades` | `POST /v1/historical/okx/spot/trades` | `get_okx_spot_trades` | `native`, `aligned_1s` |
| `bybit_spot_trades` | `POST /v1/historical/bybit/spot/trades` | `get_bybit_spot_trades` | `native`, `aligned_1s` |
| `etf_daily_metrics` | `POST /v1/historical/etf/daily_metrics` | `get_etf_daily_metrics` | `native`, `aligned_1s` |
| `fred_series_metrics` | `POST /v1/historical/fred/series_metrics` | `get_fred_series_metrics` | `native`, `aligned_1s` |
| `bitcoin_block_headers` | `POST /v1/historical/bitcoin/block_headers` | `get_bitcoin_block_headers` | `native`, `aligned_1s` |
| `bitcoin_block_transactions` | `POST /v1/historical/bitcoin/block_transactions` | `get_bitcoin_block_transactions` | `native`, `aligned_1s` |
| `bitcoin_mempool_state` | `POST /v1/historical/bitcoin/mempool_state` | `get_bitcoin_mempool_state` | `native`, `aligned_1s` |
| `bitcoin_block_fee_totals` | `POST /v1/historical/bitcoin/block_fee_totals` | `get_bitcoin_block_fee_totals` | `native`, `aligned_1s` |
| `bitcoin_block_subsidy_schedule` | `POST /v1/historical/bitcoin/block_subsidy_schedule` | `get_bitcoin_block_subsidy_schedule` | `native`, `aligned_1s` |
| `bitcoin_network_hashrate_estimate` | `POST /v1/historical/bitcoin/network_hashrate_estimate` | `get_bitcoin_network_hashrate_estimate` | `native`, `aligned_1s` |
| `bitcoin_circulating_supply` | `POST /v1/historical/bitcoin/circulating_supply` | `get_bitcoin_circulating_supply` | `native`, `aligned_1s` |

Spot kline convenience routes:
- `POST /v1/historical/binance/spot/klines`
- `POST /v1/historical/okx/spot/klines`
- `POST /v1/historical/bybit/spot/klines`

## Deferred datasets
- `spot_agg_trades` (deferred from historical Python/HTTP surface)
- `futures_trades` (deferred from historical Python/HTTP surface)

## Response contract
- `mode`
- `source`
- `sources`
- `row_count`
- `schema`
- `warnings`
- `rows`
- `rights_state`
- `rights_provisional`

## Status and error contract
- `200` success
- `404` no rows for requested window
- `409` contract/auth/strict warning failure
- `503` runtime/backend failure
