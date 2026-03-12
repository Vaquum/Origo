# S26 Historical Exchange Spot Parity

## Metadata
- Owner: Origo API
- Last updated: 2026-03-11
- Slice reference: S26 (API v0.1.17)

## Purpose and scope
- Enable historical exchange spot routes and Python methods for both `native` and `aligned_1s`.
- Keep scope limited to three in-scope datasets:
  - `binance_spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Legacy Binance non-spot dataset keys are explicitly removed from historical scope.

## Inputs and outputs with contract shape
- Historical trades HTTP routes:
  - `/v1/historical/binance/spot/trades`
  - `/v1/historical/okx/spot/trades`
  - `/v1/historical/bybit/spot/trades`
- Historical klines HTTP routes:
  - `/v1/historical/binance/spot/klines`
  - `/v1/historical/okx/spot/klines`
  - `/v1/historical/bybit/spot/klines`
- Historical Python methods:
  - `get_binance_spot_trades`
  - `get_okx_spot_trades`
  - `get_bybit_spot_trades`
  - `get_binance_spot_klines`
  - `get_okx_spot_klines`
  - `get_bybit_spot_klines`
- Shared request params used in both surfaces:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
  - trades-only: `include_datetime_col`
  - klines-only: `kline_size`
- Response envelope remains:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `warnings`, `rows`, `rights_state`, `rights_provisional`

## Data definitions (field names, types, units, timezone, nullability)
- `mode=native` trade rows:
  - `trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`
- `mode=aligned_1s` trade rows:
  - `aligned_at_utc`, `open_price`, `high_price`, `low_price`, `close_price`, `quantity_sum`, `quote_volume_sum`, `trade_count`
- `mode=native` kline rows:
  - `datetime`, `open`, `high`, `low`, `close`, `mean`, `std`, `median`, `iqr`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`
- `mode=aligned_1s` kline rows:
  - `datetime`, `open`, `high`, `low`, `close`, `volume`, `no_of_trades`, `liquidity_sum`
- Timestamps are UTC.
- Field projection (`fields`) and filter clauses (`filters`) are fail-loud.

## Source/provenance and freshness semantics
- Historical `native` rows are read from canonical native projection tables.
- Historical `aligned_1s` rows are read from `canonical_aligned_1s_aggregates` through aligned query core.
- Canonical aligned-storage runtime contract remains enforced in aligned paths.

## Failure modes, warnings, and error codes
- `409` contract failures:
  - multiple selector modes provided
  - invalid `fields`/`filters`
- `404` no rows for selected window.
- `503` backend/runtime failures.
- `strict=true` continues to fail when mutable/sample window warnings are present.

## Determinism/replay notes
- Native and aligned historical spot routes are deterministic under fixed fixtures.
- Replay coverage for S26 is in:
  - `tests/replay/test_historical_exchange_replay.py`
  - `tests/replay/test_native_query_replay.py`

## Environment variables and required config
- No new environment variables in this slice.
- Existing API env contract in root `.env.example` remains authoritative.

## Minimal examples
- Native historical trades:
  - `{ "mode": "native", "start_date": "2024-01-01", "end_date": "2024-01-01", "strict": false }`
- Aligned historical trades:
  - `{ "mode": "aligned_1s", "start_date": "2024-01-01", "end_date": "2024-01-01", "strict": false }`
- Aligned historical klines:
  - `{ "mode": "aligned_1s", "start_date": "2024-01-01", "end_date": "2024-01-01", "kline_size": 60, "strict": false }`
- Deferred datasets note:
  - no historical routes/methods are added for legacy Binance non-spot dataset keys in S26
