# S26 Historical Exchange Trades Parity

## Metadata
- Owner: Origo API
- Last updated: 2026-03-11
- Slice reference: S26 (API v0.1.17)

## Purpose and scope
- Enable historical exchange spot-trades routes and Python methods for both `native` and `aligned_1s`.
- Keep scope limited to three in-scope datasets:
  - `spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Explicitly defer historical `spot_agg_trades` and `futures_trades`.

## Inputs and outputs with contract shape
- Historical trades HTTP routes:
  - `/v1/historical/binance/spot/trades`
  - `/v1/historical/okx/spot/trades`
  - `/v1/historical/bybit/spot/trades`
- Historical trades Python methods:
  - `get_binance_spot_trades`
  - `get_okx_spot_trades`
  - `get_bybit_spot_trades`
- Shared request params used in both surfaces:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
  - trades-only: `include_datetime_col`
- Response envelope remains:
  - `mode`, `source`, `sources`, `row_count`, `schema`, `warnings`, `rows`, `rights_state`, `rights_provisional`

## Data definitions (field names, types, units, timezone, nullability)
- `mode=native` trade rows:
  - `trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`
- `mode=aligned_1s` trade rows:
  - `aligned_at_utc`, `open_price`, `high_price`, `low_price`, `close_price`, `quantity_sum`, `quote_volume_sum`, `trade_count`
- Timestamps are UTC.
- Field projection (`fields`) and filter clauses (`filters`) are fail-loud.

## Source/provenance and freshness semantics
- Historical `native` rows are read from canonical native projection tables.
- Historical `aligned_1s` rows are read from `canonical_aligned_1s_aggregates` through aligned query core.
- Canonical aligned-storage runtime contract remains enforced in aligned paths.

## Failure modes, warnings, and error codes
- `409` contract failures:
  - multiple selector modes provided
  - unsupported mode for route family (`aligned_1s` on klines)
  - invalid `fields`/`filters`
- `404` no rows for selected window.
- `503` backend/runtime failures.
- `strict=true` continues to fail when mutable/sample window warnings are present.

## Determinism/replay notes
- Native and aligned historical trade paths are deterministic under fixed fixtures.
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
- Deferred datasets note:
  - no historical routes/methods are added for `spot_agg_trades` or `futures_trades` in S26
