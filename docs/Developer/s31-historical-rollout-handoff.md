# S31 Historical Full-Surface Cohesion and Rollout Handoff

## Metadata
- Owner: Origo API
- Last updated: 2026-03-12
- Slice reference: S31 (API v0.1.22)

## Purpose and scope
- Close historical-surface drift by enforcing one canonical dataset/mode matrix across:
  - HTTP historical routes
  - `HistoricalData` Python methods
  - user docs taxonomy
- Publish final internal rollout and rollback mapping for legacy migration.

## Historical surface matrix (in scope)
- `binance_spot_trades`
  - HTTP: `POST /v1/historical/binance/spot/trades`
  - Python: `HistoricalData.get_binance_spot_trades`
- `okx_spot_trades`
  - HTTP: `POST /v1/historical/okx/spot/trades`
  - Python: `HistoricalData.get_okx_spot_trades`
- `bybit_spot_trades`
  - HTTP: `POST /v1/historical/bybit/spot/trades`
  - Python: `HistoricalData.get_bybit_spot_trades`
- `etf_daily_metrics`
  - HTTP: `POST /v1/historical/etf/daily_metrics`
  - Python: `HistoricalData.get_etf_daily_metrics`
- `fred_series_metrics`
  - HTTP: `POST /v1/historical/fred/series_metrics`
  - Python: `HistoricalData.get_fred_series_metrics`
- `bitcoin_block_headers`
  - HTTP: `POST /v1/historical/bitcoin/block_headers`
  - Python: `HistoricalData.get_bitcoin_block_headers`
- `bitcoin_block_transactions`
  - HTTP: `POST /v1/historical/bitcoin/block_transactions`
  - Python: `HistoricalData.get_bitcoin_block_transactions`
- `bitcoin_mempool_state`
  - HTTP: `POST /v1/historical/bitcoin/mempool_state`
  - Python: `HistoricalData.get_bitcoin_mempool_state`
- `bitcoin_block_fee_totals`
  - HTTP: `POST /v1/historical/bitcoin/block_fee_totals`
  - Python: `HistoricalData.get_bitcoin_block_fee_totals`
- `bitcoin_block_subsidy_schedule`
  - HTTP: `POST /v1/historical/bitcoin/block_subsidy_schedule`
  - Python: `HistoricalData.get_bitcoin_block_subsidy_schedule`
- `bitcoin_network_hashrate_estimate`
  - HTTP: `POST /v1/historical/bitcoin/network_hashrate_estimate`
  - Python: `HistoricalData.get_bitcoin_network_hashrate_estimate`
- `bitcoin_circulating_supply`
  - HTTP: `POST /v1/historical/bitcoin/circulating_supply`
  - Python: `HistoricalData.get_bitcoin_circulating_supply`

Deferred from historical surface in this tranche:
- none

## Contract and guardrail closure
- Historical mode support is complete for every in-scope dataset:
  - `native`
  - `aligned_1s`
- Shared historical parameter contract is stable:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Status and error taxonomy is stable:
  - `200`, `404`, `409`, `503`
- Rights/auth/strict warning behavior is uniform across dataset families.

## Zero-drift enforcement
- Contract test coverage added for matrix closure and docs parity:
  - `tests/contract/test_historical_surface_cohesion_contract.py`
- The test fails loudly if:
  - any in-scope `RawQuerySource` lacks an HTTP historical route
  - any in-scope `RawQuerySource` lacks a `HistoricalData` method mapping
  - deferred datasets leak into historical-surface matrix
  - docs omit registered historical routes/datasets

## Final migration mapping
- Legacy generic spot methods are retired:
  - `get_spot_trades`, `get_spot_klines`, `get_spot_agg_trades`, `get_futures_trades`, `get_futures_klines`
- Migration target is explicit dataset-method/route usage from the matrix above.
- Legacy compatibility policy:
  - no fallback aliases
  - no silent compatibility shims
  - contract errors are fail-loud (`409`)

## Rollback mapping
- If rollback is required:
  1. Keep Origo historical routes live.
  2. Repoint affected internal clients to prior legacy service base URL and frozen payload contract.
  3. Keep Origo as shadow validator for parity checks until legacy issue is resolved.
- Rollback must preserve fail-loud behavior; do not introduce fallback aliases in Origo code.
