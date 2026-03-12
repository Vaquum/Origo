# S30 Historical Bitcoin Operationalization

## Metadata
- Owner: Origo API
- Last updated: 2026-03-12
- Slice reference: S30 (API v0.1.21)

## Purpose and scope
- Operationalize all seven onboarded Bitcoin datasets on historical Python and HTTP surfaces.
- Scope:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`

## Capability delivered
- Added historical Python methods:
  - `HistoricalData.get_bitcoin_block_headers`
  - `HistoricalData.get_bitcoin_block_transactions`
  - `HistoricalData.get_bitcoin_mempool_state`
  - `HistoricalData.get_bitcoin_block_fee_totals`
  - `HistoricalData.get_bitcoin_block_subsidy_schedule`
  - `HistoricalData.get_bitcoin_network_hashrate_estimate`
  - `HistoricalData.get_bitcoin_circulating_supply`
- Added historical HTTP endpoints:
  - `POST /v1/historical/bitcoin/block_headers`
  - `POST /v1/historical/bitcoin/block_transactions`
  - `POST /v1/historical/bitcoin/mempool_state`
  - `POST /v1/historical/bitcoin/block_fee_totals`
  - `POST /v1/historical/bitcoin/block_subsidy_schedule`
  - `POST /v1/historical/bitcoin/network_hashrate_estimate`
  - `POST /v1/historical/bitcoin/circulating_supply`
- Added shared historical query helper:
  - `query_bitcoin_dataset_data(...)`
- Enabled both historical modes for every Bitcoin dataset:
  - `native`
  - `aligned_1s`

## Contract behavior
- Shared request params:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Window mode is fail-loud:
  - at most one of date-window, `n_latest_rows`, `n_random_rows`
  - no selector means full available history
- Date-window uses strict UTC day bounds:
  - start at `start_dateT00:00:00Z`
  - end at `(end_date + 1 day)T00:00:00Z` (end-day inclusive behavior)
- Filter-then-projection order is enforced consistently.

## Guardrails
- Auth:
  - `X-API-Key` required on all historical Bitcoin endpoints
- Rights:
  - rights gate resolution + response metadata (`rights_state`, `rights_provisional`)
- Strict mode:
  - `strict=true` fails with `409` when mutable/sample window warnings exist
- Status/error taxonomy:
  - `200`, `404`, `409`, `503`

## Proof coverage
- Contract suite:
  - `tests/contract/test_historical_spot_api_contract.py`
- Replay/parity suite:
  - `tests/replay/test_historical_exchange_replay.py`
- Coverage includes:
  - Python method contract/signature cohesion
  - route registration and response envelope checks
  - `native`/`aligned_1s` mode coverage for all seven endpoints
  - field/filter plumbing checks
  - strict warning escalation checks
  - `404` no-data behavior checks
  - replay determinism and historical-vs-raw parity checks for all seven datasets

## Key implementation files
- `origo/data/_internal/generic_endpoints.py`
- `origo/data/historical_data.py`
- `api/origo_api/schemas.py`
- `api/origo_api/main.py`
- `tests/contract/test_historical_spot_api_contract.py`
- `tests/replay/test_historical_exchange_replay.py`
