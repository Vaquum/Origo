# Historical Bitcoin Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S30, S31 (API v0.1.22)

## Purpose and scope
- User reference for historical Bitcoin dataset access on dedicated historical endpoints.
- Covers all seven onboarded Bitcoin datasets in `native` and `aligned_1s`.

## Endpoints
- `POST /v1/historical/bitcoin/block_headers`
- `POST /v1/historical/bitcoin/block_transactions`
- `POST /v1/historical/bitcoin/mempool_state`
- `POST /v1/historical/bitcoin/block_fee_totals`
- `POST /v1/historical/bitcoin/block_subsidy_schedule`
- `POST /v1/historical/bitcoin/network_hashrate_estimate`
- `POST /v1/historical/bitcoin/circulating_supply`

## Request contract
- Shared parameters:
  - `mode`
  - `start_date`
  - `end_date`
  - `n_latest_rows`
  - `n_random_rows`
  - `fields`
  - `filters`
  - `strict`
- Modes:
  - `native`
  - `aligned_1s`
- Window rules:
  - at most one selector mode
  - no selector means full available history (`earliest -> now`)
  - date format is strict `YYYY-MM-DD`

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

## Error contract
- `200` success
- `404` no rows for requested window
- `409` request contract/auth/strict warning failure
- `503` runtime/backend failure

## Dataset keys
- `bitcoin_block_headers`
- `bitcoin_block_transactions`
- `bitcoin_mempool_state`
- `bitcoin_block_fee_totals`
- `bitcoin_block_subsidy_schedule`
- `bitcoin_network_hashrate_estimate`
- `bitcoin_circulating_supply`

## Minimal examples
- Native headers:
  - `{ "mode":"native", "start_date":"2024-04-20", "end_date":"2024-04-21", "fields":["height","difficulty","datetime"], "strict":false }`
- Aligned mempool:
  - `{ "mode":"aligned_1s", "start_date":"2024-04-20", "end_date":"2024-04-20", "fields":["aligned_at_utc","tx_count","fee_rate_sat_vb_avg"], "strict":false }`
