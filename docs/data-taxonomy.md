# Data Taxonomy

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S1-S8, S10, S11, S13, S14, S15, S16, S17, S18, S19, S20, S21 (platform v0.1.14)

## Purpose and scope
- Canonical user reference for all currently exposed sources, fields, modes, and status taxonomies.
- Scope includes query datasets, scraper source keys, warning/error taxonomies, and export lifecycle taxonomy.

## Inputs and outputs with contract shape
- This document is a reference artifact; it does not define an endpoint.
- It is consumed by users when building requests to:
  - `POST /v1/raw/query`
  - `POST /v1/raw/export`
  - `GET /v1/raw/export/{export_id}`
- Query contract currently uses `sources` (single item today), `view_id`, `view_version`, `fields`, `time_range|n_rows|n_random`, `filters`, `strict`.

## Data definitions (fields, types, units, timezone, nullability)
- Query source keys:
  - `spot_trades`
  - `spot_agg_trades`
  - `futures_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
  - `etf_daily_metrics`
  - `fred_series_metrics`
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`
- Core cross-dataset fields:
  - `datetime`: UTC timestamp
  - `timestamp`: epoch integer
  - `observed_at_utc`: UTC observation timestamp
  - `aligned_at_utc`: UTC aligned 1-second timestamp
  - `valid_from_utc`: UTC interval start (inclusive)
  - `valid_to_utc_exclusive`: UTC interval end (exclusive)
  - `view_id`: logical projection/view identifier
  - `view_version`: immutable view version integer
  - `rights_state`: rights classification used for serving/export
  - `rights_provisional`: boolean flag for temporary hosted-rights transition
- Canonical aligned serving storage contract:
  - mandatory table: `canonical_aligned_1s_aggregates`
  - mandatory in-scope source paths:
    - Binance aligned serving (`spot_trades`, `spot_agg_trades`, `futures_trades`)
    - OKX aligned serving (`okx_spot_trades`)
    - Bybit aligned serving (`bybit_spot_trades`)
    - ETF aligned serving (`etf_daily_metrics`)
    - FRED aligned serving (`fred_series_metrics`)
- ETF source taxonomy:
  - `etf_ishares_ibit_daily`
  - `etf_invesco_btco_daily`
  - `etf_bitwise_bitb_daily`
  - `etf_ark_arkb_daily`
  - `etf_vaneck_hodl_daily`
  - `etf_franklin_ezbc_daily`
  - `etf_grayscale_gbtc_daily`
  - `etf_fidelity_fbtc_daily`
  - `etf_coinshares_brrr_daily`
  - `etf_hashdex_defi_daily`
- FRED source taxonomy:
  - `fred_fedfunds`
  - `fred_cpiaucsl`
  - `fred_unrate`
  - `fred_dgs10`
- OKX native trade fields:
  - `instrument_name`: symbol (`BTC-USDT`)
  - `trade_id`: source trade identifier
  - `side`: `buy|sell`
  - `price`: execution price
  - `size`: base-asset quantity
  - `quote_quantity`: quote-asset quantity (`price * size`)
- Bybit native trade fields:
  - `symbol`: symbol (`BTCUSDT`)
  - `trade_id`: integer offset parsed from `trd_match_id` suffix (`m-<digits>`)
  - `trd_match_id`: source match identifier (`m-<digits>`)
  - `side`: `buy|sell`
  - `price`: execution price
  - `size`: base-asset quantity
  - `quote_quantity`: quote-asset quantity
  - `tick_direction`: source tick direction label
  - `gross_value`: source gross value
  - `home_notional`: source home notional
  - `foreign_notional`: source foreign notional
- Bitcoin stream fields:
  - `bitcoin_block_headers`:
    - `height`, `block_hash`, `prev_hash`, `merkle_root`, `version`, `nonce`, `difficulty`, `timestamp`, `datetime`, `source_chain`
  - `bitcoin_block_transactions`:
    - `block_height`, `block_hash`, `block_timestamp`, `transaction_index`, `txid`, `inputs`, `outputs`, `values`, `scripts`, `witness_data`, `coinbase`, `datetime`, `source_chain`
  - `bitcoin_mempool_state`:
    - `snapshot_at`, `snapshot_at_unix_ms`, `txid`, `fee_rate_sat_vb`, `vsize`, `first_seen_timestamp`, `rbf_flag`, `source_chain`
- Bitcoin derived fields:
  - `bitcoin_block_fee_totals`: `fee_total_btc`
  - `bitcoin_block_subsidy_schedule`: `halving_interval`, `subsidy_sats`, `subsidy_btc`
  - `bitcoin_network_hashrate_estimate`: `difficulty`, `observed_interval_seconds`, `hashrate_hs`
  - `bitcoin_circulating_supply`: `circulating_supply_sats`, `circulating_supply_btc`

## Source/provenance and freshness semantics
- Binance canonical historical truth source is exchange-hosted daily/monthly files, persisted first to canonical events and served via projection tables.
- OKX canonical historical truth source is exchange-hosted daily files resolved from OKX first-party API.
- OKX native/aligned serving is event-driven from canonical OKX projections as of Slice 18.
- Bybit canonical historical truth source is exchange-hosted daily csv.gz files.
- Bybit native/aligned serving is event-driven from canonical Bybit projections as of Slice 19.
- ETF canonical source hierarchy is issuer official sources.
- ETF native/aligned serving is event-driven from canonical ETF projections as of Slice 16.
- FRED canonical source is direct API calls to FRED.
- FRED native/aligned serving is event-driven from canonical FRED projections as of Slice 17.
- Bitcoin canonical source is direct Bitcoin Core RPC from one unpruned self-hosted node.
- Bitcoin native and derived aligned serving are event-driven from canonical Bitcoin projections as of Slice 20.
- Provenance fields (`provenance_json`, checksums, source keys) are replay-critical.

## Failure modes, warnings, and error codes
- Query warning taxonomy:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- Query status taxonomy:
  - `200`, `404`, `409`, `503`
- Aligned storage contract failure taxonomy (runtime fail-loud):
  - missing aligned aggregate table
  - missing required aligned columns
  - required aligned column type drift
- Export submit status taxonomy:
  - `202`
- Export lifecycle taxonomy:
  - `queued`, `running`, `succeeded`, `failed`
- Rights state taxonomy:
  - `Hosted Allowed`
  - `BYOK Required`
  - `Ingest Only`
- Rights provisional taxonomy:
  - `false`: normal rights state
  - `true`: temporary hosted path during legal transition

## Determinism/replay notes
- Deterministic replay artifacts are maintained under `spec/slices/`.
- Baseline fixtures record source checksums and repeated-run fingerprints.
- Query and export determinism is validated slice-by-slice before guardrail closeout.
- Binance event-sourcing replay/guardrail proofs are maintained in `spec/slices/slice-15-binance-event-sourcing-port/`.
- FRED event-sourcing replay/guardrail proofs are maintained in `spec/slices/slice-17-fred-event-sourcing-port/`.
- OKX event-sourcing replay/guardrail proofs are maintained in `spec/slices/slice-18-okx-event-sourcing-port/`.
- Bybit event-sourcing replay/guardrail proofs are maintained in `spec/slices/slice-19-bybit-event-sourcing-port/`.
- Bitcoin event-sourcing replay/guardrail proofs are maintained in `spec/slices/slice-20-bitcoin-event-sourcing-port/`.

## Environment variables and required config
- API/runtime:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_QUERY_MAX_CONCURRENCY`
  - `ORIGO_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
  - `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
- Freshness/rights:
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Storage:
  - `CLICKHOUSE_*`
  - `ORIGO_OBJECT_STORE_*`
- Event-runtime guardrail config:
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`

## Minimal examples
- Query source list example:
  - `sources=["spot_trades"]`
- OKX source list example:
  - `sources=["okx_spot_trades"]`
- Bybit source list example:
  - `sources=["bybit_spot_trades"]`
- Bitcoin source list examples:
  - `sources=["bitcoin_block_headers"]`
  - `sources=["bitcoin_block_fee_totals"]`
- Query filter clause example:
  - `{ "field":"price", "op":"gt", "value":1000 }`
- Export status artifact fields:
  - `uri`, `row_count`, `checksum_sha256`
- Operational reference:
  - `docs/deployment-reference.md`
