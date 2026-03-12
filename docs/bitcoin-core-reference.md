# Bitcoin Core Dataset Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S13, S20, S29, S30, S31 (API v0.1.22)

## Purpose and scope
- User-facing reference for Bitcoin Core datasets available through raw query/export and historical Bitcoin routes.
- Scope includes stream datasets, derived datasets, aligned support, historical route coverage, contracts, and guardrail behavior.

## Inputs and outputs with contract shape
- Query:
  - endpoint: `POST /v1/raw/query`
  - `sources`: exactly one source key
  - `mode`: `native | aligned_1s`
- Export:
  - endpoint: `POST /v1/raw/export`
  - `dataset`: one dataset key
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
- Historical:
  - endpoints:
    - `POST /v1/historical/bitcoin/block_headers`
    - `POST /v1/historical/bitcoin/block_transactions`
    - `POST /v1/historical/bitcoin/mempool_state`
    - `POST /v1/historical/bitcoin/block_fee_totals`
    - `POST /v1/historical/bitcoin/block_subsidy_schedule`
    - `POST /v1/historical/bitcoin/network_hashrate_estimate`
    - `POST /v1/historical/bitcoin/circulating_supply`
  - request contract: `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Historical Python methods:
  - `HistoricalData.get_bitcoin_block_headers`
  - `HistoricalData.get_bitcoin_block_transactions`
  - `HistoricalData.get_bitcoin_mempool_state`
  - `HistoricalData.get_bitcoin_block_fee_totals`
  - `HistoricalData.get_bitcoin_block_subsidy_schedule`
  - `HistoricalData.get_bitcoin_network_hashrate_estimate`
  - `HistoricalData.get_bitcoin_circulating_supply`
- Bitcoin source keys:
  - `bitcoin_block_headers` (native + aligned_1s)
  - `bitcoin_block_transactions` (native + aligned_1s)
  - `bitcoin_mempool_state` (native + aligned_1s)
  - `bitcoin_block_fee_totals` (native + aligned_1s)
  - `bitcoin_block_subsidy_schedule` (native + aligned_1s)
  - `bitcoin_network_hashrate_estimate` (native + aligned_1s)
  - `bitcoin_circulating_supply` (native + aligned_1s)

## Data definitions (field names, types, units, timezone, nullability)
- Block headers:
  - `height`, `block_hash`, `prev_hash`, `merkle_root`, `version`, `nonce`, `difficulty`, `timestamp`, `datetime`, `source_chain`
- Block transactions:
  - `block_height`, `block_hash`, `block_timestamp`, `transaction_index`, `txid`, `inputs`, `outputs`, `values`, `scripts`, `witness_data`, `coinbase`, `datetime`, `source_chain`
- Mempool state:
  - `snapshot_at`, `snapshot_at_unix_ms`, `txid`, `fee_rate_sat_vb`, `vsize`, `first_seen_timestamp`, `rbf_flag`, `source_chain`
- Derived datasets:
  - fee totals: `fee_total_btc`
  - subsidy schedule: `halving_interval`, `subsidy_sats`, `subsidy_btc`
  - hashrate estimate: `difficulty`, `observed_interval_seconds`, `hashrate_hs`
  - circulating supply: `circulating_supply_sats`, `circulating_supply_btc`
- Aligned fields for derived datasets:
  - `aligned_at_utc`, `source_id`, `metric_name`, `metric_unit`, `metric_value_float`
  - `valid_from_utc`, `valid_to_utc_exclusive`, `records_in_bucket`, `latest_ingested_at_utc`
- Aligned fields for stream datasets:
  - `bitcoin_block_headers`:
    - `aligned_at_utc`, `records_in_bucket`, `latest_height`, `latest_block_hash`, `latest_prev_hash`, `latest_merkle_root`, `latest_version`, `latest_nonce`, `latest_difficulty`, `latest_timestamp_ms`, `latest_source_chain`, `latest_ingested_at_utc`, `first_source_offset_or_equivalent`, `last_source_offset_or_equivalent`, `bucket_sha256`
  - `bitcoin_block_transactions`:
    - `aligned_at_utc`, `records_in_bucket`, `tx_count`, `coinbase_tx_count`, `total_input_value_btc`, `total_output_value_btc`, `first_block_height`, `last_block_height`, `latest_block_timestamp_ms`, `latest_txid`, `latest_source_chain`, `latest_ingested_at_utc`, `first_source_offset_or_equivalent`, `last_source_offset_or_equivalent`, `bucket_sha256`
  - `bitcoin_mempool_state`:
    - `aligned_at_utc`, `records_in_bucket`, `tx_count`, `fee_rate_sat_vb_min`, `fee_rate_sat_vb_max`, `fee_rate_sat_vb_avg`, `total_vsize`, `first_seen_timestamp_min`, `first_seen_timestamp_max`, `rbf_true_count`, `latest_snapshot_at_unix_ms`, `latest_txid`, `latest_source_chain`, `latest_ingested_at_utc`, `first_source_offset_or_equivalent`, `last_source_offset_or_equivalent`, `bucket_sha256`

## Source/provenance and freshness semantics
- Source of truth is self-hosted unpruned Bitcoin Core RPC data.
- `source_chain` denotes node chain (e.g., `main`).
- Native and aligned serving are event-driven from canonical Bitcoin projection paths as of Slice 20.
- Aligned rows are derived from canonical events with deterministic 1-second bucket and forward-fill semantics.

## Failure modes, warnings, and error codes
- Rights/legal checks are fail-closed before query/export execution.
- Canonical aligned storage contract violations fail loudly (`409`):
  - missing `canonical_aligned_1s_aggregates`
  - missing required aligned columns
  - required aligned column type drift
- Standard warning/error taxonomy from raw query/export docs applies.

## Determinism/replay notes
- Slice-13 proofs and baseline fixture:
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p1-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p2-derived-native-aligned-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p3-determinism.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p4-parity.json`
  - `spec/slices/slice-13-bitcoin-core-signals/baseline-fixture-2024-04-20_2024-04-21.json`
- Slice-13 live-node proof artifacts (generated when Bitcoin Core node vars are configured):
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-live-node-p1-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-live-node-p2-derived-native-aligned-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-live-node-p3-determinism.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-live-node-p4-parity.json`
  - `spec/slices/slice-13-bitcoin-core-signals/baseline-fixture-live-node-<day-start>_<day-end>.json`
- Slice-20 event-sourcing proof artifacts:
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p1-acceptance.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p2-parity.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p3-determinism.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p4-exactly-once-ingest.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p5-no-miss-completeness.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-g1-bitcoin-integrity.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-g2-g5-api-guardrails.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-g3-reconciliation-quarantine.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-g4-raw-fidelity-precision.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/proof-s20-p7-live-node-gate.json`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/baseline-fixture-2024-04-20_2024-04-20.json`
- Slice-29 aligned completion artifacts:
  - `spec/slices/slice-29-bitcoin-stream-aligned-completion/baseline-fixture-2024-04-20_2024-04-20.json`
  - `spec/slices/slice-29-bitcoin-stream-aligned-completion/run-notes.md`

## Environment variables and required config
- API:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Bitcoin source:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`

## Minimal examples
- Native query (headers):
  - `{ "mode":"native", "sources":["bitcoin_block_headers"], "fields":["height","difficulty","timestamp"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Native query (transactions):
  - `{ "mode":"native", "sources":["bitcoin_block_transactions"], "fields":["block_height","transaction_index","txid","coinbase"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Aligned query (derived):
  - `{ "mode":"aligned_1s", "sources":["bitcoin_block_fee_totals"], "fields":["aligned_at_utc","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Aligned query (stream):
  - `{ "mode":"aligned_1s", "sources":["bitcoin_block_headers"], "fields":["aligned_at_utc","latest_height","latest_block_hash","records_in_bucket"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Native export (derived):
  - `{ "mode":"native", "format":"csv", "dataset":"bitcoin_circulating_supply", "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
