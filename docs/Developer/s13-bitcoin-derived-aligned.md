# Slice 13 Developer: Bitcoin Derived + Aligned Contracts

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice reference: S13 (`S13-C5`, `S13-C6`, `S13-C7`, `S13-P2`, `S13-P4`)
- Version reference: `Origo API v0.1.5`, `origo-control-plane v1.2.55`

## Purpose and scope
- Defines deterministic derived metric pipelines and aligned query/export integration for:
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`
- Scope includes formulas, aligned bucket semantics, and native/aligned query planner integration.

## Inputs and outputs with contract shape
- Inputs:
  - Canonical block/header payloads from Bitcoin Core RPC.
- Outputs:
  - Native tables in ClickHouse (one table per derived dataset).
  - `mode=native` query/export support through `origo/query/bitcoin_native.py`.
  - `mode=aligned_1s` query/export support through:
    - `origo/query/bitcoin_derived_aligned_1s.py`
    - `origo/query/aligned_core.py`
    - API/Export dataset contract updates.

## Data definitions (field names, types, units, timezone, nullability)
- `bitcoin_block_fee_totals`:
  - `fee_total_btc` Float64 (BTC)
- `bitcoin_block_subsidy_schedule`:
  - `halving_interval` UInt32
  - `subsidy_sats` UInt64
  - `subsidy_btc` Float64 (BTC)
- `bitcoin_network_hashrate_estimate`:
  - `difficulty` Float64
  - `observed_interval_seconds` UInt64
  - `hashrate_hs` Float64 (H/s)
- `bitcoin_circulating_supply`:
  - `circulating_supply_sats` UInt64
  - `circulating_supply_btc` Float64 (BTC)
- Aligned fields (derived datasets):
  - `aligned_at_utc`, `source_id`, `metric_name`, `metric_unit`
  - `metric_value_float` (dataset-specific value channel)
  - `dimensions_json`, `provenance_json`
  - `latest_ingested_at_utc`, `records_in_bucket`
  - forward-fill intervals: `valid_from_utc`, `valid_to_utc_exclusive`

## Source/provenance and freshness semantics
- `source_id` is fixed to `bitcoin_core`.
- `provenance_json` includes source and dataset names.
- Aligned forward-fill semantics:
  - month/time-range windows use forward-fill intervals with explicit validity bounds.
  - latest/random windows use observation rows only.

## Failure modes, warnings, and error codes
- Formula or contract mismatches fail loudly during normalization and integrity checks.
- `aligned_1s` query/export mode rejects unsupported datasets with contract errors.
- Native/aligned planner rejects unsupported projection fields.

## Determinism/replay notes
- Deterministic ordering:
  - native rows ordered by block height / transaction index contracts.
  - aligned rows ordered by `aligned_at_utc`, `source_id`, `metric_name`.
- Determinism artifacts:
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p2-derived-native-aligned-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p3-determinism.json`
  - `tests/replay/test_native_query_replay.py` (SQL determinism checks)

## Environment variables and required config
- Query/export and rights gate:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_EXPORT_AUDIT_LOG_PATH`
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query (`bitcoin_block_fee_totals`):
  - `{ "mode":"native", "sources":["bitcoin_block_fee_totals"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Aligned query (`bitcoin_network_hashrate_estimate`):
  - `{ "mode":"aligned_1s", "sources":["bitcoin_network_hashrate_estimate"], "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "fields":["aligned_at_utc","metric_name","metric_value_float","valid_from_utc","valid_to_utc_exclusive"], "strict":false }`
- Native export (`bitcoin_circulating_supply`):
  - `{ "mode":"native", "format":"parquet", "dataset":"bitcoin_circulating_supply", "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
