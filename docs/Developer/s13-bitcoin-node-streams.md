# Slice 13 Developer: Bitcoin Core Node Streams

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice reference: S13 (`S13-C1`, `S13-C2`, `S13-C3`, `S13-C4`, `S13-P1`)
- Version reference: `Origo API v0.1.5`, `origo-control-plane v1.2.55`

## Purpose and scope
- Defines the deterministic ingest contracts for Bitcoin Core node stream datasets:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
- Scope includes env contract, RPC contract, normalization, integrity checks, and ClickHouse write behavior.

## Inputs and outputs with contract shape
- Inputs:
  - Bitcoin Core JSON-RPC responses from one unpruned self-hosted node.
  - Config from env:
    - `ORIGO_BITCOIN_CORE_RPC_URL`
    - `ORIGO_BITCOIN_CORE_RPC_USER`
    - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
    - `ORIGO_BITCOIN_CORE_NETWORK`
    - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
    - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
    - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`
  - ClickHouse env (`CLICKHOUSE_*`).
- Outputs:
  - ClickHouse writes to migration-backed tables:
    - `bitcoin_block_headers`
    - `bitcoin_block_transactions`
    - `bitcoin_mempool_state`
  - Asset metadata payload includes row counts, checksums, node contract info, and integrity reports.

## Data definitions (field names, types, units, timezone, nullability)
- `bitcoin_block_headers`:
  - `height` UInt32
  - `block_hash` String
  - `prev_hash` String
  - `merkle_root` String
  - `version` Int32
  - `nonce` UInt32
  - `difficulty` Float64
  - `timestamp` UInt64 (epoch ms)
  - `datetime` DateTime64(3, UTC)
  - `source_chain` LowCardinality(String)
- `bitcoin_block_transactions`:
  - key fields: `block_height`, `block_hash`, `transaction_index`, `txid`
  - normalized payload fields: `inputs`, `outputs`, `values`, `scripts`, `witness_data` (canonical JSON strings)
  - `coinbase` UInt8 (`1|0`)
  - `datetime` DateTime64(3, UTC)
- `bitcoin_mempool_state`:
  - `snapshot_at` DateTime64(3, UTC)
  - `snapshot_at_unix_ms` UInt64
  - `txid` String
  - `fee_rate_sat_vb` Float64
  - `vsize` UInt64
  - `first_seen_timestamp` UInt64
  - `rbf_flag` UInt8 (`1|0`)
  - `source_chain` LowCardinality(String)

## Source/provenance and freshness semantics
- Source of truth is direct Bitcoin Core RPC (`getblockchaininfo`, `getblockhash`, `getblockheader`, `getblock`, `getrawmempool`).
- Node contract is fail-loud:
  - chain must match `ORIGO_BITCOIN_CORE_NETWORK`
  - node must be unpruned
  - node must not be in initial block download mode
  - tip height must cover configured ingest range
- Mempool rows are point-in-time snapshots keyed by `snapshot_at_unix_ms`.

## Failure modes, warnings, and error codes
- Missing/empty env values raise `RuntimeError` at source initialization.
- RPC non-2xx, malformed JSON, missing fields, or contract mismatches raise `RuntimeError`.
- Integrity violations raise `ValueError` from `origo_control_plane.utils.bitcoin_integrity`:
  - schema/type violations
  - block linkage violations
  - transaction index/linkage violations
  - mempool snapshot consistency violations
- Insert verification mismatch raises `RuntimeError`.

## Determinism/replay notes
- Normalized payloads are canonicalized with sorted-key JSON and stable ordering.
- Block ranges use deterministic height iteration.
- Proof artifacts:
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p1-acceptance.json`
  - `spec/slices/slice-13-bitcoin-core-signals/proof-s13-p3-determinism.json`
  - `spec/slices/slice-13-bitcoin-core-signals/baseline-fixture-2024-04-20_2024-04-21.json`

## Environment variables and required config
- Required Bitcoin Core vars:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`
- Required ClickHouse vars:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Run block-header ingest job:
  - Dagster job name: `insert_bitcoin_block_headers_to_origo_job`
- Run block-transaction ingest job:
  - Dagster job name: `insert_bitcoin_block_transactions_to_origo_job`
- Run mempool snapshot ingest job:
  - Dagster job name: `insert_bitcoin_mempool_state_to_origo_job`
