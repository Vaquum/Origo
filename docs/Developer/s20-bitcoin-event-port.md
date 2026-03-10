# S20 Bitcoin Event Port

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S20 (`Origo API v0.1.14`, `origo-control-plane v1.2.63`)

## Purpose and scope
- Developer reference for Slice 20 migration of all currently onboarded Bitcoin datasets to canonical event-driven ingest and serving.
- Scope covers canonical event writes, native/aligned projection paths, query/export serving cutover, and proof artifact mapping.

## Inputs and outputs with contract shape
- Ingest input:
  - validated Bitcoin rows from self-hosted Bitcoin Core RPC (headers, transactions, mempool, derived).
  - writer identity key: `(source_id, stream_id, partition_id, source_offset_or_equivalent)`.
- Ingest output:
  - canonical rows in `canonical_event_log` (`source_id='bitcoin_core'`, stream ids listed below).
- Native serving output:
  - `canonical_bitcoin_block_headers_native_v1`
  - `canonical_bitcoin_block_transactions_native_v1`
  - `canonical_bitcoin_mempool_state_native_v1`
  - `canonical_bitcoin_block_fee_totals_native_v1`
  - `canonical_bitcoin_block_subsidy_schedule_native_v1`
  - `canonical_bitcoin_network_hashrate_estimate_native_v1`
  - `canonical_bitcoin_circulating_supply_native_v1`
- Aligned serving output:
  - `canonical_aligned_1s_aggregates` for:
    - `bitcoin_block_fee_totals`
    - `bitcoin_block_subsidy_schedule`
    - `bitcoin_network_hashrate_estimate`
    - `bitcoin_circulating_supply`

## Data definitions (field names, types, units, timezone, nullability)
- Stream identity:
  - headers offset: `height`
  - transactions offset: `block_height:transaction_index:txid`
  - mempool offset: `snapshot_at_unix_ms:txid`
  - derived offsets: `block_height`
- Canonical provenance fields:
  - `payload_raw`
  - `payload_sha256_raw`
  - `payload_json` (precision-normalized)
- Precision rules are source/stream/path-scoped and enforced fail-loud by canonical writer:
  - source id: `bitcoin_core`
  - stream ids: all seven Bitcoin datasets

## Source/provenance and freshness semantics
- Source truth is self-hosted Bitcoin Core node RPC.
- Canonical source bytes are preserved in `payload_raw`; checksum continuity is anchored with `payload_sha256_raw`.
- Aligned buckets are projected from canonical events with deterministic source-offset ordering.

## Failure modes, warnings, and error codes
- Ingest fails loudly on:
  - duplicate canonical identities violating exactly-once semantics
  - no-miss reconciliation gaps (`STREAM_QUARANTINED` after quarantine trigger)
  - precision rule violations (missing rule, invalid int, decimal scale overflow)
  - integrity linkage/formula failures in projection path
- API behavior:
  - strict mode warning escalation: `STRICT_MODE_WARNING_FAILURE`
  - freshness warning code: `ALIGNED_FRESHNESS_STALE`
  - rights fail-closed: `QUERY_RIGHTS_MISSING_STATE`

## Determinism/replay notes
- Slice 20 proof artifacts:
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

## Environment variables and required config
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Bitcoin node:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`
- Event-runtime guardrails:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`

## Minimal examples
- Run `S20-P1..P3` proof batch:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python control-plane/origo_control_plane/s20_p1_p3_bitcoin_serving_proofs.py`
- Run no-miss proof:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python control-plane/origo_control_plane/s20_p5_no_miss_completeness_proof.py`
- Run API guardrail proof:
  - `PYTHONPATH=.:control-plane CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python api/origo_api/s20_g2_g5_bitcoin_api_guardrails_proof.py`
