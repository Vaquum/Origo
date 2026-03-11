# S19 Bybit Event Port

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S19 (`Origo API v0.1.13`, `origo-control-plane v1.2.62`)

## Purpose and scope
- Developer reference for Slice 19 migration of `bybit_spot_trades` to canonical event-driven ingest and serving.
- Scope covers canonical event writes, native/aligned projection paths, query/export cutover, and proof artifact mapping.

## Inputs and outputs with contract shape
- Ingest input:
  - validated Bybit spot-trade rows parsed from first-party daily csv.gz files
  - writer identity key: `(source_id, stream_id, partition_id, source_offset_or_equivalent)`
- Ingest output:
  - canonical rows in `canonical_event_log` (`source_id='bybit'`, `stream_id='bybit_spot_trades'`)
- Native serving output:
  - `canonical_bybit_spot_trades_native_v1`
- Aligned serving output:
  - `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`, `source_id='bybit'`, `stream_id='bybit_spot_trades'`)

## Data definitions (field names, types, units, timezone, nullability)
- Canonical event fields:
  - `payload_raw` bytes
  - `payload_sha256_raw` SHA256 hex
  - `payload_json` precision-canonical JSON
  - `source_event_time_utc` UTC timestamp (required for aligned projection)
- Bybit identity mapping:
  - `trd_match_id` must be `m-<digits>`
  - `trade_id` and canonical `source_offset_or_equivalent` are parsed from the numeric suffix
- Native Bybit projection fields:
  - `symbol`, `trade_id`, `trd_match_id`, `side`
  - `price`, `size`, `quote_quantity`
  - `timestamp`, `datetime`, `tick_direction`
  - `gross_value`, `home_notional`, `foreign_notional`
  - lineage: `event_id`, `source_offset_or_equivalent`, `source_event_time_utc`, `ingested_at_utc`
- Aligned Bybit serving fields:
  - `aligned_at_utc`
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source truth remains first-party Bybit daily spot-trade files.
- Source checksums (`zip_sha256`, `csv_sha256`) are preserved in proof artifacts.
- Canonical source bytes are preserved in `payload_raw` and verified by `payload_sha256_raw`.
- Aligned rows are reconstructed from canonical aligned buckets keyed by source event time.

## Failure modes, warnings, and error codes
- Canonical ingest fails loudly on:
  - duplicate identity conflicts violating exactly-once semantics
  - completeness gaps detected by reconciliation/quarantine logic
  - precision mapping violations in canonical payload handling
  - invalid `trd_match_id` identity format (`m-<digits>` required)
- Query/API behavior:
  - rights gate is fail-closed
  - provisional rights metadata is emitted (`rights_state`, `rights_provisional`)
  - `strict=true` escalates warnings to `409`
- Aligned contract behavior:
  - missing/drifted `canonical_aligned_1s_aggregates` contract fails loudly

## Determinism/replay notes
- Slice 19 proof artifacts:
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p1-acceptance.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p2-parity.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p3-determinism.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p4-exactly-once-ingest.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p5-no-miss-completeness.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g1-exchange-integrity.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g2-api-guardrails.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g3-reconciliation-quarantine.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g4-raw-fidelity-precision.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/baseline-fixture-2024-01-04_2024-01-04.json`

## Environment variables and required config
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Event-runtime guardrails:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- API guardrails:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Run capability/proof batch (`S19-P1..P3`):
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s19_p1_p3_bybit_serving_proofs.py`
- Run exactly-once proof:
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s19_p4_exactly_once_ingest_proof.py`
- Run no-miss and precision proofs:
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s19_p5_no_miss_completeness_proof.py`
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s19_p6_raw_fidelity_precision_proof.py`
- Run API guardrail proof:
  - `PYTHONPATH=.:control-plane uv run python api/origo_api/s19_g2_bybit_api_guardrails_proof.py`
