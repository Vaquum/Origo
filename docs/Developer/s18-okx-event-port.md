# S18 OKX Event Port

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S18 (`Origo API v0.1.12`, `origo-control-plane v1.2.61`)

## Purpose and scope
- Developer reference for Slice 18 migration of `okx_spot_trades` to canonical event-driven ingest and serving.
- Scope covers canonical event writes, native/aligned projection paths, query/export cutover, and proof artifact mapping.

## Inputs and outputs with contract shape
- Ingest input:
  - validated OKX spot trade rows parsed from first-party source files
  - writer identity key: `(source_id, stream_id, partition_id, source_offset_or_equivalent)`
- Ingest output:
  - canonical rows in `canonical_event_log` (`source_id='okx'`, `stream_id='okx_spot_trades'`)
- Native serving output:
  - `canonical_okx_spot_trades_native_v1`
- Aligned serving output:
  - `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`, `source_id='okx'`, `stream_id='okx_spot_trades'`)

## Data definitions (field names, types, units, timezone, nullability)
- Canonical event fields:
  - `payload_raw` bytes
  - `payload_sha256_raw` SHA256 hex
  - `payload_json` precision-canonical JSON
  - `source_event_time_utc` UTC timestamp (required for aligned projection)
- Native OKX projection fields:
  - `instrument_name`, `trade_id`, `side`
  - `price`, `size`, `quote_quantity`
  - `timestamp`, `datetime`
  - lineage: `event_id`, `source_offset_or_equivalent`, `source_event_time_utc`, `ingested_at_utc`
- Aligned OKX serving fields:
  - `aligned_at_utc`
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source truth remains first-party OKX daily trade files resolved from OKX API.
- Source checksums (`zip_sha256`, `csv_sha256`) are preserved in proof artifacts.
- Canonical source bytes are preserved in `payload_raw` and verified by `payload_sha256_raw`.
- Aligned rows are reconstructed from canonical aligned buckets keyed by source event time.

## Failure modes, warnings, and error codes
- Canonical ingest fails loudly on:
  - duplicate identity conflicts violating exactly-once semantics
  - completeness gaps detected by reconciliation/quarantine logic
  - precision mapping violations in canonical payload handling
- Query/API behavior:
  - rights gate is fail-closed
  - provisional rights metadata is emitted (`rights_state`, `rights_provisional`)
  - `strict=true` escalates warnings to `409`
- Aligned contract behavior:
  - missing/drifted `canonical_aligned_1s_aggregates` contract fails loudly

## Determinism/replay notes
- Slice 18 proof artifacts:
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p1-acceptance.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p2-parity.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p3-determinism.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p4-exactly-once-ingest.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p5-no-miss-completeness.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-g1-exchange-integrity.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-g2-api-guardrails.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-g3-reconciliation-quarantine.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-g4-raw-fidelity-precision.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/baseline-fixture-2024-01-04_2024-01-04.json`

## Environment variables and required config
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Event runtime guardrails:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- API guardrails:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Run capability/proof batch (`S18-P1..P3`):
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s18_p1_p3_okx_serving_proofs.py`
- Run exactly-once proof:
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s18_p4_exactly_once_ingest_proof.py`
- Run no-miss and precision proofs:
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s18_p5_no_miss_completeness_proof.py`
  - `PYTHONPATH=.:control-plane uv run python control-plane/origo_control_plane/s18_p6_raw_fidelity_precision_proof.py`
- Run API guardrail proof:
  - `PYTHONPATH=.:control-plane uv run python api/origo_api/s18_g2_okx_api_guardrails_proof.py`
