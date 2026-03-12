# S15 Binance Event Port

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S15 (`Origo API v0.1.8`, `origo-control-plane v1.2.58`)

## Purpose and scope
- Developer reference for Slice 15 migration of Binance `binance_spot_trades` to canonical event-driven serving.
- Scope covers ingest writes to `canonical_event_log`, native/aligned projection paths, and query-table cutover.

## Inputs and outputs with contract shape
- Ingest inputs:
  - Binance CSV-derived trade rows parsed by `binance_canonical_event_ingest` utilities.
  - Writer identity key: `(source_id, stream_id, partition_id, source_offset_or_equivalent)`.
- Ingest output:
  - Canonical rows in `canonical_event_log` with deterministic `event_id`, `payload_raw`, `payload_sha256_raw`, and canonicalized `payload_json`.
- Native projection output tables:
  - `canonical_binance_spot_trades_native_v1`
  - `canonical_binance_spot_agg_trades_native_v1`
  - `canonical_binance_futures_trades_native_v1`
- Aligned projection output table:
  - `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)

## Data definitions (field names, types, units, timezone, nullability)
- Canonical event fields used by this slice:
  - `source_id` string (`binance`)
  - `stream_id` enum-like string (`binance_spot_trades`)
  - `partition_id` UTC date (`YYYY-MM-DD`)
  - `source_offset_or_equivalent` string source sequence id
  - `source_event_time_utc` nullable UTC timestamp
  - `payload_raw` raw bytes from source payload JSON
  - `payload_sha256_raw` raw payload SHA256
  - `payload_json` canonical precision-normalized JSON string
- Native projection fields preserve Binance contract columns plus event lineage:
  - source trade identifiers, price/quantity/quote quantities, boolean side flags, UTC datetime
  - projection lineage: `event_id`, `source_offset_or_equivalent`, `source_event_time_utc`, `ingested_at_utc`
- Aligned projection fields are bucketed per second in UTC and replay-deterministic:
  - `aligned_at_utc`, OHLC, `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source truth is Binance first-party daily/monthly files; ingest never uses third-party APIs.
- Raw payload bytes are preserved exactly in canonical log and verified via SHA256.
- Query freshness for aligned mode is warning-based (`ALIGNED_FRESHNESS_STALE`) and source-driven.

## Failure modes, warnings, and error codes
- Ingest/projection fail loud on contract violations:
  - non-canonical payload typing
  - duplicate identity hash conflict
  - projector checkpoint/state conflicts
- Integrity guardrail is enforced in both ingest and native projection paths:
  - sequence gaps, schema mismatch, non-positive numeric anomalies fail with explicit errors.
- Query/API guardrails for Binance aligned mode:
  - stale freshness emits warning; `strict=true` escalates to `STRICT_MODE_WARNING_FAILURE`.

## Determinism/replay notes
- Slice 15 capability/proof artifacts:
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p1-acceptance.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p2-parity.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p3-determinism.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p4-exactly-once-ingest.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p5-no-miss-completeness.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p6-raw-fidelity-precision.json`
- Guardrail artifacts:
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-g1-exchange-integrity.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-g2-g5-api-guardrails.json`

## Environment variables and required config
- ClickHouse runtime:
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
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Run Binance capability proofs:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p1_p3_binance_serving_proofs`
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p4_exactly_once_ingest_proof`
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p5_no_miss_completeness_proof`
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p6_raw_fidelity_precision_proof`
- Run guardrail proofs:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_g1_exchange_integrity_guardrail_proof`
  - `PYTHONPATH=.:control-plane uv run --with httpx --with fastapi --with pydantic --with clickhouse-connect --with clickhouse-driver --with polars --with pyarrow python -m api.origo_api.s15_g2_g5_binance_query_guardrails_proof`
