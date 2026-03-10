# S21 Canonical Aligned Storage Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S21 (`Origo API v0.1.9`, `origo-control-plane v1.2.58`)

## Purpose and scope
- Defines the mandatory aligned-storage contract for Binance aligned serving.
- Scope is runtime enforcement for `canonical_aligned_1s_aggregates`, proof harness migration behavior, and fail-loud drift handling.

## Inputs and outputs with contract shape
- Input path:
  - `query_binance_aligned_1s_data` reads aligned rows from ClickHouse.
  - Runtime now validates storage contract before query execution.
- Output path:
  - Aligned query result rows when contract passes.
  - Explicit runtime error when table is missing or schema/type drift is detected.
- Guardrail proof path:
  - S15 API guardrail proof provisions proof DB via migration runner and then seeds aligned rows.

## Data definitions (field names, types, units, timezone, nullability)
- Mandatory table: `canonical_aligned_1s_aggregates`
- Required contract columns:
  - `view_id String`
  - `view_version UInt32`
  - `source_id LowCardinality(String)`
  - `stream_id LowCardinality(String)`
  - `partition_id String`
  - `aligned_at_utc DateTime64(3, 'UTC')`
  - `bucket_event_count UInt32`
  - `first_event_id UUID`
  - `last_event_id UUID`
  - `first_source_offset_or_equivalent String`
  - `last_source_offset_or_equivalent String`
  - `latest_source_event_time_utc Nullable(DateTime64(9, 'UTC'))`
  - `latest_ingested_at_utc DateTime64(3, 'UTC')`
  - `payload_rows_json String`
  - `bucket_sha256 FixedString(64)`
  - `projector_id String`
  - `projected_at_utc DateTime64(3, 'UTC')`

## Source/provenance and freshness semantics
- Binance aligned rows are computed from canonical event-derived aligned buckets only.
- No alternate aligned storage table is allowed in runtime planner paths.
- Freshness behavior remains warning-based (`ALIGNED_FRESHNESS_STALE`) with strict-mode escalation.

## Failure modes, warnings, and error codes
- Runtime contract check failures:
  - missing table -> explicit runtime error
  - missing required columns -> explicit runtime error
  - type drift on required columns -> explicit runtime error
- API-level consequence:
  - aligned query call fails loudly via backend error path when storage contract is broken.

## Determinism/replay notes
- Contract enforcement must not change deterministic replay fingerprints for fixed windows.
- Slice 21 proof evidence:
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-g2-g5-api-guardrails.json`
  - `spec/slices/slice-15-binance-event-sourcing-port/proof-s15-p3-determinism.json`

## Environment variables and required config
- ClickHouse runtime:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Event-runtime proofs:
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`

## Minimal examples
- Contract-gate tests:
  - `control-plane/.venv/bin/pytest tests/contract/test_binance_event_projection_query_contract.py`
- API guardrail proof with migration-backed aligned storage:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo control-plane/.venv/bin/python -m api.origo_api.s15_g2_g5_binance_query_guardrails_proof`
- Replay proof confirmation:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo ORIGO_STREAM_QUARANTINE_STATE_PATH=./storage/audit/stream-quarantine-state.json ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH=./storage/audit/canonical-runtime-events.jsonl ORIGO_AUDIT_LOG_RETENTION_DAYS=365 control-plane/.venv/bin/python -m control-plane.origo_control_plane.s15_p1_p3_binance_serving_proofs`
