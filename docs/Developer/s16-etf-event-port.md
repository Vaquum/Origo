# S16 ETF Event Port

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S16 (`Origo API v0.1.10`, `origo-control-plane v1.2.59`)

## Purpose and scope
- Developer reference for Slice 16 migration of `etf_daily_metrics` to canonical event-driven ingest and serving.
- Scope covers canonical event writes, native/aligned projection paths, and query/export cutover behavior.

## Inputs and outputs with contract shape
- Ingest input:
  - normalized ETF records from scraper pipeline (`NormalizedMetricRecord`)
  - writer identity key: `(source_id, stream_id, partition_id, source_offset_or_equivalent)`
- Ingest output:
  - canonical rows in `canonical_event_log` (`source_id='etf'`, `stream_id='etf_daily_metrics'`)
- Native serving output:
  - `canonical_etf_daily_metrics_native_v1`
- Aligned serving output:
  - `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`, `source_id='etf'`, `stream_id='etf_daily_metrics'`)

## Data definitions (field names, types, units, timezone, nullability)
- Canonical event fields:
  - `payload_raw` bytes
  - `payload_sha256_raw` SHA256 hex
  - `payload_json` precision-canonical JSON
  - `source_event_time_utc` UTC timestamp (required for aligned projection)
- Native ETF projection fields:
  - `metric_id`, `source_id`, `metric_name`, `metric_unit`
  - `metric_value_string`, `metric_value_int`, `metric_value_float`, `metric_value_bool`
  - `observed_at_utc`, `dimensions_json`, `provenance_json`, `ingested_at_utc`
  - lineage: `event_id`, `source_offset_or_equivalent`, `source_event_time_utc`
- Aligned ETF serving fields:
  - `aligned_at_utc`
  - `source_id`, `metric_name`, `metric_unit`
  - `metric_value_*`
  - `dimensions_json`, `provenance_json`
  - `latest_ingested_at_utc`, `records_in_bucket`
  - forward-fill interval fields: `valid_from_utc`, `valid_to_utc_exclusive`

## Source/provenance and freshness semantics
- Source truth remains issuer-first data normalized by the ETF scraper path.
- Canonical source bytes are preserved in `payload_raw` and verified by `payload_sha256_raw`.
- ETF quality warnings are projection-driven and source-time driven:
  - stale records
  - missing day-level records
  - incomplete day-level records

## Failure modes, warnings, and error codes
- Canonical ingest fails loudly on:
  - duplicate identity conflicts that violate exactly-once semantics
  - payload precision violations (unexpected numeric fields, invalid decimal paths)
  - no-miss cadence gaps (with stream quarantine)
- Query/API behavior:
  - rights gate is fail-closed
  - provisional rights metadata is always emitted when applicable
  - `strict=true` escalates ETF warnings to `409`

## Determinism/replay notes
- Slice 16 proof artifacts:
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p1-acceptance.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p2-parity.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p3-determinism.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p4-exactly-once-ingest.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p5-no-miss-completeness.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-g1-g2-api-guardrails.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-g3-reconciliation-quarantine.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/proof-s16-g4-raw-fidelity-precision.json`
  - `spec/slices/slice-16-etf-event-sourcing-port/baseline-fixture-2026-03-08_2026-03-09.json`

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
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Run capability/proof batch (`S16-P1..P3`):
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_p1_p3_etf_serving_proofs`
- Run exactly-once proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_p4_exactly_once_ingest_proof`
- Run no-miss and precision proofs:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_p5_no_miss_completeness_proof`
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_p6_raw_fidelity_precision_proof`
- Run API guardrail proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m api.origo_api.s16_g1_g2_etf_api_guardrails_proof`
