# S17 FRED Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S17 (`Origo API v0.1.11`, `origo-control-plane v1.2.60`)

## Purpose and scope
- Operations runbook for Slice 17 FRED event-ingest and serving guardrails.
- Scope includes publish-freshness warnings, rights/legal behavior, reconciliation/quarantine, and precision/fidelity enforcement.

## Inputs and outputs with contract shape
- Inputs:
  - canonical FRED events (`source_id='fred'`, `stream_id='fred_series_metrics'`)
  - completeness checkpoints and quarantine state
  - projection outputs for native and aligned FRED serving
- Outputs:
  - guardrail proof artifacts in `spec/slices/slice-17-fred-event-sourcing-port/`
  - explicit API warnings/errors in `POST /v1/raw/query` and export status payloads
  - runtime audit log entries and quarantine state updates

## Data definitions (field names, types, units, timezone, nullability)
- Reconciliation checkpoint fields:
  - `expected_event_count`, `observed_event_count`, `gap_count`, `status`, `gap_details`
- Quarantine fields:
  - `source_id`, `stream_id`, `partition_id`, `reason`, `missing_offsets`
- FRED warning codes:
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- Rights metadata fields:
  - `rights_state`
  - `rights_provisional`

## Source/provenance and freshness semantics
- Provenance anchor is immutable canonical `payload_raw` + `payload_sha256_raw`.
- Publish-freshness warnings are computed from `provenance_json.last_updated_utc` in native projection rows.
- Missing source coverage and stale publish timestamps are explicit warning signals; strict mode escalates them.

## Failure modes, warnings, and error codes
- Fail-loud guardrails:
  - no-miss cadence gap detection raises runtime error and quarantines stream
  - quarantined stream blocks cursor advancement (`STREAM_QUARANTINED`)
  - precision guardrail rejects unexpected numeric payload paths
  - missing rights mapping fails closed (`QUERY_RIGHTS_MISSING_STATE`)
- API warning behavior:
  - `strict=false`: warnings returned in payload
  - `strict=true`: warning presence fails with `STRICT_MODE_WARNING_FAILURE` (`409`)

## Determinism/replay notes
- Guardrail proof artifacts:
  - `proof-s17-g1-g2-api-guardrails.json`
  - `proof-s17-g3-reconciliation-quarantine.json`
  - `proof-s17-g4-raw-fidelity-precision.json`
- Reconciliation and precision wrappers delegate to deterministic core proof runners (`S17-P5`, `S17-P6`).

## Environment variables and required config
- Event-runtime guardrail config:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- API guardrail config:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_FRED_QUERY_SERVING_STATE`
- Storage config:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Re-run FRED API guardrails:
  - `PYTHONPATH=.:control-plane uv run python api/origo_api/s17_g1_g2_fred_api_guardrails_proof.py`
- Re-run reconciliation/quarantine guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s17_g3_reconciliation_guardrail_proof`
- Re-run precision guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s17_g4_precision_guardrail_proof`
