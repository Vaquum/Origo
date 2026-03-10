# S16 ETF Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S16 (`Origo API v0.1.10`, `origo-control-plane v1.2.59`)

## Purpose and scope
- Operations runbook for Slice 16 ETF event-ingest and serving guardrails.
- Scope includes rights/legal behavior, quality warnings, reconciliation/quarantine, and precision/fidelity enforcement.

## Inputs and outputs with contract shape
- Inputs:
  - canonical ETF events (`source_id='etf'`, `stream_id='etf_daily_metrics'`)
  - completeness checkpoints and quarantine state
  - projection outputs for native and aligned ETF serving
- Outputs:
  - guardrail proof artifacts in `spec/slices/slice-16-etf-event-sourcing-port/`
  - explicit API warnings/errors in `POST /v1/raw/query` and export status payloads
  - runtime audit log entries and quarantine state updates

## Data definitions (field names, types, units, timezone, nullability)
- Reconciliation checkpoint fields:
  - `expected_event_count`, `observed_event_count`, `gap_count`, `status`, `gap_details`
- Quarantine fields:
  - `source_id`, `stream_id`, `partition_id`, `reason`, `missing_offsets`
- ETF quality warning codes:
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
- Rights metadata fields:
  - `rights_state`
  - `rights_provisional`

## Source/provenance and freshness semantics
- Provenance anchor is immutable canonical `payload_raw` + `payload_sha256_raw`.
- Daily ETF freshness/quality warnings are computed from projection outputs over requested windows.
- Missing day coverage is treated as explicit data-quality signal, not silent fill.

## Failure modes, warnings, and error codes
- Fail-loud guardrails:
  - no-miss cadence gap detection raises runtime error and quarantines stream
  - quarantined stream blocks cursor advancement (`STREAM_QUARANTINED`)
  - precision guardrail rejects unexpected numeric payload paths
  - missing rights mapping fails closed
- API warning behavior:
  - `strict=false`: warnings returned in payload
  - `strict=true`: warning presence fails with `STRICT_MODE_WARNING_FAILURE` (`409`)

## Determinism/replay notes
- Guardrail proof artifacts:
  - `proof-s16-g1-g2-api-guardrails.json`
  - `proof-s16-g3-reconciliation-quarantine.json`
  - `proof-s16-g4-raw-fidelity-precision.json`
- Reconciliation and precision wrappers delegate to deterministic core proof runners (`S16-P5`, `S16-P6`).

## Environment variables and required config
- Event-runtime guardrail config:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- API guardrail config:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Storage config:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Re-run ETF API guardrails:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m api.origo_api.s16_g1_g2_etf_api_guardrails_proof`
- Re-run reconciliation/quarantine guardrail:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_g3_reconciliation_guardrail_proof`
- Re-run precision guardrail:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s16_g4_precision_guardrail_proof`
