# S18 OKX Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S18 (`Origo API v0.1.12`, `origo-control-plane v1.2.61`)

## Purpose and scope
- Operations runbook for Slice 18 OKX event-ingest and serving guardrails.
- Scope covers exchange integrity, rights metadata behavior, reconciliation/quarantine, and raw-fidelity/precision enforcement.

## Inputs and outputs with contract shape
- Inputs:
  - canonical OKX events (`source_id='okx'`, `stream_id='okx_spot_trades'`)
  - completeness checkpoints and quarantine state
  - projection outputs for native and aligned OKX serving
- Outputs:
  - guardrail proof artifacts in `spec/slices/slice-18-okx-event-sourcing-port/`
  - explicit API warnings/errors in `POST /v1/raw/query` and export status payloads
  - runtime audit log entries and quarantine state updates

## Data definitions (field names, types, units, timezone, nullability)
- Reconciliation checkpoint fields:
  - `expected_event_count`, `observed_event_count`, `gap_count`, `status`, `gap_details`
- Quarantine fields:
  - `source_id`, `stream_id`, `partition_id`, `reason`, `missing_offsets`
- Rights metadata fields:
  - `rights_state`
  - `rights_provisional`
- Integrity suite guardrails:
  - schema/type conformance
  - sequence gap detection
  - anomaly checks

## Source/provenance and freshness semantics
- Provenance anchor is immutable canonical `payload_raw` + `payload_sha256_raw`.
- Source completeness checks operate against deterministic source offsets and partition checkpoints.
- Quarantined partitions are blocked from cursor advancement until explicit recovery.

## Failure modes, warnings, and error codes
- Fail-loud guardrails:
  - no-miss gap detection raises runtime error and quarantines stream
  - quarantined stream blocks cursor advancement (`STREAM_QUARANTINED`)
  - precision guardrail rejects unsupported canonical numeric payload paths
  - missing rights mapping fails closed (`QUERY_RIGHTS_MISSING_STATE`)
- API warning behavior:
  - `strict=false`: warnings returned in payload
  - `strict=true`: warning presence fails with `STRICT_MODE_WARNING_FAILURE` (`409`)

## Determinism/replay notes
- Guardrail proof artifacts:
  - `proof-s18-g1-exchange-integrity.json`
  - `proof-s18-g2-api-guardrails.json`
  - `proof-s18-g3-reconciliation-quarantine.json`
  - `proof-s18-g4-raw-fidelity-precision.json`
- Reconciliation and precision wrappers delegate to deterministic core proof runners (`S18-P5`, `S18-P6`).

## Environment variables and required config
- Event-runtime guardrail config:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- API guardrail config:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Storage config:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Re-run OKX exchange-integrity guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s18_g1_exchange_integrity_guardrail_proof`
- Re-run OKX API guardrail proof:
  - `PYTHONPATH=.:control-plane uv run python api/origo_api/s18_g2_okx_api_guardrails_proof.py`
- Re-run OKX reconciliation/quarantine guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s18_g3_reconciliation_guardrail_proof`
- Re-run OKX precision guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s18_g4_precision_guardrail_proof`
