# S19 Bybit Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S19 (`Origo API v0.1.13`, `origo-control-plane v1.2.62`)

## Purpose and scope
- Operations runbook for Slice 19 Bybit event-ingest and serving guardrails.
- Scope covers exchange integrity, rights metadata behavior, reconciliation/quarantine, and raw-fidelity/precision enforcement.

## Inputs and outputs with contract shape
- Inputs:
  - canonical Bybit events (`source_id='bybit'`, `stream_id='bybit_spot_trades'`)
  - completeness checkpoints and quarantine state
  - projection outputs for native and aligned Bybit serving
- Outputs:
  - guardrail proof artifacts in `spec/slices/slice-19-bybit-event-sourcing-port/`
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
- Identity guardrail:
  - `trd_match_id` must be `m-<digits>` and maps to canonical source offset

## Source/provenance and freshness semantics
- Provenance anchor is immutable canonical `payload_raw` + `payload_sha256_raw`.
- Source completeness checks operate against deterministic `source_offset_or_equivalent` values parsed from `trd_match_id`.
- Quarantined partitions are blocked from cursor advancement until explicit recovery.

## Failure modes, warnings, and error codes
- Fail-loud guardrails:
  - no-miss gap detection raises runtime error and quarantines stream
  - quarantined stream blocks cursor advancement (`STREAM_QUARANTINED`)
  - precision guardrail rejects unsupported canonical numeric payload paths
  - missing rights mapping fails closed (`QUERY_RIGHTS_MISSING_STATE`)
  - non-canonical Bybit identity shape fails ingest (`m-<digits>` required)
- API warning behavior:
  - `strict=false`: warnings returned in payload
  - `strict=true`: warning presence fails with `STRICT_MODE_WARNING_FAILURE` (`409`)

## Determinism/replay notes
- Guardrail proof artifacts:
  - `proof-s19-g1-exchange-integrity.json`
  - `proof-s19-g2-api-guardrails.json`
  - `proof-s19-g3-reconciliation-quarantine.json`
  - `proof-s19-g4-raw-fidelity-precision.json`
- Reconciliation and precision wrappers delegate to deterministic core proof runners (`S19-P5`, `S19-P6`).

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
- Re-run Bybit exchange-integrity guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s19_g1_exchange_integrity_guardrail_proof`
- Re-run Bybit API guardrail proof:
  - `PYTHONPATH=.:control-plane uv run python api/origo_api/s19_g2_bybit_api_guardrails_proof.py`
- Re-run Bybit reconciliation/quarantine guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s19_g3_reconciliation_guardrail_proof`
- Re-run Bybit precision guardrail:
  - `PYTHONPATH=.:control-plane uv run python -m origo_control_plane.s19_g4_precision_guardrail_proof`
