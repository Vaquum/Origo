# S20 Bitcoin Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S20 (`Origo API v0.1.14`, `origo-control-plane v1.2.63`)

## Purpose and scope
- Operations runbook for Slice 20 Bitcoin event-ingest and serving guardrails.
- Scope covers integrity checks, freshness warnings, reconciliation/quarantine behavior, precision guardrails, and rights metadata behavior.

## Inputs and outputs with contract shape
- Inputs:
  - canonical Bitcoin events (`source_id='bitcoin_core'`)
  - completeness checkpoints and stream quarantine state
  - projection outputs for Bitcoin native and derived aligned serving
  - API query requests for Bitcoin datasets
- Outputs:
  - guardrail proof artifacts in `spec/slices/slice-20-bitcoin-event-sourcing-port/`
  - explicit API warnings/errors from `POST /v1/raw/query`
  - quarantine state updates and runtime audit log entries

## Data definitions (field names, types, units, timezone, nullability)
- Reconciliation checkpoint fields:
  - `expected_event_count`, `observed_event_count`, `gap_count`, `status`, `gap_details`
- Quarantine fields:
  - `source_id`, `stream_id`, `partition_id`, `reason`, `missing_offsets`
- Rights metadata fields:
  - `rights_state`
  - `rights_provisional`
- Freshness warning fields:
  - warning code `ALIGNED_FRESHNESS_STALE`
  - strict escalation behavior with `STRICT_MODE_WARNING_FAILURE`

## Source/provenance and freshness semantics
- Provenance anchor is immutable canonical `payload_raw` + `payload_sha256_raw`.
- Completeness checks run against deterministic Bitcoin stream offsets.
- Freshness behavior is derived from canonical event timing fields and surfaced at API boundary.

## Failure modes, warnings, and error codes
- Fail-loud guardrails:
  - integrity linkage/formula mismatch raises runtime error in projection path
  - no-miss gap detection raises runtime error and quarantines stream
  - quarantined streams block cursor advancement (`STREAM_QUARANTINED`)
  - precision guardrail rejects unsupported numeric paths and scale/int violations
  - missing rights mapping fails closed (`QUERY_RIGHTS_MISSING_STATE`)
- API warning behavior:
  - `strict=false`: warnings returned in payload
  - `strict=true`: warning presence returns `409` (`STRICT_MODE_WARNING_FAILURE`)

## Determinism/replay notes
- Guardrail artifacts:
  - `proof-s20-g1-bitcoin-integrity.json`
  - `proof-s20-g2-g5-api-guardrails.json`
  - `proof-s20-g3-reconciliation-quarantine.json`
  - `proof-s20-g4-raw-fidelity-precision.json`
- Guardrail wrappers for reconciliation and precision delegate to deterministic core proof paths (`S20-P5`, `S20-P6`).

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
- Re-run Bitcoin integrity guardrail proof:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python control-plane/origo_control_plane/s20_g1_bitcoin_integrity_guardrail_proof.py`
- Re-run Bitcoin API guardrail proof:
  - `PYTHONPATH=.:control-plane CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python api/origo_api/s20_g2_g5_bitcoin_api_guardrails_proof.py`
- Re-run Bitcoin reconciliation/quarantine guardrail:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python control-plane/origo_control_plane/s20_g3_reconciliation_guardrail_proof.py`
