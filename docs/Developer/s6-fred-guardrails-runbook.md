# Slice 6 Developer: FRED Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S6 (`S6-G1`, `S6-G2`, `S6-G3`, `S6-G4`)
- Version reference: `control-plane v1.2.49`

## Purpose and scope
- Runbook for FRED serving guardrails across rights, freshness, serving promotion, and alert/audit emission.
- Scope is query-path guardrails for dataset `fred_series_metrics`.

## Inputs and outputs
- Inputs:
  - source rights matrix (`contracts/source-rights-matrix.json`)
  - legal artifact (`contracts/legal/fred-hosted-allowed.md`)
  - FRED publish freshness snapshot from persisted provenance
  - runtime serving-state env
- Outputs:
  - rights decisions or typed rights failures
  - FRED warning envelope entries
  - alert/audit side effects for FRED warnings

## Data definitions
- Dataset key: `fred_series_metrics`
- Serving states:
  - `shadow`
  - `promoted`
- FRED warning codes:
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- FRED guardrail audit event type:
  - `fred_query_warning`

## Source, provenance, and freshness semantics
- Rights classification is fail-closed and sourced from matrix entries for `fred`.
- Hosted Allowed serving requires legal artifact existence.
- Freshness warnings use source publish timestamp (`provenance_json.last_updated_utc`), not ingest timestamp.

## Failure modes, warnings, and error semantics
- Rights failures:
  - `QUERY_RIGHTS_MISSING_STATE`
  - `QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING`
  - `QUERY_SERVING_SHADOW_MODE`
- Warning-evaluation failures:
  - `QUERY_WARNING_RUNTIME_ERROR`
  - `QUERY_WARNING_BACKEND_ERROR`
  - `QUERY_WARNING_UNKNOWN_ERROR`
- Alert/audit failures:
  - `QUERY_ALERT_AUDIT_RUNTIME_ERROR`
  - `QUERY_ALERT_AUDIT_UNKNOWN_ERROR`
- Missing/invalid env vars fail loudly with explicit variable names.

## Determinism and replay notes
- Guardrail proofs:
  - `guardrails-proof-s6-g1-fred-rights.json`
  - `guardrails-proof-s6-g2-fred-freshness.json`
  - `guardrails-proof-s6-g3-fred-shadow-promote.json`
  - `guardrails-proof-s6-g4-fred-alerts-audit.json`
- Proofs validate deterministic policy outcomes for fixed inputs.

## Environment variables
- Rights + serving gate:
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_FRED_QUERY_SERVING_STATE`
- Freshness:
  - `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
- Alert + audit:
  - `ORIGO_FRED_ALERT_AUDIT_LOG_PATH`
  - `ORIGO_FRED_DISCORD_WEBHOOK_URL`
  - `ORIGO_FRED_DISCORD_TIMEOUT_SECONDS`

## Minimal examples
- Rights guardrail proof:
  - `uv run --with pydantic --with fastapi python -m api.origo_api.s6_g1_fred_rights_proof`
- Freshness guardrail proof:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with pydantic --with fastapi --with clickhouse-connect --with polars --with pyarrow python -m api.origo_api.s6_g2_fred_freshness_proof`
- Serving gate proof:
  - `uv run --with pydantic python -m api.origo_api.s6_g3_fred_shadow_promote_proof`
- Alert/audit proof:
  - `uv run --with pydantic python -m api.origo_api.s6_g4_fred_alerts_audit_proof`
