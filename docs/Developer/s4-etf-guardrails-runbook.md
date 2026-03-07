# Slice 4 Developer: ETF Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-06
- Slice reference: S4 (`S4-G1` to `S4-G7`)

## Purpose and scope
- Runbook for ETF guardrail behavior in API and control-plane runtime.
- Covers legal/rights gates, schedule/retry, serving promotion gate, quality warnings, and anomaly alerting.

## Inputs and outputs
- API rights + serving gate:
  - `api.origo_api.rights.resolve_query_rights`
- ETF quality warnings:
  - `api.origo_api.etf_warnings.build_etf_daily_quality_warnings`
- Dagster runtime guardrails:
  - schedule: `origo_etf_daily_ingest_schedule`
  - retry sensor: `origo_etf_daily_retry_sensor`
  - anomaly sensor: `origo_etf_ingest_anomaly_alert_sensor`
- Outputs:
  - typed API errors/warnings
  - retry RunRequest decisions
  - anomaly JSONL log events
  - Discord webhook alerts

## Data definitions
- Rights states:
  - `Hosted Allowed`
  - `BYOK Required`
  - `Ingest Only`
- Serving promotion state for ETF query:
  - `shadow`
  - `promoted`
- ETF quality warning codes:
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`

## Source, provenance, freshness
- Rights and legal-signoff decisions load from `contracts/source-rights-matrix.json`.
- ETF freshness warnings evaluate latest observed ETF day in canonical table.
- Alert payloads log run-level anomaly context and timestamp.

## Failure modes and error semantics
- Rights/legal gates:
  - `QUERY_RIGHTS_*`
  - `EXPORT_RIGHTS_*`
- Serving promotion gate:
  - `QUERY_SERVING_SHADOW_MODE`
- Warning-evaluation failures:
  - `QUERY_WARNING_RUNTIME_ERROR`
  - `QUERY_WARNING_BACKEND_ERROR`
  - `QUERY_WARNING_UNKNOWN_ERROR`
- Retry/env contract failures and webhook non-2xx responses fail loudly.

## Determinism and replay notes
- Guardrail proofs are stored under `spec/slices/slice-4-etf-use-case/guardrails-proof-*.json`.
- Retry lineage tags preserve origin/parent/attempt determinism.
- Anomaly logs are append-only JSONL.

## Environment and required config
- Rights/legal:
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Serving gate + warnings:
  - `ORIGO_ETF_QUERY_SERVING_STATE`
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- Schedule + retry:
  - `ORIGO_ETF_DAILY_SCHEDULE_CRON`
  - `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`
  - `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS`
  - `ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS`
- Anomaly alerting:
  - `ORIGO_ETF_ANOMALY_LOG_PATH`
  - `ORIGO_ETF_DISCORD_WEBHOOK_URL`
  - `ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS`
  - `ORIGO_ETF_DISCORD_TIMEOUT_SECONDS`

## Minimal example
- Verify schedule/retry/alert proofs:
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_schedule_proof`
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_retry_proof`
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_alerts_proof`
- Verify API guardrail proofs:
  - `uv run --with pydantic python -m api.origo_api.s4_g5_shadow_promote_proof`
  - `uv run --with pydantic --with clickhouse-connect --with polars python -m api.origo_api.s4_g6_etf_warning_proof`
