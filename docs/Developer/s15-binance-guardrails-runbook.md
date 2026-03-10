# S15 Binance Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S15 (`Origo API v0.1.8`, `origo-control-plane v1.2.58`)

## Purpose and scope
- Operations runbook for diagnosing and recovering Slice 15 Binance event-ingest and serving guardrails.
- Scope includes integrity failures, no-miss quarantine events, precision/fidelity faults, and aligned freshness behavior.

## Inputs and outputs with contract shape
- Inputs:
  - Canonical events (`source_id=binance`, Binance stream ids)
  - Projector checkpoints/watermarks
  - Completeness checkpoints and quarantine state
- Outputs:
  - Guardrail proof JSON artifacts under `spec/slices/slice-15-binance-event-sourcing-port/`
  - Runtime audit and quarantine state files
  - API warning/error envelopes for aligned query strict mode

## Data definitions (field names, types, units, timezone, nullability)
- Integrity contract (native tuples):
  - contiguous monotonic trade id
  - positive `price`, `quantity`, `quote_quantity`
  - UTC `datetime` and positive `timestamp`
- Reconciliation contract:
  - completeness checkpoint counts (`expected_event_count`, `observed_event_count`, `gap_count`)
  - quarantine reason and `missing_offsets` details
- Precision/fidelity contract:
  - exact `payload_raw` bytes
  - deterministic `payload_sha256_raw`
  - canonical decimal scale (`price/qty/quote_qty` scale=8)

## Source/provenance and freshness semantics
- Provenance root is raw Binance payload bytes persisted in canonical events.
- Freshness for aligned responses is computed from result max timestamp versus current UTC clock.
- Freshness warning is observable in `warnings[]` and escalates in strict mode.

## Failure modes, warnings, and error codes
- Ingest/projection guardrail failures:
  - `sequence-gap` integrity errors
  - no-miss reconciliation runtime errors with explicit `missing_offsets`
  - quarantine enforcement (`STREAM_QUARANTINED` path)
  - payload precision/fidelity mismatch runtime errors
- API guardrail failures:
  - `STRICT_MODE_WARNING_FAILURE`
  - `QUERY_BACKEND_ERROR` when serving storage contract is missing

## Determinism/replay notes
- Re-run must preserve deterministic hashes for P1/P2/P3 outputs.
- P4/P5/P6 guardrails are deterministic under fixed fixture and controlled fault-injection paths.
- Guardrail proofs are executable and reproducible from repo root with explicit env contract.

## Environment variables and required config
- Event runtime:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- Query/API:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Storage:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Re-run guardrail integrity proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_g1_exchange_integrity_guardrail_proof`
- Re-run completeness + precision guardrails:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p5_no_miss_completeness_proof`
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s15_p6_raw_fidelity_precision_proof`
- Re-run API freshness/rights guardrail proof:
  - `PYTHONPATH=.:control-plane uv run --with httpx --with fastapi --with pydantic --with clickhouse-connect --with clickhouse-driver --with polars --with pyarrow python -m api.origo_api.s15_g2_g5_binance_query_guardrails_proof`
