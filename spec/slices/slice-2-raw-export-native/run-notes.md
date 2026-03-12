# Slice 2 Raw Export Native Run Notes

## Run metadata

- Date (UTC): 2026-03-05
- Scope: `S2-P1`, `S2-P2`, `S2-P3`, `S2-G1`, `S2-G2`, `S2-G3`, `S2-G4`, `S2-G5`
- Proof windows:
  - `binance_spot_trades month_year=[8, 2017]` for export lifecycle/determinism/parity
  - synthetic guardrail harness scenarios for rights gate, queue backpressure, audit chain, and failure classification
- Runtime environment:
  - API/export guardrail harness: `uv run --with fastapi --with httpx --with clickhouse-connect --with polars --with pyarrow --with dagster`
  - Dagster export execution proof: `origo_raw_export_native_job.execute_in_process(...)`
  - Guardrail harness export root: `/tmp/origo-s2-guardrails-proof/exports`
  - Guardrail harness audit log: `/tmp/origo-s2-guardrails-proof/audit/export-events.jsonl`

## System changes made as proof side effects

- Added guardrail proof artifact:
  - `spec/slices/slice-2-raw-export-native/guardrails-proof.json`
- Added rights contract artifacts:
  - `contracts/source-rights-matrix.json`
  - `contracts/legal/binance-hosted-allowed.md`
- Added temporary guardrail harness files under `/tmp/origo-s2-guardrails-proof/`:
  - `exports/<run_id>/raw_export.parquet`
  - `exports/<run_id>/metadata.json`
  - `exports/<run_id>/error.json`
  - `audit/export-events.jsonl`
  - `ingest-only-rights.json`

## Known warnings and disposition

- Dagster GraphQL calls are stubbed in the guardrail harness while endpoint contracts and lifecycle/audit behavior run end-to-end in-process.
- Audit log validation is intentionally strict; malformed/tampered lines fail future writes loudly.

## Deferred guardrails

- None for Slice 2. Guardrail checklist `S2-G1` through `S2-G5` is complete.

## Closeout confirmation

- `spec/2-itemized-work-plan.md` updated with `S2-G1..S2-G5` checked.
- `.env.example` updated with Slice 2 guardrail env vars:
  - `ORIGO_EXPORT_MAX_CONCURRENCY`
  - `ORIGO_EXPORT_MAX_QUEUE`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_EXPORT_AUDIT_LOG_PATH`
- `spec/slices/slice-2-raw-export-native/manifest.md` updated to current final slice state.
- `control-plane/pyproject.toml` version and `control-plane/CHANGELOG.md` updated for this closeout.
- Guardrail proof assertions in `guardrails-proof.json` all true:
  - rights gate enforced (`409`, `EXPORT_RIGHTS_INGEST_ONLY`)
  - queue backpressure enforced (`503`, `EXPORT_QUEUE_LIMIT_REACHED`)
  - failure classification exposed in status payload
  - audit chain valid with lifecycle transition events present.
