# Slice 14 Developer: Event Guardrails Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S14 (`S14-G1`..`S14-G5`)
- Version reference: `Origo API v0.1.7`, `origo-control-plane v1.2.57`

## Purpose and scope
- Operational runbook for Slice 14 event guardrails.
- Scope covers typed failure paths, runtime audit durability, stream quarantine handling, precision guard checks, and rights metadata guardrails.

## Inputs and outputs with contract shape
- Inputs:
  - canonical event writes and projector checkpoint commits
  - completeness checkpoints (`ok` or `gap_detected`)
  - export/query metadata tags and response envelopes
- Outputs:
  - immutable runtime audit events
  - quarantine state file for blocked streams
  - typed error codes in writer/projector/reconciliation paths
  - rights metadata enforced in response/tag contracts

## Data definitions (field names, types, units, timezone, nullability)
- Runtime audit events:
  - ingest event type: `canonical_ingest_inserted|canonical_ingest_duplicate`
  - projector event type: `projector_checkpointed|projector_checkpoint_duplicate`
- Quarantine state fields:
  - `quarantined_at_utc`
  - `run_id`
  - `reason`
  - `gap_details`
- Rights metadata fields:
  - `rights_state`
  - `rights_provisional`

## Source/provenance and freshness semantics
- Guardrail logs are hash-chained JSONL via `ImmutableAuditLog`.
- Quarantine state is explicit file-backed control state and is consulted before writes.
- Rights metadata is sourced from the source-rights matrix and exported run tags.

## Failure modes, warnings, and error codes
- `WRITER_AUDIT_WRITE_FAILED`: canonical runtime audit sink write failed.
- `STREAM_QUARANTINED`: stream is blocked after gap detection.
- `RECONCILIATION_QUARANTINE_REGISTRY_REQUIRED`: gap checkpoint attempted without configured registry.
- `PROJECTOR_AUDIT_WRITE_FAILED`: projector checkpoint audit sink write failed.
- Export tag parser fails on missing rights metadata tags.

## Determinism/replay notes
- Guardrail proofs:
  - `guardrails-proof-s14-g1-error-taxonomy.json`
  - `guardrails-proof-s14-g2-runtime-audit.json`
  - `guardrails-proof-s14-g3-quarantine.json`
  - `guardrails-proof-s14-g4-precision.json`
  - `guardrails-proof-s14-g5-rights-metadata.json`

## Environment variables and required config
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` (`>=365`)
- `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
- `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`

## Minimal examples
- Run G2 proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python control-plane/origo_control_plane/s14_g2_runtime_audit_proof.py`
- Run G3 proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python control-plane/origo_control_plane/s14_g3_quarantine_proof.py`
- Run G5 proof:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python api/origo_api/s14_g5_rights_metadata_guardrail_proof.py`
