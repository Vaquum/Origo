# Slice 14 Developer: Canonical Event Runtime

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-29
- Slice reference: S14 (`S14-C1`..`S14-C9`, `S14-P1`..`S14-P6`)
- Version reference: `Origo API v0.1.28`, `origo-control-plane v1.2.80`

## Purpose and scope
- Defines the event-sourcing runtime contracts introduced in Slice 14.
- Scope includes canonical event writes, checkpointed projectors, aligned 1s projection storage, precision rules, and pilot cutover contract.

## Inputs and outputs with contract shape
- Inputs:
  - Source event identity fields:
    - `source_id`
    - `stream_id`
    - `partition_id`
    - `source_offset_or_equivalent`
  - Raw payload contract:
    - `payload_raw` (source bytes)
    - `payload_content_type` (`application/json`)
    - `payload_encoding` (`utf-8`)
  - Runtime state inputs:
    - cursor/checkpoint tables
    - projector watermark tables
- Outputs:
  - Canonical event rows in `canonical_event_log`
  - Boundary-aware canonical runtime reads via `canonical_event_log_active_v1`
  - Audited logical reset rows in `canonical_partition_reset_boundaries`
  - Projector checkpoint rows in `canonical_projector_checkpoints`
  - Projector watermark rows in `canonical_projector_watermarks`
  - Aligned aggregate rows in `canonical_aligned_1s_aggregates`

## Data definitions (field names, types, units, timezone, nullability)
- Canonical event log (`canonical_event_log`):
  - envelope: `envelope_version`, `event_id`
  - identity: `source_id`, `stream_id`, `partition_id`, `source_offset_or_equivalent`
  - times: `source_event_time_utc` (nullable), `ingested_at_utc` (UTC)
  - raw payload: `payload_raw`, `payload_sha256_raw`
  - derivative payload: `payload_json`
- Canonical runtime read surface (`canonical_event_log_active_v1`):
  - hides any pre-reset rows that fall at or before the latest audited partition reset boundary
  - required for live proof, planner, projector, and writer-identity lookups after reset-and-rewrite flows
- Partition reset boundaries (`canonical_partition_reset_boundaries`):
  - identity: `source_id`, `stream_id`, `partition_id`
  - reset metadata: `reset_revision`, `reason`, `details_json`, `recorded_by_run_id`, `recorded_at_utc`
- Completeness/reconciliation:
  - `canonical_ingest_cursor_state`
  - `canonical_ingest_cursor_ledger`
  - `canonical_source_completeness_checkpoints`
- Aligned policy/rows:
  - `canonical_aligned_projection_policies`
  - `canonical_aligned_1s_aggregates`

## Source/provenance and freshness semantics
- Canonical truth is the append-only event log.
- Reconcile reset-and-rewrite preserves that append-only truth by recording logical partition reset boundaries instead of deleting canonical events.
- Serving rows are projection-derived artifacts from canonical events.
- `payload_sha256_raw` is the immutable byte-level provenance anchor.
- Freshness remains source-publish driven and warning-aware per API contract.

## Failure modes, warnings, and error codes
- Writer typed error taxonomy:
  - `WRITER_INVALID_*`
  - `WRITER_IDENTITY_*`
  - `WRITER_AUDIT_WRITE_FAILED`
  - `STREAM_QUARANTINED`
- Projector typed error taxonomy:
  - `PROJECTOR_*`
  - `ALIGNED_POLICY_*`
  - `ALIGNED_RUNTIME_*`
- Reconciliation typed error taxonomy:
  - `RECONCILIATION_*`
  - `CURSOR_*`
- Gap-detected checkpoints quarantine the stream and subsequent writes fail loudly.
- Direct live runtime reads from `canonical_event_log` after a partition reset are a contract violation; boundary-aware truth must come from `canonical_event_log_active_v1`.

## Determinism/replay notes
- Determinism proofs:
  - `proof-s14-p1-raw-payload-hash-determinism.json`
  - `proof-s14-p2-exactly-once-ingest.json`
  - `proof-s14-p5-projector-resume-determinism.json`
  - `proof-s14-p6-pilot-cutover-parity.json`
- Reconciliation and precision proofs:
  - `proof-s14-p3-no-miss-detection.json`
  - `proof-s14-p4-raw-fidelity-precision.json`

## Environment variables and required config
- Canonical runtime and retention:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Run P3 (gap detection + quarantine):
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python control-plane/origo_control_plane/s14_p3_no_miss_detection_proof.py`
- Run P5 (projector resume determinism):
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python control-plane/origo_control_plane/s14_p5_projector_resume_determinism_proof.py`
