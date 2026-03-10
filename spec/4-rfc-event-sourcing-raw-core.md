# 4. RFC Reconciliation: Event-Sourced Raw Core (Pilot First)

## Status
Planned (decision-complete)

## Locked Decisions
1. Canonical log model is a single global append-only event log.
2. Event format is typed envelope fields plus canonical `payload_json` and `payload_sha256`.
3. Projection runtime is hybrid.
4. ClickHouse materialized views are used for straightforward projections.
5. Python projectors are used for stateful or complex projections.
6. Projector checkpoints and watermarks live in ClickHouse tables.
7. Cutover approach starts with a minimal direct-cutover pilot.
8. Pilot dataset is Binance spot trades.
9. Aligned policy is tiered and aligned persistence is included in this migration wave.
10. Raw API supports multi-source requests in V1.
11. SLA breach default is serve-with-warning.
12. Correction semantics are latest-truth-only in V1.
13. Temporary hosted rights are allowed for internal and external use.
14. Provisional rights must always be exposed in response metadata.
15. MK API starts after Raw-core migration gates are complete.
16. No new source onboarding in this track.
17. Bitcoin is migrated last in sequence.

## Target Architecture
1. Ingestion writes canonical events only.
2. Canonical event storage is append-only.
3. Canonical log envelope includes:
   1. `event_id`
   2. `stream_id`
   3. `source_id`
   4. `dataset`
   5. `event_type`
   6. `schema_version`
   7. `source_event_id`
   8. `source_sequence`
   9. `event_time_utc`
   10. `source_available_at_utc`
   11. `ingested_at_utc`
   12. `payload_json`
   13. `payload_sha256`
   14. `provenance_json`
   15. `correction_of_event_id`
   16. `ingest_run_id`
   17. `producer_version`
4. Native and aligned serving data are deterministic projections from canonical events.
5. Migrated datasets must not write directly from source loaders to serving tables.

## API and Contract Changes
1. `POST /v1/raw/query` supports multi-source requests.
2. Requests and responses include `view_id` and `view_version`.
3. Responses and export status include rights metadata:
   1. `rights_state`
   2. `rights_provisional`
   3. source-level rights context
4. Freshness behavior:
   1. default mode may serve stale data with warnings
   2. strict mode fails on warnings

## Migration Sequence
1. Implement canonical event-log tables and checkpoint tables.
2. Implement projector runtime and persistent aligned aggregates.
3. Perform pilot direct cutover for Binance spot trades.
4. Prove parity and replay determinism on pilot fixture windows.
5. Expand migration source-by-source for remaining non-Bitcoin datasets.
6. Migrate Bitcoin last after node readiness and proof gates pass.
7. Start MK API implementation only after Raw migration completion gates pass.

## Acceptance Criteria
1. Pilot dataset is served only from canonical projections.
2. Fixed-window replay determinism passes across repeated runs.
3. Legacy-versus-event parity proof passes for pilot windows.
4. Multi-source Raw query contract tests pass.
5. Rights provisional metadata is present in all affected successful responses.
6. SLA warning behavior passes in default mode.
7. Strict-mode fail behavior passes when warnings are present.

## Notes
1. This document is the architecture bridge from current partial event-style behavior to complete event-sourcing.
2. Delete-and-reinsert ingestion paths are migration debt until moved behind canonical append-only event writes and projector-based reads.
