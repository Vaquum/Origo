# Slice 34 Developer: Canonical Backfill Runtime

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Slice reference: S34 (`S34-C1..C8`, `S34-C2l`, `S34-P1..P4`, `S34-G1..G2`)

## Purpose and scope
- Defines the live Slice 34 canonical backfill runtime contract.
- Scope covers authoritative backfill state, Dagster/Dagit operator truth, partition/range proof semantics, immutable manifest evidence, and closeout-prep reporting.

## Inputs and outputs with contract shape
- Dagster/Dagit launch surface:
  - input: explicit dashboard partition selection plus explicit runtime config shown in Launchpad
  - output: one Dagster run whose partition state must agree with the terminal or non-terminal proof state for the same partition
- Closeout-prep report:
  - `control-plane/origo_control_plane/s34_g1_g2_closeout_prep.py`
  - input: authoritative ClickHouse proof/manifests + immutable manifest log
  - output: per-dataset proof coverage, manifest evidence summary, remaining closeout gaps

## Data definitions (field names, types, units, timezone, nullability)
- Authoritative ClickHouse tables:
  - `canonical_backfill_source_manifests`
  - `canonical_backfill_partition_proofs`
  - `canonical_quarantined_streams`
  - `canonical_backfill_range_proofs`
- Canonical reset/read tables:
  - `canonical_event_log` is the append-only write table
  - `canonical_partition_reset_boundaries` records audited logical reset cutovers per `(source_id, stream_id, partition_id)`
  - `canonical_event_log_active_v1` is the required live read surface for proof, planner, projector, and writer-identity lookups after a reset
- Partition proof states:
  - `source_manifested`
  - `canonical_written_unproved`
  - `proved_complete`
  - `empty_proved`
  - `quarantined`
  - `reconcile_required`
- Terminal states:
  - `proved_complete`
  - `empty_proved`
- Manifest event types:
  - `s34_backfill_partition_completed`
  - `s34_backfill_range_proved`
- Required per-partition evidence now emitted into manifest events:
  - source artifact identity/checksums
  - first/last offsets or equivalents
  - source/canonical identity digests
  - proof digest
  - range digest and range proof details

## Source/provenance and freshness semantics
- Dagster/Dagit is the sole authoritative operator truth for partition status.
- Dagster/Dagit dashboard launch is the only allowed write entrypoint for Slice-34 backfill and reconcile.
- ClickHouse stores the live proof/progress/quarantine facts that Dagster operator truth must reflect without contradiction; it is not a competing operator authority surface.
- Canonical reset-and-rewrite keeps `canonical_event_log` append-only; logical partition clears are expressed by `canonical_partition_reset_boundaries` and enforced through `canonical_event_log_active_v1`.
- Manifest JSONL is immutable evidence, not resume truth.
- Projection rebuild and serving promotion are gated strictly on terminal proof coverage.
- Historical availability is bounded by the terminal-proof frontier, not by raw source existence alone.

## Failure modes, warnings, and error codes
- Normal backfill fails loudly on:
  - existing canonical rows without terminal proof
  - non-terminal proof state on the targeted partition
  - missing source manifest after a completed Dagster partition run
  - missing terminal proof after a completed Dagster partition run
- Dagster green with missing or non-terminal proof for the same partition is a hard failure.
- Dagster red with terminal proof for the same partition is a hard failure.
- A partition shown in Dagster as `failed` or `missing` while the proof store says `proved_complete` or `empty_proved` is a contract violation, not an acceptable operational nuance.
- `reconcile_required` must be explicit before launch; it may not be surfaced as ordinary failed/missing/runnable partition status.
- Reconcile is the only valid path for ambiguous or quarantined partitions.
- Any live Slice-34 runtime path that reads the base `canonical_event_log` instead of `canonical_event_log_active_v1` after a reconcile reset is a contract violation because it can resurrect stale pre-reset rows or reintroduce destructive delete pressure.
- Any helper utility outside Dagster/Dagit that launches runs or mutates canonical state is a contract violation.
- Any malformed proof/manifold JSON payload in authoritative tables or manifest log is a hard runtime error.

## Determinism/replay notes
- Partition completion is self-proved via source/canonical digest equality plus dataset-appropriate gap rules.
- Range proof digest is folded deterministically from partition proof digests in partition order.
- Slice 34 prep/closeout truth is summarized by:
  - `spec/slices/slice-34-full-canonical-backfill/proof-s34-g1-g2-closeout-prep.json`
- Live operational investigation trail is maintained in:
  - `spec/live-performance-investigation-log.md`

## Environment variables and required config
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
  - `CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS`
- Backfill runtime:
  - `DAGSTER_HOME`
  - `ORIGO_BACKFILL_MANIFEST_LOG_PATH`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_S34_BACKFILL_CONCURRENCY`
- FRED revision-history transport:
  - `FRED_API_KEY`
  - `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`
  - `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST`
  - deploy-on-merge must synchronize required FRED env keys from root `.env.example` into `/opt/origo/deploy/.env`; a merged code contract that does not reach the live server env is a hard runtime failure
- FRED reconcile planner:
  - `ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN`
  - `ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS`
  - direct Dagster/manual `reconcile` launches without explicit `origo.backfill.partition_ids` must still honor these same bounded-planning env ceilings; missing or invalid values are hard runtime failures

## Minimal examples
- Generate Slice 34 closeout-prep summary:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_g1_g2_closeout_prep`
- Valid live operator path:
  - open Dagit, select the target partition(s), confirm the explicit runtime config shown in Launchpad, and launch the run from the dashboard
