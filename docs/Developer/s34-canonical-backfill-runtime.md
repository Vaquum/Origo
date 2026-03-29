# Slice 34 Developer: Canonical Backfill Runtime

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-29
- Slice reference: S34 (`S34-C1..C8`, `S34-C2l`, `S34-P1..P4`, `S34-G1..G2`)

## Purpose and scope
- Defines the live Slice 34 canonical backfill runtime contract.
- Scope covers authoritative backfill state, partition/range proof semantics, immutable manifest evidence, and closeout-prep reporting.

## Inputs and outputs with contract shape
- Backfill runner:
  - `control-plane/origo_control_plane/s34_exchange_backfill_runner.py`
  - input: `dataset`, `end_date | partition_ids`, `execution_mode`, `projection_mode`, `runtime_audit_mode`, `concurrency`
  - output: processed partition list, range proof summary, manifest log path
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
- ClickHouse is the only live authority for backfill progress, proof, and quarantine state.
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
- Reconcile is the only valid path for ambiguous or quarantined partitions.
- Any live Slice-34 runtime path that reads the base `canonical_event_log` instead of `canonical_event_log_active_v1` after a reconcile reset is a contract violation because it can resurrect stale pre-reset rows or reintroduce destructive delete pressure.
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

## Minimal examples
- Generate Slice 34 closeout-prep summary:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_g1_g2_closeout_prep`
- Run one explicit exchange backfill tranche:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_exchange_backfill_runner --dataset okx_spot_trades --end-date 2021-09-30 --concurrency 20 --projection-mode deferred --runtime-audit-mode summary`
