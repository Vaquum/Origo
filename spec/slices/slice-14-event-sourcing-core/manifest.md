# Slice 14 Manifest

## What was done
- Implemented canonical event-sourcing core for Slice 14:
  - global append-only canonical event tables and contracts
  - deterministic canonical event writer with raw-payload checksum anchoring
  - ingest cursor/ledger + completeness checkpoint reconciliation
  - projector runtime with checkpoint/watermark state in ClickHouse
  - persistent aligned projection framework and policy state
- Completed pilot cutover for Binance `spot_trades` through canonical events with parity proofs for `native` and `aligned_1s`.
- Added fail-loud guardrails:
  - typed runtime error taxonomy
  - immutable runtime audit transitions
  - stream quarantine on gap detection
  - no-float canonical precision guard checks
  - rights metadata emission guard checks
- Closed Slice 14 docs guardrails:
  - developer docs in `docs/Developer/`
  - user docs and taxonomy updates in `docs/`
- Deviation from full target scope:
  - Raw API validates `view_id`/`view_version` and response metadata contracts, but request execution remains single-source for now; multi-source execution is deferred to later migration slices.

## Current state
- Canonical event runtime is active in code and backed by migration-managed ClickHouse tables.
- Canonical ingest behavior is proven for:
  - deterministic raw payload hashing
  - idempotent duplicate replay
  - crash/restart retry safety
  - fail-loud gap detection with quarantine enforcement
- Projector resume/checkpoint determinism is proven with fixed fixtures.
- Pilot serving parity for Binance `spot_trades` is proven between legacy baseline and projection-driven path.
- Query/export response contracts now include rights metadata (`rights_state`, `rights_provisional`) and optional view metadata (`view_id`, `view_version`).

## Watch out
- Slice-14 pilot fixtures are synthetic in-process fixture rows for deterministic contract testing; they are not external source file replays.
- Runtime audit and quarantine paths are hard-required by env contract:
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
- Event-sourcing migration is only piloted for Binance `spot_trades` in this slice; full Binance port remains Slice 15 scope.
- Multi-source request execution remains intentionally constrained to one source per request until follow-on slices complete.
