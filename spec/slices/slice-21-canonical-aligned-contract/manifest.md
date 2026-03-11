## What was done
- Enforced runtime canonical aligned-storage contract checks in Binance aligned query path:
  - table existence check for `canonical_aligned_1s_aggregates`
  - required-column presence check
  - required-column type check
- Refactored API guardrail proof harness to provision proof schema via migration runner and removed ad-hoc aligned table DDL.
- Added contract tests for fail-loud aligned storage violations (missing table, missing column, and type drift).
- Re-ran guardrail and replay proofs to confirm no deterministic replay drift after enforcement.
- Added Slice 21 developer and user documentation updates for canonical aligned-storage contract semantics.

## Current state
- Binance aligned serving is now explicitly contract-gated at runtime before query execution.
- Any aligned storage drift in the canonical table fails loudly instead of proceeding with implicit backend behavior.
- Proof harness for S15 aligned API guardrails is migration-backed and no longer manually creates aligned storage tables.
- Deterministic replay fingerprints for Binance fixed window (`2024-01-04T00:00:00Z` to `2024-01-04T00:00:03Z`) remain unchanged.

## Watch out
- The API guardrail proof requires explicit ClickHouse env values (`CLICKHOUSE_*`) and cannot run when those are absent.
- Replay proof runner requires event-runtime guardrail env vars:
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
- Slice 21 proof references are split across Slice 15 proof artifacts because the retrofit is intentionally applied on the existing Binance event-serving proof surface.
