## What was done
- Completed Slice 16 ETF event-sourcing port for `etf_daily_metrics`.
- Ported ETF ingest writes to canonical events and kept source provenance fidelity (`payload_raw` + `payload_sha256_raw`).
- Migrated ETF serving to event-driven projections:
  - native serving from `canonical_etf_daily_metrics_native_v1`
  - aligned serving from `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)
- Replaced legacy ETF aligned query path assumptions by decoding canonical aligned payload buckets and building deterministic aligned observations/forward-fill intervals.
- Executed and captured full proof suite (`S16-P1..S16-P6`) and guardrail proofs (`S16-G1..S16-G4`).
- Completed docs closeout (`S16-G5`, `S16-G6`) and taxonomy/reference updates.

## Current state
- ETF raw serving is event-driven end-to-end:
  - ingest writes immutable canonical events
  - native queries read ETF native projection table
  - aligned queries read canonical aligned aggregate table and enforce aligned storage contract at runtime
- Exactly-once ETF ingest, no-miss cadence detection/quarantine, and raw-fidelity/precision guardrails are proven with fixed fixtures.
- Rights behavior is fail-closed and ETF responses include `rights_state` and `rights_provisional` metadata.
- Slice 16 proof artifacts and deterministic baseline fixture are present in this directory.

## Watch out
- ETF aligned query now depends on `canonical_aligned_1s_aggregates` payload bucket structure (`payload_rows_json` with canonical event row payloads); schema drift here will fail loudly.
- No-miss guardrail for ETF cadence intentionally quarantines on detected missing day offsets; ingest recovery requires explicit replay/recovery handling.
- API guardrail proof creates and drops an isolated proof DB (`<CLICKHOUSE_DATABASE>_s16_g1_g2_proof`); do not point it at production databases.
