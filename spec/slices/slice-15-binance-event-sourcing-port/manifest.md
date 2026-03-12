## What was done
- Completed Binance event-sourcing port for `binance_spot_trades`.
- Migrated serving path to canonical projections:
  - native queries now read `canonical_binance_*_native_v1` tables.
  - aligned queries now read `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`).
- Executed and captured full S15 proof suite:
  - acceptance/parity/determinism (`S15-P1..P3`)
  - exactly-once ingest (`S15-P4`)
  - no-miss reconciliation with quarantine (`S15-P5`)
  - raw-fidelity and precision (`S15-P6`)
- Added and executed guardrail proofs:
  - ingest + projection integrity enforcement (`proof-s15-g1-exchange-integrity.json`)
  - aligned freshness + strict escalation + rights metadata in API responses (`proof-s15-g2-g5-api-guardrails.json`).
- Added Slice-15 developer and user docs:
  - `docs/Developer/s15-binance-event-port.md`
  - `docs/Developer/s15-binance-guardrails-runbook.md`
  - `docs/binance-reference.md`
  - taxonomy/reference updates in existing docs.

## Current state
- Binance raw serving is event-driven end-to-end:
  - ingest writes canonical events with deterministic identity and preserved raw payload bytes.
  - native/aligned serving is projection-only for Binance datasets (no legacy direct-serving path).
- Guardrails active for Binance path:
  - fail-loud exchange integrity checks in ingest and native projection batches.
  - fail-loud no-miss detection with completeness checkpointing and quarantine enforcement.
  - fail-loud raw-fidelity and precision invariants in canonical writer path.
  - aligned freshness warning semantics with strict escalation.
  - rights metadata (`rights_state`, `rights_provisional`) emitted in query/export response contracts.
- Slice 15 proof artifacts are present under this directory and reproducible with fixed fixtures.

## Watch out
- API guardrail proof for S15 creates an isolated proof database (`<CLICKHOUSE_DATABASE>_s15_g2_g5_proof`) and drops it on completion; do not point that proof at production data.
- Projection integrity guardrail now blocks writes on sequence/anomaly violations; ingestion gaps that previously passed silently will now fail loudly by design.
- Canonical runtime audit and quarantine env variables are mandatory for event-writer flows:
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS`
  - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`
  - `ORIGO_STREAM_QUARANTINE_STATE_PATH`
