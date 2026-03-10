## What was done
- Completed Slice 17 FRED event-sourcing port for `fred_series_metrics`.
- Ported FRED ingest writes to canonical events with immutable payload bytes and SHA256 provenance:
  - `source_id='fred'`
  - `stream_id='fred_series_metrics'`
- Added FRED event-driven serving projections:
  - native serving from `canonical_fred_series_metrics_native_v1`
  - aligned serving from `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)
- Cut FRED query/export serving to event projections and removed legacy direct-serving reads from runtime paths.
- Executed and captured full proof suite (`S17-P1..S17-P6`) and guardrail proofs (`S17-G1..S17-G4`).
- Completed docs closeout (`S17-G5`, `S17-G6`) and taxonomy/reference updates.

## Current state
- FRED raw serving is event-driven end-to-end:
  - ingest writes immutable canonical events
  - native queries read `canonical_fred_series_metrics_native_v1`
  - aligned queries read `canonical_aligned_1s_aggregates` with enforced aligned storage contract
- Exactly-once FRED ingest, no-miss cadence detection/quarantine, and raw-fidelity/precision guardrails are proven with fixed fixtures.
- Publish-freshness warning behavior is projection-based (`provenance_json.last_updated_utc`) and strict escalation is proven.
- Rights behavior is fail-closed and FRED responses include `rights_state` and `rights_provisional` metadata.
- Slice 17 proof artifacts and deterministic baseline fixture are present in this directory.

## Watch out
- FRED aligned query now depends on `canonical_aligned_1s_aggregates` payload bucket structure (`payload_rows_json` containing canonical event rows); schema drift fails loudly.
- FRED no-miss guardrail currently enforces cadence checks via source-offset day extraction in proof harnesses; production cadence policy changes must keep offset parsing deterministic.
- API guardrail proof patches Discord webhook posting to avoid external network dependency; keep this patching strategy in future API guardrail proofs to preserve deterministic local runs.
