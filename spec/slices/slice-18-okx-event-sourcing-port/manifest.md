## What was done
- Completed Slice 18 OKX event-sourcing port for `okx_spot_trades`.
- Ported OKX ingest writes to canonical events with immutable payload bytes and SHA256 provenance:
  - `source_id='okx'`
  - `stream_id='okx_spot_trades'`
- Added OKX event-driven serving projections:
  - native serving from `canonical_okx_spot_trades_native_v1`
  - aligned serving from `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)
- Cut OKX query/export serving to event projections and removed legacy direct-serving reads from runtime paths.
- Executed and captured full proof suite (`S18-P1..S18-P6`) and guardrail proofs (`S18-G1..S18-G4`).
- Completed docs closeout (`S18-G5`, `S18-G6`) and taxonomy/reference updates.
- Deviation from plan: fixed aligned projector bucket key at runtime core level to use `source_event_time_utc` instead of ingest timestamp to preserve aligned parity against source event time.

## Current state
- OKX raw serving is event-driven end-to-end:
  - ingest writes immutable canonical events
  - native queries read `canonical_okx_spot_trades_native_v1`
  - aligned queries read `canonical_aligned_1s_aggregates` with enforced aligned storage contract
- Exactly-once OKX ingest, no-miss gap detection/quarantine, and raw-fidelity/precision guardrails are proven on fixed fixtures.
- Exchange integrity suite is enforced in ingest and projection paths for OKX.
- Rights behavior is fail-closed and OKX responses include `rights_state` and `rights_provisional` metadata.
- Slice 18 proof artifacts and deterministic baseline fixture are present in this directory.

## Watch out
- OKX aligned query depends on canonical payload bucket shape (`payload_rows_json.rows[*].payload_json` plus deterministic source offset ordering); schema drift fails loudly.
- OKX projection table currently stores numeric serving fields as `Float64`; canonical event payload remains the precision anchor (`payload_raw` + checksum).
- API guardrail proof patches Discord webhook posting to avoid network dependency in local proof runs; preserve that patching strategy for deterministic guardrail runs.
