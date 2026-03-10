## What was done
- Completed Slice 19 Bybit event-sourcing port for `bybit_spot_trades`.
- Ported Bybit ingest writes to canonical events with immutable payload bytes and SHA256 provenance:
  - `source_id='bybit'`
  - `stream_id='bybit_spot_trades'`
- Added Bybit event-driven serving projections:
  - native serving from `canonical_bybit_spot_trades_native_v1`
  - aligned serving from `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)
- Cut Bybit query/export serving to event projections and removed legacy direct-serving reads from runtime paths.
- Executed and captured full proof suite (`S19-P1..S19-P6`) and guardrail proofs (`S19-G1..S19-G4`).
- Completed docs closeout (`S19-G5`, `S19-G6`) and taxonomy/reference updates.
- Deviation from initial implementation: Bybit `trade_id`/canonical source offset mapping was corrected to parse from `trd_match_id` (`m-<digits>`) instead of synthetic CSV row order, because row-order offsets masked true no-miss gap behavior.
- Deviation from initial proof harness output path: proof scripts were corrected to always write artifacts under root `spec/slices/slice-19-bybit-event-sourcing-port/` independent of working directory.

## Current state
- Bybit raw serving is event-driven end-to-end:
  - ingest writes immutable canonical events
  - native queries read `canonical_bybit_spot_trades_native_v1`
  - aligned queries read `canonical_aligned_1s_aggregates` with enforced aligned storage contract
- Exactly-once Bybit ingest, no-miss gap detection/quarantine, and raw-fidelity/precision guardrails are proven on fixed fixtures.
- Exchange integrity suite is enforced in ingest and projection paths for Bybit.
- Rights behavior is fail-closed and Bybit responses include `rights_state` and `rights_provisional` metadata.
- Slice 19 proof artifacts and deterministic baseline fixture are present in this directory.

## Watch out
- Bybit ingest now requires canonical `trd_match_id` identity shape (`m-<digits>`); malformed IDs fail ingest by design.
- Bybit aligned query depends on canonical payload bucket shape (`payload_rows_json.rows[*].payload_json` plus deterministic source-offset ordering); schema drift fails loudly.
- Bybit projection table currently stores numeric serving fields as `Float64`; canonical event payload remains the precision anchor (`payload_raw` + checksum).
- API guardrail proof patches Discord webhook posting to avoid network dependency in local proof runs; preserve that patching strategy for deterministic guardrail runs.
