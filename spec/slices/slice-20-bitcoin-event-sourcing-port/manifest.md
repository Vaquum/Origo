## What was done
- Completed Slice 20 Bitcoin event-sourcing capability for all currently onboarded Bitcoin datasets:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`
- Implemented and validated Bitcoin native serving projections for all seven datasets and aligned serving projections for the four derived datasets.
- Executed and captured proof artifacts:
  - `S20-P1` acceptance
  - `S20-P2` parity
  - `S20-P3` replay determinism
  - `S20-P4` exactly-once ingest
  - `S20-P5` no-miss completeness + quarantine
  - `S20-P6` raw fidelity + precision
  - `S20-G1` integrity suite
  - `S20-G2` freshness warning strict semantics
  - `S20-G3` reconciliation/quarantine guardrail
  - `S20-G4` precision guardrail
  - `S20-G5` rights/legal metadata and fail-closed behavior
- Deviation/fix made during proof execution:
  - fixed `S20-P5` gap fixture generation for `bitcoin_block_headers` to mutate canonical `height` (not `block_height`) to match stream contract and precision registry.
- Deviation/fix made during proof execution:
  - fixed Bitcoin native projection ordering to deterministic stream keys before integrity enforcement to avoid false linkage failures from runtime fetch ordering.

## Current state
- Bitcoin ingest path is event-driven end-to-end for all seven onboarded datasets:
  - canonical event writes are immutable and provenance-preserving (`payload_raw`, `payload_sha256_raw`)
  - serving paths read projection tables, not legacy direct-serving tables
- Bitcoin native mode:
  - serves all seven datasets from canonical projection tables.
- Bitcoin aligned mode:
  - serves only the four derived datasets from canonical aligned aggregates.
- Exactly-once ingest, no-miss gap detection/quarantine, and raw-fidelity/precision behavior are proven on deterministic fixtures for all seven Bitcoin datasets.
- API guardrails are active for Bitcoin responses:
  - freshness warnings and strict escalation
  - fail-closed rights behavior
  - rights metadata shape (`rights_state`, `rights_provisional`)

## Watch out
- `S20-P7` live-node gate passed only after running against reachable server-node RPC path; local runtime with Docker-internal hostnames still cannot execute this gate directly without an equivalent reachable endpoint.
- Runtime audit sink currently uses a single shared temp state-file name per log path; parallel proof runs targeting the same audit log path can race. Run proof scripts serially for deterministic local verification.
- Bitcoin aligned scope in this slice remains derived-only by contract; stream datasets (`headers`, `transactions`, `mempool`) are native-only.
