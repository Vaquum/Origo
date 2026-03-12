## What was done
- Implemented deterministic `aligned_1s` query shaping for:
  - `bitcoin_block_headers`
  - `bitcoin_block_transactions`
  - `bitcoin_mempool_state`
- Added aligned projector utility for Bitcoin stream datasets and wired stream assets to execute aligned projection writes to `canonical_aligned_1s_aggregates`.
- Integrated the three stream datasets into aligned planner/query/export/API allowlists.
- Added stream-specific contract proofs for aligned SQL targeting, planner routing, deterministic shaping, replay determinism, and canonical aligned storage negative checks.
- Completed slice closeout updates:
  - `spec/1-top-level-plan.md`
  - `spec/2-itemized-work-plan.md`
  - version/changelog updates
  - developer and user docs updates

## Current state
- All currently onboarded raw datasets are aligned-capable in `mode=aligned_1s`.
- Bitcoin stream aligned query path is projection-driven from `canonical_aligned_1s_aggregates` with strict fail-loud storage contract enforcement.
- Bitcoin stream ingest assets now write both native and aligned projection outputs in the same run.
- Query/export surfaces include aligned support for all seven Bitcoin datasets.

## Watch out
- Stream-aligned shaping currently emits Float64 fields for aggregated numeric outputs (`latest_difficulty`, value sums, fee stats); canonical no-float requirement still applies to event payload storage, not projection convenience output.
- Baseline fixture in this slice is retrospective and contract-level (unit-fixture driven), not a fresh live-node capture.
- Local `uv run` emits a non-blocking warning about stale `origo-0.1.17.dist-info` uninstall metadata in `.venv`; gates still pass.
