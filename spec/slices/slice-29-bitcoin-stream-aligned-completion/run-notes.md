## Run metadata
- Date: 2026-03-12
- Scope: S29 capability/proof/guardrails for full Bitcoin stream `aligned_1s` completion (`bitcoin_block_headers`, `bitcoin_block_transactions`, `bitcoin_mempool_state`).
- Fixture window: 2024-04-20 UTC day (contract-level deterministic fixtures for stream-aligned shaping and planner/storage proofs).
- Runtime environment: local development run (`uv run` with `PYTHONPATH=.:control-plane` for test gates).

## System changes made as proof side effects
- Added stream-aligned query module and planner integration.
- Added stream-aligned projector utility and wired aligned projection execution into Bitcoin stream assets.
- Added aligned query/export/API allowlist coverage for Bitcoin stream datasets.
- Added contract and replay proofs for stream-aligned path.

## Known warnings and disposition
- Canonical aligned storage contract failures are expected negative-proof behavior and remain fail-loud (`missing table`, `missing columns`, `column type mismatch`).
- Mutable-window warnings (`WINDOW_LATEST_ROWS_MUTABLE`, `WINDOW_RANDOM_SAMPLE`) remain expected and are escalated by `strict=true`.
- `uv run` emits non-blocking local environment warning for stale uninstall metadata (`origo-0.1.17.dist-info`); this did not affect gate outcomes.

## Deferred guardrails
- None.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for Slice 29.
- Versions bumped to `Origo API v0.1.20` and `origo-control-plane v1.2.64`, with changelog updates.
- Root `.env.example` reviewed; no new or changed environment variables were required in this slice.
