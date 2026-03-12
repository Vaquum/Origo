## What was done
- Added S31 full-surface cohesion guardrail contract test:
  - `tests/contract/test_historical_surface_cohesion_contract.py`
- Enforced historical matrix closure for in-scope datasets across:
  - HTTP routes (`/v1/historical/...`)
  - Python methods (`HistoricalData.get_*`)
  - docs taxonomy references
- Added final rollout handoff/rollback mapping documentation:
  - `docs/Developer/s31-historical-rollout-handoff.md`
  - `docs/historical-reference.md`
- Updated historical/taxonomy references for canonical closure:
  - `docs/historical-contract-reference.md`
  - `docs/data-taxonomy.md`
  - `docs/historical-bitcoin-reference.md`
  - `docs/bitcoin-core-reference.md`
- Completed slice closeout updates:
  - `spec/1-top-level-plan.md`
  - `spec/2-itemized-work-plan.md`
  - version/changelog updates

## Current state
- Historical surface is fully cohesive for all in-scope datasets in both modes:
  - `native`
  - `aligned_1s`
- Deferred historical datasets are explicit and unchanged:
  - `spot_agg_trades`
  - `futures_trades`
- Zero-drift guardrail is now test-enforced for historical matrix and docs-route consistency.
- Historical rollout/cutover mapping is documented with explicit rollback guidance.

## Watch out
- S31 matrix guardrail intentionally fails loudly on doc drift; route or dataset additions now require synchronized docs updates in the same slice/PR.
- Baseline fixture in this slice is retrospective and contract-level (no fresh live-source checksum capture was executed for this closure slice).
- Local `.venv` may emit non-blocking stale uninstall metadata warnings during `uv run`; gate outcomes remain valid.
