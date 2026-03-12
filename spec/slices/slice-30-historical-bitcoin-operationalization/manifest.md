## What was done
- Added explicit historical Bitcoin query capability on both surfaces:
  - Python `HistoricalData` methods for all seven Bitcoin datasets.
  - HTTP routes under `/v1/historical/bitcoin/*` for all seven datasets.
- Added shared helper `query_bitcoin_dataset_data(...)` to enforce one Bitcoin historical execution path with:
  - shared window semantics
  - shared field/filter semantics
  - `native` and `aligned_1s` support
- Added API request model and handler wiring for Bitcoin historical routes with existing rights/auth/strict/error guardrails.
- Added S30 proof coverage in contract and replay suites for:
  - route registration
  - method signatures
  - mode parity
  - field/filter plumbing
  - strict/no-data behavior
  - replay determinism and historical-vs-raw parity
- Completed closeout updates:
  - `spec/1-top-level-plan.md`
  - `spec/2-itemized-work-plan.md`
  - `CHANGELOG.md`
  - docs updates for developer and user references

## Current state
- Historical Bitcoin coverage is complete for all seven datasets in both `native` and `aligned_1s` on:
  - Python interface (`HistoricalData`)
  - HTTP interface (`/v1/historical/bitcoin/*`)
- Historical Bitcoin requests use the shared historical contract:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Historical Bitcoin responses use the shared raw-envelope parity shape with rights metadata.
- Historical guardrails for auth/rights/strict warning escalation and fail-loud status taxonomy are active on all Bitcoin historical routes.

## Watch out
- Baseline fixture in this slice is retrospective and contract-level; source checksum slots are marked not-applicable for self-hosted Bitcoin RPC streams.
- Several Bitcoin aligned projection fields are convenience `Float64` outputs; canonical no-float requirement remains enforced at canonical event payload layer, not projection convenience layer.
- `uv run` may emit a local `.venv` uninstall metadata warning (`origo-0.1.17.dist-info`); this warning is non-blocking and did not affect S30 gate outcomes.
