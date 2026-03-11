## What was done
- Normalized historical route and `HistoricalData` method parameter contracts to shared names: `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`.
- Implemented optional selector semantics for historical and raw query paths: no selector now defaults to full available history (`earliest -> now`).
- Added projection (`fields`) and filter (`filters`) support to historical spot trades/klines query helpers with fail-loud validation.
- Added deterministic all-rows native query window (`AllRowsWindow`) with explicit ordering.
- Updated contract tests for no-selector behavior and multiple-selector rejection (`at most one` semantics).
- Deviation from target end-state: historical request contract accepts `mode=aligned_1s`, but execution is intentionally fail-loud in this slice and remains scheduled for S26 capability.

## Current state
- `POST /v1/raw/query` now accepts zero or one selector (`time_range | n_rows | n_random`).
- Historical spot routes now accept zero or one selector (`date-window | n_latest_rows | n_random_rows`).
- Historical no-selector requests execute against full available source history.
- Historical route/method contracts now include `mode`, `fields`, and `filters`; strict-warning escalation remains active.
- Historical spot route execution currently supports `native` mode only; `aligned_1s` requests return contract error.

## Watch out
- Full-history no-selector windows can return large payloads; callers should use `fields`, `filters`, or bounded selectors when practical.
- Historical `aligned_1s` is contract-reserved only in this slice; S26 must implement execution parity.
- Existing docs from pre-S25 that still mention exact-one selector semantics must not be used as truth.
