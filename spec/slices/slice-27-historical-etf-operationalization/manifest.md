## What was done
- Added historical ETF Python capability via `HistoricalData.get_etf_daily_metrics` with shared historical contract semantics.
- Added historical ETF HTTP capability via `POST /v1/historical/etf/daily_metrics`.
- Implemented ETF historical serving for both `native` and `aligned_1s` in shared historical endpoint plumbing.
- Added ETF historical field projection and filter handling with fail-loud validation.
- Added ETF historical rights and warning guardrails (window warnings + ETF quality warnings) with strict-mode failure parity.
- Added S27 contract/replay/parity test coverage and updated historical ETF developer and user docs.

## Current state
- Historical ETF is available on both internal Python and HTTP surfaces with shared request parameters:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Historical ETF supports both modes:
  - `native`
  - `aligned_1s`
- Date-window requests in ETF aligned mode resolve to `TimeRangeWindow`, which routes to forward-fill aligned planner semantics.
- Historical ETF responses include rights metadata and fail-loud status taxonomy (`200/404/409/503`).

## Watch out
- ETF rights checks require `ORIGO_ETF_QUERY_SERVING_STATE=promoted`; `shadow` blocks serving by design.
- Full `tests/contract` and `tests/integrity` execution in local environment still depends on `origo_control_plane` import availability.
- Baseline source checksums in this slice are retrospective placeholders because this closeout did not re-ingest raw scraper artifacts in-window during local proof run.
