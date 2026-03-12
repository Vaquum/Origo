## What was done
- Added historical FRED Python capability via `HistoricalData.get_fred_series_metrics` with shared historical contract semantics.
- Added historical FRED HTTP capability via `POST /v1/historical/fred/series_metrics`.
- Implemented FRED historical serving for both `native` and `aligned_1s` in shared historical endpoint plumbing.
- Added FRED historical field projection and filter handling with fail-loud validation.
- Added FRED historical rights and warning guardrails (window warnings + publish-freshness warnings) with strict-mode failure parity.
- Added FRED historical alert/audit parity by invoking `emit_fred_warning_alerts_and_audit` for historical FRED warning paths.
- Added S28 contract/replay/parity test coverage and updated historical FRED developer and user docs.

## Current state
- Historical FRED is available on both internal Python and HTTP surfaces with shared request parameters:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Historical FRED supports both modes:
  - `native`
  - `aligned_1s`
- Date-window requests in FRED aligned mode resolve to `TimeRangeWindow`, which routes to forward-fill aligned planner semantics.
- Historical FRED responses include rights metadata and fail-loud status taxonomy (`200/404/409/503`).

## Watch out
- FRED rights checks require `ORIGO_FRED_QUERY_SERVING_STATE=promoted`; `shadow` blocks serving by design.
- FRED warning alert/audit paths require `ORIGO_FRED_ALERT_AUDIT_LOG_PATH`, `ORIGO_FRED_DISCORD_WEBHOOK_URL`, and `ORIGO_FRED_DISCORD_TIMEOUT_SECONDS`.
- Baseline source checksums in this slice are retrospective placeholders because this closeout did not re-run source ingestion checksum capture in-window during local proof run.
