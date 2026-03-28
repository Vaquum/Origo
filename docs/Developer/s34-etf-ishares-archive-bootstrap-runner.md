# Slice 34 Developer: ETF iShares Archive Bootstrap Runner

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-28
- Slice reference: S34 (`S34-C5h`, `S34-05h`, `S34-G2`)

## Purpose and scope
- Defines the repo-native Slice 34 iShares historical archive bootstrap runner.
- Scope is limited to `etf_ishares_ibit_daily`.
- The runner fetches official iShares historical CSV artifacts via the `asOfDate` parameter, validates that each artifact normalizes to the requested UTC partition day, and persists only valid raw artifacts into object storage.
- This runner builds archive coverage. It does not write canonical ETF events or project ETF serving tables.

## Inputs and outputs with contract shape
- Runner entrypoint:
  - `control-plane/origo_control_plane/s34_etf_ishares_archive_bootstrap_runner.py`
- Required inputs:
  - object-store runtime env
- Optional inputs:
  - `run_id`
  - `start_date`
  - `end_date`
- Output:
  - `dataset`
  - `source_id`
  - `control_run_id`
  - `requested_start_date`
  - `requested_end_date`
  - `requested_weekday_count`
  - `skipped_existing_days`
  - `skipped_no_data_days`
  - `persisted_days`
  - `persisted_artifact_count`

## Data definitions (field names, types, units, timezone, nullability)
- Fixed dataset:
  - `etf_daily_metrics`
- Fixed source:
  - `etf_ishares_ibit_daily`
- Historical bootstrap floor:
  - `2024-01-11`
- `requested_weekday_count`:
  - count of Monday-Friday request days in the inclusive requested window
- `skipped_existing_days`:
  - already-archived days with at least one valid persisted raw artifact that still parses and normalizes cleanly
- `skipped_no_data_days`:
  - request days where the official iShares CSV returned the explicit no-data holdings marker
- `persisted_days`:
  - days newly persisted into raw-artifact storage on this run
- `persisted_no_data_days`:
  - days where the official iShares response was explicit no-data and whose raw artifacts were still persisted as first-party negative evidence

## Source/provenance and freshness semantics
- Source of truth is the official iShares IBIT holdings CSV endpoint with `asOfDate=<YYYYMMDD>`.
- The runner persists raw artifacts first and leaves canonical replay to the ETF backfill job.
- Existing archive days are revalidated by loading archived bytes from object storage and replaying the real adapter `parse()` and `normalize()` methods.
- Market holidays are not inferred heuristically. The runner only treats a day as legitimate no-data when the official artifact contains the explicit iShares no-data marker.
- Official no-data artifacts are still persisted to object storage. ETF replay later consumes them as negative evidence so those dates are excluded from required historical coverage instead of becoming fake gaps.

## Failure modes, warnings, and error codes
- `start_date < 2024-01-11`: fail loudly.
- Invalid CLI date format: fail loudly.
- Missing object-store env vars: fail loudly.
- Archived artifact manifest/content mismatch: fail loudly.
- Artifact parse/normalize mismatch for the requested day: fail loudly.
- Unexpected no-data shape or malformed iShares response: fail loudly.

## Determinism/replay notes
- The bootstrap runner itself is not a canonical replay path; it builds raw-artifact coverage for later replay.
- Existing-day detection is deterministic because it revalidates archived artifacts through the real adapter logic before considering a day already covered.
- Contract coverage lives in:
  - `tests/contract/test_s34_etf_ishares_archive_bootstrap_runner_contract.py`

## Environment variables and required config
- Object store:
  - `ORIGO_OBJECT_STORE_ENDPOINT_URL`
  - `ORIGO_OBJECT_STORE_REGION`
  - `ORIGO_OBJECT_STORE_ACCESS_KEY_ID`
  - `ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY`
  - `ORIGO_OBJECT_STORE_BUCKET`
- Retry/runtime:
  - `ORIGO_SCRAPER_FETCH_MAX_ATTEMPTS`
  - `ORIGO_SCRAPER_FETCH_BACKOFF_INITIAL_SECONDS`
  - `ORIGO_SCRAPER_FETCH_BACKOFF_MULTIPLIER`

## Minimal examples
- Bootstrap the full known iShares history:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_ishares_archive_bootstrap_runner --run-id s34-ishares-bootstrap-20260328 --start-date 2024-01-11 --end-date 2026-03-28`
- Bootstrap a bounded repair window:
  - `PYTHONPATH=.:control-plane control-plane/.venv/bin/python -m origo_control_plane.s34_etf_ishares_archive_bootstrap_runner --run-id s34-ishares-bootstrap-repair --start-date 2024-02-01 --end-date 2024-02-29`
