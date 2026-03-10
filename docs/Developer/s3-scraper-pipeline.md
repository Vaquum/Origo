# Slice 3 Developer: Scraper Pipeline and Guardrails

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S3 (`S3-C4` to `S3-C12`, `S3-G1` to `S3-G4`)

## Purpose and scope
- Documents runtime pipeline for fetch -> parse -> normalize -> persist.
- Covers guardrail behavior: error taxonomy, retry hooks, rights classification hook, and immutable audit events.

## Inputs and outputs
- Pipeline entrypoint: `origo.scraper.run_scraper_pipeline`.
- Input:
  - `ScraperAdapter`
  - `ScrapeRunContext`
- Output:
  - `PipelineRunResult` with per-source `PipelineSourceResult`.

## Data definitions
- Fetch modules:
  - `fetch_http_source` (HTTP-first)
  - `fetch_browser_source` (browser fallback)
- Parse modules:
  - `parse_html_artifact`
  - `parse_csv_artifact`
  - `parse_json_artifact`
  - `parse_pdf_artifact`
- Normalize module:
  - `normalize_parsed_records`
- Persistence modules:
  - `persist_raw_artifact` (S3-compatible object store)
  - `persist_normalized_records_to_clickhouse` (staging table)

## Source, provenance, freshness
- Rights classification hook runs per source before fetch.
- Normalized metrics include provenance references to source/artifact/parser.
- Object store manifest includes source URI, hash, run id, and fetch metadata.

## Failure modes and error codes
- Typed `ScraperError` codes include:
  - `SCRAPER_DISCOVER_ERROR`
  - `SCRAPER_RIGHTS_MISSING_STATE`
  - `SCRAPER_FETCH_ERROR`
  - `SCRAPER_PARSE_ERROR`
  - `SCRAPER_NORMALIZE_ERROR`
  - `SCRAPER_OBJECT_STORE_ERROR`
  - `SCRAPER_STAGING_PERSIST_ERROR`
  - `SCRAPER_RETRY_EXHAUSTED`
  - `SCRAPER_AUDIT_WRITE_ERROR`
  - `SCRAPER_RUN_ERROR`

## Determinism and replay notes
- Deterministic replay checks are recorded in `spec/slices/slice-3-generic-scraper/`.
- Audit log is append-only hash-chained JSONL and sequence-validated before append.
- Audit state sidecar (`<log>.state.json`) detects tamper/truncation across restarts.
- Missing rights state fails closed.

## Environment and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_SCRAPER_FETCH_MAX_ATTEMPTS`
- `ORIGO_SCRAPER_FETCH_BACKOFF_INITIAL_SECONDS`
- `ORIGO_SCRAPER_FETCH_BACKOFF_MULTIPLIER`
- `ORIGO_SCRAPER_AUDIT_LOG_PATH`
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)
- `ORIGO_OBJECT_STORE_ENDPOINT_URL`
- `ORIGO_OBJECT_STORE_ACCESS_KEY_ID`
- `ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY`
- `ORIGO_OBJECT_STORE_BUCKET`
- `ORIGO_OBJECT_STORE_REGION`
- `CLICKHOUSE_*` for staging writes

## Minimal example
- Instantiate adapter implementing `ScraperAdapter`.
- Build `ScrapeRunContext(run_id=..., started_at_utc=...)`.
- Call `run_scraper_pipeline(adapter=..., run_context=...)`.
- Inspect `PipelineRunResult` totals and per-source records.
