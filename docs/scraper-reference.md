# Scraper Platform Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S3, S4 (platform v0.1)

## Purpose and scope
- User-facing reference for scraper-derived capabilities in Origo.
- Scope includes generic scraper interfaces, supported fetch/parse modes, and normalized output behavior.

## Inputs and outputs with contract shape
- Adapter interface methods:
  - `discover_sources`
  - `fetch`
  - `parse`
  - `normalize`
- Pipeline input:
  - source descriptors and runtime context
- Pipeline output:
  - raw artifact persistence records
  - normalized long-metric records staged in ClickHouse
  - per-source run status and counts

## Data definitions (fields, types, units, timezone, nullability)
- Raw artifact fields:
  - `artifact_id` (string, non-null)
  - `content_sha256` (string, non-null)
  - `artifact_format` (`html|json|csv|pdf|binary`)
  - `fetched_at_utc` (UTC timestamp, non-null)
- Normalized long-metric core fields:
  - `metric_id`, `source_id`, `metric_name`, `metric_unit`
  - value channel fields (`string|int|float|bool`, nullable by metric)
  - `observed_at_utc`, `dimensions_json`, `provenance_json`, `ingested_at_utc`
- Time semantics are UTC across fetch/parse/normalize outputs.

## Source/provenance and freshness semantics
- Provenance is mandatory from normalized records back to raw artifacts.
- Source-origin and rights checks execute before serving/export enablement.
- Freshness follows source publish cadence and successful ingestion timestamp.

## Failure modes, warnings, and error codes
- Typed scraper failure classes include:
  - `SCRAPER_DISCOVER_ERROR`
  - `SCRAPER_FETCH_ERROR`
  - `SCRAPER_PARSE_ERROR`
  - `SCRAPER_NORMALIZE_ERROR`
  - `SCRAPER_STAGING_PERSIST_ERROR`
  - `SCRAPER_OBJECT_STORE_ERROR`
  - `SCRAPER_AUDIT_WRITE_ERROR`
- Missing rights state fails closed.
- Exceptions are surfaced as hard failures; no silent fallback behavior.

## Determinism/replay notes
- Replay artifacts and manifests are maintained per slice under `spec/slices/`.
- Artifact IDs and metric IDs are deterministic for identical inputs.
- Deterministic parser-first policy applies to PDF path.

## Environment variables and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_OBJECT_STORE_ENDPOINT_URL`
- `ORIGO_OBJECT_STORE_ACCESS_KEY_ID`
- `ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY`
- `ORIGO_OBJECT_STORE_BUCKET`
- `ORIGO_OBJECT_STORE_REGION`
- `ORIGO_SCRAPER_FETCH_MAX_ATTEMPTS`
- `ORIGO_SCRAPER_FETCH_BACKOFF_INITIAL_SECONDS`
- `ORIGO_SCRAPER_FETCH_BACKOFF_MULTIPLIER`
- `ORIGO_SCRAPER_AUDIT_LOG_PATH`
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- ETF capability is implemented on top of this platform:
  - Source key set includes `etf_ishares_ibit_daily` through `etf_hashdex_defi_daily`.
- Normalized records become queryable through raw API source `etf_daily_metrics`.
