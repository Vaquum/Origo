# FRED Dataset Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice/version reference: S6 (API v0.1.0)

## Purpose and scope
- User-facing reference for FRED data in Origo.
- Scope covers native and aligned query behavior, field taxonomy, freshness semantics, and guardrails.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
- Source key: `fred_series_metrics`
- Request shape:
  - `mode`: `native | aligned_1s`
  - `sources`: must include `["fred_series_metrics"]`
  - one window selector: `time_range | n_rows | n_random`
  - optional `fields`, `filters`, `strict`, `auth_token`
- Response shape:
  - `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`

## Data definitions (fields, types, units, timezone, nullability)
- Core identity:
  - `source_id` (string, non-null)
  - `metric_name` (string, non-null)
  - `metric_unit` (string, non-null)
- Values:
  - `metric_value_float` (float, nullable by metric type)
  - `metric_value_string` (string, nullable by metric type)
  - `metric_value_int`, `metric_value_bool` (nullable, dataset-specific)
- Time/provenance:
  - `observed_at_utc` (UTC timestamp, non-null)
  - `aligned_at_utc` (UTC timestamp in aligned mode)
  - `valid_from_utc`, `valid_to_utc_exclusive` (aligned forward-fill intervals)
  - `provenance_json` (JSON string, non-null)
  - `ingested_at_utc` or `latest_ingested_at_utc` (UTC timestamp)

## Source/provenance and freshness semantics
- Data is fetched directly from original FRED API endpoints.
- Freshness warnings use source publish metadata (`provenance_json.last_updated_utc`), not ingest time.
- Source and rights state are enforced before serving.

## Failure modes, warnings, and error codes
- FRED warning codes:
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- Generic warning codes may also appear:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`
- `strict=true` fails on warnings with `409`.
- Common status/error classes:
  - `404` no data
  - `409` rights/contract/strict conflict
  - `503` backend/runtime/warning-evaluation/audit failure

## Determinism/replay notes
- Determinism proofs and reproducibility artifacts:
  - `spec/slices/slice-6-fred-integration/proof-s6-p1-acceptance.json`
  - `spec/slices/slice-6-fred-integration/proof-s6-p2-determinism.json`
  - `spec/slices/slice-6-fred-integration/proof-s6-p3-metadata-version-reproducibility.json`

## Environment variables and required config
- `FRED_API_KEY`
- `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`
- `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
- `ORIGO_FRED_QUERY_SERVING_STATE`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query:
  - `{ "mode":"native", "sources":["fred_series_metrics"], "time_range":["2024-01-01T00:00:00Z","2024-04-01T00:00:00Z"], "fields":["source_id","metric_name","observed_at_utc","metric_value_float"], "strict":false }`
- Aligned query:
  - `{ "mode":"aligned_1s", "sources":["fred_series_metrics"], "n_rows":1000, "fields":["aligned_at_utc","source_id","metric_name","metric_value_float"], "strict":false }`
