# Slice 2 Developer: Raw Export API

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S2 (`S2-C1` to `S2-C6`, `S2-G1` to `S2-G3`)

## Purpose and scope
- Defines the native raw export API flow for `POST /v1/raw/export` and `GET /v1/raw/export/{export_id}`.
- Scope includes `native` and `aligned_1s` modes for Binance, ETF, and FRED datasets through Dagster-dispatched async export jobs.

## Inputs and outputs
- Input model: `RawExportRequest` in `api/origo_api/schemas.py`.
- Submit output model: `RawExportSubmitResponse`.
- Status output model: `RawExportStatusResponse`.
- Header contract: `X-API-Key` required for internal auth.

## Data definitions
- `mode`: `native | aligned_1s`.
- `format`: `parquet | csv`.
- `dataset`: `spot_trades | spot_agg_trades | futures_trades | etf_daily_metrics | fred_series_metrics`.
- Window selector: exactly one of `month_year | n_rows | n_random | time_range`.
- Status lifecycle: `queued | running | succeeded | failed`.
- Artifact metadata: `uri`, `row_count`, `checksum_sha256`.
- Failure metadata: `error_code`, `error_message` from `error.json` when present.

## Source, provenance, freshness
- Export content is sourced from ClickHouse through native query core (`SQL -> Arrow -> Polars`).
- Dagster run tags carry dataset/mode/format/request payload metadata.
- Freshness is bounded by Binance ingestion completion in ClickHouse.

## Failure modes and status mapping
- `404`: export id not found.
- `409`: auth/rights/contract failures (`AUTH_INVALID_API_KEY`, rights gate codes, `EXPORT_AUTH_TOKEN_UNSUPPORTED`).
- `503`: Dagster dispatch/status problems, queue backpressure, audit-write failures, artifact metadata read failures.
- `202`: submit accepted (`POST /v1/raw/export`).
- `200`: status payloads for active and terminal exports, including classified terminal failure payloads.

## Determinism and replay notes
- Deterministic export checks for fixed windows are recorded in Slice 2 proof artifacts.
- Export parity against query output is recorded in `spec/slices/slice-2-raw-export-native/parity-proof.json`.

## Environment and required config
- `ORIGO_INTERNAL_API_KEY`
- `ORIGO_QUERY_MAX_CONCURRENCY`
- `ORIGO_QUERY_MAX_QUEUE`
- `ORIGO_EXPORT_MAX_CONCURRENCY`
- `ORIGO_EXPORT_MAX_QUEUE`
- `ORIGO_DAGSTER_GRAPHQL_URL`
- `ORIGO_DAGSTER_REPOSITORY_NAME`
- `ORIGO_DAGSTER_LOCATION_NAME`
- `ORIGO_DAGSTER_EXPORT_JOB_NAME`
- `ORIGO_EXPORT_ROOT_DIR`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_EXPORT_AUDIT_LOG_PATH`
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)

## Minimal example
- Submit:
  - `POST /v1/raw/export`
  - `X-API-Key: <internal key>`
  - body: `{ \"mode\": \"native\", \"format\": \"parquet\", \"dataset\": \"spot_trades\", \"month_year\": [8, 2017], \"strict\": false }`
- Poll:
  - `GET /v1/raw/export/<export_id>`
  - returns lifecycle status and artifact metadata when available.
