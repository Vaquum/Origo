# Raw Export API Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S2, S5, S6, S8, S11, S13 (API v0.1.6)

## Purpose and scope
- This is the user-facing reference for asynchronous raw exports.
- Scope includes `native` and `aligned_1s` export flows with `parquet` and `csv`.

## Inputs and outputs with contract shape
- Endpoints:
  - `POST /v1/raw/export`
  - `GET /v1/raw/export/{export_id}`
- Required header: `X-API-Key`
- Submit request contract:
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
  - `dataset`: `spot_trades | spot_agg_trades | futures_trades | okx_spot_trades | bybit_spot_trades | etf_daily_metrics | fred_series_metrics | bitcoin_block_headers | bitcoin_block_transactions | bitcoin_mempool_state | bitcoin_block_fee_totals | bitcoin_block_subsidy_schedule | bitcoin_network_hashrate_estimate | bitcoin_circulating_supply`
  - `fields`: optional field projection
  - Exactly one window selector:
    - `month_year`
    - `time_range`
    - `n_rows`
    - `n_random`
  - `strict`: boolean
  - `auth_token`: currently rejected for export dispatch in this slice
- Submit response (`202`):
  - `export_id`, `status`, `submitted_at`, `status_path`
- Status response (`200`):
  - `export_id`, `status`, `mode`, `format`, `dataset`, `submitted_at`, `updated_at`
  - optional `artifact` with `{format, uri, row_count, checksum_sha256}`
  - optional `error_code`, `error_message`

## Data definitions (fields, types, units, timezone, nullability)
- Export artifact metadata:
  - `uri`: absolute path string (non-null on success)
  - `row_count`: integer >= 0 (non-null on success)
  - `checksum_sha256`: hex digest string (non-null on success)
- Status lifecycle:
  - `queued`, `running`, `succeeded`, `failed`
- Exported row field definitions follow dataset taxonomy in `docs/data-taxonomy.md`.

## Source/provenance and freshness semantics
- Export payload comes from canonical ClickHouse data queried via Origo query core.
- Dagster run tags preserve mode/format/dataset and request hash context.
- Export freshness follows the latest successful ingestion for requested sources.
- `mode=aligned_1s` supports only aligned-capable datasets:
  - `spot_trades`
  - `spot_agg_trades`
  - `futures_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
  - `etf_daily_metrics`
  - `fred_series_metrics`
  - `bitcoin_block_fee_totals`
  - `bitcoin_block_subsidy_schedule`
  - `bitcoin_network_hashrate_estimate`
  - `bitcoin_circulating_supply`

## Failure modes, warnings, and error codes
- `404`: export ID not found
- `409`: auth/rights/contract failure
- `503`: dispatch/backpressure/runtime/audit/metadata failure
- Common failure codes:
  - `EXPORT_QUEUE_LIMIT_REACHED`
  - `EXPORT_DISPATCH_ERROR`
  - `EXPORT_RIGHTS_*`
  - `EXPORT_AUDIT_WRITE_ERROR`
  - `EXPORT_ARTIFACT_METADATA_ERROR`
  - `EXPORT_RUN_FAILED` / `EXPORT_RUN_CANCELED`

## Determinism/replay notes
- Export determinism and query/export parity are validated in slice proof artifacts:
  - `spec/slices/slice-2-raw-export-native/`
  - `spec/slices/slice-5-raw-query-aligned-1s/`
  - `spec/slices/slice-8-okx-spot-trades-aligned/`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/`
  - `spec/slices/slice-13-bitcoin-core-signals/`

## Environment variables and required config
- `ORIGO_INTERNAL_API_KEY`
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

## Minimal examples
- Submit native export:
  - `{ "mode":"native", "format":"parquet", "dataset":"spot_trades", "month_year":[8,2017], "strict":false }`
- Submit OKX native export:
  - `{ "mode":"native", "format":"csv", "dataset":"okx_spot_trades", "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Submit Bybit native export:
  - `{ "mode":"native", "format":"csv", "dataset":"bybit_spot_trades", "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
- Submit Bitcoin native export:
  - `{ "mode":"native", "format":"parquet", "dataset":"bitcoin_block_headers", "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Submit Bitcoin aligned export:
  - `{ "mode":"aligned_1s", "format":"csv", "dataset":"bitcoin_block_fee_totals", "time_range":["2024-04-20T00:00:00Z","2024-04-22T00:00:00Z"], "strict":false }`
- Submit aligned export:
  - `{ "mode":"aligned_1s", "format":"csv", "dataset":"etf_daily_metrics", "time_range":["2026-03-05T00:00:00Z","2026-03-07T00:00:00Z"], "strict":false }`
- Poll status:
  - `GET /v1/raw/export/<export_id>`
