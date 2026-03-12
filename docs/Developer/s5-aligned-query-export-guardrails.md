# Slice 5 Aligned Query and Export Guardrails

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S5 (`S5-C1..S5-G4`)
- Version reference: `control-plane v1.2.35`

## Purpose and scope
- Define implementation contracts for `mode=aligned_1s` query and export paths.
- Cover planner dispatch, forward-fill semantics, response/export shapes, and active guardrails.
- Scope excludes Slice 5 documentation closeout tasks `S5-G5/S5-G6` and any Slice 6 source additions.

## Inputs and outputs
- Query API input (`POST /v1/raw/query`):
  - `mode`: `native | aligned_1s`
  - `sources`: one source key in current capability:
    - `binance_spot_trades | etf_daily_metrics | fred_series_metrics`
  - `fields`: optional projection
  - exactly one window selector (`time_range | n_rows | n_random`)
  - `filters`: optional projection filter clauses (`eq|ne|gt|gte|lt|lte|in|not_in`)
  - `strict`: fail on warnings when `true`
- Query API output:
  - envelope: `{mode, source, row_count, schema, freshness, warnings, rows}`
  - aligned mode always returns `mode=aligned_1s`
- Export API input (`POST /v1/raw/export`):
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
  - `dataset`, `fields`, one window selector, `strict`
- Export output:
  - async status lifecycle (`queued | running | succeeded | failed`)
  - artifact metadata: `{format, uri, row_count, checksum_sha256}`

## Data definitions
- Binance aligned rows (1-second buckets):
  - `aligned_at_utc` (UTC second)
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`
- ETF aligned observation rows:
  - `aligned_at_utc`, `source_id`, `metric_name`
  - value channels: `metric_value_string|int|float|bool`
  - lineage: `dimensions_json`, `provenance_json`, `latest_ingested_at_utc`, `records_in_bucket`
- ETF forward-fill interval rows:
  - same core metric columns, plus:
  - `valid_from_utc`, `valid_to_utc_exclusive`

## Provenance and freshness semantics
- Provenance fields are carried from `etf_daily_metrics_long` using `argMax` selection semantics.
- ETF prior-state selection for forward fill is ordered by `(observed_at_utc, ingested_at_utc)`.
- Aligned freshness metadata:
  - computed from newest row timestamp candidate (`valid_to_utc_exclusive` then `aligned_at_utc`)
  - response payload: `freshness.as_of_utc`, `freshness.lag_seconds`
  - warning `ALIGNED_FRESHNESS_STALE` emitted when lag exceeds threshold.

## Failure modes, warnings, and error codes
- Query warnings:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`
  - ETF quality warnings (`ETF_DAILY_*`)
- Query errors:
  - `STRICT_MODE_WARNING_FAILURE`
  - `ALIGNED_QUERY_QUEUE_LIMIT_REACHED`
  - existing query contract/runtime/backend error codes
- Export rights/audit:
  - rights fail-closed; ingest-only sources reject export (`EXPORT_RIGHTS_INGEST_ONLY`)
  - export submit rejections are audit logged with `mode` in payload.

## Determinism and replay notes
- Fixed-window aligned queries are deterministic (proof: `proof-s5-p2-determinism.json`).
- Forward-fill UTC boundary transitions are deterministic and validated (proof: `proof-s5-p3-boundary.json`).
- Aligned exports (parquet/csv) parity-validated against query output (proof: `capability-proof-s5-c6-c7-aligned-export.json`).

## Environment variables
- Required query/env guardrails:
  - `ORIGO_QUERY_MAX_CONCURRENCY`
  - `ORIGO_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
  - `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
- Required rights/audit env:
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_EXPORT_AUDIT_LOG_PATH`
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)
  - `ORIGO_ETF_QUERY_SERVING_STATE`

## Minimal examples
- Aligned query (Binance):
  - `POST /v1/raw/query` body:
    - `{ "mode": "aligned_1s", "sources": ["binance_spot_trades"], "time_range": ["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"], "fields": ["aligned_at_utc","open_price","close_price","trade_count"], "strict": false }`
- Aligned query (ETF forward fill):
  - `{ "mode": "aligned_1s", "sources": ["etf_daily_metrics"], "time_range": ["2026-03-05T12:00:00Z","2026-03-07T12:00:00Z"], "fields": ["source_id","metric_name","valid_from_utc","valid_to_utc_exclusive","metric_value_string"], "strict": false }`
- Aligned export:
  - `POST /v1/raw/export` with `mode=aligned_1s` and `format=parquet|csv`
