# Slice 8 Developer: OKX Query, Aligned, and Guardrails

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S8 (`S8-C3`, `S8-C4`, `S8-P2`, `S8-P3`, `S8-G1`, `S8-G3`)
- Version reference: `Origo API v0.1.3`

## Purpose and scope
- Defines how `okx_spot_trades` is exposed through native query, `aligned_1s` query, and raw export.
- Scope covers planner contracts, allowed fields, rights/legal gates, strict/warnings/freshness behavior, and audit parity.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
  - `sources=["okx_spot_trades"]`
  - modes: `native` and `aligned_1s`
  - one window selector: `time_range | n_rows | n_random`
- Export endpoint: `POST /v1/raw/export`
  - `dataset="okx_spot_trades"`
  - modes: `native | aligned_1s`
  - formats: `parquet | csv`
- Response/export contract follows existing wide-row and artifact metadata envelopes.

## Data definitions (field names, types, units, timezone, nullability)
- Native allowlist fields:
  - `instrument_name`, `trade_id`, `side`, `price`, `size`, `quote_quantity`, `timestamp`, `datetime`
- Aligned allowlist fields:
  - `aligned_at_utc`, `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`
- Aligned aggregation:
  - 1-second bucket over UTC-normalized `datetime`
  - open/close via deterministic `argMin/argMax` by `trade_id`

## Source/provenance and freshness semantics
- Native reads from `okx_spot_trades` ClickHouse table.
- Aligned reads are derived directly from native rows (no third-party intermediaries).
- Freshness metadata for aligned mode is produced by existing aligned freshness guardrail flow.

## Failure modes, warnings, and error codes
- Rights/legal fail-closed behavior:
  - rights matrix entry required (`contracts/source-rights-matrix.json`)
  - hosted serving/export requires legal signoff artifact (`contracts/legal/okx-hosted-allowed.md`)
- Query/export strict behavior:
  - `strict=true` rejects mutable windows and warning-bearing responses
- Aligned warning and status/error mapping parity:
  - warning codes and `200/202/404/409/503` maps are shared with existing aligned sources
- Cleanup errors in OKX aligned query client close are surfaced (no silent swallow).

## Determinism/replay notes
- Determinism proof artifacts:
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p2-aligned-acceptance.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p3-determinism.json`
- Replay tests:
  - `tests/replay/test_native_query_replay.py` includes deterministic SQL compile checks for OKX native and aligned paths.

## Environment variables and required config
- Query/export runtime:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_QUERY_MAX_CONCURRENCY`
  - `ORIGO_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
  - `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
- Rights/audit:
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_EXPORT_AUDIT_LOG_PATH`
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query:
  - `{ "mode":"native", "sources":["okx_spot_trades"], "fields":["trade_id","timestamp","price","size","side"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Aligned query:
  - `{ "mode":"aligned_1s", "sources":["okx_spot_trades"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Native export:
  - `{ "mode":"native", "format":"parquet", "dataset":"okx_spot_trades", "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
