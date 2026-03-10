# Slice 11 Developer: Bybit Query, Aligned, and Guardrails

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S11 (`S11-C3`, `S11-C4`, `S11-P2`, `S11-P3`, `S11-G1`, `S11-G3`)
- Version reference: `Origo API v0.1.4`

## Purpose and scope
- Defines how `bybit_spot_trades` is exposed through native query, `aligned_1s` query, and raw export.
- Scope covers planner contracts, allowed fields, rights/legal gates, strict/warnings/freshness behavior, and audit parity.

## Inputs and outputs with contract shape
- Query endpoint: `POST /v1/raw/query`
  - `sources=["bybit_spot_trades"]`
  - modes: `native` and `aligned_1s`
  - one window selector: `time_range | n_rows | n_random`
- Export endpoint: `POST /v1/raw/export`
  - `dataset="bybit_spot_trades"`
  - modes: `native | aligned_1s`
  - formats: `parquet | csv`
- Response/export contract follows existing wide-row and artifact metadata envelopes.

## Data definitions (field names, types, units, timezone, nullability)
- Native allowlist fields:
  - `symbol`, `trade_id`, `trd_match_id`, `side`, `price`, `size`, `quote_quantity`, `timestamp`, `datetime`, `tick_direction`, `gross_value`, `home_notional`, `foreign_notional`
- Aligned allowlist fields:
  - `aligned_at_utc`, `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`
- Aligned aggregation:
  - 1-second bucket over UTC-normalized `datetime`
  - open/close via deterministic `argMin/argMax` by `trade_id`

## Source/provenance and freshness semantics
- Native reads from `bybit_spot_trades` ClickHouse table.
- Aligned reads are derived directly from native rows.
- Freshness metadata for aligned mode is produced by existing aligned freshness guardrail flow.

## Failure modes, warnings, and error codes
- Rights/legal fail-closed behavior:
  - rights matrix entry required (`contracts/source-rights-matrix.json`)
  - hosted serving/export requires legal signoff artifact (`contracts/legal/bybit-hosted-allowed.md`)
- Query/export strict behavior:
  - `strict=true` rejects mutable windows and warning-bearing responses
- Aligned warning and status/error mapping parity:
  - warning codes and `200/202/404/409/503` maps are shared with existing aligned sources
- Cleanup errors in Bybit aligned query client close are surfaced (no silent swallow).

## Determinism/replay notes
- Determinism proof artifacts:
  - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p2-aligned-acceptance.json`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p3-determinism.json`
- Replay tests:
  - `tests/replay/test_native_query_replay.py` includes deterministic SQL compile checks for Bybit native and aligned paths.

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
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Native query:
  - `{ "mode":"native", "sources":["bybit_spot_trades"], "fields":["trade_id","timestamp","price","size","side","trd_match_id"], "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
- Aligned query:
  - `{ "mode":"aligned_1s", "sources":["bybit_spot_trades"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
- Native export:
  - `{ "mode":"native", "format":"parquet", "dataset":"bybit_spot_trades", "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
