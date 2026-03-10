# Bybit Spot Trades Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S19 (API v0.1.13)

## Purpose and scope
- User-facing reference for `bybit_spot_trades` in Raw API query/export paths.
- Scope includes native and `aligned_1s` usage, field taxonomy, provenance semantics, and guardrails.

## Inputs and outputs with contract shape
- Query:
  - endpoint: `POST /v1/raw/query`
  - `sources=["bybit_spot_trades"]`
  - `mode`: `native | aligned_1s`
  - one window selector: `time_range | n_rows | n_random`
- Export:
  - endpoint: `POST /v1/raw/export`
  - `dataset="bybit_spot_trades"`
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
- Output envelopes:
  - query: `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`
  - export status: `status`, optional `artifact` and terminal error metadata

## Data definitions (field names, types, units, timezone, nullability)
- Native fields:
  - `symbol` string (`BTCUSDT`)
  - `trade_id` integer parsed from `trd_match_id` suffix (`m-<digits>`)
  - `trd_match_id` string source match id (`m-<digits>`)
  - `side` string (`buy|sell`)
  - `price` float
  - `size` float (base quantity)
  - `quote_quantity` float (quote quantity)
  - `timestamp` integer epoch milliseconds UTC
  - `datetime` UTC timestamp
  - `tick_direction` string
  - `gross_value` float
  - `home_notional` float
  - `foreign_notional` float
- Aligned fields (`aligned_1s`):
  - `aligned_at_utc` UTC second timestamp
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source of truth is first-party Bybit daily trade files.
- Ingest writes immutable canonical events and serves from canonical projection tables.
- Ingest preserves source checksum metadata per day (`zip_sha256`, `csv_sha256`).
- Canonical source bytes are preserved via `payload_raw` and `payload_sha256_raw`.
- Aligned rows are derived from `canonical_aligned_1s_aggregates`.
- Aligned responses include freshness metadata and may emit stale warnings.

## Failure modes, warnings, and error codes
- Status map:
  - query: `200`, `404`, `409`, `503`
  - export submit: `202`
- Guardrail behavior:
  - rights matrix + legal artifact required for hosted query/export access
  - `strict=true` fails when warnings exist or mutable window constraints are violated
  - no-miss reconciliation gaps fail loudly and quarantine the affected partition
  - non-canonical Bybit identity shape (`trd_match_id` not `m-<digits>`) fails ingest
- Common warning codes:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`

## Determinism/replay notes
- Slice-19 fixed-window proof artifacts:
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p1-acceptance.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p2-parity.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p3-determinism.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p4-exactly-once-ingest.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p5-no-miss-completeness.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g1-exchange-integrity.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g2-api-guardrails.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g3-reconciliation-quarantine.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/proof-s19-g4-raw-fidelity-precision.json`
  - `spec/slices/slice-19-bybit-event-sourcing-port/baseline-fixture-2024-01-04_2024-01-04.json`

## Environment variables and required config
- `ORIGO_INTERNAL_API_KEY`
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
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
  - `{ "mode":"native", "format":"csv", "dataset":"bybit_spot_trades", "time_range":["2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"], "strict":false }`
