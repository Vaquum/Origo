# OKX Spot Trades Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S8, S18 (API v0.1.12)

## Purpose and scope
- User-facing reference for `okx_spot_trades` in Raw API query/export paths.
- Scope includes native and `aligned_1s` usage, event-driven serving semantics, provenance semantics, and guardrails.

## Inputs and outputs with contract shape
- Query:
  - endpoint: `POST /v1/raw/query`
  - `sources=["okx_spot_trades"]`
  - `mode`: `native | aligned_1s`
  - one window selector: `time_range | n_rows | n_random`
- Export:
  - endpoint: `POST /v1/raw/export`
  - `dataset="okx_spot_trades"`
  - `mode`: `native | aligned_1s`
  - `format`: `parquet | csv`
- Output envelopes:
  - query: `mode`, `source`, `row_count`, `schema`, `freshness`, `warnings`, `rows`
  - export status: `status`, optional `artifact` and terminal error metadata

## Data definitions (field names, types, units, timezone, nullability)
- Native fields:
  - `instrument_name` string
  - `trade_id` integer
  - `side` string (`buy|sell`)
  - `price` float
  - `size` float (base quantity)
  - `quote_quantity` float (`price * size`)
  - `timestamp` integer epoch milliseconds UTC
  - `datetime` UTC timestamp
  - lineage fields:
    - `event_id` UUID
    - `source_offset_or_equivalent` string
    - `source_event_time_utc` nullable UTC timestamp
    - `ingested_at_utc` UTC timestamp
- Aligned fields (`aligned_1s`):
  - `aligned_at_utc` UTC second timestamp
  - `open_price`, `high_price`, `low_price`, `close_price`
  - `quantity_sum`, `quote_volume_sum`, `trade_count`

## Source/provenance and freshness semantics
- Source of truth is first-party OKX daily trade files resolved from OKX API.
- Ingest preserves source checksum metadata per day (`zip_sha256`, `csv_sha256`, `Content-MD5`) in proof artifacts.
- Native and aligned serving paths are event-driven from canonical events as of Slice 18:
  - native: `canonical_okx_spot_trades_native_v1`
  - aligned: `canonical_aligned_1s_aggregates` (`view_id=aligned_1s_raw`, `view_version=1`)
- Canonical source payload fidelity is anchored by `payload_raw` and `payload_sha256_raw`.
- Aligned responses include freshness metadata and may emit stale warnings.

## Failure modes, warnings, and error codes
- Status map:
  - query: `200`, `404`, `409`, `503`
  - export submit: `202`
- Guardrail behavior:
  - rights matrix + legal artifact required for hosted query/export access
  - `strict=true` fails when warnings exist or mutable window constraints are violated
  - aligned runtime fails loudly on canonical aligned table/schema contract drift
- Common warning codes:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`

## Determinism/replay notes
- Slice-8 fixed-window proof artifacts:
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p1-acceptance.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p2-aligned-acceptance.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p3-determinism.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/baseline-fixture-2024-01-01_2024-01-02.json`
- Slice-18 event-sourcing proof artifacts:
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p1-acceptance.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p2-parity.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p3-determinism.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p4-exactly-once-ingest.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p5-no-miss-completeness.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/proof-s18-p6-raw-fidelity-precision.json`
  - `spec/slices/slice-18-okx-event-sourcing-port/baseline-fixture-2024-01-04_2024-01-04.json`

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
  - `{ "mode":"native", "sources":["okx_spot_trades"], "fields":["trade_id","timestamp","price","size","side"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Aligned query:
  - `{ "mode":"aligned_1s", "sources":["okx_spot_trades"], "fields":["aligned_at_utc","open_price","close_price","trade_count"], "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
- Native export:
  - `{ "mode":"native", "format":"csv", "dataset":"okx_spot_trades", "time_range":["2024-01-01T16:00:00Z","2024-01-02T16:00:00Z"], "strict":false }`
