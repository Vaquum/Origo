# Slice 11 Developer: Bybit Daily Ingest Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice reference: S11 (`S11-C1`, `S11-C2`, `S11-P1`, `S11-P4`, `S11-G2`)
- Version reference: `control-plane v1.2.54`

## Purpose and scope
- Defines the strict ingest contract for `bybit_spot_trades` daily files into ClickHouse.
- Scope covers source URL contract, checksum capture, CSV parsing, deterministic writes, and integrity checks.

## Inputs and outputs with contract shape
- Dagster asset: `insert_daily_bybit_spot_trades_to_origo`.
- Partition input: `YYYY-MM-DD` UTC source day.
- Source flow:
  - GET `https://public.bybit.com/trading/BTCUSDT/BTCUSDTYYYY-MM-DD.csv.gz`
- Required source contract:
  - non-empty gzip payload
  - exact CSV header:
    - `timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional`
  - non-empty `ETag` header
- Output payload:
  - `date`, `source_filename`, `source_url`, `rows_inserted`
  - `gzip_sha256`, `csv_sha256`, `source_etag`
  - `data_verification` (`min_trade_id`, `max_trade_id`, `avg_price`, `id_uniqueness_check`)

## Data definitions (field names, types, units, timezone, nullability)
- ClickHouse table: `bybit_spot_trades` (`control-plane/migrations/sql/0009__create_bybit_spot_trades.sql`).
- Columns:
  - `symbol` `LowCardinality(String)` (non-null)
  - `trade_id` `UInt64` synthetic deterministic row sequence (non-null)
  - `trd_match_id` `String` source match id (non-null)
  - `side` `LowCardinality(String)` (`buy|sell`, non-null)
  - `price` `Float64` (non-null)
  - `size` `Float64` (non-null)
  - `quote_quantity` `Float64` (non-null)
  - `timestamp` `UInt64` epoch milliseconds UTC (non-null)
  - `datetime` `DateTime64(3, 'UTC')` (non-null)
  - `tick_direction` `LowCardinality(String)` (non-null)
  - `gross_value` `Float64` (non-null)
  - `home_notional` `Float64` (non-null)
  - `foreign_notional` `Float64` (non-null)

## Source/provenance and freshness semantics
- Source of truth is first-party Bybit-hosted daily files.
- Provenance-critical checksums are captured per day (`gzip_sha256`, `csv_sha256`, `ETag`).
- Ingest enforces strict UTC day-window membership for all rows.
- Aligned freshness is computed downstream from latest native `datetime`.

## Failure modes, warnings, and error codes
- Hard failures (no fallback):
  - missing/empty source payload
  - missing/empty `ETag`
  - gzip decompression failure
  - header mismatch or row-shape mismatch
  - timestamp parse/window violations
  - invalid symbol/side/IDs
  - row-count mismatch after insert
  - integrity suite failure (schema/type, sequence, monotonic-time, anomalies)
- Disconnect failures are surfaced:
  - if cleanup runs under an active exception, disconnect failure is attached as a note
  - if cleanup fails without an active exception, ingest fails loudly

## Determinism/replay notes
- Proof artifacts and baseline fixture:
  - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p1-acceptance.json`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p3-determinism.json`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p4-source-checksums.json`
  - `spec/slices/slice-11-bybit-spot-trades-aligned/baseline-fixture-2024-01-01_2024-01-02.json`
- Replay contract: same source fixtures must produce identical fingerprints across repeated runs.

## Environment variables and required config
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

## Minimal examples
- Run one partition asset (Dagster):
  - partition: `2024-01-02`
  - expected: non-zero `rows_inserted` and checksum fields present.
