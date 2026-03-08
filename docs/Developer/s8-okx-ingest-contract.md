# Slice 8 Developer: OKX Daily Ingest Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S8 (`S8-C1`, `S8-C2`, `S8-P1`, `S8-P4`, `S8-G2`)
- Version reference: `control-plane v1.2.53`

## Purpose and scope
- Defines the strict ingest contract for `okx_spot_trades` daily files into ClickHouse.
- Scope covers source resolution, checksum verification, CSV parsing, deterministic writes, and integrity checks.

## Inputs and outputs with contract shape
- Dagster asset: `insert_daily_okx_spot_trades_to_origo`.
- Partition input: `YYYY-MM-DD` source day.
- Source flow:
  - POST `https://www.okx.com/priapi/v5/broker/public/trade-data/download-link`
  - GET returned first-party static zip URL.
- Output payload:
  - `date`, `source_filename`, `source_url`, `rows_inserted`
  - `zip_sha256`, `csv_sha256`, `source_content_md5`
  - `data_verification` (`min_trade_id`, `max_trade_id`, `avg_price`, `id_uniqueness_check`)

## Data definitions (field names, types, units, timezone, nullability)
- ClickHouse table: `okx_spot_trades` (`control-plane/migrations/sql/0008__create_okx_spot_trades.sql`).
- Columns:
  - `instrument_name` `String` (non-null)
  - `trade_id` `UInt64` (non-null)
  - `side` `String` (`buy|sell`, non-null)
  - `price` `Float64` (non-null)
  - `size` `Float64` (non-null)
  - `quote_quantity` `Float64` (non-null)
  - `timestamp` `UInt64` epoch milliseconds UTC (non-null)
  - `datetime` `DateTime64(3, 'UTC')` (non-null)
- Source day normalization:
  - OKX daily files are source-local UTC+8 windows.
  - Ingest delete/verify filters use the matching UTC window for the partition day.

## Source/provenance and freshness semantics
- Source of truth is original OKX-hosted daily zip files.
- Provenance-critical checksums are captured per day (`zip_sha256`, `csv_sha256`, `Content-MD5`).
- Freshness for downstream aligned queries is computed from latest native timestamp in queried output.

## Failure modes, warnings, and error codes
- Hard failures (no fallback):
  - non-zero OKX API code
  - missing/empty `Content-MD5`
  - checksum mismatch
  - unexpected zip members
  - CSV schema/shape/type mismatch
  - row-count mismatch after insert
  - integrity suite failure (schema/type, sequence, anomalies)
- Disconnect failures are surfaced:
  - if cleanup runs under an active exception, failure is attached as a note
  - if cleanup fails without an active exception, raise immediately

## Determinism/replay notes
- Proof artifacts and baseline fixture:
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p1-acceptance.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p3-determinism.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p4-source-checksums.json`
  - `spec/slices/slice-8-okx-spot-trades-aligned/baseline-fixture-2024-01-01_2024-01-02.json`
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
