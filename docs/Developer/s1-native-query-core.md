# Slice 1 Developer: Native Query Core

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S1 (`S1-C1` to `S1-C6`)

## Purpose and scope
- Defines the internal native query kernel path: `ClickHouse SQL -> Arrow -> Polars`.
- Scope is Binance native datasets only: spot trades, spot agg trades, futures trades.

## Inputs and outputs
- Input contract: `NativeQuerySpec` in `origo/query/native_core.py`.
- Output contract: deterministic `polars.DataFrame` with optional UTC ISO datetime rendering.

## Data definitions
- `datetime`: UTC, millisecond precision (`DateTime64(3)` equivalent), nullable: no.
- `timestamp`: unsigned integer epoch (ms/micros normalized to canonical output), nullable: no.
- Dataset-specific IDs:
  - `trade_id` (spot trades)
  - `agg_trade_id` (spot agg trades)
  - `futures_trade_id` (futures trades)

## Source, provenance, freshness
- Source tables in ClickHouse under configured database:
  - `binance_trades`
  - `binance_agg_trades`
  - `binance_futures_trades`
- Table DDL is migration-managed only (`control-plane/migrations/sql`); legacy Dagster `create_*` assets are intentionally disabled.
- Freshness is inherited from loaded Binance file cadence.

## Failure modes and errors
- Invalid query contract/window: `ValueError`.
- Missing env/config: `RuntimeError` from env contract loaders.
- Backend query/runtime failures bubble to API adapter for status mapping.

## Determinism and replay notes
- Deterministic ordering by `datetime` + ID for bounded windows.
- Replay fixtures and deterministic proofs stored under `spec/slices/slice-1-raw-query-native/`.

## Environment and required config
- Required env vars (effective):
  - `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`

## Minimal example
- See `origo/query/binance_native.py`:
  - build `NativeQuerySpec`
  - call `execute_native_query(...)`
  - receive `polars.DataFrame`
