## What was done
- Implemented OKX daily-file ingest capability from original first-party sources:
  - daily URL resolution via OKX API
  - zip/csv checksum capture and verification
  - strict CSV contract parsing
  - deterministic ClickHouse load into `okx_spot_trades`
- Added migration-backed schema:
  - `control-plane/migrations/sql/0008__create_okx_spot_trades.sql`
- Integrated OKX into platform paths:
  - native query planner and API contracts
  - `aligned_1s` query planner and export paths
  - rights/legal gates and raw export dispatch
  - exchange integrity suite profile for OKX
- Produced S8 proof artifacts and baseline fixture under `spec/slices/slice-8-okx-spot-trades-aligned/`.
- Notable implementation deviation:
  - daily partition day had to be interpreted as OKX source-day UTC+8 window; ingest delete/verify filters were corrected to explicit timestamp-window bounds.

## Current state
- Dataset `okx_spot_trades` is queryable in both `native` and `aligned_1s` modes.
- Export supports `okx_spot_trades` in both `parquet` and `csv` through existing async export lifecycle.
- Rights matrix includes OKX as `Hosted Allowed` with legal signoff artifact.
- Integrity checks enforce schema/type/sequence/anomaly rules for OKX rows before insert.
- Slice-8 proof artifacts include fixed-window acceptance, replay determinism, source checksums, and baseline fixture fingerprints.

## Watch out
- OKX daily files are source-local UTC+8 windows; UTC day assumptions will create false mismatches.
- Local host ClickHouse HTTP port may not map to Docker service unless compose ports are explicitly published; proof scripts may need to run inside Docker network context.
- Strict cleanup semantics are enforced in new OKX paths; client disconnect failures can now fail runs when no active exception is in flight.
