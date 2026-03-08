## What was done
- Implemented first-party Bybit daily ingest capability and migration-backed storage:
  - `control-plane/migrations/sql/0009__create_bybit_spot_trades.sql`
  - `control-plane/origo_control_plane/assets/daily_bybit_spot_trades_to_origo.py`
- Integrated `bybit_spot_trades` in native + `aligned_1s` query and export paths:
  - planner modules and routing updates in `origo/query/`, `origo/data/_internal/generic_endpoints.py`, `api/origo_api/`, and `control-plane/origo_control_plane/jobs/raw_export_native.py`.
- Added rights/legal + integrity guardrails:
  - `contracts/source-rights-matrix.json`
  - `contracts/legal/bybit-hosted-allowed.md`
  - monotonic-time + sequence/anomaly enforcement via exchange integrity suite.
- Produced S11 proof artifacts and baseline fixture under `spec/slices/slice-11-bybit-spot-trades-aligned/`.
- Deviation from plan:
  - initial ingest proof execution silently did nothing because one-off `docker run` command lacked `-i` for heredoc input.
  - fixed root cause by using `docker run -i`; re-ran ingest/proof and captured final artifacts.

## Current state
- Dataset `bybit_spot_trades` is available in:
  - `POST /v1/raw/query` (`native` + `aligned_1s`)
  - `POST /v1/raw/export` (`native` + `aligned_1s`, `csv|parquet`)
- Rights state is `Hosted Allowed` with required legal artifact present.
- Proof window is fixed for source days `2024-01-01` and `2024-01-02`, with deterministic replay match set to `true`.
- Baseline fixture includes source checksums, row counts, offsets, volume sums, and row hashes for run-1/run-2.

## Watch out
- Bybit source files use second-level timestamps with fractional precision; ingest rounds to millisecond using `ROUND_HALF_UP`.
- `trade_id` is deterministic synthetic sequence per file row order, not source-native trade id; source-native id is stored as `trd_match_id`.
- Proof/ingest commands must run with Docker stdin attached (`-i`) when using heredoc payloads.
- If Bybit changes CSV header shape or drops `ETag`, ingest fails loudly by design.
