## What was done
- Imported `tdw-control-plane` code into monorepo `control-plane` and applied TDW -> Origo naming changes across package/module/config metadata.
- Fixed migration breakpoints found during bring-up: ClickHouse table DDL compatibility and UTC timestamp conversion in daily Binance ingest.
- Added missing runtime dependencies needed by imported client/control-plane components.
- Enforced fail-fast compose configuration for ClickHouse password to avoid silent auth-disabled startup.
- Added deterministic dependency locking for control-plane with `uv.lock`, including frozen sync validation and lockfile-based Docker install path.
- Added SQL migration framework scaffold in `control-plane` (ordered SQL files, `schema_migrations` ledger, checksum validation, and strict migrate/status runner).
- Added env contract guardrail with root `.env.example`, fail-loud env resolvers, and removal of deployment-default ClickHouse values in active runtime paths.
- Closed `S0-G3` by introducing a shared immutable audit sink used by export/FRED/scraper audit writers with:
  - append-only hash-chained JSONL events
  - state sidecar continuity checks (`<log>.state.json`) for tamper/truncation detection
  - enforced retention policy contract (`ORIGO_AUDIT_LOG_RETENTION_DAYS >= 365`)
- Added `S0-G3` guardrail proof artifact: `guardrails-proof-s0-g3-immutable-audit.json`.
- Imported Limen client-side `data` module into `origo/data` (excluding `standard_bars.py`) as an intentional scope choice.
- Executed fixed-window ingest proof and deterministic replay for `2017-08-17` to `2017-08-19`; evidence captured in `baseline-fixture-2017-08-17_2017-08-19.json`.

## Current state
- Slice 0 capability and proof are partially complete and recorded in `spec/2-itemized-work-plan.md` (`S0-C1`, `S0-C3`, `S0-C4`, `S0-C5`, `S0-P1`, `S0-P2`, `S0-G1`, `S0-G4`, `S0-G5`, `S0-G6`).
- `control-plane` can create `origo` database/table and ingest Binance daily data for the tested window with deterministic replay results.
- Monorepo scaffolding folders now exist (`api`, `contracts`, `storage`, `docs`, `spec`), with control-plane and client code present.
- Migration audit template exists at `spec/templates/migration-audit-record.md`; this run is documented in `run-notes.md`.
- SQL migration path now exists under `control-plane/migrations/sql` and is operational via `uv run python -m origo_control_plane.migrations`.
- Runtime env config is now explicit and fail-fast: required ClickHouse vars are documented in root `.env.example` and validated centrally.
- Audit sinks are unified and immutable with retention guardrails enforced by env contract and sink-side continuity validation.

## Watch out
- `S0-C2` is still open because code history preservation has not been implemented in this repo state.
- `S0-C6` is open: local infra wiring still needs closeout checks, but Dagster metadata is already SQLite-backed on persistent volume in current config.
- `S0-P3` is open: no formal pre-migration baseline comparison has been completed yet.
- `S0-G2` is open: TLS enforcement on authenticated links remains deferred to Cloudflare layer.
- Migration scaffolding is currently bootstrap-level (`0001` probe table); production schema objects still need to be moved into versioned SQL over subsequent steps.
- Legacy `create_*` Dagster schema assets are now disabled and fail loudly by design; schema changes must run through `control-plane/migrations/sql`.
- Some compose/runtime values remain intentionally fixed for local developer ergonomics (service names, public port mappings) and may need explicit env-contract treatment when infra wiring (`S0-C6`) is finalized.
