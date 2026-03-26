# 2. Itemized Work Plan (Slices and Sub-Slices)

## Summary
This document breaks each slice into explicit checkbox steps and one-day sub-slices.

Global rule for every slice:
1. Capability steps first.
2. Proof steps second.
3. Guardrails steps third.

No slice advances before all three are complete.
Static-analysis hard gate applies throughout: `ruff` + `pyright` strict, repo-wide.

## Slice Checkbox Sections

## Slice 0: Monorepo Migration (`tdw-control-plane` -> `origo-control-plane`)

### Capability
- [x] `S0-C1` Create monorepo folders: `control-plane`, `api`, `contracts`, `storage`, `docs`, `spec`.
- [x] `S0-C2` Import full `tdw-control-plane` code and history into `control-plane`.
- [x] `S0-C3` Rename package/module identifiers from TDW naming to Origo naming.
- [x] `S0-C4` Update Python project metadata and import paths.
- [x] `S0-C5` Ensure existing Binance historical ingestion runs from monorepo.
- [x] `S0-C6` Wire local infra config for ClickHouse + Dagster + persistent SQLite metadata.

### Proof
- [x] `S0-P1` Run fixed Binance fixture window ingestion and capture baseline artifacts.
- [x] `S0-P2` Replay same fixture window twice and verify deterministic row counts/checksums.
- [x] `S0-P3` Compare outputs against pre-migration baseline and confirm no regression. (crossed over: pre-migration comparable baseline was not available)

### Guardrails
- [x] `S0-G1` Add migration audit record template and required run notes.
- [x] `S0-G2` Enforce TLS on authenticated service links introduced in this slice. (crossed over for now: TLS termination deferred to Cloudflare layer)
- [x] `S0-G3` Enable immutable audit-log sink with 1-year retention policy.
- [x] `S0-G4` Adopt `uv.lock` for deterministic Python dependency resolution in `control-plane`.
- [x] `S0-G5` Scaffold SQL migration framework (ordered files + ledger + checksums + runner).
- [x] `S0-G6` Enforce env contract (`.env.example` source of truth + fail-loud required vars + no deployment defaults in runtime paths).
- [x] `S0-G7` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S0-G8` User docs closeout for slice (`docs/`, full reference + taxonomy updates).
- [x] `S0-G9` Add CI hard quality gates (repo-wide `ruff`, strict `pyright`, plus split `contract`, `replay`, and `integrity` gates on pull requests).
- [x] `S0-G10` Implement exchange integrity suite in Binance ingestion paths (schema/type, sequence-gap, anomaly checks).

## Slice 1: Raw Query API (`mode=native`) Minimal Path

### Capability
- [x] `S1-C1` Build internal native query core (`ClickHouse SQL -> Arrow -> Polars`) with no HTTP dependency.
- [x] `S1-C2` Implement native query planner for Binance source tables in one pass (`binance_trades`, `binance_agg_trades`, `binance_futures_trades`).
- [x] `S1-C3` Implement projection/select field filtering with strict field allowlists.
- [x] `S1-C4` Implement time-range filtering and deterministic sorting semantics.
- [x] `S1-C5` Canonicalize query timestamps to UTC (`DateTime64(3)` storage/processing, ISO output).
- [x] `S1-C6` Implement wide-row response envelope with schema metadata.
- [x] `S1-C7` Add FastAPI `/v1/raw/query` HTTP adapter on top of proven internal query core.

### Proof
- [x] `S1-P1` Execute acceptance scenarios for valid native queries.
- [x] `S1-P2` Execute replay scenarios on fixed windows.
- [x] `S1-P3` Verify deterministic outputs across repeated runs.

### Guardrails
- [x] `S1-G1` Add internal static API-key auth middleware.
- [x] `S1-G2` Implement status/error mapping (`200/404/409/503`).
- [x] `S1-G3` Implement structured warnings envelope.
- [x] `S1-G4` Implement `strict=true` fail behavior.
- [x] `S1-G5` Add concurrency and queue limits for query execution.
- [x] `S1-G6` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S1-G7` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 2: Raw Export API (`mode=native`) via Dagster

### Capability
- [x] `S2-C1` Define `/v1/raw/export` submit and status contracts.
- [x] `S2-C2` Implement API endpoint that submits Dagster export run.
- [x] `S2-C3` Implement export status endpoint with run-state mapping.
- [x] `S2-C4` Implement Parquet export path for native mode.
- [x] `S2-C5` Implement CSV export path for native mode.
- [x] `S2-C6` Persist artifact metadata and retrieval pointers.

### Proof
- [x] `S2-P1` Validate submit -> poll -> artifact metadata lifecycle.
- [x] `S2-P2` Verify deterministic export artifacts on fixed windows.
- [x] `S2-P3` Verify export parity against query results for same window.

### Guardrails
- [x] `S2-G1` Enforce source rights gate before export run execution.
- [x] `S2-G2` Add immutable audit events for export lifecycle transitions.
- [x] `S2-G3` Add export queue backpressure and failure classification.
- [x] `S2-G4` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S2-G5` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 3: Generic Scraper Platform

### Capability
- [x] `S3-C1` Define scraper adapter interface: `discover_sources`, `fetch`, `parse`, `normalize`.
- [x] `S3-C2` Define normalized long-metric schema contract.
- [x] `S3-C3` Define provenance metadata schema.
- [x] `S3-C4` Implement shared HTTP fetch module.
- [x] `S3-C5` Implement browser fallback fetch module.
- [x] `S3-C6` Implement parser for HTML (`lxml` + `BeautifulSoup`).
- [x] `S3-C7` Implement parser for CSV.
- [x] `S3-C8` Implement parser for JSON.
- [x] `S3-C9` Implement deterministic PDF parser path.
- [x] `S3-C10` Implement raw artifact persistence to object storage (always on).
- [x] `S3-C11` Implement normalization pipeline to long-metric records.
- [x] `S3-C12` Implement persistence of normalized records to ClickHouse staging.

### Proof
- [x] `S3-P1` Run end-to-end pipeline on one sample target.
- [x] `S3-P2` Replay sample target inputs and verify deterministic normalized outputs.
- [x] `S3-P3` Validate raw artifact + provenance completeness for replayability.

### Guardrails
- [x] `S3-G1` Standardize adapter error taxonomy and failure codes.
- [x] `S3-G2` Add retry/backoff framework hooks.
- [x] `S3-G3` Add rights lookup hook (fail-closed on missing rights state).
- [x] `S3-G4` Add audit events for scrape runs and normalization outcomes.
- [x] `S3-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S3-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 4: ETF Use Case on Scraper Platform (All 10 Issuers)

### Capability
- [x] `S4-C1` Implement adapters for ETF issuers 1-3.
- [x] `S4-C2` Implement adapters for ETF issuers 4-7.
- [x] `S4-C3` Implement adapters for ETF issuers 8-10.
- [x] `S4-C4` Map issuer fields into normalized ETF daily metric schema.
- [x] `S4-C5` Normalize all ETF records to UTC daily semantics.
- [x] `S4-C6` Implement canonical daily load into ClickHouse long-metric table.
- [x] `S4-C7` Implement full available-history backfill per issuer.
- [x] `S4-C8` Expose ETF fields through native raw query mode.

### Proof
- [x] `S4-P1` Execute 10-issuer daily parity proof window against official issuer sources.
- [x] `S4-P2` Enforce proof threshold: >=99.5% parity on mandatory metrics.
- [x] `S4-P3` Replay proof window ingestion and verify deterministic outputs.
- [x] `S4-P4` Verify raw artifact/provenance references for all proof-window records.

### Guardrails
- [x] `S4-G1` Enforce legal sign-off artifact requirement before external serving/export.
- [x] `S4-G2` Enforce rights matrix checks in ingestion and API paths.
- [x] `S4-G3` Add daily run schedule.
- [x] `S4-G4` Add 24h same-day retry window behavior.
- [x] `S4-G5` Add shadow-then-promote gating for serving enablement.
- [x] `S4-G6` Add warning generation for stale/missing/incomplete daily records.
- [x] `S4-G7` Add logs + Discord alerts for ETF ingestion anomalies.
- [x] `S4-G8` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S4-G9` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 5: Raw Query `mode=aligned_1s` (Binance + ETF)

### Capability
- [x] `S5-C1` Implement Binance aligned-1s materialization definitions.
- [x] `S5-C2` Implement ETF aligned-1s materialization definitions.
- [x] `S5-C3` Implement logical forward-fill for low-frequency ETF metrics.
- [x] `S5-C4` Implement unified aligned query planner path.
- [x] `S5-C5` Implement aligned-mode response envelope and schema metadata.
- [x] `S5-C6` Enable aligned-mode export in Parquet.
- [x] `S5-C7` Enable aligned-mode export in CSV.

### Proof
- [x] `S5-P1` Execute aligned acceptance scenarios across Binance + ETF windows.
- [x] `S5-P2` Replay aligned windows and verify deterministic outputs.
- [x] `S5-P3` Validate forward-fill semantics on UTC day boundaries.

### Guardrails
- [x] `S5-G1` Apply strict/warning policy to aligned-mode responses.
- [x] `S5-G2` Add freshness metadata and warning generation for aligned mode.
- [x] `S5-G3` Add resource limits and queue controls for aligned queries.
- [x] `S5-G4` Add aligned export audit and rights checks.
- [x] `S5-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S5-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 6: FRED Integration

### Capability
- [x] `S6-C1` Implement FRED series connector and metadata fetch.
- [x] `S6-C2` Normalize FRED records into long-metric schema.
- [x] `S6-C3` Implement historical backfill path.
- [x] `S6-C4` Implement incremental update path.
- [x] `S6-C5` Persist native FRED data to ClickHouse and raw artifacts to object store.
- [x] `S6-C6` Expose FRED through native raw query mode.
- [x] `S6-C7` Integrate FRED into aligned-1s materialization path.

### Proof
- [x] `S6-P1` Execute acceptance scenarios for FRED native and aligned paths.
- [x] `S6-P2` Replay fixed windows and verify deterministic outputs.
- [x] `S6-P3` Validate metadata/version reproducibility for replay.

### Guardrails
- [x] `S6-G1` Enforce rights classification and legal gating.
- [x] `S6-G2` Add freshness checks based on source publish timestamps.
- [x] `S6-G3` Add shadow-then-promote gating.
- [x] `S6-G4` Add FRED alerts and audit-event coverage.
- [x] `S6-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S6-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 7: Full Platform Docker Wrapping and Local Proof

### Capability
- [x] `S7-C1` Add root-level Docker stack for `clickhouse`, `dagster-webserver`, `dagster-daemon`, and `api`.
- [x] `S7-C2` Add monorepo Dockerfiles for API and control-plane runtimes (no missing package imports at runtime).
- [x] `S7-C3` Add deterministic Docker bootstrap path for migrations and fixed-window seed ingest.
- [x] `S7-C4` Wire API + Dagster connectivity through Docker network with explicit env contract.
- [x] `S7-C5` Add operational commands for stack lifecycle (`up/down/logs/bootstrap/proof`).

### Proof
- [x] `S7-P1` Start full stack from clean state and verify service health checks pass.
- [x] `S7-P2` Run migrations + fixed Binance daily ingest inside Docker and verify loaded rows.
- [x] `S7-P3` Execute `/v1/raw/query` and `/v1/raw/export` lifecycle against Docker API and capture deterministic replay fingerprints.
- [x] `S7-P4` Restart stack and verify ClickHouse data + Dagster SQLite metadata persist.

### Guardrails
- [x] `S7-G1` Enforce Docker env contract: no deployment-specific hardcoded runtime values.
- [x] `S7-G2` Add repeatable Docker local-proof smoke runner and failure-classified outputs.
- [x] `S7-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S7-G4` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 8: OKX Spot Trades Daily Ingest + `aligned_1s` Integration (Binance-Mirror Pattern)

### Capability
- [x] `S8-C1` Implement OKX daily source fetch/checksum/parse ingest path mirroring Binance daily ingest behavior.
- [x] `S8-C2` Add ClickHouse migration-backed OKX raw table schema and deterministic write path.
- [x] `S8-C3` Integrate OKX dataset into native raw query planner and response contracts.
- [x] `S8-C4` Integrate OKX dataset into `aligned_1s` materialization/query/export paths.

### Proof
- [x] `S8-P1` Execute fixed-window OKX ingest + native query acceptance runs against original source files.
- [x] `S8-P2` Execute fixed-window OKX `aligned_1s` acceptance runs and validate semantics.
- [x] `S8-P3` Replay same fixtures and verify deterministic output fingerprints for both native and `aligned_1s`.
- [x] `S8-P4` Validate loaded data checksums/row stats against source artifacts.

### Guardrails
- [x] `S8-G1` Add rights/legal classification artifacts for OKX serving/export decisions.
- [x] `S8-G2` Apply exchange integrity suite profile for OKX dataset (schema/type, monotonic trade-id ordering, exact-duplicate detection, anomaly checks).
- [x] `S8-G3` Apply aligned-mode guardrails to OKX paths (strict/warnings/freshness/rights+audit parity with existing aligned sources).
- [x] `S8-G4` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S8-G5` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 10: Image-Based Merge Deployment (Server Bootstrap Included)

### Capability
- [x] `S10-C1` Add CI workflow that triggers on push to `main` and builds commit-pinned API/control-plane images.
- [x] `S10-C2` Publish built images to registry with deterministic commit tags.
- [x] `S10-C3` Add remote deploy compose contract that uses prebuilt images only (no server-side build).
- [x] `S10-C4` Implement remote apply path in CI: copy deploy manifests, pull images, run migrations, restart stack.

### Proof
- [x] `S10-P1` Execute deployment workflow on a merged commit and verify successful server apply.
- [x] `S10-P2` Verify first-run bootstrap path installs missing Docker/compose dependencies and proceeds to deploy.
- [x] `S10-P3` Verify replay deployment determinism: same commit tag yields same runtime image set and stable compose state.

### Guardrails
- [x] `S10-G1` Enforce fail-loud secret/env checks for server credentials and runtime env-file contract.
- [x] `S10-G2` Enforce deployment audit artifacts in workflow logs (image tags, migration step, compose status).
- [x] `S10-G3` Add deployment safety controls (serialized deploy concurrency and branch/environment restrictions).
- [x] `S10-G4` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S10-G5` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 11: Bybit Spot Trades Daily Ingest + `aligned_1s` Integration (Binance-Mirror Pattern)

### Capability
- [x] `S11-C1` Implement Bybit daily source fetch/checksum/parse ingest path mirroring Binance daily ingest behavior.
- [x] `S11-C2` Add ClickHouse migration-backed Bybit raw table schema and deterministic write path.
- [x] `S11-C3` Integrate Bybit dataset into native raw query planner and response contracts.
- [x] `S11-C4` Integrate Bybit dataset into `aligned_1s` materialization/query/export paths.

### Proof
- [x] `S11-P1` Execute fixed-window Bybit ingest + native query acceptance runs against original source files.
- [x] `S11-P2` Execute fixed-window Bybit `aligned_1s` acceptance runs and validate semantics.
- [x] `S11-P3` Replay same fixtures and verify deterministic output fingerprints for both native and `aligned_1s`.
- [x] `S11-P4` Validate loaded data checksums/row stats against source artifacts.

### Guardrails
- [x] `S11-G1` Add rights/legal classification artifacts for Bybit serving/export decisions.
- [x] `S11-G2` Apply exchange integrity suite profile for Bybit dataset (schema/type, monotonic-time and uniqueness checks, anomaly checks).
- [x] `S11-G3` Apply aligned-mode guardrails to Bybit paths (strict/warnings/freshness/rights+audit parity with existing aligned sources).
- [x] `S11-G4` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S11-G5` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 12: Reddit Hourly Ingest + CryptoBERT Sentiment Signals

### Capability
- [x] `S12-C1` Implement official Reddit API OAuth client and subreddit fetch contract for configured subreddit set. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-C2` Implement hourly Reddit content ingest path with deterministic raw artifact capture and checksums. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-C3` Persist normalized Reddit content events to ClickHouse with migration-backed schema. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-C4` Integrate `ElKulako/cryptobert` inference path for per-item sentiment scoring. (crossed over: explicitly dropped from roadmap; not implemented)
- [x] `S12-C5` Implement hourly sentiment signal aggregation dataset with explicit model/version metadata. (crossed over: explicitly dropped from roadmap; not implemented)
- [x] `S12-C6` Integrate Slice-12 datasets into raw query/export paths for both `native` and `aligned_1s`. (crossed over: blocked by Reddit access/policy constraints in current phase)

### Proof
- [x] `S12-P1` Execute fixed-window Reddit ingest + sentiment inference acceptance runs across configured subreddit scope. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-P2` Execute fixed-window `native` + `aligned_1s` acceptance runs for Slice-12 datasets. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-P3` Replay fixed fixtures and verify deterministic output fingerprints (ingest, inference, and aggregation outputs). (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-P4` Validate source artifact checksums, row stats, and model-version provenance against proof artifacts. (crossed over: blocked by Reddit access/policy constraints in current phase)

### Guardrails
- [x] `S12-G1` Add rights/legal classification artifacts for Reddit API content ingestion and serving/export decisions. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-G2` Enforce Reddit rate-limit/backoff guardrails with fail-loud quota/error classification. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-G3` Add data integrity and freshness guardrails for subreddit coverage, duplicate detection, and hourly completeness. (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-G4` Apply aligned-mode guardrails to Slice-12 datasets (strict/warnings/freshness/rights+audit parity). (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes). (crossed over: blocked by Reddit access/policy constraints in current phase)
- [x] `S12-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates). (crossed over: blocked by Reddit access/policy constraints in current phase)

## Slice 13: Bitcoin Core Node V1 Streams + Derived Network/Supply Signals

### Capability
- [x] `S13-C1` Stand up one unpruned Bitcoin Core node data connector and deterministic block/mempool pull contract.
- [x] `S13-C2` Implement block header stream ingestion (`height`, `timestamp`, `difficulty`, `nonce`, `version`, `merkle_root`, `prev_hash`).
- [x] `S13-C3` Implement block transaction stream ingestion (`inputs`, `outputs`, `values`, `scripts`, `witness_data`, `coinbase`).
- [x] `S13-C4` Implement mempool state ingestion (`txid`, `fee_rate_sat_vb`, `vsize`, `first_seen_timestamp`, `rbf_flag`).
- [x] `S13-C5` Implement deterministic derived datasets:
  - block fee totals per block in BTC
  - subsidy schedule by height/halving interval
  - network hashrate estimate from difficulty + observed block intervals
  - BTC circulating supply by height/time
- [x] `S13-C6` Implement `aligned_1s` materialization/query definitions for S13 derived signals/metrics (fees, subsidy, hashrate, supply) with deterministic bucket semantics.
- [x] `S13-C7` Integrate Slice-13 datasets into raw query/export paths for both `native` and `aligned_1s`.

### Proof
- [x] `S13-P1` Execute fixed-window acceptance runs for node ingest outputs (headers, transactions, mempool snapshots).
- [x] `S13-P2` Execute fixed-window acceptance runs for derived datasets (fees, subsidy, hashrate, supply) in both `native` and `aligned_1s`.
- [x] `S13-P3` Replay fixed fixtures and verify deterministic fingerprints across ingest and derived outputs.
- [x] `S13-P4` Validate dataset parity checks (row counts/checksums/consistency constraints) against canonical block data.

### Guardrails
- [x] `S13-G1` Add rights/legal classification artifacts for Bitcoin Core node-sourced serving/export decisions.
- [x] `S13-G2` Enforce data integrity suite for block/tx/mempool streams (schema/type, linkage, anomaly checks).
- [x] `S13-G3` Enforce derived-metric integrity checks (fee/subsidy/hashrate/supply formula invariants).
- [x] `S13-G4` Apply aligned-mode guardrails to Slice-13 datasets (strict/warnings/freshness/rights+audit parity).
- [x] `S13-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S13-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 14: Event-Sourcing Core + Persistent Aligned Framework

### Capability
- [x] `S14-C1` Add canonical event envelope schema and single global append-only event log tables.
- [x] `S14-C2` Implement deterministic canonical event writer (typed envelope + `payload_raw` + `payload_sha256_raw` + derivative `payload_json`) with idempotency keys.
- [x] `S14-C3` Implement ingest cursor/ledger and source-completeness reconciliation tables.
- [x] `S14-C4` Implement projector runtime and ClickHouse-backed checkpoint/watermark tables.
- [x] `S14-C5` Implement persistent aligned aggregate framework (tiered policy).
- [x] `S14-C6` Upgrade Raw API contract target for multi-source queries plus explicit `view_id` / `view_version`.
- [x] `S14-C7` Add provisional rights metadata contract fields to query/export status responses.
- [x] `S14-C8` Add source precision mapping registry and canonical numeric typing rules (no float canonical storage).
- [x] `S14-C9` Execute minimal pilot cutover path on Binance spot trades through canonical events.

### Proof
- [x] `S14-P1` Validate canonical raw-payload hash determinism across repeated fixed-fixture ingests.
- [x] `S14-P2` Validate exactly-once ingest under duplicate replay and crash/restart scenarios.
- [x] `S14-P3` Validate no-miss detection with injected gaps and reconciliation checks.
- [x] `S14-P4` Validate raw-fidelity and numeric-precision round-trip proofs on fixed fixtures.
- [x] `S14-P5` Validate projector replay determinism with checkpoint resume behavior.
- [x] `S14-P6` Validate pilot cutover acceptance and parity (`native` + `aligned_1s`) on fixed windows.

### Guardrails
- [x] `S14-G1` Add fail-loud invariant checks and typed error taxonomy for event writer/projectors.
- [x] `S14-G2` Add immutable audit events for event ingestion and projector checkpoint transitions.
- [x] `S14-G3` Enforce continuous source-completeness reconciliation with fail-loud stream quarantine on gaps.
- [x] `S14-G4` Enforce canonical precision guardrails (no float storage, explicit decimal scale metadata).
- [x] `S14-G5` Enforce provisional-rights metadata emission in API responses.
- [x] `S14-G6` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S14-G7` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 15: Binance Event-Sourcing Port (`binance_spot_trades`)

### Capability
- [x] `S15-C1` Port Binance spot trades ingest writes to canonical events.
- [x] `S15-C2` Consolidate Binance ingest scope to `binance_spot_trades` only.
- [x] `S15-C3` Remove legacy non-spot Binance ingest paths from active serving contracts.
- [x] `S15-C4` Implement Binance native serving projections from canonical events.
- [x] `S15-C5` Implement Binance persistent aligned serving projections from canonical events.
- [x] `S15-C6` Cut query/export serving to Binance event projections and remove legacy direct-serving path.

### Proof
- [x] `S15-P1` Execute fixed-window acceptance for Binance `native` + `aligned_1s`.
- [x] `S15-P2` Execute parity checks versus current Binance fixture baselines.
- [x] `S15-P3` Execute replay determinism for both Binance modes after cutover.
- [x] `S15-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) for `binance_spot_trades`.
- [x] `S15-P5` Execute no-miss completeness proof (reconciliation + gap injection) for `binance_spot_trades`.
- [x] `S15-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) for `binance_spot_trades`.

### Guardrails
- [x] `S15-G1` Apply exchange integrity suite in event-ingest and projection paths.
- [x] `S15-G2` Apply freshness warning semantics using source-availability timestamps.
- [x] `S15-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for Binance gaps.
- [x] `S15-G4` Enforce raw-fidelity and precision guardrails in Binance canonical ingest path.
- [x] `S15-G5` Enforce provisional rights metadata behavior for Binance responses.
- [x] `S15-G6` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S15-G7` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 16: ETF Event-Sourcing Port (`etf_daily_metrics`)

### Capability
- [x] `S16-C1` Port ETF normalized output writes to canonical events with existing provenance fidelity.
- [x] `S16-C2` Implement ETF native serving projections from canonical events.
- [x] `S16-C3` Implement ETF persistent aligned serving projections with deterministic forward-fill semantics.
- [x] `S16-C4` Cut query/export serving to ETF event projections and remove legacy direct-serving path.

### Proof
- [x] `S16-P1` Execute fixed-window acceptance for ETF `native` + `aligned_1s`.
- [x] `S16-P2` Execute parity checks versus current ETF fixture baselines.
- [x] `S16-P3` Execute replay determinism for both ETF modes after cutover.
- [x] `S16-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) for ETF records.
- [x] `S16-P5` Execute no-miss completeness proof (reconciliation + gap injection/cadence checks) for ETF records.
- [x] `S16-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) for ETF records.

### Guardrails
- [x] `S16-G1` Enforce legal/rights behavior and provisional rights metadata for ETF responses.
- [x] `S16-G2` Enforce stale/missing/incomplete ETF quality warnings from projection outputs.
- [x] `S16-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for ETF ingest.
- [x] `S16-G4` Enforce raw-fidelity and precision guardrails in ETF canonical ingest path.
- [x] `S16-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S16-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 17: FRED Event-Sourcing Port (`fred_series_metrics`)

### Capability
- [x] `S17-C1` Port FRED ingest writes to canonical events.
- [x] `S17-C2` Implement FRED native serving projections from canonical events.
- [x] `S17-C3` Implement FRED persistent aligned serving projections from canonical events.
- [x] `S17-C4` Cut query/export serving to FRED event projections and remove legacy direct-serving path.

### Proof
- [x] `S17-P1` Execute fixed-window acceptance for FRED `native` + `aligned_1s`.
- [x] `S17-P2` Execute parity checks versus current FRED fixture baselines.
- [x] `S17-P3` Execute replay determinism for both FRED modes after cutover.
- [x] `S17-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) for FRED events.
- [x] `S17-P5` Execute no-miss completeness proof (reconciliation + gap injection/cadence checks) for FRED events.
- [x] `S17-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) for FRED events.

### Guardrails
- [x] `S17-G1` Enforce publish-freshness warning behavior from canonical event timing fields.
- [x] `S17-G2` Enforce legal/rights behavior and provisional rights metadata for FRED responses.
- [x] `S17-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for FRED ingest.
- [x] `S17-G4` Enforce raw-fidelity and precision guardrails in FRED canonical ingest path.
- [x] `S17-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S17-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 18: OKX Event-Sourcing Port (`okx_spot_trades`)

### Capability
- [x] `S18-C1` Port OKX ingest writes to canonical events with existing source checksum/provenance guarantees.
- [x] `S18-C2` Implement OKX native serving projections from canonical events.
- [x] `S18-C3` Implement OKX persistent aligned serving projections from canonical events.
- [x] `S18-C4` Cut query/export serving to OKX event projections and remove legacy direct-serving path.

### Proof
- [x] `S18-P1` Execute fixed-window acceptance for OKX `native` + `aligned_1s`.
- [x] `S18-P2` Execute parity checks versus current OKX fixture baselines.
- [x] `S18-P3` Execute replay determinism for both OKX modes after cutover.
- [x] `S18-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) for OKX events.
- [x] `S18-P5` Execute no-miss completeness proof (reconciliation + gap injection) for OKX events.
- [x] `S18-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) for OKX events.

### Guardrails
- [x] `S18-G1` Apply exchange integrity suite checks in OKX event/projection paths.
- [x] `S18-G2` Enforce legal/rights behavior and provisional rights metadata for OKX responses.
- [x] `S18-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for OKX ingest.
- [x] `S18-G4` Enforce raw-fidelity and precision guardrails in OKX canonical ingest path.
- [x] `S18-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S18-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 19: Bybit Event-Sourcing Port (`bybit_spot_trades`)

### Capability
- [x] `S19-C1` Port Bybit ingest writes to canonical events with existing source checksum/provenance guarantees.
- [x] `S19-C2` Implement Bybit native serving projections from canonical events.
- [x] `S19-C3` Implement Bybit persistent aligned serving projections from canonical events.
- [x] `S19-C4` Cut query/export serving to Bybit event projections and remove legacy direct-serving path.

### Proof
- [x] `S19-P1` Execute fixed-window acceptance for Bybit `native` + `aligned_1s`.
- [x] `S19-P2` Execute parity checks versus current Bybit fixture baselines.
- [x] `S19-P3` Execute replay determinism for both Bybit modes after cutover.
- [x] `S19-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) for Bybit events.
- [x] `S19-P5` Execute no-miss completeness proof (reconciliation + gap injection) for Bybit events.
- [x] `S19-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) for Bybit events.

### Guardrails
- [x] `S19-G1` Apply exchange integrity suite checks in Bybit event/projection paths.
- [x] `S19-G2` Enforce legal/rights behavior and provisional rights metadata for Bybit responses.
- [x] `S19-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for Bybit ingest.
- [x] `S19-G4` Enforce raw-fidelity and precision guardrails in Bybit canonical ingest path.
- [x] `S19-G5` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S19-G6` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 20: Bitcoin Event-Sourcing Port (All Onboarded Bitcoin Datasets)

### Capability
- [x] `S20-C1` Port Bitcoin block headers ingest writes to canonical events.
- [x] `S20-C2` Port Bitcoin block transactions ingest writes to canonical events.
- [x] `S20-C3` Port Bitcoin mempool state ingest writes to canonical events.
- [x] `S20-C4` Port Bitcoin derived datasets writes to canonical events.
- [x] `S20-C5` Implement native serving projections for all seven onboarded Bitcoin datasets.
- [x] `S20-C6` Implement persistent aligned serving projections for the four derived Bitcoin datasets only.
- [x] `S20-C7` Cut query/export serving to Bitcoin event projections and remove legacy direct-serving path.

### Proof
- [x] `S20-P1` Execute fixed-window acceptance for Bitcoin native (all seven datasets) and aligned (four derived datasets).
- [x] `S20-P2` Execute parity checks versus current Bitcoin fixture baselines.
- [x] `S20-P3` Execute replay determinism across Bitcoin event and projection paths.
- [x] `S20-P4` Execute exactly-once ingest proof (duplicate replay + crash/restart) across all seven Bitcoin datasets.
- [x] `S20-P5` Execute no-miss completeness proof (height/sequence reconciliation + gap injection) across all seven Bitcoin datasets.
- [x] `S20-P6` Execute raw-fidelity/precision proof (raw payload hash + numeric scale checks) across all seven Bitcoin datasets.
- [x] `S20-P7` Execute live-node proof gate once non-IBD conditions are met.

### Guardrails
- [x] `S20-G1` Apply stream linkage and derived formula integrity suites in Bitcoin event/projection paths.
- [x] `S20-G2` Enforce freshness warning behavior from canonical event timing fields.
- [x] `S20-G3` Enforce source-completeness reconciliation and fail-loud quarantine behavior for Bitcoin ingest.
- [x] `S20-G4` Enforce raw-fidelity and precision guardrails in Bitcoin canonical ingest path.
- [x] `S20-G5` Enforce legal/rights behavior and provisional rights metadata for Bitcoin responses.
- [x] `S20-G6` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S20-G7` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 21: Canonical `aligned_1s` Aggregate Contract Enforcement + Binance Retrofit

### Capability
- [x] `S21-C1` Enforce runtime canonical aligned storage contract checks in Binance aligned query path (`canonical_aligned_1s_aggregates` table existence + required columns/types).
- [x] `S21-C2` Remove ad-hoc aligned table DDL from Binance proof harnesses and require migration-runner provisioning only.
- [x] `S21-C3` Verify Binance aligned projector/query path uses canonical aligned table contract exclusively (no alternate/fallback aligned storage path).

### Proof
- [x] `S21-P1` Execute Binance aligned guardrail proof on migration-provisioned proof database (no manual table bootstrap).
- [x] `S21-P2` Execute contract negative proofs for missing-table, missing-column, and type-drift failure modes (fail-loud expected).
- [x] `S21-P3` Replay fixed Binance aligned windows and verify deterministic fingerprints remain stable after contract enforcement.

### Guardrails
- [x] `S21-G1` Add/extend contract-gate coverage for canonical aligned storage contract enforcement in runtime query modules.
- [x] `S21-G2` Lock planning/docs contract language so every source slice treats canonical aligned aggregate contract as mandatory (no exceptions).
- [x] `S21-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S21-G4` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

## Slice 22: Historical Binance Lock-In (Operational HTTP Surface)

### Capability
- [x] `S22-C1` Refactor `HistoricalData` Binance methods to explicit contracts: `get_binance_spot_trades`, `get_binance_spot_klines`.
- [x] `S22-C2` Replace historical window contract with strict `start_date`/`end_date` and row windows (`n_latest_rows`, `n_random_rows`), no aliases.
- [x] `S22-C3` Remove dropped historical methods (`get_spot_trades`, `get_spot_klines`, `get_spot_agg_trades`, `get_futures_trades`, `get_futures_klines`).
- [x] `S22-C4` Add Binance historical HTTP endpoints: `/v1/historical/binance/spot/trades` and `/v1/historical/binance/spot/klines`.

### Proof
- [x] `S22-P1` Add contract tests for six-method Python interface baseline and dropped-method absence.
- [x] `S22-P2` Add HTTP contract tests for route registration, envelope shape, strict validation, and fail-loud status mapping.
- [x] `S22-P3` Add deterministic replay test coverage for historical request validation and stable route behavior in repeated runs.

### Guardrails
- [x] `S22-G1` Apply auth + rights + strict warning guardrails to Binance historical endpoints.
- [x] `S22-G2` Enforce error taxonomy (`200/404/409/503`) for new historical endpoint surface.
- [x] `S22-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S22-G4` User docs closeout for slice (`docs/`, full historical endpoint reference + taxonomy updates).

## Slice 23: Historical OKX Parallelization

### Capability
- [x] `S23-C1` Add OKX historical Python methods with signatures identical to Binance contracts.
- [x] `S23-C2` Add OKX historical HTTP endpoints: `/v1/historical/okx/spot/trades` and `/v1/historical/okx/spot/klines`.
- [x] `S23-C3` Normalize OKX spot-trades output to shared schema (`trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`).
- [x] `S23-C4` Implement OKX maker-side mapping guardrail (`buy -> 0`, `sell -> 1`) in trades and kline paths.

### Proof
- [x] `S23-P1` Add fixed-contract validation proofs for strict date-window rules and mutually-exclusive window mode selection.
- [x] `S23-P2` Add endpoint cohesion proofs showing OKX route payload shape parity with Binance historical routes.
- [x] `S23-P3` Add replay determinism proofs for repeated OKX historical route calls under identical mocked fixtures.

### Guardrails
- [x] `S23-G1` Apply auth + rights + strict warning guardrails to OKX historical endpoints.
- [x] `S23-G2` Enforce no old-parameter/method aliases in runtime and contract tests.
- [x] `S23-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S23-G4` User docs closeout for slice (`docs/`, full historical endpoint reference + taxonomy updates).

## Slice 24: Historical Bybit Parallelization + Cutover

### Capability
- [x] `S24-C1` Add Bybit historical Python methods with signatures identical to Binance/OKX contracts.
- [x] `S24-C2` Add Bybit historical HTTP endpoints: `/v1/historical/bybit/spot/trades` and `/v1/historical/bybit/spot/klines`.
- [x] `S24-C3` Normalize Bybit spot-trades output to shared schema (`trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`).
- [x] `S24-C4` Implement Bybit maker-side mapping guardrail (`buy -> 0`, `sell -> 1`) in trades and kline paths.
- [x] `S24-C5` Publish old-system-to-new historical endpoint migration mapping runbook.

### Proof
- [x] `S24-P1` Add six-route HTTP contract proof coverage for request/response cohesion and strict status/error behavior.
- [x] `S24-P2` Add six-method Python cohesion proof coverage for signature identity and schema parity by family (trades/klines).
- [x] `S24-P3` Add replay determinism proofs for all historical routes under repeated fixtures.

### Guardrails
- [x] `S24-G1` Apply auth + rights + strict warning guardrails to Bybit historical endpoints.
- [x] `S24-G2` Verify cutover guardrails (migration mapping complete; no fallback aliases retained).
- [x] `S24-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S24-G4` User docs closeout for slice (`docs/`, full historical endpoint reference + taxonomy updates).

## Slice 25: Historical Contract Normalization (Uniform Native Behavior + Unbounded Default Window)

### Capability
- [x] `S25-C1` Replace per-route historical request parsing with one shared historical request contract (`mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`).
- [x] `S25-C2` Enforce parameter-name uniformity between historical HTTP routes and `HistoricalData` methods (same names, same option semantics where applicable).
- [x] `S25-C3` Implement optional-window semantics (`no selector -> earliest to now`) for historical queries.
- [x] `S25-C4` Implement optional-window semantics for `/v1/raw/query` with the same unbounded default behavior.
- [x] `S25-C5` Remove legacy selector assumptions that require exactly one window mode in historical and raw query validators.

### Proof
- [x] `S25-P1` Add contract tests proving shared parameter names/options across all existing historical routes and methods.
- [x] `S25-P2` Add acceptance tests proving unbounded default-window behavior (`no selector`) for `native` and `aligned_1s`.
- [x] `S25-P3` Add replay determinism tests for bounded and unbounded windows on historical and raw query paths.

### Guardrails
- [x] `S25-G1` Enforce fail-loud validation and no alias fallback behavior in shared historical and raw-query contracts.
- [x] `S25-G2` Enforce status/error taxonomy parity (`200/404/409/503`) after contract migration.
- [x] `S25-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S25-G4` User docs closeout for slice (`docs/`, full historical/native/aligned contract + taxonomy updates).

## Slice 26: Historical Exchange Spot Route Completion (`binance_spot_trades`, `okx_spot_trades`, `bybit_spot_trades`) with `native`/`aligned_1s` Parity

### Capability
- [x] `S26-C1` Explicitly drop legacy Binance non-spot dataset keys from historical Python/HTTP scope for this tranche and lock removal as hard contract.
- [x] `S26-C2` Add historical `mode=native|aligned_1s` support for in-scope exchange trade datasets (`binance_spot_trades`, `okx_spot_trades`, `bybit_spot_trades`).
- [x] `S26-C3` Keep exchange method signatures uniform across in-scope exchange trade datasets and both modes.
- [x] `S26-C4` Keep existing spot-kline convenience routes operational, add `aligned_1s` execution support, and align shared selector/filter/strict semantics with historical core.

### Proof
- [x] `S26-P1` Execute fixed-window acceptance runs for all three in-scope exchange trade datasets in `native` and `aligned_1s`.
- [x] `S26-P2` Execute replay determinism proofs for all three in-scope exchange trade datasets in both modes.
- [x] `S26-P3` Execute schema/behavior cohesion proofs across exchange historical Python and HTTP interfaces.

### Guardrails
- [x] `S26-G1` Apply rights/auth/strict-warning/error-taxonomy guardrails uniformly to all exchange historical trade routes.
- [x] `S26-G2` Add guardrail tests for full-history default-window behavior on exchange historical routes.
- [x] `S26-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S26-G4` User docs closeout for slice (`docs/`, full exchange historical taxonomy updates for both modes).

## Slice 27: Historical ETF Operationalization (`etf_daily_metrics` in `native` + `aligned_1s`)

### Capability
- [x] `S27-C1` Add `HistoricalData` method `get_etf_daily_metrics` with shared historical parameters and `mode`.
- [x] `S27-C2` Add HTTP endpoint `/v1/historical/etf/daily_metrics` with shared historical contract.
- [x] `S27-C3` Implement `native` and `aligned_1s` ETF historical serving with deterministic forward-fill semantics.
- [x] `S27-C4` Add ETF historical field projection and filter support aligned with shared historical contract.

### Proof
- [x] `S27-P1` Execute fixed-window ETF acceptance runs for `native` and `aligned_1s` historical paths.
- [x] `S27-P2` Execute replay determinism proofs for ETF historical paths in both modes.
- [x] `S27-P3` Execute parity checks between historical ETF outputs and existing raw query/output expectations.

### Guardrails
- [x] `S27-G1` Apply ETF rights/freshness/warning guardrails to historical ETF endpoint behavior.
- [x] `S27-G2` Enforce fail-loud strict-mode behavior parity with raw query for ETF historical paths.
- [x] `S27-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S27-G4` User docs closeout for slice (`docs/`, full ETF historical taxonomy updates for both modes).

## Slice 28: Historical FRED Operationalization (`fred_series_metrics` in `native` + `aligned_1s`)

### Capability
- [x] `S28-C1` Add `HistoricalData` method `get_fred_series_metrics` with shared historical parameters and `mode`.
- [x] `S28-C2` Add HTTP endpoint `/v1/historical/fred/series_metrics` with shared historical contract.
- [x] `S28-C3` Implement `native` and `aligned_1s` FRED historical serving with deterministic publish-freshness semantics.
- [x] `S28-C4` Add FRED historical field projection and filter support aligned with shared historical contract.

### Proof
- [x] `S28-P1` Execute fixed-window FRED acceptance runs for `native` and `aligned_1s` historical paths.
- [x] `S28-P2` Execute replay determinism proofs for FRED historical paths in both modes.
- [x] `S28-P3` Execute parity checks between historical FRED outputs and existing raw query/output expectations.

### Guardrails
- [x] `S28-G1` Apply FRED rights/publish-freshness/warning guardrails to historical FRED endpoint behavior.
- [x] `S28-G2` Enforce fail-loud strict-mode behavior parity with raw query for FRED historical paths.
- [x] `S28-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S28-G4` User docs closeout for slice (`docs/`, full FRED historical taxonomy updates for both modes).

## Slice 29: Bitcoin Full `aligned_1s` Completion (`bitcoin_block_headers`, `bitcoin_block_transactions`, `bitcoin_mempool_state`)

### Capability
- [x] `S29-C1` Implement deterministic `aligned_1s` projections for `bitcoin_block_headers`.
- [x] `S29-C2` Implement deterministic `aligned_1s` projections for `bitcoin_block_transactions`.
- [x] `S29-C3` Implement deterministic `aligned_1s` projections for `bitcoin_mempool_state`.
- [x] `S29-C4` Integrate all three new Bitcoin aligned datasets into raw query/export paths with canonical-aligned contract enforcement.
- [x] `S29-C5` Remove remaining aligned-capability exclusions so every onboarded dataset is aligned-capable.

### Proof
- [x] `S29-P1` Execute fixed-window acceptance runs for new Bitcoin aligned datasets.
- [x] `S29-P2` Execute replay determinism proofs for new Bitcoin aligned datasets.
- [x] `S29-P3` Execute canonical aligned storage contract negative proofs (missing table/column/type drift) for new Bitcoin aligned paths.

### Guardrails
- [x] `S29-G1` Apply Bitcoin linkage/integrity/freshness guardrails to new aligned paths.
- [x] `S29-G2` Enforce fail-loud API contract behavior for all Bitcoin datasets in `aligned_1s` mode.
- [x] `S29-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S29-G4` User docs closeout for slice (`docs/`, full Bitcoin taxonomy updates with full aligned coverage).

## Slice 30: Historical Bitcoin Operationalization (All Seven Bitcoin Datasets in `native` + `aligned_1s`)

### Capability
- [x] `S30-C1` Add explicit `HistoricalData` methods for all seven Bitcoin datasets (`get_bitcoin_block_headers`, `get_bitcoin_block_transactions`, `get_bitcoin_mempool_state`, `get_bitcoin_block_fee_totals`, `get_bitcoin_block_subsidy_schedule`, `get_bitcoin_network_hashrate_estimate`, `get_bitcoin_circulating_supply`) using shared historical parameter contract and `mode`.
- [x] `S30-C2` Add explicit HTTP endpoints for all seven Bitcoin datasets (`/v1/historical/bitcoin/block_headers`, `/v1/historical/bitcoin/block_transactions`, `/v1/historical/bitcoin/mempool_state`, `/v1/historical/bitcoin/block_fee_totals`, `/v1/historical/bitcoin/block_subsidy_schedule`, `/v1/historical/bitcoin/network_hashrate_estimate`, `/v1/historical/bitcoin/circulating_supply`).
- [x] `S30-C3` Wire both `native` and `aligned_1s` historical serving for all seven Bitcoin datasets.
- [x] `S30-C4` Add field projection and filter handling for historical Bitcoin endpoints with shared semantics.

### Proof
- [x] `S30-P1` Execute fixed-window acceptance runs for all seven Bitcoin historical endpoints in both modes.
- [x] `S30-P2` Execute fixed-window acceptance runs for all seven Bitcoin historical Python methods in both modes.
- [x] `S30-P3` Execute replay determinism proofs across all seven Bitcoin datasets for both historical surfaces.

### Guardrails
- [x] `S30-G1` Apply rights/auth/strict-warning/error-taxonomy guardrails uniformly to Bitcoin historical endpoints.
- [x] `S30-G2` Enforce full-history default-window behavior and fail-loud validation for Bitcoin historical paths.
- [x] `S30-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S30-G4` User docs closeout for slice (`docs/`, full Bitcoin historical taxonomy and endpoint reference updates).

## Slice 31: Historical Full-Surface Cohesion + Rollout Handoff

### Capability
- [x] `S31-C1` Build complete historical endpoint and Python method matrix coverage for every active dataset in `native` and `aligned_1s`.
- [x] `S31-C2` Harmonize any remaining signature, envelope, or semantic drift across dataset families.
- [x] `S31-C3` Publish final internal cutover mapping from legacy data endpoints to Origo historical surfaces.

### Proof
- [x] `S31-P1` Run full contract suite over all historical HTTP endpoints and `HistoricalData` methods.
- [x] `S31-P2` Run full replay suite over all historical HTTP endpoints and `HistoricalData` methods.
- [x] `S31-P3` Run full integrity suite over all historical datasets and both modes.

### Guardrails
- [x] `S31-G1` Enforce zero-drift guardrail: no undocumented endpoint/method contract exceptions remain.
- [x] `S31-G2` Enforce rollout-readiness guardrail with explicit operational runbook and rollback mapping.
- [x] `S31-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [x] `S31-G4` User docs closeout for slice (`docs/`, full canonical reference + taxonomy closure).

## Slice 32: Deploy Env-Contract Drift Fix (`ORIGO_AUDIT_LOG_RETENTION_DAYS`) + Live Validation Closure

### Capability
- [x] `S32-C1` Enforce deploy-time sync of `ORIGO_AUDIT_LOG_RETENTION_DAYS` from root `.env.example` into `/opt/origo/deploy/.env`.
- [x] `S32-C2` Enforce compose-time API requirement for `ORIGO_AUDIT_LOG_RETENTION_DAYS` in server compose contract.

### Proof
- [x] `S32-P1` Run full quality gates (`style`, `type`, `contract`, `replay`, `integrity`) on the deploy-contract fix branch.
- [x] `S32-P2` Verify merge-triggered deploy succeeds and live server health checks pass over domain endpoint.

### Guardrails
- [x] `S32-G1` Update deployment contract and troubleshooting docs to reflect retention-key enforcement and fail signatures.
- [x] `S32-G2` Add Slice 32 closeout artifacts (`manifest`, `run-notes`, `baseline fixture`) with explicit deploy evidence and caveats.

## Slice 33: Binance Dataset Contract Cleanup (drop legacy non-spot keys, enforce `binance_spot_trades`)

### Capability
- [x] `S33-C1` Enforce Binance dataset contract identifier as `binance_spot_trades` across raw query/export, aligned query, historical surfaces, and internal dataset registries.
- [x] `S33-C2` Remove legacy non-spot Binance dataset keys from active API contracts, control-plane orchestration, and projection/ingest runtimes (hard remove, no alias).
- [x] `S33-C3` Remove/de-register Binance agg/futures Dagster assets/jobs and keep only Binance spot ingest/project path.
- [x] `S33-C4` Update rights matrix, precision registry, and legal/source contracts to the cleaned Binance dataset scope.

### Proof
- [x] `S33-P1` Run contract-gate coverage proving `binance_spot_trades` is the only Binance raw dataset key and legacy non-spot keys are rejected fail-loud.
- [x] `S33-P2` Run replay/integrity proofs on fixed Binance spot windows to confirm deterministic behavior after rename and scope drop.
- [x] `S33-P3` Run cross-surface acceptance checks (`/v1/raw/query`, `/v1/raw/export`, historical HTTP, historical Python) for `binance_spot_trades` in `native` and `aligned_1s`.
- [x] `S33-P4` Run full quality gates (`style`, `type`, `contract`, `replay`, `integrity`) with no waivers.

### Guardrails
- [x] `S33-G1` Enforce fail-loud unsupported-dataset error contract for removed Binance datasets on all query/export paths.
- [x] `S33-G2` Ensure no fallback aliases remain for removed legacy Binance dataset keys.
- [x] `S33-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete cleaned dataset contracts/operations notes).
- [x] `S33-G4` User docs closeout for slice (`docs/`, full taxonomy/reference updated to `binance_spot_trades` and removed datasets).

## Slice 34: Full Canonical Backfill (All Onboarded Datasets)

### Capability
- [x] `S34-C1` Freeze canonical backfill inventory and execution order for every onboarded dataset.
- [x] `S34-C2` Implement/verify backfill orchestration contract (partition planner, cursor ledger, resumable run controls, fail-loud gap checks).
- [x] `S34-C2a` Stabilize ETF canonical event payload for replay idempotency (exclude run-volatile provenance timestamps from canonical payload hash contract).
- [x] `S34-C2b` Replace direct asset invocation with Dagster-native partition execution for live backfill dispatch/monitoring.
- [x] `S34-C2c` Move authoritative backfill progress, proof, and quarantine state fully into ClickHouse and remove file-backed live state paths.
- [x] `S34-C2d` Implement strict partition state machine and explicit reconcile flow for completed, ambiguous, and quarantined partitions.
- [x] `S34-C2e` Implement deterministic partition-local source/canonical proof records and range-level proof folding.
- [x] `S34-C2f` Gate fast insert, resume, projection rebuild, and serving promotion on terminal proof state only.
- [x] `S34-C2g` Add reconcile fast path that proves existing canonical partitions directly from source and canonical evidence without duplicate-writer replay.
- [x] `S34-C2h` Remove Binance canonical backfill Python bottlenecks in source-proof and fresh-write preparation using staged/vectorized execution.
- [x] `S34-C2i` Remove immutable runtime-audit append bottleneck from Binance fresh-write path without weakening audit immutability guarantees.
- [x] `S34-C2j` Fix exchange fast-insert guard semantics so pre-manifest empty-partition assessment cannot self-poison fresh backfill runs.
- [x] `S34-C3` Execute Binance backfill from first available source partitions for `binance_spot_trades`.
- [ ] `S34-C4` Execute OKX and Bybit backfill from first available source partitions (`okx_spot_trades`, `bybit_spot_trades`).
- [x] `S34-C4a` Build repo-native exchange sequence controller for post-Binance `okx_spot_trades -> bybit_spot_trades` execution.
- [x] `S34-C4b` Fix OKX source-duplicate contract so raw-row counts remain truthful while exact duplicate trade rows are accepted and conflicting duplicate payloads fail loudly.
- [x] `S34-C4c` Fix OKX offset-order contract so proof/integrity treat `trade_id` as numeric monotonic but non-contiguous.
- [ ] `S34-C5` Execute ETF full-history backfill (`etf_daily_metrics`) from issuer-source artifacts.
- [x] `S34-C6` Execute FRED full-history backfill (`fred_series_metrics`) from source series history.
- [ ] `S34-C7` Execute Bitcoin full-history backfill for base and derived datasets (`bitcoin_block_headers`, `bitcoin_block_transactions`, `bitcoin_mempool_state`, `bitcoin_block_fee_totals`, `bitcoin_block_subsidy_schedule`, `bitcoin_network_hashrate_estimate`, `bitcoin_circulating_supply`).
- [ ] `S34-C8` Rebuild native and canonical aligned projections from canonical events after backfill completion.

### Proof
- [ ] `S34-P1` Run completeness proofs per dataset/partition (source coverage, row counts, checksums, and fail-loud gap validation).
- [x] `S34-P1a` Run partition state-machine proofs (new, completed, ambiguous, quarantined, and reconcile-required transitions).
- [ ] `S34-P1b` Run exactly-once/no-miss crash-recovery proofs (duplicate replay, crash after canonical write, explicit reconcile recovery).
- [ ] `S34-P1c` Run deterministic range-proof validation for completed backfill windows.
- [x] `S34-P1d` Run formal live/server-side performance proof on Binance backfill phases and capture throughput/phase-timing evidence.
- [ ] `S34-P2` Run fixed-window replay determinism proofs across every dataset family after backfill and projection rebuild.
- [ ] `S34-P3` Run cross-surface acceptance matrix on backfilled windows (`/v1/raw/query`, `/v1/raw/export`, historical HTTP, historical Python) for `native` and `aligned_1s` where applicable.
- [ ] `S34-P4` Run live deploy validation against server environment and confirm backfilled windows are queryable end-to-end.

### Guardrails
- [ ] `S34-G1` Enforce immutable backfill audit trail and per-partition manifest evidence (source checksums, cursor windows, fingerprints, proof digests).
- [ ] `S34-G2` Enforce fail-loud resume/quarantine behavior for incomplete or corrupted backfill partitions (no fallback/no silent skip), with ClickHouse as the only live authority.
- [ ] `S34-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete backfill contracts/operations notes).
- [ ] `S34-G4` User docs closeout for slice (`docs/`, full dataset-history coverage/taxonomy and backfill status reference).

## Slice 35: Automated Daily Backfill Scheduling (Configured Daily Runtime)

### Capability
- [ ] `S35-C1` Add Dagster orchestration to run daily backfill automatically for all daily datasets in fixed contract order (`binance_spot_trades`, `okx_spot_trades`, `bybit_spot_trades`, `etf_daily_metrics`, `fred_series_metrics`).
- [ ] `S35-C2` Add schedule env contract and fail-loud validation for daily runtime configuration (`ORIGO_BACKFILL_DAILY_SCHEDULE_CRON`, `ORIGO_BACKFILL_DAILY_SCHEDULE_TIMEZONE`, `ORIGO_BACKFILL_DAILY_LAG_DAYS`).
- [ ] `S35-C3` Compute `target_end_date` from UTC date and lag days and pass it consistently to each dataset backfill dispatch.
- [ ] `S35-C4` Enforce per-dataset non-overlap (single active scheduled run per dataset) and fail loudly on overlap attempts.

### Proof
- [ ] `S35-P1` Add contract tests for schedule env validation and target-boundary computation (including missing/invalid values).
- [ ] `S35-P2` Add orchestration proof for deterministic dataset dispatch order and per-dataset run argument shape.
- [ ] `S35-P3` Execute live scheduled-run proof on server and verify automatic partition advancement for the configured daily boundary.
- [ ] `S35-P4` Run full quality gates (`style`, `type`, `contract`, `replay`, `integrity`) on slice branch.

### Guardrails
- [ ] `S35-G1` Add fail-loud alerting hooks for scheduled run failures and stale dataset watermark thresholds.
- [ ] `S35-G2` Add operational runbook controls for pause/resume/force catch-up of scheduled backfill.
- [ ] `S35-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete schedule/backfill operations contract).
- [ ] `S35-G4` User docs closeout for slice (`docs/`, full reference for daily schedule behavior, freshness boundaries, and status interpretation).


## Slice Detail Sub-Slices

## Slice 0 Sub-Slices
1. `S0-01`
Action: Create monorepo skeleton and root tooling.
Done looks like: folders and root commands exist.
Constraints: no capability redesign.
2. `S0-02`
Action: Import control-plane code/history.
Done looks like: imported tree builds/runs.
Constraints: preserve history.
3. `S0-03`
Action: Rename TDW modules to Origo names.
Done looks like: imports and startup pass.
Constraints: rename-only.
4. `S0-04`
Action: Wire local ClickHouse + Dagster + persistent SQLite metadata.
Done looks like: services boot, metadata persists across restarts, and Dagster reconnects cleanly.
Constraints: no new feature work.
5. `S0-05`
Action: Execute Binance fixture baseline.
Done looks like: baseline output artifacts saved.
Constraints: fixed fixture window.
6. `S0-06`
Action: Replay determinism validation.
Done looks like: repeated outputs are identical.
Constraints: same fixture inputs.
7. `S0-07`
Action: Apply slice guardrails.
Done looks like: audit/TLS/immutable-log checks pass.
Constraints: guardrails only.
8. `S0-08`
Action: Adopt `uv` lockfile workflow for control-plane dependencies.
Done looks like: `control-plane/uv.lock` exists, `uv sync --frozen` succeeds, and dependency install docs use `uv`.
Constraints: lock exact dependency graph only; do not expand capability scope.
9. `S0-09`
Action: Scaffold ClickHouse SQL migration framework.
Done looks like: `control-plane/migrations/sql` exists, ledger table is enforced, and migrate/status runner validates and applies versioned SQL.
Constraints: SQL-only migration path; no ORM/query-builder abstraction.
10. `S0-10`
Action: Enforce environment contract and remove deployment-specific hardcoded runtime defaults.
Done looks like: root `.env.example` exists with required vars, runtime loaders fail loudly on missing/empty vars, active ClickHouse paths no longer rely on deployment defaults, and sub-slice closeout explicitly asks "was any deployment-specific value hard-coded?"
Constraints: env contract only; no capability expansion.
11. `S0-11`
Action: Developer docs closeout for Slice 0.
Done looks like: `docs/Developer/` has short topic files for monorepo/runtime/migration/env workflows with clear contracts, commands, and failure modes.
Constraints: documentation only; no feature changes.
12. `S0-12`
Action: User docs closeout for Slice 0.
Done looks like: `docs/` contains updated user-facing reference and taxonomy for currently available platform capabilities.
Constraints: documentation only; no feature changes.
13. `S0-13`
Action: Add CI hard quality gates.
Done looks like: pull-request workflows enforce repo-wide `ruff`, strict `pyright`, and separate `contract-gate`, `replay-gate`, and `integrity-gate`.
Constraints: quality gate only; no capability expansion.
14. `S0-14`
Action: Implement exchange integrity suite in Binance ingestion paths.
Done looks like: ingestion fails loudly on schema/type mismatches, ID sequence gaps, and anomaly checks before write.
Constraints: guardrail only; no source expansion.

## Slice 1 Sub-Slices
1. `S1-01`
Action: Build internal native query kernel (`SQL compiler -> Arrow execute -> Polars shape`) without HTTP.
Done looks like: a typed internal query call returns deterministic Polars frames from ClickHouse.
Constraints: native mode only; production system is comparison oracle, not code source.
2. `S1-02`
Action: Implement Binance native planner for all three core tables (`trades`, `agg_trades`, `binance_spot_trades`) in one pass.
Done looks like: fixed-window native queries work for all three datasets through one planner contract.
Constraints: no auth/hardening yet.
3. `S1-03`
Action: Implement strict projection/filter/time semantics and UTC timestamp canonicalization.
Done looks like: field allowlists, deterministic ordering, and UTC-normalized output are stable and correct.
Constraints: no warning/strict behavior yet.
4. `S1-04`
Action: Acceptance proof run.
Done looks like: acceptance suite passes.
Constraints: fixed fixtures.
5. `S1-05`
Action: Replay determinism proof.
Done looks like: repeated runs match exactly.
Constraints: unchanged inputs.
6. `S1-06`
Action: Add auth/errors/strict/warnings/queue controls.
Done looks like: guardrail suite passes.
Constraints: no new capability scope.
7. `S1-07`
Action: Bind proven query core to FastAPI `/v1/raw/query` route and request/response contracts.
Done looks like: HTTP endpoint validates request contract and delegates to internal native query core.
Constraints: no planner/capability expansion in this step.
8. `S1-08`
Action: Developer docs closeout for Slice 1.
Done looks like: `docs/Developer/` has short topic files for native query core/planner/API adapter/guardrail behavior and operational debugging.
Constraints: documentation only; no feature changes.
9. `S1-09`
Action: User docs closeout for Slice 1.
Done looks like: `docs/` contains complete raw query reference (datasets, fields, windows, strict/warnings, error codes, examples) and taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 2 Sub-Slices
1. `S2-01`
Action: Build export contracts and endpoints.
Done looks like: submit/status routes are live.
Constraints: native mode only.
2. `S2-02`
Action: Integrate API export dispatch to Dagster run.
Done looks like: run ID returned and status poll works.
Constraints: orchestration path only.
3. `S2-03`
Action: Implement Parquet export.
Done looks like: deterministic Parquet artifact generated.
Constraints: fixed schema.
4. `S2-04`
Action: Implement CSV export.
Done looks like: deterministic CSV artifact generated.
Constraints: parity with query output.
5. `S2-05`
Action: Proof suite for export determinism/parity.
Done looks like: proof checks pass.
Constraints: fixed windows.
6. `S2-06`
Action: Add rights/audit/backpressure guardrails.
Done looks like: guardrail checks pass.
Constraints: no capability changes.
7. `S2-07`
Action: Developer docs closeout for Slice 2.
Done looks like: `docs/Developer/` has short topic files for export contracts, Dagster dispatch, artifact metadata flow, and guardrail ops.
Constraints: documentation only; no feature changes.
8. `S2-08`
Action: User docs closeout for Slice 2.
Done looks like: `docs/` contains complete export API reference and taxonomy updates for export artifacts, formats, statuses, and errors.
Constraints: documentation only; no feature changes.

## Slice 3 Sub-Slices
1. `S3-01`
Action: Define plugin interface and common types.
Done looks like: adapters compile against fixed contract.
Constraints: no source-specific code.
2. `S3-02`
Action: Implement fetch layer + raw artifact persistence.
Done looks like: artifacts stored with provenance IDs.
Constraints: deterministic naming.
3. `S3-03`
Action: Implement HTML/CSV/JSON parse+normalize pipeline.
Done looks like: normalized rows stored in staging.
Constraints: deterministic parser rules.
4. `S3-04`
Action: Implement deterministic PDF parsing path.
Done looks like: stable output from fixed PDF samples.
Constraints: no runtime LLM parsing.
5. `S3-05`
Action: Implement browser fallback path.
Done looks like: JS-target fetch works through fallback.
Constraints: HTTP-first remains default.
6. `S3-06`
Action: Capability proof on sample target.
Done looks like: acceptance + replay + determinism pass.
Constraints: fixed sample set.
7. `S3-07`
Action: Add scraper guardrails.
Done looks like: error taxonomy, retry hooks, rights lookup, audit pass.
Constraints: generic platform only.
8. `S3-08`
Action: Developer docs closeout for Slice 3.
Done looks like: `docs/Developer/` has short topic files for adapter contracts, parser behavior, provenance, artifact storage, and failure handling.
Constraints: documentation only; no feature changes.
9. `S3-09`
Action: User docs closeout for Slice 3.
Done looks like: `docs/` contains complete scraper-derived dataset reference and taxonomy (metrics, provenance meanings, freshness semantics).
Constraints: documentation only; no feature changes.

## Slice 4 Sub-Slices
1. `S4-01`
Action: Implement issuer adapters 1-3.
Done looks like: daily UTC normalized records queryable.
Constraints: official sources only.
2. `S4-02`
Action: Implement issuer adapters 4-7.
Done looks like: 7 issuers operational.
Constraints: same schema contract.
3. `S4-03`
Action: Implement issuer adapters 8-10.
Done looks like: all 10 issuers operational.
Constraints: no guardrail work yet.
4. `S4-04`
Action: Implement full-history backfill per issuer.
Done looks like: earliest available records loaded.
Constraints: provenance mandatory.
5. `S4-05`
Action: Execute 10-issuer proof parity checks.
Done looks like: >=99.5% threshold met.
Constraints: fixed proof window.
6. `S4-06`
Action: Replay determinism proof.
Done looks like: repeated runs match.
Constraints: fixed input artifacts.
7. `S4-07`
Action: Add legal sign-off and rights enforcement guardrails.
Done looks like: external serving blocked without legal artifact.
Constraints: fail-closed.
8. `S4-08`
Action: Add daily schedule/retries/alerts/shadow promotion.
Done looks like: guardrail checks and promotion gate pass.
Constraints: no capability expansion.
9. `S4-09`
Action: Developer docs closeout for Slice 4.
Done looks like: `docs/Developer/` has short topic files for issuer adapters, normalization contracts, parity-proof workflow, and runbook guardrails.
Constraints: documentation only; no feature changes.
10. `S4-10`
Action: User docs closeout for Slice 4.
Done looks like: `docs/` contains complete ETF taxonomy/reference (issuers, fields, units, cadence, freshness, warnings, error semantics).
Constraints: documentation only; no feature changes.

## Slice 5 Sub-Slices
1. `S5-01`
Action: Add Binance aligned-1s materialization.
Done looks like: Binance aligned queries return expected rows.
Constraints: native path unchanged.
2. `S5-02`
Action: Add ETF aligned-1s materialization.
Done looks like: ETF daily metrics align on 1s grid.
Constraints: logical fill only.
3. `S5-03`
Action: Implement forward-fill semantics.
Done looks like: transitions and gaps behave as specified.
Constraints: no physical full explode requirement.
4. `S5-04`
Action: Implement unified aligned query planner.
Done looks like: multi-source aligned query works.
Constraints: projection/filter only.
5. `S5-05`
Action: Extend exports for aligned mode.
Done looks like: aligned exports in Parquet and CSV.
Constraints: existing export contract retained.
6. `S5-06`
Action: Run aligned proof suite.
Done looks like: acceptance + replay + determinism pass.
Constraints: fixed test windows.
7. `S5-07`
Action: Add aligned guardrails.
Done looks like: strict/warnings, freshness metadata, limits, audit pass.
Constraints: no new capability scope.
8. `S5-08`
Action: Developer docs closeout for Slice 5.
Done looks like: `docs/Developer/` has short topic files for aligned materialization semantics, forward-fill behavior, planner flow, and limits.
Constraints: documentation only; no feature changes.
9. `S5-09`
Action: User docs closeout for Slice 5.
Done looks like: `docs/` contains complete aligned-mode reference and taxonomy (modes, semantics, caveats, freshness, outputs).
Constraints: documentation only; no feature changes.

## Slice 6 Sub-Slices
1. `S6-01`
Action: Implement FRED connector and series registry mapping.
Done looks like: required series fetch and normalize correctly.
Constraints: native capability first.
2. `S6-02`
Action: Add historical and incremental ingest jobs.
Done looks like: backfill and ongoing updates both run.
Constraints: deterministic transforms.
3. `S6-03`
Action: Expose FRED in native query path.
Done looks like: native queries return FRED fields correctly.
Constraints: API shape unchanged.
4. `S6-04`
Action: Add FRED to aligned-1s path.
Done looks like: aligned queries include FRED metrics.
Constraints: fill semantics consistent.
5. `S6-05`
Action: Run FRED proof suite.
Done looks like: acceptance + replay + determinism pass.
Constraints: fixed windows.
6. `S6-06`
Action: Add FRED guardrails.
Done looks like: rights/freshness/shadow/alerts/audit checks pass.
Constraints: no capability expansion.
7. `S6-07`
Action: Developer docs closeout for Slice 6.
Done looks like: `docs/Developer/` has short topic files for FRED connector contracts, ingest/update flow, mapping logic, and guardrails.
Constraints: documentation only; no feature changes.
8. `S6-08`
Action: User docs closeout for Slice 6.
Done looks like: `docs/` contains complete FRED reference/taxonomy (series coverage, field semantics, cadence, revisions, freshness and warnings).
Constraints: documentation only; no feature changes.

## Slice 7 Sub-Slices
1. `S7-01`
Action: Scaffold root Docker stack and monorepo Dockerfiles for API + control-plane services.
Done looks like: `docker compose up` boots `clickhouse`, `dagster-webserver`, `dagster-daemon`, and `api` containers without import/runtime crashes.
Constraints: capability wiring only; no feature-scope expansion.
2. `S7-02`
Action: Wire Docker env contract and network routing for API<->Dagster<->ClickHouse.
Done looks like: all required env vars are explicit; no deployment-specific values hard-coded in runtime code paths.
Constraints: fail-loud env contract only.
3. `S7-03`
Action: Add deterministic bootstrap runner for migrations and fixed Binance daily seed ingest inside Docker.
Done looks like: bootstrap command applies migrations and loads deterministic seed window.
Constraints: bootstrap path only.
4. `S7-04`
Action: Execute clean-state stack health proof.
Done looks like: health checks pass for all services and startup is reproducible.
Constraints: fixed environment.
5. `S7-05`
Action: Execute API query/export lifecycle proof on Docker stack.
Done looks like: `/v1/raw/query` and `/v1/raw/export` succeed on seeded data, and export artifacts are materialized.
Constraints: fixed test windows.
6. `S7-06`
Action: Execute replay and restart-persistence proof.
Done looks like: repeated proof runs produce deterministic fingerprints; data/metadata persist across restart.
Constraints: same fixtures and config.
7. `S7-07`
Action: Add Docker smoke runner and failure-classified output artifacts.
Done looks like: one command reruns bootstrap+proof and emits structured pass/fail evidence.
Constraints: guardrail only.
8. `S7-08`
Action: Developer docs closeout for Slice 7.
Done looks like: `docs/Developer/` has short files for Docker stack architecture, bootstrap/proof workflow, and troubleshooting.
Constraints: documentation only; no feature changes.
9. `S7-09`
Action: User docs closeout for Slice 7.
Done looks like: `docs/` includes Docker local run/reference docs with required env vars, endpoints, and expected outcomes.
Constraints: documentation only; no feature changes.

## Slice 8 Sub-Slices
1. `S8-01`
Action: Define OKX source contract and table migration plan mirroring Binance daily ingest semantics.
Done looks like: source URL/checksum/columns/timestamps contract is explicit and migration files are staged.
Constraints: capability prep only.
2. `S8-02`
Action: Implement OKX daily fetch + checksum + parse + load capability path.
Done looks like: fixed daily file ingests into OKX raw table with deterministic writes.
Constraints: no query/API expansion yet.
3. `S8-03`
Action: Integrate OKX native query planner path and field allowlists.
Done looks like: native query endpoint can return OKX rows for fixed windows.
Constraints: native mode only.
4. `S8-04`
Action: Integrate OKX into `aligned_1s` materialization/query/export paths.
Done looks like: aligned query/export can serve OKX fields for fixed windows.
Constraints: aligned mode only.
5. `S8-05`
Action: Execute acceptance ingest/query proof on fixed OKX windows for native + aligned modes.
Done looks like: capability acceptance suite passes for both modes.
Constraints: fixed fixtures.
6. `S8-06`
Action: Execute replay determinism proof for native + aligned modes.
Done looks like: repeated runs match fingerprints exactly in both modes.
Constraints: unchanged source artifacts.
7. `S8-07`
Action: Add rights/legal/integrity + aligned guardrails for OKX.
Done looks like: rights gate, legal artifact, integrity suite, and aligned strict/warnings/freshness/audit parity are enforced fail-loud.
Constraints: guardrails only.
8. `S8-08`
Action: Add operational monitoring and queue/backpressure checks for OKX paths.
Done looks like: alerts/audit coverage and overload controls are in place.
Constraints: no capability expansion.
9. `S8-09`
Action: Developer docs closeout for Slice 8.
Done looks like: `docs/Developer/` has short files for OKX ingest/query/aligned contracts and guardrail operations.
Constraints: documentation only; no feature changes.
10. `S8-10`
Action: User docs closeout for Slice 8.
Done looks like: `docs/` includes complete OKX dataset reference and taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 10 Sub-Slices
1. `S10-01`
Action: Define deployment contract and runtime compose manifest for image-only server apply.
Done looks like: server deploy compose file exists and references only prebuilt image tags.
Constraints: no source/build execution on server.
2. `S10-02`
Action: Implement merge-triggered image build + publish workflow.
Done looks like: push to `main` builds API/control-plane images and publishes commit-pinned tags.
Constraints: deterministic tagging only.
3. `S10-03`
Action: Implement remote bootstrap/install-if-missing checks for Docker engine and compose plugin.
Done looks like: first deploy on a clean host installs dependencies and continues automatically.
Constraints: idempotent bootstrap.
4. `S10-04`
Action: Implement remote deployment apply path (manifest sync, image pull, migration run, compose restart).
Done looks like: deployed stack updates to merged commit images and migrations run before full restart.
Constraints: fail-loud on any step failure.
5. `S10-05`
Action: Execute deployment acceptance proof on merge path.
Done looks like: workflow run evidence confirms successful deploy and running services.
Constraints: fixed merge commit proof.
6. `S10-06`
Action: Execute first-run bootstrap proof and replay deployment proof.
Done looks like: clean-host bootstrap works and repeated deploy of same commit is stable.
Constraints: same commit/image tags.
7. `S10-07`
Action: Add deployment guardrails (secret/env contract checks, concurrency lock, restricted triggers).
Done looks like: unsafe trigger paths blocked and missing env/secret conditions fail immediately.
Constraints: guardrails only.
8. `S10-08`
Action: Developer docs closeout for Slice 10.
Done looks like: `docs/Developer/` has short files for deployment workflow contract, bootstrap behavior, and troubleshooting.
Constraints: documentation only; no feature changes.
9. `S10-09`
Action: User docs closeout for Slice 10.
Done looks like: `docs/` contains deployment reference for release behavior, operational expectations, and failure semantics.
Constraints: documentation only; no feature changes.

## Slice 11 Sub-Slices
1. `S11-01`
Action: Define Bybit source contract and table migration plan mirroring Binance daily ingest semantics.
Done looks like: source URL/file/header/timestamp contract is explicit and migration files are staged.
Constraints: capability prep only.
2. `S11-02`
Action: Implement Bybit daily fetch + parse + load capability path.
Done looks like: fixed daily file ingests into Bybit raw table with deterministic writes.
Constraints: no query/API expansion yet.
3. `S11-03`
Action: Integrate Bybit native query planner path and field allowlists.
Done looks like: native query endpoint can return Bybit rows for fixed windows.
Constraints: native mode only.
4. `S11-04`
Action: Integrate Bybit into `aligned_1s` materialization/query/export paths.
Done looks like: aligned query/export can serve Bybit fields for fixed windows.
Constraints: aligned mode only.
5. `S11-05`
Action: Execute acceptance ingest/query proof on fixed Bybit windows for native + aligned modes.
Done looks like: capability acceptance suite passes for both modes.
Constraints: fixed fixtures.
6. `S11-06`
Action: Execute replay determinism proof for native + aligned modes.
Done looks like: repeated runs match fingerprints exactly in both modes.
Constraints: unchanged source artifacts.
7. `S11-07`
Action: Add rights/legal/integrity + aligned guardrails for Bybit.
Done looks like: rights gate, legal artifact, integrity suite, and aligned strict/warnings/freshness/audit parity are enforced fail-loud.
Constraints: guardrails only.
8. `S11-08`
Action: Add operational monitoring and queue/backpressure checks for Bybit paths.
Done looks like: alerts/audit coverage and overload controls are in place.
Constraints: no capability expansion.
9. `S11-09`
Action: Developer docs closeout for Slice 11.
Done looks like: `docs/Developer/` has short files for Bybit ingest/query/aligned contracts and guardrail operations.
Constraints: documentation only; no feature changes.
10. `S11-10`
Action: User docs closeout for Slice 11.
Done looks like: `docs/` includes complete Bybit dataset reference and taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 12 Sub-Slices
1. `S12-01`
Action: Define Reddit source contract, OAuth env contract, and subreddit taxonomy for Slice 12.
Done looks like: fixed subreddit list, API contract, and required env vars are explicit in spec/docs.
Constraints: capability prep only.
2. `S12-02`
Action: Implement Reddit OAuth client and hourly fetch capability using official Reddit API only.
Done looks like: hourly fetch retrieves deterministic raw payloads for the configured subreddit set.
Constraints: no inference/model work yet.
3. `S12-03`
Action: Implement normalized Reddit event schema + migration-backed ClickHouse persistence.
Done looks like: ingested Reddit items persist with deterministic IDs/timestamps and source provenance/checksums.
Constraints: ingest/storage only.
4. `S12-04`
Action: Implement `ElKulako/cryptobert` inference capability for per-item sentiment scoring. (crossed over: explicitly dropped from roadmap; not implemented)
Done looks like: crossed over as not planned for implementation.
Constraints: dropped scope.
5. `S12-05`
Action: Implement hourly sentiment aggregation dataset and deterministic write contract. (crossed over: explicitly dropped from roadmap; not implemented)
Done looks like: crossed over as not planned for implementation.
Constraints: dropped scope.
6. `S12-06`
Action: Integrate Slice-12 datasets into raw query/export (`native` + `aligned_1s`) paths.
Done looks like: query/export contracts can serve Reddit sentiment datasets in both modes.
Constraints: contract parity with existing datasets.
7. `S12-07`
Action: Execute fixed-window acceptance proof for ingest, inference, query, and aligned paths.
Done looks like: acceptance artifacts validate end-to-end Slice-12 behavior.
Constraints: fixed fixtures only.
8. `S12-08`
Action: Execute replay determinism proof and source/model provenance validation.
Done looks like: repeated runs match fingerprints and include source checksum + model version traces.
Constraints: unchanged fixtures/model inputs.
9. `S12-09`
Action: Apply rights/rate-limit/integrity/freshness/aligned guardrails.
Done looks like: fail-loud guardrails are enforced and covered by guardrail proof outputs.
Constraints: guardrails only.
10. `S12-10`
Action: Developer docs closeout for Slice 12.
Done looks like: `docs/Developer/` has short files for Reddit ingest, CryptoBERT inference, signal aggregation, and guardrail operations.
Constraints: documentation only; no feature changes.
11. `S12-11`
Action: User docs closeout for Slice 12.
Done looks like: `docs/` includes complete Reddit sentiment dataset reference and taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 13 Sub-Slices
1. `S13-01`
Action: Define Bitcoin Core source contract, node env contract, and dataset taxonomy for S13.
Done looks like: unpruned-node requirement, output schemas, and required env vars are explicit in spec/docs.
Constraints: capability prep only.
2. `S13-02`
Action: Implement Bitcoin Core connector and deterministic block-header ingest path.
Done looks like: header stream persists with stable keys and canonical chain linkage.
Constraints: node ingest only.
3. `S13-03`
Action: Implement block-transaction ingest path (inputs/outputs/values/scripts/witness/coinbase).
Done looks like: transaction stream persists with deterministic ordering and block linkage.
Constraints: no derived metrics yet.
4. `S13-04`
Action: Implement mempool state ingest snapshots (`txid`, fee rate, vsize, first-seen, RBF flag).
Done looks like: mempool dataset updates hourly with deterministic snapshot semantics.
Constraints: mempool only.
5. `S13-05`
Action: Implement block fee totals and subsidy schedule derived datasets.
Done looks like: per-block fees and deterministic subsidy by height are materialized reproducibly.
Constraints: deterministic formulas only.
6. `S13-06`
Action: Implement network hashrate estimate and BTC circulating supply derived datasets.
Done looks like: derived series is generated from canonical chain data with stable formulas.
Constraints: no capability expansion beyond defined S13 metrics.
7. `S13-07`
Action: Implement `aligned_1s` materialization/query definitions for S13 derived signals/metrics.
Done looks like: fee/subsidy/hashrate/supply datasets are available in deterministic aligned-1s form.
Constraints: deterministic bucket semantics only.
8. `S13-08`
Action: Integrate S13 datasets into raw query/export (`native` + `aligned_1s`) paths.
Done looks like: query/export contracts can serve S13 datasets in both modes.
Constraints: contract parity with existing datasets.
9. `S13-09`
Action: Execute fixed-window acceptance proof for node ingest and derived outputs.
Done looks like: acceptance artifacts validate all S13 datasets on fixed windows, including aligned-1s derived signals.
Constraints: fixed fixtures only.
10. `S13-10`
Action: Execute replay determinism proof and consistency checks.
Done looks like: repeated runs match fingerprints and satisfy linkage/formula invariants.
Constraints: unchanged fixtures.
11. `S13-11`
Action: Apply rights/integrity/freshness/aligned guardrails for S13.
Done looks like: fail-loud guardrails are enforced and covered by guardrail proof outputs.
Constraints: guardrails only.
12. `S13-12`
Action: Developer docs closeout for Slice 13.
Done looks like: `docs/Developer/` has short files for node ingest, aligned/deterministic derived formulas, and guardrail operations.
Constraints: documentation only; no feature changes.
13. `S13-13`
Action: User docs closeout for Slice 13.
Done looks like: `docs/` includes complete Bitcoin Core stream/derived signal reference and taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 14 Sub-Slices
1. `S14-01`
Action: Define canonical event envelope spec and global append-only event-log table migrations.
Done looks like: envelope fields, keys, types, and migration SQL are explicit and applied with ledger tracking.
Constraints: schema and migration only; no source cutover.
2. `S14-02`
Action: Implement deterministic canonical event writer contract with `payload_raw`, `payload_sha256_raw`, and derivative `payload_json`.
Done looks like: repeated writes of identical source events produce stable event IDs and identical raw-payload hashes.
Constraints: single write path only; no projector changes.
3. `S14-03`
Action: Implement ingest cursor/ledger and source-completeness reconciliation tables.
Done looks like: each source stream/partition has explicit ingest cursor state and completeness checkpoints.
Constraints: ingest-state only.
4. `S14-04`
Action: Implement projector runtime core and ClickHouse checkpoint/watermark tables.
Done looks like: projector can start, advance checkpoint, stop, and resume deterministically.
Constraints: runtime scaffolding only.
5. `S14-05`
Action: Implement persistent aligned aggregate framework (hot/warm policy + deterministic bucket contract).
Done looks like: aligned projection tables are created and backfilled through projector runtime.
Constraints: aligned framework only; no source-specific optimization.
6. `S14-06`
Action: Extend Raw API target contracts for multi-source requests and explicit `view_id`/`view_version`.
Done looks like: query/export schemas and validators accept the target contract shape without fallback aliases.
Constraints: contract and validation only.
7. `S14-07`
Action: Add provisional rights metadata fields to query/export response contracts.
Done looks like: response schemas include rights state and provisional flags end-to-end.
Constraints: metadata plumbing only.
8. `S14-08`
Action: Add source precision mapping registry and canonical numeric typing rules.
Done looks like: numeric fields have explicit integer/decimal+scale mappings and canonical float usage is prohibited.
Constraints: canonical typing only.
9. `S14-09`
Action: Run pilot cutover on Binance `binance_spot_trades` through canonical events into native and aligned serving.
Done looks like: pilot window serves via projection path with deterministic parity against baseline.
Constraints: pilot source limited to `binance_spot_trades`.
10. `S14-10`
Action: Execute exactly-once/no-miss/fidelity proof suite (duplicate replay, crash/restart, injected gaps, raw/precision round-trip).
Done looks like: proofs demonstrate idempotent writes, fail-loud gap detection, and precision-preserving round-trip behavior.
Constraints: unchanged fixtures and controlled fault-injection only.
11. `S14-11`
Action: Apply event-runtime guardrails (typed errors, invariant checks, immutable audit transitions, reconciliation quarantine, precision linting).
Done looks like: fail-loud behavior and audit coverage are enforced for writer/projector state and reconciliation outcomes.
Constraints: guardrails only.
12. `S14-12`
Action: Developer docs closeout for Slice 14.
Done looks like: `docs/Developer/` has short files for envelope contract, raw-fidelity semantics, exactly-once/no-miss mechanics, projector lifecycle, and failure handling.
Constraints: documentation only; no feature changes.
13. `S14-13`
Action: User docs closeout for Slice 14.
Done looks like: `docs/` includes event-driven serving semantics, view/version reference, exactly-once/no-miss guarantees, precision semantics, and rights metadata taxonomy updates.
Constraints: documentation only; no feature changes.

## Slice 15 Sub-Slices
1. `S15-01`
Action: Port Binance `binance_spot_trades` ingest writes to canonical event log.
Done looks like: spot-trade source writes only canonical events with source provenance and deterministic IDs.
Constraints: no legacy write fallback.
2. `S15-02`
Action: Port Binance `binance_spot_trades` ingest writes to canonical event log.
Done looks like: agg-trade source emits canonical events with deterministic envelope fields.
Constraints: no legacy write fallback.
3. `S15-03`
Action: Port Binance `binance_spot_trades` ingest writes to canonical event log.
Done looks like: futures-trade source emits canonical events with deterministic envelope fields.
Constraints: no legacy write fallback.
4. `S15-04`
Action: Implement Binance native serving projections from canonical events for all three datasets.
Done looks like: native queries for all Binance datasets resolve only through projections.
Constraints: native serving only.
5. `S15-05`
Action: Implement Binance aligned-1s projections from canonical events for all three datasets.
Done looks like: aligned queries/exports include all Binance datasets from persistent aligned projections.
Constraints: aligned serving only.
6. `S15-06`
Action: Cut query/export serving to projection path and remove legacy direct-serving path for Binance.
Done looks like: no Binance query/export call touches legacy serving tables.
Constraints: no fallback paths retained.
7. `S15-07`
Action: Execute acceptance and parity proof for Binance `native` + `aligned_1s`.
Done looks like: fixed-window results match approved baselines for both modes.
Constraints: fixed fixtures only.
8. `S15-08`
Action: Execute replay determinism proof for Binance event and projection paths.
Done looks like: repeated runs match fingerprints across write, project, and serve.
Constraints: unchanged fixtures/config.
9. `S15-09`
Action: Execute exactly-once ingest proof for Binance datasets (duplicate replay + crash/restart).
Done looks like: idempotency holds under retries/replays and fault-injection restarts without duplicate canonical rows.
Constraints: fixed fixtures and controlled fault injection only.
10. `S15-10`
Action: Execute no-miss + raw-fidelity/precision proof for Binance datasets.
Done looks like: injected gaps are detected fail-loud and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
11. `S15-11`
Action: Apply Binance guardrails (integrity, freshness, reconciliation/quarantine, precision checks, provisional rights metadata).
Done looks like: integrity/freshness/reconciliation/precision/rights checks fail loudly and are visible in API behavior.
Constraints: guardrails only.
12. `S15-12`
Action: Developer docs closeout for Slice 15.
Done looks like: `docs/Developer/` has short files for Binance event schema mapping, exactly-once/no-miss mechanics, raw-fidelity/precision checks, projector flow, and troubleshooting.
Constraints: documentation only; no feature changes.
13. `S15-13`
Action: User docs closeout for Slice 15.
Done looks like: `docs/` includes complete Binance dataset taxonomy and mode behavior updates for event-driven serving plus precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 16 Sub-Slices
1. `S16-01`
Action: Port ETF normalized records to canonical event log with full provenance.
Done looks like: ETF writes land as canonical events preserving issuer/source/checksum lineage.
Constraints: no legacy write fallback.
2. `S16-02`
Action: Implement ETF native serving projections from canonical events.
Done looks like: ETF native queries resolve only through event-driven projections.
Constraints: native serving only.
3. `S16-03`
Action: Implement ETF aligned-1s projections with deterministic forward-fill semantics.
Done looks like: aligned ETF rows are projection-driven with explicit carry-forward rules.
Constraints: no implicit or undocumented fill behavior.
4. `S16-04`
Action: Cut ETF query/export serving to projection path and remove legacy direct-serving path.
Done looks like: ETF query/export has a single projection-driven path for both modes.
Constraints: no fallback paths retained.
5. `S16-05`
Action: Execute acceptance and parity proof for ETF `native` + `aligned_1s`.
Done looks like: fixed-window outputs in both modes match approved ETF baselines.
Constraints: fixed fixtures only.
6. `S16-06`
Action: Execute replay determinism proof for ETF event/projection paths.
Done looks like: repeated runs match deterministic fingerprints and fill transitions.
Constraints: unchanged fixtures/config.
7. `S16-07`
Action: Execute exactly-once ingest proof for ETF records (duplicate replay + crash/restart).
Done looks like: retries/replays and fault-injection restarts do not duplicate canonical ETF events.
Constraints: fixed fixtures and controlled fault injection only.
8. `S16-08`
Action: Execute no-miss + raw-fidelity/precision proof for ETF records.
Done looks like: cadence/gap checks fail loudly on injected misses and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
9. `S16-09`
Action: Apply ETF guardrails (rights/legal, stale/missing warnings, reconciliation/quarantine, precision checks, provisional rights metadata).
Done looks like: ETF response behavior enforces fail-loud rights, quality, completeness, and precision semantics.
Constraints: guardrails only.
10. `S16-10`
Action: Developer docs closeout for Slice 16.
Done looks like: `docs/Developer/` has short files for ETF event contracts, aligned fill rules, exactly-once/no-miss mechanics, precision checks, and guardrail operations.
Constraints: documentation only; no feature changes.
11. `S16-11`
Action: User docs closeout for Slice 16.
Done looks like: `docs/` includes complete ETF event-driven taxonomy and aligned semantics updates plus precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 17 Sub-Slices
1. `S17-01`
Action: Port FRED ingest writes to canonical event log with series metadata.
Done looks like: FRED rows and metadata revisions are persisted as canonical events.
Constraints: no legacy write fallback.
2. `S17-02`
Action: Implement FRED native serving projections from canonical events.
Done looks like: FRED native queries resolve through projections only.
Constraints: native serving only.
3. `S17-03`
Action: Implement FRED aligned-1s projections from canonical events.
Done looks like: aligned FRED series is served from persistent projections with deterministic time handling.
Constraints: aligned serving only.
4. `S17-04`
Action: Cut FRED query/export serving to projection path and remove legacy direct-serving path.
Done looks like: no FRED query/export path bypasses event-driven projections.
Constraints: no fallback paths retained.
5. `S17-05`
Action: Execute acceptance and parity proof for FRED `native` + `aligned_1s`.
Done looks like: fixed-window outputs in both modes match approved FRED baselines.
Constraints: fixed fixtures only.
6. `S17-06`
Action: Execute replay determinism proof for FRED event/projection paths.
Done looks like: repeated runs match deterministic fingerprints including metadata/version traces.
Constraints: unchanged fixtures/config.
7. `S17-07`
Action: Execute exactly-once ingest proof for FRED events (duplicate replay + crash/restart).
Done looks like: retries/replays and fault-injection restarts do not duplicate canonical FRED events.
Constraints: fixed fixtures and controlled fault injection only.
8. `S17-08`
Action: Execute no-miss + raw-fidelity/precision proof for FRED events.
Done looks like: cadence/gap checks fail loudly on injected misses and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
9. `S17-09`
Action: Apply FRED guardrails (freshness, rights/legal, reconciliation/quarantine, precision checks, provisional rights metadata).
Done looks like: freshness/rights/completeness/precision semantics are enforced fail-loud in API responses.
Constraints: guardrails only.
10. `S17-10`
Action: Developer docs closeout for Slice 17.
Done looks like: `docs/Developer/` has short files for FRED event mapping, projection semantics, exactly-once/no-miss mechanics, precision checks, and freshness operations.
Constraints: documentation only; no feature changes.
11. `S17-11`
Action: User docs closeout for Slice 17.
Done looks like: `docs/` includes complete FRED taxonomy updates for event-driven native/aligned behavior plus precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 18 Sub-Slices
1. `S18-01`
Action: Port OKX ingest writes to canonical event log with existing checksum/provenance guarantees.
Done looks like: OKX writes flow only into canonical events with deterministic identifiers.
Constraints: no legacy write fallback.
2. `S18-02`
Action: Implement OKX native serving projections from canonical events.
Done looks like: OKX native queries resolve only through projection path.
Constraints: native serving only.
3. `S18-03`
Action: Implement OKX aligned-1s projections from canonical events.
Done looks like: aligned OKX outputs are served from persistent projections.
Constraints: aligned serving only.
4. `S18-04`
Action: Cut OKX query/export serving to projection path and remove legacy direct-serving path.
Done looks like: no OKX query/export path bypasses projections.
Constraints: no fallback paths retained.
5. `S18-05`
Action: Execute acceptance and parity proof for OKX `native` + `aligned_1s`.
Done looks like: fixed-window outputs in both modes match approved OKX baselines.
Constraints: fixed fixtures only.
6. `S18-06`
Action: Execute replay determinism proof for OKX event/projection paths.
Done looks like: repeated runs match deterministic fingerprints across both modes.
Constraints: unchanged fixtures/config.
7. `S18-07`
Action: Execute exactly-once ingest proof for OKX events (duplicate replay + crash/restart).
Done looks like: retries/replays and fault-injection restarts do not duplicate canonical OKX events.
Constraints: fixed fixtures and controlled fault injection only.
8. `S18-08`
Action: Execute no-miss + raw-fidelity/precision proof for OKX events.
Done looks like: injected gaps are detected fail-loud and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
9. `S18-09`
Action: Apply OKX guardrails (integrity, rights/legal, reconciliation/quarantine, precision checks, provisional rights metadata).
Done looks like: integrity/rights/completeness/precision semantics are enforced fail-loud in serving paths.
Constraints: guardrails only.
10. `S18-10`
Action: Developer docs closeout for Slice 18.
Done looks like: `docs/Developer/` has short files for OKX event contracts, projections, exactly-once/no-miss mechanics, precision checks, and guardrail runbooks.
Constraints: documentation only; no feature changes.
11. `S18-11`
Action: User docs closeout for Slice 18.
Done looks like: `docs/` includes complete OKX taxonomy and event-driven mode behavior updates plus precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 19 Sub-Slices
1. `S19-01`
Action: Port Bybit ingest writes to canonical event log with existing checksum/provenance guarantees.
Done looks like: Bybit writes flow only into canonical events with deterministic identifiers.
Constraints: no legacy write fallback.
2. `S19-02`
Action: Implement Bybit native serving projections from canonical events.
Done looks like: Bybit native queries resolve only through projection path.
Constraints: native serving only.
3. `S19-03`
Action: Implement Bybit aligned-1s projections from canonical events.
Done looks like: aligned Bybit outputs are served from persistent projections.
Constraints: aligned serving only.
4. `S19-04`
Action: Cut Bybit query/export serving to projection path and remove legacy direct-serving path.
Done looks like: no Bybit query/export path bypasses projections.
Constraints: no fallback paths retained.
5. `S19-05`
Action: Execute acceptance and parity proof for Bybit `native` + `aligned_1s`.
Done looks like: fixed-window outputs in both modes match approved Bybit baselines.
Constraints: fixed fixtures only.
6. `S19-06`
Action: Execute replay determinism proof for Bybit event/projection paths.
Done looks like: repeated runs match deterministic fingerprints across both modes.
Constraints: unchanged fixtures/config.
7. `S19-07`
Action: Execute exactly-once ingest proof for Bybit events (duplicate replay + crash/restart).
Done looks like: retries/replays and fault-injection restarts do not duplicate canonical Bybit events.
Constraints: fixed fixtures and controlled fault injection only.
8. `S19-08`
Action: Execute no-miss + raw-fidelity/precision proof for Bybit events.
Done looks like: injected gaps are detected fail-loud and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
9. `S19-09`
Action: Apply Bybit guardrails (integrity, rights/legal, reconciliation/quarantine, precision checks, provisional rights metadata).
Done looks like: integrity/rights/completeness/precision semantics are enforced fail-loud in serving paths.
Constraints: guardrails only.
10. `S19-10`
Action: Developer docs closeout for Slice 19.
Done looks like: `docs/Developer/` has short files for Bybit event contracts, projections, exactly-once/no-miss mechanics, precision checks, and guardrail runbooks.
Constraints: documentation only; no feature changes.
11. `S19-11`
Action: User docs closeout for Slice 19.
Done looks like: `docs/` includes complete Bybit taxonomy and event-driven mode behavior updates plus precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 20 Sub-Slices
1. `S20-01`
Action: Port Bitcoin block-header ingest writes to canonical event log.
Done looks like: block-header stream writes canonical events with stable chain linkage keys.
Constraints: no legacy write fallback.
2. `S20-02`
Action: Port Bitcoin block-transaction ingest writes to canonical event log.
Done looks like: transaction stream writes canonical events with deterministic block linkage and ordering keys.
Constraints: no legacy write fallback.
3. `S20-03`
Action: Port Bitcoin mempool-state ingest writes to canonical event log.
Done looks like: mempool snapshots write canonical events with deterministic snapshot identity.
Constraints: no legacy write fallback.
4. `S20-04`
Action: Port Bitcoin derived-signal writes (fees/subsidy/hashrate/supply) to canonical event log.
Done looks like: derived datasets are eventized with explicit provenance back to canonical chain events.
Constraints: no formula changes in this step.
5. `S20-05`
Action: Implement native serving projections for all seven onboarded Bitcoin datasets.
Done looks like: native mode serves headers, transactions, mempool, and all four derived datasets from projections.
Constraints: native serving only.
6. `S20-06`
Action: Implement aligned-1s projections for the four derived Bitcoin datasets.
Done looks like: aligned mode serves fees/subsidy/hashrate/supply from persistent projections.
Constraints: aligned scope remains derived-only in this tranche.
7. `S20-07`
Action: Cut Bitcoin query/export serving to projection path and remove legacy direct-serving path.
Done looks like: no Bitcoin query/export path bypasses event-driven projections.
Constraints: no fallback paths retained.
8. `S20-08`
Action: Execute acceptance and parity proof for Bitcoin native (7 datasets) + aligned (4 derived datasets).
Done looks like: fixed-window outputs meet approved Bitcoin baselines for both modes.
Constraints: fixed fixtures only.
9. `S20-09`
Action: Execute replay determinism proof for Bitcoin event/projection paths.
Done looks like: repeated runs match deterministic fingerprints across event writes and serving outputs.
Constraints: unchanged fixtures/config.
10. `S20-10`
Action: Execute exactly-once ingest proof for all seven Bitcoin datasets (duplicate replay + crash/restart).
Done looks like: retries/replays and fault-injection restarts do not duplicate canonical Bitcoin events.
Constraints: fixed fixtures and controlled fault injection only.
11. `S20-11`
Action: Execute no-miss + raw-fidelity/precision proof across all seven Bitcoin datasets.
Done looks like: injected gaps are detected fail-loud and raw-hash/precision checks pass against source artifacts.
Constraints: fixed fixtures only.
12. `S20-12`
Action: Execute live-node proof gate once non-IBD preconditions are met.
Done looks like: live-node run confirms event and projection paths behave deterministically under current chain tip.
Constraints: gate requires node out of IBD and fully stable.
13. `S20-13`
Action: Apply Bitcoin guardrails (linkage/invariant checks, freshness warnings, reconciliation/quarantine, precision checks, rights metadata behavior).
Done looks like: integrity/freshness/completeness/precision/rights checks fail loudly and are reflected in API warnings/errors.
Constraints: guardrails only.
14. `S20-14`
Action: Developer docs closeout for Slice 20.
Done looks like: `docs/Developer/` has short files for Bitcoin event contracts, projection model, exactly-once/no-miss mechanics, precision checks, invariants, and operational troubleshooting.
Constraints: documentation only; no feature changes.
15. `S20-15`
Action: User docs closeout for Slice 20.
Done looks like: `docs/` includes complete Bitcoin taxonomy for event-driven native and aligned outputs, including derived-only aligned scope and precision/guarantee semantics.
Constraints: documentation only; no feature changes.

## Slice 21 Sub-Slices
1. `S21-01`
Action: Add explicit runtime contract check for `canonical_aligned_1s_aggregates` in Binance aligned query path.
Done looks like: aligned query fails loudly with explicit contract error when canonical table is missing or schema/type contract drifts.
Constraints: no fallback paths.
2. `S21-02`
Action: Refactor Binance aligned guardrail proof harness to provision aligned storage only through migration runner.
Done looks like: proof DB is created/migrated by `MigrationRunner`, with no manual aligned DDL in proof code.
Constraints: proof harness only.
3. `S21-03`
Action: Add contract-gate tests for canonical aligned storage failure modes.
Done looks like: tests cover missing-table, missing-column, and type-drift fail-loud behavior.
Constraints: contract tests only.
4. `S21-04`
Action: Execute Binance aligned acceptance/replay proof after contract enforcement.
Done looks like: aligned query behavior remains deterministic with unchanged approved fingerprints for fixed windows.
Constraints: fixed fixtures only.
5. `S21-05`
Action: Developer docs closeout for Slice 21.
Done looks like: `docs/Developer/` has short files for aligned storage contract, runtime enforcement path, and troubleshooting.
Constraints: documentation only; no feature changes.
6. `S21-06`
Action: User docs closeout for Slice 21.
Done looks like: `docs/` explicitly states aligned storage contract requirements and related failure semantics.
Constraints: documentation only; no feature changes.

## Slice 22 Sub-Slices
1. `S22-01`
Action: Refactor Binance historical Python methods to explicit `get_binance_spot_trades` and `get_binance_spot_klines`.
Done looks like: only explicit Binance spot methods exist; old generic/futures methods are removed.
Constraints: no method aliases.
2. `S22-02`
Action: Implement strict window/date contract in historical query helpers.
Done looks like: window/date validation is enforced with strict UTC `YYYY-MM-DD` semantics and fail-loud behavior (selector semantics later normalized by S25).
Constraints: no fallback parameter aliases.
3. `S22-03`
Action: Add Binance historical HTTP routes with raw-envelope parity response shape.
Done looks like: Binance trades/klines routes return `mode`, `source`, `sources`, `row_count`, `schema`, `warnings`, `rows`, and rights metadata.
Constraints: endpoint scope only.
4. `S22-04`
Action: Add contract tests for route registration, validation failures, strict warning behavior, and no-data behavior.
Done looks like: tests cover 409 contract error, strict warning 409, and 404 no-data behavior.
Constraints: contract tests only.
5. `S22-05`
Action: Apply guardrails and docs closeout.
Done looks like: auth/rights/error-taxonomy guardrails are active and docs are updated.
Constraints: guardrails + docs only.

## Slice 23 Sub-Slices
1. `S23-01`
Action: Add OKX historical Python methods with the same signature family as Binance.
Done looks like: `get_okx_spot_trades` and `get_okx_spot_klines` match Binance method signatures.
Constraints: no signature divergence.
2. `S23-02`
Action: Add OKX historical HTTP routes.
Done looks like: `/v1/historical/okx/spot/trades` and `/v1/historical/okx/spot/klines` are active and protected by internal API key.
Constraints: endpoint scope only.
3. `S23-03`
Action: Normalize OKX spot-trades schema and maker-side mapping.
Done looks like: output schema matches shared trades contract and side mapping enforces `buy -> 0`, `sell -> 1` fail-loud.
Constraints: no schema fallback.
4. `S23-04`
Action: Prove OKX-to-Binance cohesion through contract tests.
Done looks like: route and method cohesion tests pass under repeated runs.
Constraints: contract/replay tests only.
5. `S23-05`
Action: Apply guardrails and docs closeout.
Done looks like: rights, strict-warning, and error-taxonomy guardrails are active and docs are updated.
Constraints: guardrails + docs only.

## Slice 24 Sub-Slices
1. `S24-01`
Action: Add Bybit historical Python methods with signature parity.
Done looks like: `get_bybit_spot_trades` and `get_bybit_spot_klines` match Binance/OKX method signatures.
Constraints: no signature divergence.
2. `S24-02`
Action: Add Bybit historical HTTP routes.
Done looks like: `/v1/historical/bybit/spot/trades` and `/v1/historical/bybit/spot/klines` are active and guarded.
Constraints: endpoint scope only.
3. `S24-03`
Action: Normalize Bybit spot-trades schema and maker-side mapping.
Done looks like: output schema matches shared trades contract and mapping enforces `buy -> 0`, `sell -> 1` fail-loud.
Constraints: no schema fallback.
4. `S24-04`
Action: Prove six-endpoint and six-method cohesion.
Done looks like: historical contract test suite confirms route presence, request validation, and method signature parity.
Constraints: contract/replay tests only.
5. `S24-05`
Action: Publish cutover runbook and close slice guardrails/docs.
Done looks like: old endpoint retirement mapping is documented and developer/user docs are updated.
Constraints: docs + migration mapping only.

## Slice 25 Sub-Slices
1. `S25-01`
Action: Implement shared historical request model and parser path.
Done looks like: all historical routes and `HistoricalData` methods consume one canonical parameter contract.
Constraints: no alias fallback parameters.
2. `S25-02`
Action: Implement unbounded default-window semantics for historical and raw query paths.
Done looks like: missing window selectors resolve to `earliest -> now` deterministically.
Constraints: fail-loud on invalid combinations only.
3. `S25-03`
Action: Migrate existing six exchange historical routes to unified contract.
Done looks like: routes preserve behavior while adopting shared selector/filter/strict semantics.
Constraints: no silent compatibility shims.
4. `S25-04`
Action: Add contract/replay proofs for bounded and unbounded windows.
Done looks like: deterministic behavior is proven with and without selectors across both surfaces.
Constraints: proof-only changes.
5. `S25-05`
Action: Apply guardrails and close docs.
Done looks like: validation/error semantics are fail-loud and docs fully describe unified contract.
Constraints: guardrails + docs only.

## Slice 26 Sub-Slices
1. `S26-01`
Action: Mark legacy Binance non-spot dataset keys as explicitly removed from historical Python/HTTP scope.
Done looks like: no legacy Binance non-spot dataset appears in S26 historical matrix or runtime contracts.
Constraints: no implicit partial implementation.
2. `S26-02`
Action: Add exchange historical aligned-mode parity across all exchange trade datasets.
Done looks like: three in-scope exchange trade datasets support `native` and `aligned_1s`.
Constraints: no dataset-mode gaps remain.
3. `S26-03`
Action: Harmonize exchange signatures and request/response semantics.
Done looks like: method/endpoint contracts are source-cohesive and parameter-identical where applicable.
Constraints: no source-specific naming drift.
4. `S26-04`
Action: Run exchange-wide acceptance/replay/cohesion proofs.
Done looks like: all in-scope exchange trade datasets pass deterministic proofs in both modes.
Constraints: fixed fixtures only.
5. `S26-05`
Action: Apply guardrails and close docs.
Done looks like: exchange historical routes enforce auth/rights/strict/error contracts and docs are updated.
Constraints: guardrails + docs only.

## Slice 27 Sub-Slices
1. `S27-01`
Action: Add historical ETF Python method + HTTP endpoint.
Done looks like: `get_etf_daily_metrics` and `/v1/historical/etf/daily_metrics` are available with shared contract semantics.
Constraints: shared contract only.
2. `S27-02`
Action: Implement ETF `native` + `aligned_1s` historical serving.
Done looks like: both modes return deterministic ETF outputs with forward-fill semantics.
Constraints: no mode-specific contract drift.
3. `S27-03`
Action: Add ETF selector/field/filter support under shared historical contract.
Done looks like: ETF endpoint honors shared parameter semantics and fail-loud validation.
Constraints: no ETF-only parameter names.
4. `S27-04`
Action: Run ETF acceptance/replay/parity proofs.
Done looks like: deterministic outputs match baseline expectations in both modes.
Constraints: fixed fixtures only.
5. `S27-05`
Action: Apply guardrails and close docs.
Done looks like: rights/freshness/strict/error semantics are active and docs are updated.
Constraints: guardrails + docs only.

## Slice 28 Sub-Slices
1. `S28-01`
Action: Add historical FRED Python method + HTTP endpoint.
Done looks like: `get_fred_series_metrics` and `/v1/historical/fred/series_metrics` are available with shared contract semantics.
Constraints: shared contract only.
2. `S28-02`
Action: Implement FRED `native` + `aligned_1s` historical serving.
Done looks like: both modes return deterministic FRED outputs with publish-freshness semantics.
Constraints: no mode-specific contract drift.
3. `S28-03`
Action: Add FRED selector/field/filter support under shared historical contract.
Done looks like: FRED endpoint honors shared parameter semantics and fail-loud validation.
Constraints: no FRED-only parameter names.
4. `S28-04`
Action: Run FRED acceptance/replay/parity proofs.
Done looks like: deterministic outputs match baseline expectations in both modes.
Constraints: fixed fixtures only.
5. `S28-05`
Action: Apply guardrails and close docs.
Done looks like: rights/freshness/strict/error semantics are active and docs are updated.
Constraints: guardrails + docs only.

## Slice 29 Sub-Slices
1. `S29-01`
Action: Implement aligned projections for `bitcoin_block_headers`.
Done looks like: aligned headers are queryable with deterministic timestamp semantics.
Constraints: canonical-aligned storage contract only.
2. `S29-02`
Action: Implement aligned projections for `bitcoin_block_transactions`.
Done looks like: aligned transaction aggregates are queryable with deterministic semantics.
Constraints: canonical-aligned storage contract only.
3. `S29-03`
Action: Implement aligned projections for `bitcoin_mempool_state`.
Done looks like: aligned mempool-state aggregates are queryable with deterministic semantics.
Constraints: canonical-aligned storage contract only.
4. `S29-04`
Action: Integrate new aligned Bitcoin datasets into query/export with proofs.
Done looks like: all Bitcoin datasets support `aligned_1s` and pass acceptance/replay/negative-contract proofs.
Constraints: no temporary exclusions.
5. `S29-05`
Action: Apply guardrails and close docs.
Done looks like: integrity/freshness/strict/error semantics are active and docs are updated for full Bitcoin aligned coverage.
Constraints: guardrails + docs only.

## Slice 30 Sub-Slices
1. `S30-01`
Action: Add explicit historical Python methods for all seven Bitcoin datasets.
Done looks like: seven explicit methods exist (`get_bitcoin_block_headers`, `get_bitcoin_block_transactions`, `get_bitcoin_mempool_state`, `get_bitcoin_block_fee_totals`, `get_bitcoin_block_subsidy_schedule`, `get_bitcoin_network_hashrate_estimate`, `get_bitcoin_circulating_supply`) with shared historical parameters and `mode`.
Constraints: no generic fallback-only public API.
2. `S30-02`
Action: Add explicit historical HTTP endpoints for all seven Bitcoin datasets.
Done looks like: seven endpoints exist (`/v1/historical/bitcoin/block_headers`, `/v1/historical/bitcoin/block_transactions`, `/v1/historical/bitcoin/mempool_state`, `/v1/historical/bitcoin/block_fee_totals`, `/v1/historical/bitcoin/block_subsidy_schedule`, `/v1/historical/bitcoin/network_hashrate_estimate`, `/v1/historical/bitcoin/circulating_supply`) with shared contract behavior.
Constraints: endpoint naming must be deterministic and taxonomy-aligned.
3. `S30-03`
Action: Wire native/aligned serving and filter/projection behavior for all seven Bitcoin endpoints.
Done looks like: both modes behave identically by contract semantics across all seven datasets.
Constraints: no dataset-specific contract exceptions.
4. `S30-04`
Action: Run full Bitcoin historical acceptance/replay/cohesion proofs.
Done looks like: method/endpoint outputs are deterministic and cohesive in both modes for all seven datasets.
Constraints: fixed fixtures only.
5. `S30-05`
Action: Apply guardrails and close docs.
Done looks like: auth/rights/strict/error guardrails are active and docs fully cover Bitcoin historical interfaces.
Constraints: guardrails + docs only.

## Slice 31 Sub-Slices
1. `S31-01`
Action: Build complete historical dataset/mode coverage matrix and enforce it in contract tests.
Done looks like: every in-scope dataset has historical Python + HTTP coverage in `native` and `aligned_1s`, with deferred dataset exclusions explicit.
Constraints: zero uncovered dataset-mode pairs.
2. `S31-02`
Action: Execute full historical replay/integrity suites across all datasets and both modes.
Done looks like: full-surface deterministic and integrity gates pass without waivers.
Constraints: no partial-suite closeout.
3. `S31-03`
Action: Publish final migration mapping and operational runbook for internal rollout.
Done looks like: legacy endpoint-to-Origo mapping and rollback guidance are explicit and tested.
Constraints: no tribal-knowledge handoff.
4. `S31-04`
Action: Reconcile docs taxonomy and endpoint/method references against live truth.
Done looks like: docs contain canonical endpoint/method matrix and no stale contracts.
Constraints: documentation-only behavior updates.
5. `S31-05`
Action: Final guardrail closeout and readiness sign-off.
Done looks like: work-plan checkboxes, version/changelog/env contract, manifests, baseline fixtures, and run-notes are complete for rollout gate.
Constraints: closeout artifacts required before declaring rollout-ready.

## Slice 32 Sub-Slices
1. `S32-01`
Action: Patch deploy workflow to source `ORIGO_AUDIT_LOG_RETENTION_DAYS` from root `.env.example`, validate numeric policy bounds, and persist value into `/opt/origo/deploy/.env`.
Done looks like: deploy workflow fails before compose on invalid policy value and always writes a validated retention key into runtime env file.
Constraints: no fallback defaults outside root `.env.example`.
2. `S32-02`
Action: Patch server compose contract to require `ORIGO_AUDIT_LOG_RETENTION_DAYS` in API environment wiring.
Done looks like: compose expansion fails loudly if key is absent at runtime.
Constraints: compose contract only; no API behavior changes.
3. `S32-03`
Action: Execute full local quality gates for fix branch (`ruff`, `pyright`, `tests/contract`, `tests/replay`, `tests/integrity`).
Done looks like: all gates pass with no waivers.
Constraints: fixed test suites only.
4. `S32-04`
Action: Execute merge-triggered deploy proof and live-domain health/API smoke checks.
Done looks like: deploy workflow succeeds and remote API passes health plus representative endpoint checks.
Constraints: proof runs against deployed `main` commit image set.
5. `S32-05`
Action: Close docs/artifacts/version/changelog/env contract for Slice 32.
Done looks like: docs are current, version/changelog/env are updated, and Slice 32 artifacts are present for handoff.
Constraints: closeout artifacts required before slice can be marked done.

## Slice 33 Sub-Slices
1. `S33-01`
Action: Freeze the Binance dataset cleanup contract for runtime, tests, docs, and specs.
Done looks like: scope is explicit and locked (hard remove legacy Binance non-spot keys; enforce `binance_spot_trades` identifier).
Constraints: contract only; no hidden compatibility scope.
2. `S33-02`
Action: Apply runtime contract cleanup in query/export schemas and planners.
Done looks like: all active code paths accept `binance_spot_trades` and reject removed datasets fail-loud.
Constraints: no alias/fallback behavior.
3. `S33-03`
Action: Apply control-plane cleanup for Binance ingestion/projectors/assets/jobs.
Done looks like: only Binance spot ingest remains and writes/reads under `binance_spot_trades` stream contract.
Constraints: no agg/futures execution path retained.
4. `S33-04`
Action: Update rights/legal/precision registries and contract artifacts.
Done looks like: contracts reference only active Binance dataset scope and stream IDs.
Constraints: fail-closed rights behavior preserved.
5. `S33-05`
Action: Execute contract/replay/integrity proofs and full quality gates.
Done looks like: all gates pass with renamed dataset and removed dataset hard-fail behavior.
Constraints: no waivers.
6. `S33-06`
Action: Close docs/artifacts/version/changelog/env contract for Slice 33.
Done looks like: docs/spec/artifacts fully match cleaned contract and provide no stale dataset references in active surfaces.
Constraints: closeout only; no new capability expansion.

## Slice 34 Sub-Slices
1. `S34-01`
Action: Freeze full backfill contract and ordered inventory for all onboarded datasets.
Done looks like: one canonical ordered dataset list and per-dataset earliest-source boundary are documented and locked for execution.
Constraints: planning/contract only; no ingestion execution yet.
2. `S34-02`
Action: Prepare backfill orchestration controls (partition planner, cursor ledger, resume policy, fail-loud gap checks).
Done looks like: backfill runs are resumable from terminal partition proof state, stop loudly on detected gaps or checksum mismatches, and exchange Dagster runs carry explicit projection/execution/audit tags while fast canonical insert is allowed only for brand-new empty partitions in deferred backfill mode.
Constraints: no source data mutation.
3. `S34-02a`
Action: Stabilize ETF canonical payload idempotency by excluding run-volatile provenance timestamps from canonical event payload construction.
Done looks like: rerun of the same ETF source-event identity does not change canonical payload hash when only runtime provenance timestamps differ.
Constraints: preserve source/artifact lineage fields and keep fail-loud identity conflict semantics for true payload drift.
4. `S34-02b`
Action: Replace direct asset invocation with Dagster-native partition execution for live backfill dispatch and monitoring.
Done looks like: backfill submission launches partitioned Dagster runs instead of calling asset functions directly, and monitor tooling only observes or launches Dagster runs.
Constraints: canonical writer remains the exact-once layer; no alternate execution path.
5. `S34-02c`
Action: Move authoritative backfill progress, proof, and quarantine state fully into ClickHouse and remove file-backed live state paths.
Done looks like: live backfill correctness no longer depends on JSON/file state, and authoritative state is stored in ClickHouse tables for manifests, partition proofs, quarantines, and range proofs.
Constraints: file artifacts may remain as immutable evidence only.
6. `S34-02d`
Action: Implement strict partition state machine and explicit reconcile flow.
Done looks like: normal backfill fails loudly on completed or ambiguous partitions, and only reconcile can resolve `reconcile_required` or `quarantined` partitions.
Constraints: no silent cursor advancement and no blind canonical rewrites.
7. `S34-02e`
Action: Implement deterministic partition-local proofs and range-level proof folding.
Done looks like: each partition records source/canonical identity digests, counts, first/last offsets where applicable, gap metrics, and terminal proof status; completed ranges emit machine-checkable range proofs.
Constraints: proof contract must be source-native and deterministic.
8. `S34-02f`
Action: Gate fast insert, resume, projection rebuild, and serving promotion on terminal proof state only.
Done looks like: only genuinely new partitions may use fast insert, resume truth comes from terminal proofs, and projections serve only up to the proved canonical boundary.
Constraints: no fallback to bare cursor maxima.
9. `S34-02g`
Action: Add reconcile fast path that proves existing canonical partitions directly from source and canonical evidence.
Done looks like: reconcile on partitions with existing canonical rows no longer routes through duplicate-writer replay, and either emits terminal proof or quarantine from source-vs-canonical evidence alone.
Constraints: no blind canonical rewrite and no fallback duplicate path in reconcile mode.
10. `S34-02h`
Action: Remove Binance source-proof and fresh-write Python bottlenecks with staged/vectorized execution.
Done looks like: source proof and fresh canonical write preparation no longer depend on Python per-row loops or giant Python list materialization, and daily partition runtime drops to the seconds range on the live server benchmark path.
Constraints: preserve canonical identity, payload fidelity, and fail-loud proof behavior.
11. `S34-02i`
Action: Remove immutable runtime-audit append bottleneck from Binance fresh-write path.
Done looks like: canonical fresh writes no longer spend tens of seconds re-validating the full runtime-audit chain per append, while audit append remains immutable, fail-loud, and cryptographically chained.
Constraints: no weakened audit integrity contract, no silent skip of audit validation, and no fallback to lossy/no-op audit behavior.
12. `S34-02j`
Action: Fix exchange fast-insert guard semantics so fresh backfill decisions are based on pre-manifest partition assessment.
Done looks like: Binance, OKX, and Bybit daily assets can still use fast insert for genuinely new empty partitions after recording `source_manifested`, because the fast-insert decision is taken from the pre-manifest execution assessment rather than re-reading the just-written proof state.
Constraints: no relaxed empty-partition requirements, no bypass of quarantine checks, and no hidden fallback to the writer path.
13. `S34-03`
Action: Run Binance full-history backfill for `binance_spot_trades`.
Done looks like: canonical events are complete from earliest available partition to current boundary with per-partition provenance fingerprints.
Constraints: first-party Binance source artifacts only.
14. `S34-04`
Action: Run OKX and Bybit full-history backfill (`okx_spot_trades`, `bybit_spot_trades`).
Done looks like: both datasets are complete in canonical events with partition-level source checksums and gap-free coverage.
Constraints: first-party exchange source artifacts only.
14. `S34-04a`
Action: Build generic daily-dataset tranche controller for exchange backfills (`binance_spot_trades`, `okx_spot_trades`, `bybit_spot_trades`).
Done looks like: one controller can plan immediate next batches from authoritative proof state, choose reconcile vs backfill fail-loud, and chain batches without idle gaps.
Constraints: daily partition datasets only; no hidden fallback planning rules.
15. `S34-04b`
Action: Build repo-native exchange sequence controller for `okx_spot_trades -> bybit_spot_trades`.
Done looks like: one controller can execute the post-Binance exchange order deterministically, refuse out-of-order or non-exchange datasets, and stop loudly on the first dataset that does not fully complete.
Constraints: exchange datasets only; no schedule automation and no manual shell chaining contract.
16. `S34-04b`
Action: Fix OKX source-duplicate contract for Slice 34 backfill.
Done looks like: raw-row counts remain truthful, exact duplicate OKX trade rows are accepted as duplicate delivery of the same source event, conflicting duplicate trade payloads still fail loudly, and canonical/source-proof identity stays stable for already-ingested unique partitions.
Constraints: no weakening of no-miss or exactly-once semantics, no silent deduplication of conflicting rows, and no identity drift for previously valid OKX canonical events.
17. `S34-04c`
Action: Fix OKX offset-order contract for Slice 34 backfill.
Done looks like: OKX integrity and partition proofs treat `trade_id` as numeric monotonic and unique-per-symbol rather than contiguous, while source-vs-canonical identity equality remains the no-miss proof.
Constraints: no weakening of exactly-once semantics, no suppression of conflicting duplicate IDs, and no fallback to opaque ordering.
18. `S34-05`
Action: Run ETF full-history backfill (`etf_daily_metrics`) across all configured issuers.
Done looks like: issuer history is complete in canonical events with deterministic provenance and UTC-day normalization.
Constraints: issuer official-source hierarchy only.
19. `S34-06`
Action: Run FRED full-history backfill (`fred_series_metrics`) across configured series.
Done looks like: configured series history is complete in canonical events with deterministic publish/revision provenance.
Constraints: official FRED source only.
20. `S34-07`
Action: Run Bitcoin full-history backfill for base streams (`bitcoin_block_headers`, `bitcoin_block_transactions`, `bitcoin_mempool_state`).
Done looks like: chain and mempool base datasets are complete in canonical events with deterministic linkage and no-miss checks.
Constraints: self-hosted Bitcoin Core node source only.
21. `S34-08`
Action: Run Bitcoin full-history backfill for derived datasets (`bitcoin_block_fee_totals`, `bitcoin_block_subsidy_schedule`, `bitcoin_network_hashrate_estimate`, `bitcoin_circulating_supply`).
Done looks like: derived datasets are complete in canonical events and reproducible from canonical base-chain events.
Constraints: deterministic formulas only.
22. `S34-09`
Action: Rebuild native projections and `canonical_aligned_1s_aggregates` from canonical events for all relevant datasets.
Done looks like: projection watermarks reach backfill boundary and both serving modes are queryable for the full intended history.
Constraints: projection rebuild only; no alternate serving tables.
23. `S34-10`
Action: Execute comprehensive proof suite (completeness, acceptance, replay determinism, state-machine, and range-proof validation) across raw and historical surfaces.
Done looks like: all backfilled datasets pass cross-surface proofs for `native` and `aligned_1s` where applicable, and backfill correctness is self-proved exactly-once/no-miss.
Constraints: fixed proof windows and deterministic fixtures.
24. `S34-10a`
Action: Run formal live/server-side performance proof on Binance backfill phases.
Done looks like: phase timings, throughput, and resource snapshots are captured for source fetch, source proof, canonical write, reconcile, and proof phases, with bottlenecks explicitly identified from live server evidence.
Constraints: live server only; no local dry-run substitutes.
25. `S34-11`
Action: Close guardrails and artifacts (audit manifests, docs, version/changelog, `.env.example`, slice artifacts).
Done looks like: slice closeout package is complete with no dangling contract gaps for next-slice execution.
Constraints: closeout only; no new capability expansion.
26. `S34-11a`
Action: Build Slice 34 closeout-prep reporting from authoritative backfill proof/manifests.
Done looks like: one deterministic prep tool can summarize per-dataset proof coverage, manifest evidence, and remaining closeout gaps straight from ClickHouse/live manifest artifacts without hand-editing.
Constraints: prep/reporting only; do not mark Slice 34 closed and do not create fake final artifacts.

## Slice 35 Sub-Slices
1. `S35-01`
Action: Freeze the automated daily backfill schedule contract (scope, dataset order, boundary semantics).
Done looks like: contract states exact dataset order, exact schedule env keys, and exact boundary formula (`target_end_date = utc_today - lag_days`).
Constraints: planning contract only; no runtime wiring yet.
2. `S35-02`
Action: Wire schedule configuration env validation and Dagster schedule definition.
Done looks like: invalid/missing schedule env values fail loudly and the schedule object resolves with explicit cron/timezone from env.
Constraints: no hidden defaults for required schedule contract fields.
3. `S35-03`
Action: Implement scheduled orchestrator dispatch with per-dataset non-overlap guard.
Done looks like: each scheduled cycle dispatches datasets in contract order and rejects overlapping in-flight dataset runs fail-loud.
Constraints: no fallback path that silently skips overlap.
4. `S35-04`
Action: Build contract/proof tests for boundary math and dispatch order determinism.
Done looks like: fixed-clock tests prove computed boundaries, dispatch order, and runner-argument shape are deterministic and contract-compliant.
Constraints: proofs use deterministic fixtures and explicit clock control.
5. `S35-05`
Action: Run live server proof cycle for scheduled automation and verify partition advancement.
Done looks like: scheduled execution produces manifest evidence and cursor/watermark movement for the expected boundary window.
Constraints: proof must run against deployed server stack with real source calls.
6. `S35-06`
Action: Close guardrails/docs/artifacts/version/changelog/env contract for Slice 35.
Done looks like: docs and slice artifacts are complete and the schedule contract is fully reflected in developer and user references.
Constraints: closeout only; no new capability expansion.
