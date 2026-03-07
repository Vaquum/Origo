# 2. Itemized Work Plan (Slices and Sub-Slices)

## Summary
This document breaks each slice into explicit checkbox steps and one-day sub-slices.

Global rule for every slice:
1. Capability steps first.
2. Proof steps second.
3. Guardrails steps third.

No slice advances before all three are complete.
Static-analysis hard gate applies throughout: `ruff` + `pyright` strict, repo-wide.

## Slice 0: Monorepo Migration (`tdw-control-plane` -> `origo-control-plane`)

### Capability
- [x] `S0-C1` Create monorepo folders: `control-plane`, `api`, `contracts`, `storage`, `docs`, `spec`.
- [ ] `S0-C2` Import full `tdw-control-plane` code and history into `control-plane`.
- [x] `S0-C3` Rename package/module identifiers from TDW naming to Origo naming.
- [x] `S0-C4` Update Python project metadata and import paths.
- [x] `S0-C5` Ensure existing Binance historical ingestion runs from monorepo.
- [ ] `S0-C6` Wire local infra config for ClickHouse + Dagster + persistent SQLite metadata.

### Proof
- [x] `S0-P1` Run fixed Binance fixture window ingestion and capture baseline artifacts.
- [x] `S0-P2` Replay same fixture window twice and verify deterministic row counts/checksums.
- [ ] `S0-P3` Compare outputs against pre-migration baseline and confirm no regression.

### Guardrails
- [x] `S0-G1` Add migration audit record template and required run notes.
- [ ] `S0-G2` Enforce TLS on authenticated service links introduced in this slice.
- [ ] `S0-G3` Enable immutable audit-log sink with 1-year retention policy.
- [x] `S0-G4` Adopt `uv.lock` for deterministic Python dependency resolution in `control-plane`.
- [x] `S0-G5` Scaffold SQL migration framework (ordered files + ledger + checksums + runner).
- [x] `S0-G6` Enforce env contract (`.env.example` source of truth + fail-loud required vars + no deployment defaults in runtime paths).
- [ ] `S0-G7` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [ ] `S0-G8` User docs closeout for slice (`docs/`, full reference + taxonomy updates).
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

## Slice 8: OKX Spot Trades Daily Ingest (Binance-Mirror Pattern)

### Capability
- [ ] `S8-C1` Implement OKX daily source fetch/checksum/parse ingest path mirroring Binance daily ingest behavior.
- [ ] `S8-C2` Add ClickHouse migration-backed OKX raw table schema and deterministic write path.
- [ ] `S8-C3` Integrate OKX dataset into native raw query planner and response contracts.

### Proof
- [ ] `S8-P1` Execute fixed-window OKX ingest acceptance runs against original source files.
- [ ] `S8-P2` Replay same fixtures and verify deterministic output fingerprints.
- [ ] `S8-P3` Validate loaded data checksums/row stats against source artifacts.

### Guardrails
- [ ] `S8-G1` Add rights/legal classification artifacts for OKX serving/export decisions.
- [ ] `S8-G2` Apply exchange integrity suite profile for OKX dataset (schema/type, sequence-gap, anomaly checks).
- [ ] `S8-G3` Developer docs closeout for slice (`docs/Developer/`, short topic files, complete contracts/operations notes).
- [ ] `S8-G4` User docs closeout for slice (`docs/`, full reference + taxonomy updates).

---

## One-Day Sub-Slices

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
Action: Implement Binance native planner for all three core tables (`trades`, `agg_trades`, `futures_trades`) in one pass.
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
Action: Execute acceptance ingest/query proof on fixed OKX windows.
Done looks like: capability acceptance suite passes.
Constraints: fixed fixtures.
5. `S8-05`
Action: Execute replay determinism proof.
Done looks like: repeated runs match fingerprints exactly.
Constraints: unchanged source artifacts.
6. `S8-06`
Action: Add rights/legal/integrity guardrails for OKX.
Done looks like: rights gate, legal artifact, and integrity suite all enforced fail-loud.
Constraints: guardrails only.
7. `S8-07`
Action: Add operational monitoring and queue/backpressure checks for OKX paths.
Done looks like: alerts/audit coverage and overload controls are in place.
Constraints: no capability expansion.
8. `S8-08`
Action: Developer docs closeout for Slice 8.
Done looks like: `docs/Developer/` has short files for OKX ingest/query contracts and guardrail operations.
Constraints: documentation only; no feature changes.
9. `S8-09`
Action: User docs closeout for Slice 8.
Done looks like: `docs/` includes complete OKX dataset reference and taxonomy updates.
Constraints: documentation only; no feature changes.
