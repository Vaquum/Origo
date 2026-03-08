# 1. Top-Level Plan (Capability First)

## Summary
Origo is built with one strict execution doctrine:

1. Capability first.
2. Proof second.
3. Guardrails third.

No slice can advance to the next slice until all three stages are complete for the current slice.

## Guardrail #1: Execution Doctrine

### Stage Definitions
1. **Capability**: implement only the minimum functional path for the slice objective.
2. **Proof**: prove the capability is rock-solid.
3. **Guardrails**: add safety, ops, security, and compliance around the proven capability.

### Hard Rule
1. Strict per-slice loop: `Capability -> Proof -> Guardrails`.
2. The next slice cannot start before current-slice guardrails are complete.
3. No hardening is allowed before capability proof for that same slice.

### Proof Bar
Every slice must pass:
1. Acceptance tests.
2. Replay tests on fixed fixtures.
3. Deterministic output checks across repeated runs.

## Static Analysis Contract
1. `ruff` and `pyright` are hard gates.
2. Gate scope is repo-wide (existing and new files), not only changed files.
3. `pyright` runs in strict mode.
4. A slice cannot close with outstanding lint/type debt.
5. Any temporary ignore requires explicit rationale, owner, and expiry slice.

## Documentation Contract (Definition of a Perfect Entry)
1. Every slice ends with two mandatory guardrail outputs, in this order:
   1. Developer docs update in `docs/Developer/`.
   2. User docs update in `docs/`.
2. Documentation is split into many short files (one topic per file), not long omnibus documents.
3. Every documentation entry must include:
   1. Purpose and scope.
   2. Inputs and outputs with contract shape.
   3. Data definitions (field names, types, units, timezone, nullability).
   4. Source/provenance and freshness semantics.
   5. Failure modes, warnings, and error codes.
   6. Determinism/replay notes (what must stay stable).
   7. Environment variables and required config.
   8. Minimal examples (valid request/query and expected output shape).
   9. Owner, last-updated date, and slice/version reference.
4. A documentation entry is complete only if a new engineer can implement and debug the topic without tribal knowledge.
5. User docs must maintain a complete current reference taxonomy of all exposed datasets, fields, modes, and meanings.

## Pinned Stack

### Repo and Runtime
1. Single Origo monorepo.
2. `tdw-control-plane` is moved into this monorepo and renamed to `origo-control-plane`.
3. Python-first platform runtime.

### API and Contracts
1. FastAPI for API service.
2. Pydantic v2 for request/response contracts.
3. Uvicorn process runtime.

### Orchestration and Jobs
1. Dagster for all orchestration.
2. API async workflows dispatch Dagster runs.
3. Dagster metadata backend is SQLite on persistent storage.

### Data and Storage
1. ClickHouse for serving/query and materialized views.
2. S3-compatible object store for WAL/archive.
3. Parquet + manifest/checksum for canonical WAL.

### Scraping
1. Generic scraper platform with standard plugin interface.
2. HTTP-first fetching with browser fallback.
3. HTML parsing via `lxml + BeautifulSoup`.
4. PDF parsing is deterministic parser-first; LLM can be used offline to help write parser rules, not in runtime parse path.

### Exports
1. Async export endpoint.
2. Export formats: Parquet and CSV.

### Deployment
1. Image-based deployment on merge to `main`.
2. Runtime deploy target is remote Linux server over SSH using repo secrets:
   1. `ORIGO_SERVER_IP`
   2. `ORIGO_SERVER_USER`
   3. `ORIGO_SERVER_PASSWORD`
   4. `ORIGO_SERVER_ENV_B64` (base64-encoded `/opt/origo/deploy/.env`; required on first deploy when server env file does not yet exist)
   5. `ORIGO_SERVER_KNOWN_HOSTS` (pinned host-key entries for strict SSH host verification)
3. First deploy must bootstrap missing host dependencies (Docker engine + compose plugin + core OS deps) via install-if-missing checks in CI workflow.
4. Deployment flow remains single-workflow driven (no manual server bootstrap).

## ClickHouse Query and Migration Policy
1. ClickHouse schema and query logic is SQL-first and versioned.
2. Migrations are authored as ordered SQL files and tracked in a migration ledger table.
3. Applied migrations are immutable and forward-only.
4. Production query paths use explicit SQL; no query-builder abstraction (including Ibis) in core paths for Phase-1.
5. ClickHouse-native SQL features are preferred over abstraction portability.
6. Downstream data handoff must preserve Arrow-to-Polars compatibility.

### Slice 0 Guardrail Additions
1. `S0-G5`: scaffold the SQL migration framework in `control-plane`:
   1. ordered migration directory
   2. migration ledger table
   3. checksum validation
   4. strict apply/status runner
2. `S0-G6`: enforce environment contract:
   1. root `.env.example` is the single contract for required runtime secrets/config
   2. deployment-specific values must come from env vars (no hard-coded defaults)
   3. missing or empty required env vars fail loudly
   4. every sub-slice closeout asks: "was any deployment-specific value hard-coded?"

## Safety Properties

### Serving and Error Safety
1. Raw API default mode is `native` (source-native events).
2. Raw API also supports `aligned_1s` mode.
3. Typed status/error map is fixed: `200/202/404/409/503`.
4. If requested data is unavailable, return error.
5. `strict=false`: serve data with warnings.
6. `strict=true`: fail when warnings exist.

### Integrity and Durability Safety
1. Event-log + materialized-view architecture.
2. WAL is Parquet with manifest and checksums.
3. Schema registry/versioning is required.
4. Historical corrections are append-only (no destructive rewrite of canonical raw events).
5. Exchange integrity suite is mandatory in guardrails:
   1. Schema/type checks.
   2. Sequence-gap checks.
   3. Anomaly checks.
6. Exchange canonical historical truth source is daily files.

### Rights and Secret Safety
1. Source rights matrix is mandatory in repo.
2. Rights states:
   1. `Hosted Allowed`
   2. `BYOK Required`
   3. `Ingest Only`
3. Missing rights state is fail-closed.
4. Legal/access blocked sources auto-defer to Phase-2.
5. `Hosted Allowed` requires legal sign-off artifact before external serving/export.
6. Credentials are provided at source init/config stage.
7. Credentials are not persisted in raw form.
8. Authenticated links must use TLS.
9. Internal API key rotation is manual-only.

### Runtime and Recovery Safety
1. Freshness policy is uniform from source publish time.
2. Runtime overload protection uses concurrency + queue limits.
3. Rollout pattern is shadow-then-promote.
4. Minimum PR quality gate is contract + replay + integrity tests.
5. DR targets: `RPO 5m` and `RTO 60m`.
6. DR drills are quarterly.
7. Audit logs are immutable with 1-year retention.
8. Alerting baseline is logs + Discord bot.
9. Source quarantine policy is manual-only for now.

## Public API Scope (Phase)

### `POST /v1/raw/query`
1. Modes: `native` and `aligned_1s`.
2. Inputs: `sources`, `fields`, `time_range`, `filters`, `strict`, `n_rows`, `n_random`.
3. Query DSL scope: projection + filters only.
4. Output shape: wide rows + schema metadata.
5. Timestamp format: ISO only.
6. Current capability executes one source per request (`sources` list size must be exactly 1).

### `POST /v1/raw/export`
1. Supports `mode=native|aligned_1s`.
2. Supports `parquet|csv` outputs.
3. Async lifecycle: submit, poll status, retrieve artifact metadata.

## Source Onboarding Completion Rule
1. Every new source slice must deliver both `native` and `aligned_1s` integration before slice closeout.
2. Source onboarding proof must include acceptance and replay determinism for both modes.
3. Source onboarding is incomplete if either mode is missing from query/export paths for that source.

## Source Prioritization
1. Source order is decided by scoring, not fixed upfront.
2. Candidate pool includes all Phase-1 sources.
3. Score weights:
   1. Alpha impact: 50%
   2. Complexity: 20%
   3. Legal/rights risk: 15%
   4. Operational reliability: 15%
4. Feasibility gate required before source is selectable:
   1. Rights classification exists.
   2. Access path confirmed.
   3. Named engineering owner.

## Slice Roadmap
1. Slice 0: Monorepo migration and control-plane rename.
2. Slice 1: Raw query `native` capability.
3. Slice 2: Raw export `native` capability.
4. Slice 3: Generic scraping platform capability.
5. Slice 4: ETF scraper use-case on generic platform (10 issuers).
6. Slice 5: Raw query `aligned_1s` capability.
7. Slice 6: FRED integration.
8. Slice 7: Full platform Docker wrapping and local end-to-end proof.
9. Slice 8: OKX spot trades daily-file ingest plus `aligned_1s` integration capability.
10. Slice 10: Image-based merge-to-server deployment with first-run host bootstrap.
11. Slice 11: Bybit spot trades daily-file ingest plus `aligned_1s` integration capability.

## Slice 10 (Deployment) Locked Details
1. Trigger: merge to `main` (implemented as push to `main`).
2. Build source: monorepo Dockerfiles (`api`, `control-plane`) from merge commit.
3. Distribution: prebuilt images are deployed to server (no server-side image build).
4. Bootstrap policy: deployment job installs missing host dependencies idempotently.
5. Runtime apply path:
   1. login to image registry
   2. pull commit-pinned image tags
   3. run migrations
   4. restart stack via `docker compose`
6. Failure policy: fail loudly on missing server credentials, missing runtime env file, bootstrap/install failures, registry auth failures, compose failures, or migration failures.

## Slice 4 (ETF) Locked Details
1. Coverage: all 10 issuers.
2. Canonical source hierarchy: issuer official pages/files only.
3. Canonical cadence: daily.
4. Time normalization: UTC.
5. Historical policy: full available history per issuer.
6. Internal storage shape: long metric table.
7. Raw artifact persistence: always on.
8. Retry policy: daily run with 24h same-day retry window.
9. Proof threshold: >=99.5% parity on mandatory metrics in proof window.

## Slice 11 (Bybit) Locked Details
1. Source must be first-party Bybit historical files only (no third-party APIs).
2. Canonical source path for BTCUSDT daily spot trades is `https://public.bybit.com/trading/BTCUSDT/BTCUSDTYYYY-MM-DD.csv.gz`.
3. Ingest must mirror existing exchange daily ingest semantics:
   1. deterministic file selection by UTC partition day
   2. deterministic parse and normalized write contract
   3. explicit source artifact fingerprints in proof outputs
4. Bybit onboarding follows source completion rule: `native` + `aligned_1s` before slice closeout.

## Defaults and Assumptions
1. Phase scope is Raw API only (MK API excluded).
2. Breaking reset is allowed during migration.
3. Query latency target is aspirational: `P95 <= 1 minute`.
4. No hard query caps; full result delivery is required, with SLO-violation logging.
