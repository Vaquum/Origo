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

## Slice Artifact Contract
1. Every slice must leave three closeout artifacts in `spec/slices/<slice-id>/`:
   1. `manifest.md` with exactly three sections:
      1. `## What was done`
      2. `## Current state`
      3. `## Watch out`
   2. `run-notes.md` with standardized sections:
      1. `## Run metadata`
      2. `## System changes made as proof side effects`
      3. `## Known warnings and disposition`
      4. `## Deferred guardrails`
      5. `## Closeout confirmation`
   3. `baseline-fixture-<window>.json` using canonical fixture schema.
2. Canonical baseline fixture schema is:
   1. `column_key`
   2. `deterministic_match`
   3. `fixture_window`
   4. `source_checksums`
   5. `run_1_fingerprints`
   6. `run_2_fingerprints`
3. Baseline fingerprint content requirements:
   1. Fixture window dates/timestamps must be explicit UTC values.
   2. Source checksums must include `zip_sha256` and `csv_sha256` slots when applicable to the source contract.
   3. Run fingerprints must include row count and deterministic hash fields.
   4. `deterministic_match` must reflect run-1/run-2 replay equality for the canonical fingerprint payload.
4. Historical artifact drift policy:
   1. Canonical reference format is the Slice 7+ baseline-fixture family.
   2. Historical pre-standard artifacts may be retained for provenance but must carry an explicit warning note and must not be used as structure templates.

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
4. Self-hosted unpruned Bitcoin Core mainnet node for Slice-13 stream ingestion.

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
7. Canonical ingest must satisfy exactly-once semantics at source-event identity.
8. Canonical ingest must satisfy no-miss completeness checks per source stream/partition.
9. Canonical event log must preserve raw source bytes (or exact raw response body) with cryptographic checksum.
10. Canonical numeric fields must preserve source precision with integer/decimal types and explicit scale metadata (no float in canonical storage).

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

## Ingestion Guarantee Contract
1. Exactly-once is defined at canonical source-event identity, not at batch/job level.
2. Canonical source-event identity must include:
   1. `source_id`
   2. `stream_id`
   3. `partition_id` (or equivalent shard key)
   4. `source_offset` (or deterministic equivalent event key)
3. Duplicate source events from retries/replays must be idempotent and must not create additional canonical events.
4. No-miss is defined by contiguous source offset progression or deterministic cadence completeness checks when offsets do not exist.
5. Any detected gap is a hard error and must fail the run loudly.
6. Gap handling is fail-closed: affected stream/partition is quarantined until explicit replay/recovery.
7. Reconciliation is mandatory: canonical ingest must continuously compare expected vs observed source coverage.

## Raw Fidelity and Precision Contract
1. Canonical event log stores raw source payload bytes as first-class truth (`payload_raw`) with `payload_sha256_raw`.
2. Parsed payload (`payload_json`) is derivative and must be reproducible from `payload_raw`.
3. Canonical ingest must preserve source field values as delivered; normalization happens in projection layers.
4. Canonical numeric representation forbids float types; use integer/decimal with explicit scale metadata.
5. Time fields must preserve highest available source precision; normalized timestamps are derivative fields.
6. Every source migration must include round-trip fidelity/precision proof artifacts on fixed fixtures.

## Event-Sourcing and Continuous Aggregate Contract
1. Canonical raw truth is a single global append-only event log.
2. Canonical event format is typed envelope fields plus canonical `payload_raw` and `payload_sha256_raw`.
3. Canonical raw events are never rewritten or deleted.
4. Canonical writer enforces unique source-event identity and idempotent retries/replays.
5. Canonical ingest runs continuous no-miss reconciliation and fails loudly on detected gaps.
6. All serving paths (`native`, `aligned_1s`, and future MK views) are projection-driven from canonical events.
7. Projection runtime is hybrid:
   1. ClickHouse materialized views for straightforward aggregate projections.
   2. Python projectors for stateful or complex transforms.
8. Projector checkpoints and watermarks are stored in ClickHouse tables.
9. Persistent aligned aggregates are part of the migration tranche (not deferred).
10. Correction semantics in V1 are latest-truth only.
11. Freshness SLA breach default is serve-with-warning; strict-mode behavior still fails on warnings.
12. Temporary hosted rights may be used during legal transition and must be exposed as provisional in responses.
13. `canonical_aligned_1s_aggregates` is the mandatory aligned projection sink contract; aligned serving paths must not use alternate storage tables.

## Public API Scope (Phase)

### `POST /v1/raw/query`
1. Modes: `native` and `aligned_1s`.
2. Inputs: `sources`, `fields`, `time_range`, `filters`, `strict`, `n_rows`, `n_random`.
3. Query DSL scope: projection + filters only.
4. Output shape: wide rows + schema metadata.
5. Timestamp format: ISO only.
6. Current capability executes one source per request (`sources` list size must be exactly 1).
7. Event-sourcing migration target supports multi-source requests with deterministic output ordering and explicit `view_id` / `view_version`.
8. Event-sourcing migration target returns rights metadata (`rights_state`, `rights_provisional`) in query responses.

### `POST /v1/raw/export`
1. Supports `mode=native|aligned_1s`.
2. Supports `parquet|csv` outputs.
3. Async lifecycle: submit, poll status, retrieve artifact metadata.
4. Event-sourcing migration target returns rights metadata (`rights_state`, `rights_provisional`) in export status responses.

## Source Onboarding Completion Rule
1. Every new source slice must deliver both `native` and `aligned_1s` integration before slice closeout.
2. Source onboarding proof must include acceptance and replay determinism for both modes.
3. Source onboarding is incomplete if either mode is missing from query/export paths for that source.

## Source Migration Completion Rule
1. Every migrated source slice must port writes to canonical event log and reads to projection-driven serving paths.
2. Every migrated source slice must close with both `native` and `aligned_1s` integration in the same slice, unless the slice has an explicit locked exclusion.
3. Every migrated source slice must pass parity and replay determinism proofs before cutover closeout.
4. Every migrated source slice must prove exactly-once canonical ingest behavior under duplicate replay and crash/restart scenarios.
5. Every migrated source slice must prove no-miss completeness with source-appropriate gap detection/reconciliation.
6. Every migrated source slice must prove raw-fidelity and numeric-precision preservation against fixed fixture artifacts.
7. Every migrated source slice with `aligned_1s` output must prove `canonical_aligned_1s_aggregates` contract compliance (migration-backed table presence + schema/type contract + fail-loud behavior on drift).

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
12. Slice 12: Reddit hourly ingest + CryptoBERT sentiment signal capability.
13. Slice 13: Bitcoin Core node streams and derived network/supply signals capability.
14. Slice 14: Event-sourcing core (canonical event log + projector/checkpoint framework + persistent aligned core) and pilot cutover.
15. Slice 15: Binance event-sourcing port (`spot_trades`, `spot_agg_trades`, `futures_trades`) with `native` + `aligned_1s` parity.
16. Slice 16: ETF event-sourcing port (`etf_daily_metrics`) with `native` + `aligned_1s` parity.
17. Slice 17: FRED event-sourcing port (`fred_series_metrics`) with `native` + `aligned_1s` parity.
18. Slice 18: OKX event-sourcing port (`okx_spot_trades`) with `native` + `aligned_1s` parity.
19. Slice 19: Bybit event-sourcing port (`bybit_spot_trades`) with `native` + `aligned_1s` parity.
20. Slice 20: Bitcoin event-sourcing port (all current Bitcoin datasets; aligned scope remains derived-only in this tranche).
21. Slice 21: Canonical `aligned_1s` aggregate contract enforcement and Binance retrofit.

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

## Slice 12 (Reddit Sentiment) Locked Details
1. Source must be the official Reddit API only (no third-party Reddit APIs or aggregators).
2. Initial subreddit scope is fixed:
   1. `r/BitcoinMarkets`
   2. `r/Bitcoin`
   3. `r/BitcoinMining`
   4. `r/BitcoinDiscussion`
   5. `r/TheLightningNetwork`
   6. `r/CryptoMarkets`
   7. `r/investing`
   8. `r/stocks`
   9. `r/options`
   10. `r/MacroEconomics`
   11. `r/algotrading`
   12. `r/quant`
   13. `r/SecurityAnalysis`
   14. `r/CryptoCurrency`
   15. `r/BitcoinBeginners`
   16. `r/wallstreetbets`
   17. `r/SatoshiStreetBets`
   18. `r/Buttcoin`
3. Cadence is hourly pulls for all listed subreddits.
4. Sentiment model for Slice 12 is fixed to `ElKulako/cryptobert`.
5. Model outputs must be stored with model identity metadata and deterministic aggregation contract.
6. Slice completion follows source completion rule: provide both `native` and `aligned_1s` query/export integration for Slice-12 output datasets.

## Slice 13 (Bitcoin Core Node V1) Locked Details
1. Data source must be one unpruned self-hosted Bitcoin Core node (no third-party blockchain APIs).
2. Initial V1 output scope is fixed to:
   1. Block Header Stream (`height`, `timestamp`, `difficulty`, `nonce`, `version`, `merkle_root`, `prev_hash`)
   2. Block Transaction Stream (`inputs`, `outputs`, `values`, `scripts`, `witness_data`, `coinbase`)
   3. Mempool State (`txid`, `fee_rate_sat_vb`, `vsize`, `first_seen_timestamp`, `rbf_flag`)
   4. Block Fee Totals (total transaction fees per block in BTC)
   5. Block Subsidy Schedule (deterministic subsidy per block height/halving interval)
   6. Network Hashrate Estimate (derived from difficulty and observed block intervals)
   7. BTC Circulating Supply (total mined supply by height/time)
3. V1 is node-first ingestion: capability must be proven on raw node outputs before hardening.
4. Derived metrics must be deterministic and reproducible from canonical block data.
5. Derived signals/metrics must be exposed in `aligned_1s` with explicit deterministic bucket semantics in Slice 13.
6. Slice completion follows source completion rule: provide both `native` and `aligned_1s` query/export integration for Slice-13 datasets.

## Slice 14 (Event-Sourcing Core) Locked Details
1. Canonical event storage is single global append-only log.
2. Event envelope must include typed fields, canonical `payload_raw`, and `payload_sha256_raw`.
3. Writer identity contract is mandatory: (`source_id`, `stream_id`, `partition_id`, `source_offset_or_equivalent`) unique key.
4. Ingest cursor/ledger and reconciliation tables are mandatory and fail-loud on detected gaps.
5. Proof in this slice must include failure-injection cases:
   1. duplicate replay
   2. crash/restart mid-batch
   3. injected source gap
6. Projector runtime is hybrid (`ClickHouse MV` + `Python projector`) with checkpoint tables in ClickHouse.
7. Raw API migration target in this slice adds:
   1. multi-source query contract
   2. `view_id` and `view_version`
   3. provisional rights metadata in responses
8. Persistent aligned aggregate framework is implemented in this slice.
9. Slice includes minimal pilot cutover on Binance spot trades before full Binance source port slice.

## Slice 15 (Binance Event-Sourcing Port) Locked Details
1. Scope includes `spot_trades`, `spot_agg_trades`, and `futures_trades`.
2. Writes move to canonical events; serving reads move to projection tables.
3. Slice closeout requires `native` + `aligned_1s` parity for all three Binance datasets.
4. Legacy direct-serving path is removed after parity/determinism proof passes.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for all three datasets.

## Slice 16 (ETF Event-Sourcing Port) Locked Details
1. Scope includes `etf_daily_metrics`.
2. Writes move to canonical events with existing provenance fidelity preserved.
3. Slice closeout requires `native` + `aligned_1s` parity, including deterministic forward-fill semantics.
4. Rights/legal behavior remains fail-loud with provisional metadata surfaced when applicable.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for ETF records.

## Slice 17 (FRED Event-Sourcing Port) Locked Details
1. Scope includes `fred_series_metrics`.
2. Writes move to canonical events; serving reads move to projection tables.
3. Slice closeout requires `native` + `aligned_1s` parity with deterministic publish-freshness warning behavior.
4. Existing series taxonomy and replay determinism must remain stable through cutover.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for FRED series events.

## Slice 18 (OKX Event-Sourcing Port) Locked Details
1. Scope includes `okx_spot_trades`.
2. Writes move to canonical events with existing source checksum/provenance guarantees preserved.
3. Slice closeout requires `native` + `aligned_1s` parity for OKX.
4. Exchange integrity suite remains mandatory in event + projection paths.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for OKX events.

## Slice 19 (Bybit Event-Sourcing Port) Locked Details
1. Scope includes `bybit_spot_trades`.
2. Writes move to canonical events with existing source checksum/provenance guarantees preserved.
3. Slice closeout requires `native` + `aligned_1s` parity for Bybit.
4. Exchange integrity suite remains mandatory in event + projection paths.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for Bybit events.

## Slice 20 (Bitcoin Event-Sourcing Port) Locked Details
1. Scope includes all currently onboarded Bitcoin datasets:
   1. `bitcoin_block_headers`
   2. `bitcoin_block_transactions`
   3. `bitcoin_mempool_state`
   4. `bitcoin_block_fee_totals`
   5. `bitcoin_block_subsidy_schedule`
   6. `bitcoin_network_hashrate_estimate`
   7. `bitcoin_circulating_supply`
2. Writes move to canonical events; serving reads move to projection tables.
3. Aligned scope in this tranche stays derived-only:
   1. `bitcoin_block_fee_totals`
   2. `bitcoin_block_subsidy_schedule`
   3. `bitcoin_network_hashrate_estimate`
   4. `bitcoin_circulating_supply`
4. Bitcoin remains last in migration order and retains live-node proof gating constraints.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof across all seven Bitcoin datasets.
6. Execution status as of 2026-03-10:
   1. `S20-P1..S20-P6` complete
   2. `S20-G1..S20-G7` complete
   3. `S20-P7` complete (executed against reachable non-IBD server node runtime)

## Slice 21 (Canonical `aligned_1s` Aggregate Contract) Locked Details
1. `canonical_aligned_1s_aggregates` is mandatory for aligned serving across all sources; no alternate aligned storage contract is permitted.
2. Runtime aligned query paths must fail loudly with explicit contract errors when the canonical aligned table is missing or schema/type contract drifts.
3. Proof harnesses must provision aligned storage via migration runner only; manual ad-hoc table DDL in proofs is disallowed.
4. Slice includes Binance retrofit of aligned query/proof paths to this contract before moving to the next source migration slice.
5. Slice closeout requires deterministic replay parity to remain unchanged after enforcing the canonical aligned contract.

## Defaults and Assumptions
1. Phase scope is Raw API only (MK API excluded).
2. Breaking reset is allowed during migration.
3. Query latency target is aspirational: `P95 <= 1 minute`.
4. No hard query caps; full result delivery is required, with SLO-violation logging.
