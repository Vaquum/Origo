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

## Program Status (As of 2026-03-12)
1. Slices `0-11` and `13-32` are merged to `main`.
2. Slice `12` is crossed over for this phase:
   1. `S12-C1..C3` and `S12-C6` are blocked by Reddit access/policy constraints.
   2. `S12-C4` and `S12-C5` are explicitly dropped from roadmap scope.
3. PR gates actively enforced on `main` are:
   1. `style-gate`
   2. `type-gate`
   3. `contract-gate`
   4. `replay-gate`
   5. `integrity-gate`

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

## Itemized Work Plan Contract (`spec/2-itemized-work-plan.md`)
1. No work is executed out-of-slice.
2. Every implementation/proof/guardrail task must map to an explicit checkbox in the currently active slice before execution starts.
3. If new work is discovered mid-slice, it must be added to the active slice checklist first (or explicitly deferred to a future slice) before implementation.
4. `spec/2-itemized-work-plan.md` has exactly two structural parts, in this order:
   1. Top section: all slice checkbox sections (`## Slice N: ...`) with `Capability`, `Proof`, and `Guardrails`.
   2. Bottom section: all slice detail sections (`## Slice N Sub-Slices`) with one-day breakdowns.
5. Every slice must exist in both parts:
   1. one checkbox section in the top part
   2. one matching sub-slice section in the bottom part
6. Slice sections must not be interleaved or mixed across parts.
7. No duplicate slice sections are allowed.

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
5. Runtime env contract is fail-loud at API/control-plane startup:
   1. `/opt/origo/deploy/.env` must contain all required non-empty vars.
   2. `ORIGO_AUDIT_LOG_RETENTION_DAYS` must be present and `>= 365`.
   3. Root `.env.example` remains the canonical env contract source.

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
10. Canonical event payload numeric fields must preserve source precision with integer/decimal types and explicit scale metadata (no float in canonical event payload storage).
11. Current reality note: several `canonical_*_native_v1` projection tables still use `Float64`; this is tracked precision debt until migrated.

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
8. Authenticated links must use TLS at the external boundary (edge termination acceptable; origin transport can remain private-network HTTP).
9. Internal API key rotation is manual-only.

### Runtime and Recovery Safety
1. Freshness policy is uniform from source publish time.
2. Runtime overload protection uses concurrency + queue limits.
3. Rollout pattern is shadow-then-promote.
4. Required PR quality gates are `style + type + contract + replay + integrity`.
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
4. Canonical event-payload numeric representation forbids float types; use integer/decimal with explicit scale metadata.
5. Projection tables currently include legacy float fields in several sources; these are explicit migration debt and must not be treated as canonical precision truth.
6. Time fields must preserve highest available source precision; normalized timestamps are derivative fields.
7. Every source migration must include round-trip fidelity/precision proof artifacts on fixed fixtures.
8. Canonical payload fields that are run-volatile processing metadata (for example parse/normalize runtime timestamps) must not participate in canonical payload identity semantics; replay of unchanged source events must keep payload hash stable.

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

## Historical Uniformity Contract
1. `native` is the canonical baseline layer across all datasets.
2. `native` behavior must be uniform across datasets to the maximum feasible degree:
   1. same window semantics
   2. same filter semantics
   3. same strict/warning/error semantics
   4. same response envelope semantics
3. Historical Python and HTTP interfaces must use the same parameter names and option semantics whenever the parameter concept is shared.
4. Historical contract target parameter set is:
   1. `mode`
   2. `start_date`
   3. `end_date`
   4. `n_latest_rows`
   5. `n_random_rows`
   6. `fields`
   7. `filters`
   8. `strict`
5. All dataset surfaces are historical surfaces:
   1. `native` is historical
   2. `aligned_1s` is historical
6. Window selection is optional in target contract:
   1. if no window selector is provided, query defaults to full available history (`earliest -> now`)
7. `aligned_1s` is mandatory for every onboarded dataset; no permanent native-only dataset state is allowed.

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
9. Target window behavior is optional-selector semantics with unbounded default (`earliest -> now`) when no selector is supplied.

### `POST /v1/raw/export`
1. Supports `mode=native|aligned_1s`.
2. Supports `parquet|csv` outputs.
3. Async lifecycle: submit, poll status, retrieve artifact metadata.
4. Event-sourcing migration target returns rights metadata (`rights_state`, `rights_provisional`) in export status responses.

### Historical API and Python Interface
1. Historical surface is explicit endpoint + explicit method coverage per onboarded dataset (no partial dataset coverage).
2. Historical endpoints and `HistoricalData` methods use the shared historical parameter contract:
   1. `mode`
   2. `start_date`
   3. `end_date`
   4. `n_latest_rows`
   5. `n_random_rows`
   6. `fields`
   7. `filters`
   8. `strict`
3. Historical window behavior target:
   1. no selector -> full available history (`earliest -> now`)
   2. provided selectors -> deterministic bounded window semantics
4. Historical response shape follows query-envelope parity (`mode`, `source`, `sources`, `row_count`, `schema`, `warnings`, `rows`, rights metadata).

## Source Onboarding Completion Rule
1. Every new source slice must deliver both `native` and `aligned_1s` integration before slice closeout.
2. Source onboarding proof must include acceptance and replay determinism for both modes.
3. Source onboarding is incomplete if either mode is missing from any of:
   1. query/export paths
   2. historical HTTP path
   3. historical Python interface

## Source Migration Completion Rule
1. Every migrated source slice must port writes to canonical event log and reads to projection-driven serving paths.
2. Every migrated source slice must close with both `native` and `aligned_1s` integration in the same slice.
3. Every migrated source slice must pass parity and replay determinism proofs before cutover closeout.
4. Every migrated source slice must prove exactly-once canonical ingest behavior under duplicate replay and crash/restart scenarios.
5. Every migrated source slice must prove no-miss completeness with source-appropriate gap detection/reconciliation.
6. Every migrated source slice must prove raw-fidelity and numeric-precision preservation against fixed fixture artifacts.
7. Every migrated source slice with `aligned_1s` output must prove `canonical_aligned_1s_aggregates` contract compliance (migration-backed table presence + schema/type contract + fail-loud behavior on drift).
8. Existing derived-only aligned limitation for three Bitcoin stream datasets is treated as explicit backlog debt and is closed by Slice 29.

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
12. Slice 12: Reddit hourly ingest + sentiment signal scope (crossed over in current phase; see Slice 12 locked details).
13. Slice 13: Bitcoin Core node streams and derived network/supply signals capability.
14. Slice 14: Event-sourcing core (canonical event log + projector/checkpoint framework + persistent aligned core) and pilot cutover.
15. Slice 15: Binance event-sourcing port (`binance_spot_trades`) with `native` + `aligned_1s` parity.
16. Slice 16: ETF event-sourcing port (`etf_daily_metrics`) with `native` + `aligned_1s` parity.
17. Slice 17: FRED event-sourcing port (`fred_series_metrics`) with `native` + `aligned_1s` parity.
18. Slice 18: OKX event-sourcing port (`okx_spot_trades`) with `native` + `aligned_1s` parity.
19. Slice 19: Bybit event-sourcing port (`bybit_spot_trades`) with `native` + `aligned_1s` parity.
20. Slice 20: Bitcoin event-sourcing port (all current Bitcoin datasets; aligned scope remains derived-only in this tranche).
21. Slice 21: Canonical `aligned_1s` aggregate contract enforcement and Binance retrofit.
22. Slice 22: Historical Binance spot HTTP operationalization with strict date-window contract.
23. Slice 23: Historical OKX spot HTTP operationalization with Binance-cohesive interface/schema.
24. Slice 24: Historical Bybit spot HTTP operationalization plus old-system endpoint cutover runbook.
25. Slice 25: Historical contract normalization (`native` uniformity + shared parameter naming + unbounded default-window semantics).
26. Slice 26: Historical exchange spot route completion (`binance_spot_trades`, `okx_spot_trades`, `bybit_spot_trades`) with `native`/`aligned_1s` parity.
27. Slice 27: Historical ETF operationalization (`etf_daily_metrics` in `native` + `aligned_1s` for Python + HTTP).
28. Slice 28: Historical FRED operationalization (`fred_series_metrics` in `native` + `aligned_1s` for Python + HTTP).
29. Slice 29: Bitcoin aligned completion for `bitcoin_block_headers`, `bitcoin_block_transactions`, and `bitcoin_mempool_state`.
30. Slice 30: Historical Bitcoin operationalization (all seven Bitcoin datasets in `native` + `aligned_1s` for Python + HTTP).
31. Slice 31: Full historical-surface cohesion and rollout handoff (all datasets).
32. Slice 32: Deployment env-contract drift fix (`ORIGO_AUDIT_LOG_RETENTION_DAYS`) and live deploy validation closure.
33. Slice 33: Binance dataset contract cleanup (drop legacy non-spot Binance dataset keys and enforce `binance_spot_trades` naming everywhere).
34. Slice 34: Full canonical backfill (all onboarded datasets) with projection rebuild and end-to-end serving validation.
35. Slice 35: Automated daily backfill scheduling (time-configured orchestration for all daily datasets with canonical-cursor resume).

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
1. Execution status in this phase: crossed over.
2. Source contract remains official Reddit API only (no third-party Reddit APIs or aggregators).
3. Initial subreddit scope target is:
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
4. Cadence target is hourly pulls for all listed subreddits.
5. Historical model target for sentiment was `ElKulako/cryptobert`.
6. Execution decision for current phase:
   1. `S12-C1..C3` and `S12-C6` are crossed over as blocked by Reddit access/policy constraints.
   2. `S12-C4` and `S12-C5` are crossed over as explicitly dropped from roadmap and not planned for implementation.

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
1. Scope includes `binance_spot_trades`.
2. Writes move to canonical events; serving reads move to projection tables.
3. Slice closeout requires `native` + `aligned_1s` parity for Binance spot dataset serving paths.
4. Legacy direct-serving path is removed after parity/determinism proof passes.
5. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for `binance_spot_trades`.

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
5. OKX `trade_id` is treated as a monotonic unique-per-symbol offset, not as a contiguous sequence.
6. Slice closeout requires exactly-once/no-miss ingest proof and raw-fidelity/precision proof for OKX events.

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

## Slice 22 (Historical Binance Lock-In) Locked Details
1. Python historical contract is Binance-first rename to explicit `get_binance_spot_trades` and `get_binance_spot_klines`.
2. HTTP adds two endpoints only for this slice: `/v1/historical/binance/spot/trades` and `/v1/historical/binance/spot/klines`.
3. Window selection is fail-loud and exclusive: exactly one of `date-window(start_date/end_date)`, `n_latest_rows`, `n_random_rows`.
4. Date-window semantics are strict `YYYY-MM-DD` with UTC day bounds (start inclusive, end-day inclusive via next-day exclusive).
5. Legacy `historical_data` methods are hard removed in this slice: no aliases or fallback params.
6. This contract is historical provenance; Slice 25 supersedes selector semantics with shared optional-window behavior.

## Slice 23 (Historical OKX Parallelization) Locked Details
1. Python and HTTP interfaces must be signature-identical with Slice 22 Binance contracts.
2. HTTP adds two endpoints: `/v1/historical/okx/spot/trades` and `/v1/historical/okx/spot/klines`.
3. Spot-trades output schema is normalized to shared contract: `trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`.
4. OKX maker-side mapping is mandatory and fail-loud: `buy -> 0`, `sell -> 1`.
5. No fallback aliases for old method names or old request parameters are permitted.
6. This contract is historical provenance; Slice 25 supersedes selector semantics with shared optional-window behavior.

## Slice 24 (Historical Bybit Parallelization + Cutover) Locked Details
1. Python and HTTP interfaces must be signature-identical with Binance and OKX contracts.
2. HTTP adds two endpoints: `/v1/historical/bybit/spot/trades` and `/v1/historical/bybit/spot/klines`.
3. Bybit maker-side mapping is mandatory and fail-loud: `buy -> 0`, `sell -> 1`.
4. Old-system endpoint retirement mapping/runbook is part of slice guardrails and must be explicit.
5. Slice closeout requires cohesion proof over all six historical spot endpoints and six Python methods.
6. This contract is historical provenance; Slice 25 supersedes selector semantics with shared optional-window behavior.

## Slice 25 (Historical Contract Normalization) Locked Details
1. Historical contract becomes uniform across datasets and interfaces (`HistoricalData` + HTTP).
2. Historical parameters are standardized to: `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`.
3. Window selection target behavior is optional: no selector means full available history (`earliest -> now`).
4. Existing six exchange routes are migrated to the same contract semantics with no silent aliases.
5. Slice closeout requires contract parity proofs between historical Python and HTTP surfaces.
6. Historical route execution for `mode=aligned_1s` was deferred in S25 and is delivered for exchange historical spot routes in Slice 26.

## Slice 26 (Historical Exchange Dataset Completion) Locked Details
1. Scope includes exchange trade datasets:
   1. `binance_spot_trades`
   2. `okx_spot_trades`
   3. `bybit_spot_trades`
2. Legacy Binance non-spot dataset keys were deferred in this tranche and are now removed from active contracts.
3. Historical Python and HTTP coverage must exist for all three in-scope datasets using explicit names:
   1. Python methods:
      1. `get_binance_spot_trades`
      2. `get_okx_spot_trades`
      3. `get_bybit_spot_trades`
   2. HTTP endpoints:
      1. `/v1/historical/binance/spot/trades`
      2. `/v1/historical/okx/spot/trades`
      3. `/v1/historical/bybit/spot/trades`
4. Each in-scope dataset must support both `native` and `aligned_1s` in historical interfaces.
5. Existing spot kline routes remain convenience routes and execute both `native` and `aligned_1s` while keeping request-contract cohesion with historical core.
6. Slice closeout requires acceptance + replay determinism proofs for all three in-scope datasets in both modes.

## Slice 27 (Historical ETF Operationalization) Locked Details
1. Scope is `etf_daily_metrics` historical serving on Python + HTTP surfaces.
2. Explicit interface names are:
   1. Python method: `get_etf_daily_metrics`
   2. HTTP endpoint: `/v1/historical/etf/daily_metrics`
3. Endpoint/method behavior must follow historical uniformity contract from Slice 25.
4. Both `native` and `aligned_1s` modes are required.
5. Aligned behavior must preserve deterministic forward-fill semantics and provenance fields.
6. Slice closeout requires parity + replay proofs and rights/warning guardrail parity with raw-query behavior.
7. Execution status as of 2026-03-11:
   1. `S27-C1..S27-C4` complete.
   2. `S27-P1..S27-P3` complete.
   3. `S27-G1..S27-G4` complete.

## Slice 28 (Historical FRED Operationalization) Locked Details
1. Scope is `fred_series_metrics` historical serving on Python + HTTP surfaces.
2. Explicit interface names are:
   1. Python method: `get_fred_series_metrics`
   2. HTTP endpoint: `/v1/historical/fred/series_metrics`
3. Endpoint/method behavior must follow historical uniformity contract from Slice 25.
4. Both `native` and `aligned_1s` modes are required.
5. FRED publish-freshness warning semantics must remain deterministic and fail-loud with `strict=true`.
6. Slice closeout requires parity + replay proofs and rights/warning guardrail parity with raw-query behavior.
7. Execution status as of 2026-03-12:
   1. `S28-C1..S28-C4` complete.
   2. `S28-P1..S28-P3` complete.
   3. `S28-G1..S28-G4` complete.

## Slice 29 (Bitcoin Full Aligned Completion) Locked Details
1. Scope is aligned enablement for:
   1. `bitcoin_block_headers`
   2. `bitcoin_block_transactions`
   3. `bitcoin_mempool_state`
2. Aligned projection design must be deterministic and migration-backed in `canonical_aligned_1s_aggregates`.
3. Runtime aligned contract checks remain mandatory and fail-loud on missing table/column/type drift.
4. Slice closeout condition: every currently onboarded dataset in `RawQuerySource` is aligned-capable.
5. Slice closeout requires acceptance + replay determinism proofs for all three new Bitcoin aligned datasets.

## Slice 30 (Historical Bitcoin Operationalization) Locked Details
1. Scope includes all seven onboarded Bitcoin datasets on historical Python + HTTP surfaces.
2. Explicit interface names are:
   1. Python methods:
      1. `get_bitcoin_block_headers`
      2. `get_bitcoin_block_transactions`
      3. `get_bitcoin_mempool_state`
      4. `get_bitcoin_block_fee_totals`
      5. `get_bitcoin_block_subsidy_schedule`
      6. `get_bitcoin_network_hashrate_estimate`
      7. `get_bitcoin_circulating_supply`
   2. HTTP endpoints:
      1. `/v1/historical/bitcoin/block_headers`
      2. `/v1/historical/bitcoin/block_transactions`
      3. `/v1/historical/bitcoin/mempool_state`
      4. `/v1/historical/bitcoin/block_fee_totals`
      5. `/v1/historical/bitcoin/block_subsidy_schedule`
      6. `/v1/historical/bitcoin/network_hashrate_estimate`
      7. `/v1/historical/bitcoin/circulating_supply`
3. Historical coverage must provide both `native` and `aligned_1s` for every Bitcoin dataset.
4. Endpoint/method behavior must follow historical uniformity contract from Slice 25.
5. Historical responses must preserve canonical provenance, rights metadata, and strict warning/error behavior.
6. Slice closeout requires full seven-dataset parity + replay proofs across historical Python and HTTP interfaces.

## Slice 31 (Historical Full-Surface Cohesion + Rollout Handoff) Locked Details
1. Scope is end-to-end cohesion across every historical dataset/mode in Python + HTTP.
2. Historical scope in this tranche includes only active datasets (no legacy Binance non-spot dataset keys).
3. Contract, replay, and integrity suites must run across all in-scope historical datasets and both modes.
4. Rollout artifacts must include endpoint taxonomy, migration/cutover mapping, and operational runbook updates.
5. Legacy route/method drift is not allowed; any retained legacy path must be explicit, tested, and documented as compatibility-only.
6. Slice closeout is the rollout gate for internal user migration off legacy systems.

## Slice 32 (Deploy Env-Contract Drift Fix) Locked Details
1. Scope is deployment/runtime env-contract consistency, not data capability expansion.
2. `ORIGO_AUDIT_LOG_RETENTION_DAYS` must be fail-loud validated in deploy path before compose service startup.
3. Deploy workflow must source `ORIGO_AUDIT_LOG_RETENTION_DAYS` from root `.env.example` and synchronize it into `/opt/origo/deploy/.env`.
4. Deploy compose contract must explicitly require `ORIGO_AUDIT_LOG_RETENTION_DAYS` for API runtime.
5. Slice closeout requires successful merge-triggered deploy and live API health verification.

## Slice 33 (Binance Dataset Contract Cleanup) Locked Details
1. Scope is contract cleanup across runtime, control-plane, tests, docs, and specs:
   1. remove legacy Binance non-spot dataset keys from all active contracts and code paths
   2. remove/de-register legacy Binance non-spot ingest and projection paths
   3. enforce Binance dataset key as `binance_spot_trades` everywhere (no aliases)
2. Cleanup is hard remove only:
   1. no aliases
   2. no fallback parameters
   3. no compatibility shims
3. Cleanup includes Dagster/control-plane ingestion and projector wiring:
   1. remove/de-register Binance agg/futures assets and jobs
   2. keep Binance spot ingestion only under `binance_spot_trades`
4. Cleanup includes canonical event contracts:
   1. Binance stream id must be `binance_spot_trades`
   2. precision registry and rights matrix must be updated to the same identifier
5. Cleanup includes query/export contracts:
   1. `native` and `aligned_1s` must support `binance_spot_trades`
   2. legacy Binance non-spot dataset keys must fail-loud as unsupported
6. Slice closeout requires repo-wide gate pass (`style`, `type`, `contract`, `replay`, `integrity`) with no waiver.

## Slice 34 (Full Canonical Backfill) Locked Details
1. Scope is full-history backfill for every currently onboarded dataset:
   1. `binance_spot_trades`
   2. `okx_spot_trades`
   3. `bybit_spot_trades`
   4. `etf_daily_metrics`
   5. `fred_series_metrics`
   6. `bitcoin_block_headers`
   7. `bitcoin_block_transactions`
   8. `bitcoin_mempool_state`
   9. `bitcoin_block_fee_totals`
   10. `bitcoin_block_subsidy_schedule`
   11. `bitcoin_network_hashrate_estimate`
   12. `bitcoin_circulating_supply`
2. Backfill order is fixed and must be executed step-by-step:
   1. Slice 34 hardening tranche (execution, proof, reconcile, and promotion contracts)
   2. Binance market dataset (`binance_spot_trades`)
   3. OKX and Bybit market datasets
   4. ETF dataset
   5. FRED dataset
   6. Bitcoin base streams (`headers`, `transactions`, `mempool`)
   7. Bitcoin derived datasets (`fees`, `subsidy`, `hashrate`, `supply`)
3. Bitcoin backfill partition truth is explicitly split by source-native boundary:
   1. chain-derived Bitcoin datasets (`bitcoin_block_headers`, `bitcoin_block_transactions`, `bitcoin_block_fee_totals`, `bitcoin_block_subsidy_schedule`, `bitcoin_network_hashrate_estimate`, `bitcoin_circulating_supply`) use `height_range` partition ids
   2. `bitcoin_mempool_state` remains time-native and uses daily snapshot partitions
4. Bitcoin Slice-34 execution must use the same proof state machine as exchanges:
   1. every Bitcoin partition must record source manifest, explicit partition state transitions, and terminal proof/quarantine outcome in ClickHouse
   2. Bitcoin assets may not bypass the canonical proof path by writing canonical rows without recording `source_manifested`, `canonical_written_unproved`, and terminal proof state
   3. chain datasets and mempool share proof semantics even though their partition schemes differ
5. Bitcoin Slice-34 orchestration is explicitly split by partition model:
   1. chain-derived datasets are planned and resumed by `height_range`
   2. `bitcoin_mempool_state` is planned and resumed by daily snapshot partition
   3. blockchain history on the node is not historical mempool history; confirmed blocks do not reconstruct prior mempool state, first-seen ordering, or prior evictions
   4. `bitcoin_mempool_state` historical availability begins at the first Origo-captured snapshot boundary from the self-hosted node runtime
   5. requests before the captured mempool boundary must fail loudly; there is no synthetic historical fallback and no “reconstructed mempool from blocks” claim
   6. any repo-native Bitcoin controller must make that split explicit and fail loudly if a dataset is routed through the wrong planner
6. All backfill reads must come from original first-party sources already contracted in the repo; no third-party data APIs are allowed.
7. ETF/FRED raw-artifact persistence is part of the live Slice-34 runtime contract:
   1. the Docker runtime must provide a real S3-compatible object-store service plus deterministic bucket bootstrap
   2. deploy must synchronize the object-store env contract instead of preserving placeholder or container-localhost values
   3. missing object-store service or unreachable object-store endpoint is a hard failure for ETF/FRED backfill, not an optional warning
8. ETF historical backfill truth is artifact-driven, not live-page driven:
   1. `etf_daily_metrics` historical replay may only use archived issuer-source artifacts already captured by Origo
   2. the live ETF daily scrape job is a current-snapshot ingest path and is not itself a historical replay contract
   3. missing archived issuer artifacts for any claimed historical day are a hard failure; there is no silent fallback to current issuer pages
   4. browser-backed ETF adapters require Playwright plus a Chromium runtime in the deployed control-plane image; missing browser runtime is a hard failure, not a manual post-deploy fixup
   5. archived issuer artifacts only count toward historical coverage if they parse and normalize successfully
   6. when multiple valid archived artifacts exist for the same issuer/day, replay must choose the latest valid artifact deterministically by fetched/persisted ordering; ambiguous ties are hard failures
   7. invalid or superseded artifacts outside the required historical replay window must be surfaced in logs/audit output but must not silently poison otherwise-satisfied historical coverage
   8. ETF historical availability is issuer-specific:
      1. issuers with a proven first-party historical endpoint may claim explicit pre-captured history from their contracted first-available day
      2. snapshot-only issuers may only claim history from the first valid archived artifact day forward
      3. snapshot-only issuers with zero valid archived artifacts have an explicit empty historical claim and must not block replay of issuers whose historical claim is non-empty
      4. zero-history snapshot-only issuers must still be surfaced explicitly in proof/audit output; they may not be silently treated as historically complete
      5. stale partial canonical ETF leftovers must never define the historical replay window
      6. first-party historical ETF endpoints must also persist and honor explicit no-data evidence (for example market-holiday responses); naive weekday expansion is not an acceptable completeness contract
      7. ETF live reruns must be proof-driven:
         1. backfill mode may skip terminal-complete partitions, but it may not silently reconcile ambiguous ones
         2. any ETF partition with canonical rows and non-terminal proof state requires explicit `reconcile`
         3. explicit ETF reconcile must be scopeable to selected partition ids through runtime tags so the repo-native runner can clear poisoned partitions before continuing full-history backfill
         4. explicit ETF reconcile may perform an audited partition reset-and-rewrite when legacy canonical rows violate the current deterministic payload contract for the same source-event identities
         5. ambiguous-partition planning and closeout reporting must stay authoritative after reconcile reset; deleting canonical rows for a poisoned partition must not cause non-terminal proof/source-manifest state to disappear from the Slice-34 planner
         5. any ETF partition reset must clear canonical rows plus projector rows/checkpoints/watermarks for that partition before rewriting from archived source truth
         6. long-running ETF reconcile reset mutations must use the shared env-backed native ClickHouse receive-timeout contract so client-side timeouts do not outpace honest synchronous mutation completion
8a. FRED full-history backfill must also use a repo-native Dagster backfill path:
   1. FRED backfill groups real source observation dates into Slice-34 daily partitions rather than inventing dense calendar partitions
   2. partitions with already-matching canonical rows must close via proof-only reconcile rather than blind rewrite
   3. the repo-native FRED runner must expose terminal proof boundary and any remaining ambiguous partitions from ClickHouse
9. Canonical write path is mandatory: backfill writes canonical events first, then all native/aligned projections are rebuilt from canonical events.
10. Exchange backfill canonical ingest high-throughput contract is tag-driven and proof-gated:
   1. Dagster partition runs must carry explicit projection, execution, and runtime-audit tags.
   2. Deferred backfill mode may use fast canonical insert only when proof state confirms the target partition has no prior proof state, no canonical rows, and no active quarantine.
   3. Any partition that does not satisfy that empty-partition precondition must fail loudly; there is no implicit alternate execution path.
   4. Runtime audit mode remains explicit and fail-loud (`event` or `summary`).
11. Dagster is the execution runner for Slice 34 backfill partitions:
   1. direct asset invocation from custom process pools is disallowed in the live backfill path
   2. any submit/monitor utility may only launch or observe Dagster partition runs
   3. canonical writer and proof state remain the correctness layer; Dagster does not replace exact-once enforcement
12. ClickHouse is the only authoritative live state for backfill correctness:
   1. authoritative progress, proof, and quarantine state must live in ClickHouse
   2. file-backed run-state and file-backed quarantine are forbidden in the live control path
   3. file artifacts may exist only as immutable evidence outputs
13. Every backfill partition must follow an explicit fail-closed state machine keyed by `(source_id, stream_id, partition_id)`:
   1. `source_manifested`
   2. `canonical_written_unproved`
   3. `proved_complete`
   4. `empty_proved`
   5. `quarantined`
   6. `reconcile_required`
14. Normal backfill execution is fail-closed:
   1. completed partitions must fail loudly on rerun
   2. ambiguous partitions with canonical rows but no terminal proof must fail with `RECONCILE_REQUIRED`
   3. only explicit reconcile flow may touch `reconcile_required` or `quarantined` partitions
15. Every backfill partition must emit deterministic provenance and proof material:
   1. source artifact identity/checksum
   2. ingest cursor/offset window
   3. row-count and hash fingerprint
   4. source identity digest
   5. canonical identity digest
   6. gap and duplicate metrics
16. Backfill completion is defined only by proof state:
   1. source and canonical row counts match
   2. source and canonical identity digests match exactly
   3. first/last offsets match where applicable
   4. dataset-specific no-gap rule passes
17. Exchange offset semantics remain source-native in Slice 34 proofs:
   1. Binance `trade_id` is numeric contiguous.
   2. OKX `trade_id` is numeric monotonic but not contiguous.
   3. Bybit daily source rows are ordered lexicographically by `trdMatchID`, but historical files are not guaranteed unique by raw `trdMatchID`; Slice-34 proof for Bybit must rely on full source-vs-canonical identity digest equality rather than duplicate-offset rejection.
   4. Bybit daily source rows are not guaranteed monotonic by timestamp; integrity checks must rely on schema, identity, and UTC-day boundary validity rather than timestamp ordering.
   5. OKX decimal source fields must remain source-native decimal text through canonical payload normalization, and the canonical precision contract must admit first-party OKX `size` values up to scale `20` so valid source data does not fail live reconcile/write.
   6. Any exchange reconcile path that performs idempotent writer repair must recompute canonical proof from fresh canonical state afterward; precomputed canonical proof objects are valid only for the proof-only path.
18. Exchange source-rate constraints are first-class in Slice 34 daily backfills:
   1. OKX download-link resolution must respect an explicit source-safe request pace of `0.75s` between requests (approximately `1.33 requests/s`) derived from live probe evidence; partition concurrency alone is not an adequate safety model.
   2. Source-safe concurrency must be enforced by runtime contract or Dagster queue controls, not by operator memory.
   3. Source-rate-limit breaches must fail loudly and stop clean recovery at the first missing partition; no silent skip or silent concurrency clamp is allowed.
   4. Bybit source-safe concurrency must be backed by empirical proof against real source fetch paths; current live proof found no source failures through `1280` concurrent admission requests.
19. Backfill resume and promotion rules are proof-driven:
   1. resume truth comes from terminal partition proof state, not file state and not bare cursor max
   2. projection rebuild may consume only terminally proved partitions
   3. serving promotion requires native and aligned watermarks to exactly match the proved canonical boundary
20. Slice 34 must expose an explicit reconcile path for interrupted/drifted partitions:
   1. reconcile re-reads the first-party source for the partition
   2. reconcile recomputes source proof and canonical proof without blindly rewriting canonical truth
   3. reconcile must use proof-only when existing canonical rows already match the current source proof and must use writer-repair when existing canonical rows are partial or mismatched but still recoverable by idempotent replay
   4. explicit reconcile may perform an audited partition reset-and-rewrite when legacy canonical rows were produced under a stale source-identity contract and cannot be repaired by idempotent replay
   5. reconcile either marks the partition terminal-complete or quarantines it with a precise reason
21. Deploy contract must synchronize only backfill runtime filesystem/concurrency env from root `.env.example`; execution semantics themselves are tag-driven, not env-driven:
   1. `ORIGO_CANONICAL_RUNTIME_AUDIT_MODE`
   2. `ORIGO_BACKFILL_MANIFEST_LOG_PATH`
   3. `ORIGO_S34_BACKFILL_CONCURRENCY`
22. Slice closeout requires both serving modes to be queryable for all datasets that expose each mode:
   1. `native` everywhere
   2. `aligned_1s` everywhere currently aligned-capable by contract
23. Slice closeout requires cross-surface validation:
   1. raw query and raw export
   2. historical HTTP endpoints
   3. historical Python methods
24. Slice closeout requires machine-checkable range proof for every completed backfill range, so the system can assert that a source range is present exactly once and with no missing partitions.

## Slice 35 (Automated Daily Backfill Scheduling) Locked Details
1. Objective is to make daily backfill fully automatic at configured daily run time, without manual triggering in normal operation.
2. Scope is all currently onboarded daily-partition datasets:
   1. `binance_spot_trades`
   2. `okx_spot_trades`
   3. `bybit_spot_trades`
   4. `etf_daily_metrics`
   5. `fred_series_metrics`
3. Daily schedule time is env-configured and must be fail-loud validated:
   1. `ORIGO_BACKFILL_DAILY_SCHEDULE_CRON`
   2. `ORIGO_BACKFILL_DAILY_SCHEDULE_TIMEZONE`
4. Daily boundary is env-configured and must be fail-loud validated:
   1. `ORIGO_BACKFILL_DAILY_LAG_DAYS`
   2. `target_end_date = utc_today - lag_days`
   3. `lag_days` must be integer `>= 1`.
5. Resume state for scheduled runs must come from ClickHouse backfill proof state (`canonical_backfill_partition_proofs` terminal coverage), not file-based run-state and not raw ingest cursors.
6. Scheduled orchestration must enforce non-overlap per dataset (one active run per dataset; overlap attempt fails loudly).
7. Missing source artifacts at or before the computed target boundary are hard failures (no silent skip/no fallback).
8. Every scheduled run must emit deterministic operations evidence:
   1. run id + dataset + target boundary
   2. per-partition manifest entries
   3. post-run watermark summary per dataset
9. Slice closeout requires end-to-end server proof that scheduled runs advance partitions automatically day over day.

## Slice 36 (Bitcoin Mempool Sequenced Capture Windows) Locked Details
1. Objective is to upgrade `bitcoin_mempool_state` from snapshot-only historical honesty into an always-on sequenced capture subsystem with explicit coverage windows, explicit gap declarations, and deterministic replay from canonical mempool events.
2. Source of truth is the self-hosted Bitcoin Core node only; mempool truth is explicitly node-relative, not a claim about the global network mempool.
3. Capture proof primitive is Bitcoin Core mempool `sequence` contiguity:
   1. contiguous mempool sequence values inside a window are the proof of no-miss capture relative to the node
   2. any discontinuity, regression, restart, or subscriber loss closes the current window and must be recorded explicitly
4. Slice-36 canonical mempool event vocabulary is fixed:
   1. `WindowOpened`
   2. `WindowBroken`
   3. `TxObservedEntry`
   4. `TxObservedRemoval`
   5. `TxInferredEntry`
   6. `TxInferredRemoval`
5. Canonical source-event identity is fixed by event family:
   1. observed events: `(window_id, sequence_number)`
   2. inferred events: `(window_id, reconciliation_id, txid)`
   3. window lifecycle events: `(window_id, event_type)`
6. `WindowOpened` must be anchored by immutable snapshot evidence, not by embedding a giant inline snapshot blob in the canonical event payload:
   1. snapshot artifact is persisted immutably
   2. canonical event stores checksum/reference/row-count/mempool-sequence/tip metadata
7. Gap handling is fail-honest, not healing:
   1. broken windows remain broken forever
   2. reconciliation may establish a new window and emit inferred events
   3. reconciliation may not silently rewrite prior canonical truth to erase the blind spot
8. Metadata enrichment is a separate concern from capture completeness:
   1. sequence-observed source events remain valid even if enrichment races or times out
   2. enrichment status must be explicit in canonical events and projections
9. Native and `aligned_1s` serving must honor capture-window boundaries:
   1. requests before the first captured mempool window fail loudly
   2. requests inside a declared gap fail loudly
   3. `aligned_1s` may not interpolate across broken windows
10. Slice closeout requires live-node proof on the self-hosted Bitcoin Core runtime:
   1. observed windows with contiguous sequence proof
   2. explicit `WindowBroken -> reconcile -> WindowOpened` path on forced interruption
   3. native and `aligned_1s` query behavior that matches declared availability boundaries

## Defaults and Assumptions
1. Phase scope is Raw API only (MK API excluded).
2. Breaking reset is allowed during migration.
3. Query latency target is aspirational: `P95 <= 1 minute`.
4. No hard query caps; full result delivery is required, with SLO-violation logging.
5. Historical and raw query target semantics both allow unbounded default windows (`earliest -> now`) when no explicit selector is supplied.
