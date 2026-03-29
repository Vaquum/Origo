# Changelog

## v1.2.81 on 29th of March, 2026

- Made the live-safe FRED revision-history vintage window an explicit runtime contract:
  - added required env `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST`
  - plumbed the value through `FREDAPIConfig` and the revision-history chunk planner instead of hiding `275` as a code constant
  - fail loudly on missing, non-integer, non-positive, or `>2000` values while preserving deterministic split-on-`400`/`414`/timeout behavior for remaining FRED transport edge cases

## v1.2.80 on 29th of March, 2026

- Preserved append-only canonical truth during Slice 34 ETF/FRED reconcile resets:
  - added `canonical_partition_reset_boundaries` and the boundary-aware read surface `canonical_event_log_active_v1`
  - switched live proof/planner/projector/writer-identity reads to the active view so stale pre-reset rows do not participate in reset-and-rewrite flows
  - ETF and FRED explicit reconcile resets now clear only projection/checkpoint/watermark state and stop issuing destructive `canonical_event_log` delete mutations
  - this removes the live FRED blocker exposed after `#104`, where the job reached reset-and-rewrite and then stalled on a huge synchronous `ALTER TABLE ... DELETE`

## v1.2.79 on 29th of March, 2026

- Fixed Slice 34 FRED explicit reconcile so quarantining proof mismatches can still reach the audited reset-and-rewrite path:
  - catch `BACKFILL_PARTITION_PROOF_FAILED` during FRED reconcile writer-repair
  - load the just-recorded quarantined proof row from ClickHouse instead of assuming the proof API returns it
  - continue into the existing reset-and-rewrite flow and then re-prove
  - tightened the FRED reconcile contract test to match the real state-store raise-on-quarantine behavior

## v1.2.78 on 29th of March, 2026

- Tightened Slice 34 FRED bounded raw-bundle planning:
  - normalize each series metadata payload before bounded history/reconcile observation fetch
  - intersect the requested window with the series' actual metadata availability
  - skip true no-overlap series such as early-window `FEDFUNDS` instead of surfacing fake empty-observation failures
  - fail loudly when the entire requested window overlaps no configured FRED series
  - added focused contract coverage for the no-overlap skip path and the all-series-no-overlap fail-loud path

## v1.2.77 on 28th of March, 2026

- Added env-backed native ClickHouse receive-timeout contract (`CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS`) and wired Slice 34 native clients to use it so long-running ETF reconcile reset mutations do not fail on the client timeout boundary.

## v1.2.76 on 28th of March, 2026
- Tightened Slice 34 ETF explicit reconcile for legacy canonical payload drift:
  - explicit ETF `reconcile` now resets a poisoned partition when existing canonical rows conflict with the current deterministic payload contract for the same source-event identities
  - the reset clears canonical ETF rows plus ETF projector rows/checkpoints/watermarks for that partition before rewriting from archived source truth
  - added contract coverage for the reset mutation contract and the reset-and-rewrite reconcile path

## v1.2.75 on 28th of March, 2026
- Tightened Slice 34 ETF resume and reconcile control:
  - added `origo.backfill.partition_ids` tagging so ETF Dagster runs can scope work to explicit daily partitions
  - ETF backfill now skips terminal-complete partitions in `backfill` mode instead of failing on already-proved history
  - the repo-native ETF runner now executes explicit `reconcile` runs for ambiguous ETF partitions before returning to full-history `backfill`

## v1.2.74 on 28th of March, 2026
- Formalized the zero-history ETF boundary for snapshot-only issuers:
  - `archive_capture_forward` issuers with zero valid archived artifacts now have an explicit empty historical claim instead of blocking replay for issuers whose claim is non-empty
  - ETF archive replay still surfaces those zero-history issuers explicitly in proof/log output instead of silently treating them as complete

## v1.2.73 on 28th of March, 2026
- Tightened Slice 34 ETF iShares holiday coverage:
  - persist official iShares no-data responses as first-party negative evidence during archive bootstrap
  - honor archived no-data evidence when building required historical replay coverage for the official iShares endpoint
  - added focused contract coverage for the holiday/no-data replay seam

## v1.2.72 on 28th of March, 2026
- Tightened Slice 34 ETF historical availability boundaries:
  - ETF archive replay now derives required issuer/day coverage from an explicit per-issuer historical availability contract
  - added an iShares historical archive bootstrap runner that fetches official `asOfDate` artifacts, validates them, and persists only valid raw artifacts
  - limited snapshot-only ETF issuers to history from their first valid archived artifact forward instead of inferring replay scope from stale canonical leftovers

## v1.2.71 on 28th of March, 2026
- Tightened Slice 34 ETF archive replay selection semantics:
  - choose the latest valid archived issuer artifact deterministically for each required issuer/day
  - log invalid or superseded artifacts instead of hard-failing when valid required coverage still exists
  - extended ETF archive-replay contract coverage around revision precedence and ignored invalid artifacts

## v1.2.70 on 28th of March, 2026
- Made the Slice 34 ETF historical backfill path replay archived issuer artifacts instead of silently falling back to live issuer pages:
  - enumerated ETF raw-artifact manifests from object storage and reloaded archived bytes for adapter parse/normalize replay
  - added fail-loud archive coverage validation for missing issuer/day artifacts, invalid archived payloads, and conflicting duplicate artifacts
  - extended ETF contract coverage around archive replay and coverage validation

## v1.2.69 on 28th of March, 2026
- Added browser runtime packaging required by the hardened Slice 34 ETF backfill path:
  - added Python `playwright` to control-plane runtime dependencies
  - installed Playwright Chromium during control-plane image build
  - added contract coverage to prevent image/runtime drift for browser-backed ETF adapters

## v1.2.67 on 12th of March, 2026
- Improved canonical exchange ingest throughput for backfill workloads:
  - switched Binance/OKX/Bybit canonical ingest writers from per-event writes to batched canonical writes
  - enforced explicit `ORIGO_BACKFILL_PROJECTION_MODE` env contract in Binance daily ingest asset (no fallback)
  - defaulted `.env.example` backfill projection mode to `deferred` to keep ingest capability fast during full-history backfill
- Added regression coverage for batched immutable audit and batched canonical runtime ingest audit paths.

## v1.2.64 on 12th of March, 2026
- Completed Slice 29 Bitcoin stream aligned completion control-plane work:
  - Added stream-aligned projector utility:
    - `control-plane/origo_control_plane/utils/bitcoin_stream_aligned_projector.py`
  - Wired Bitcoin stream assets to execute aligned projections after canonical ingest/native projection:
    - `insert_bitcoin_block_headers_to_origo`
    - `insert_bitcoin_block_transactions_to_origo`
    - `insert_bitcoin_mempool_state_to_origo`
  - Extended aligned export allowlist for stream datasets:
    - `bitcoin_block_headers`
    - `bitcoin_block_transactions`
    - `bitcoin_mempool_state`
  - Added slice artifacts:
    - `spec/slices/slice-29-bitcoin-stream-aligned-completion/*`

## v1.2.55 on 8th of March, 2026
- Completed Slice 13 Bitcoin Core node streams and derived signal onboarding (`Capability -> Proof -> Guardrails`):
  - Added migration-backed Bitcoin tables:
    - `control-plane/migrations/sql/0010__create_bitcoin_block_headers.sql`
    - `control-plane/migrations/sql/0011__create_bitcoin_block_transactions.sql`
    - `control-plane/migrations/sql/0012__create_bitcoin_mempool_state.sql`
    - `control-plane/migrations/sql/0013__create_bitcoin_block_fee_totals.sql`
    - `control-plane/migrations/sql/0014__create_bitcoin_block_subsidy_schedule.sql`
    - `control-plane/migrations/sql/0015__create_bitcoin_network_hashrate_estimate.sql`
    - `control-plane/migrations/sql/0016__create_bitcoin_circulating_supply.sql`
  - Added Bitcoin stream ingest assets and Dagster jobs:
    - `insert_bitcoin_block_headers_to_origo`
    - `insert_bitcoin_block_transactions_to_origo`
    - `insert_bitcoin_mempool_state_to_origo`
  - Added deterministic derived assets and Dagster jobs:
    - `insert_bitcoin_block_fee_totals_to_origo`
    - `insert_bitcoin_block_subsidy_schedule_to_origo`
    - `insert_bitcoin_network_hashrate_estimate_to_origo`
    - `insert_bitcoin_circulating_supply_to_origo`
  - Added Bitcoin stream + derived integrity suite with fail-loud enforcement in asset paths:
    - `control-plane/origo_control_plane/utils/bitcoin_integrity.py`
    - `tests/integrity/test_bitcoin_integrity.py`
  - Added Slice-13 proof artifact generator and artifacts:
    - `scripts/s13_generate_proof_artifacts.py`
    - `spec/slices/slice-13-bitcoin-core-signals/*`
- Updated query/export integration coverage via monorepo API/query changes and marked S13 work-plan items complete.

## v1.2.54 on 8th of March, 2026
- Completed Slice 11 Bybit source onboarding (`Capability -> Proof -> Guardrails`):
  - Added migration-backed raw table schema:
    - `control-plane/migrations/sql/0009__create_bybit_spot_trades.sql`
  - Added Bybit daily ingest asset and Dagster job:
    - `control-plane/origo_control_plane/assets/daily_bybit_spot_trades_to_origo.py`
    - `insert_daily_bybit_spot_trades_to_origo_job`
  - Enforced strict first-party source checks:
    - deterministic source URL by partition day
    - required `ETag` header
    - recorded `gzip_sha256` and `csv_sha256`
    - fail-loud CSV contract parsing and deterministic writes
  - Extended exchange integrity suite for Bybit:
    - schema/type checks
    - sequence/uniqueness checks
    - monotonic-time checks
    - side/price/quantity anomaly checks
  - Integrated Bybit into query/export paths:
    - native planner
    - aligned-1s planner
    - raw export dispatch
    - API contracts and rights gates
  - Added rights/legal artifacts:
    - `contracts/source-rights-matrix.json` (`bybit` as `Hosted Allowed`)
    - `contracts/legal/bybit-hosted-allowed.md`
  - Produced Slice 11 artifacts:
    - `spec/slices/slice-11-bybit-spot-trades-aligned/ingest-results.json`
    - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p1-acceptance.json`
    - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p2-aligned-acceptance.json`
    - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p3-determinism.json`
    - `spec/slices/slice-11-bybit-spot-trades-aligned/proof-s11-p4-source-checksums.json`
    - `spec/slices/slice-11-bybit-spot-trades-aligned/baseline-fixture-2024-01-01_2024-01-02.json`
- Marked `S11-C1..S11-C4`, `S11-P1..S11-P4`, and `S11-G1..S11-G5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.53 on 7th of March, 2026
- Completed Slice 8 OKX source onboarding (`Capability -> Proof -> Guardrails`):
  - Added migration-backed raw table schema:
    - `control-plane/migrations/sql/0008__create_okx_spot_trades.sql`
  - Added OKX daily ingest asset and Dagster job:
    - `control-plane/origo_control_plane/assets/daily_okx_spot_trades_to_origo.py`
    - `insert_daily_okx_spot_trades_to_origo_job`
  - Enforced strict first-party source checks:
    - resolved daily file via OKX API
    - verified `Content-MD5`
    - recorded `zip_sha256` and `csv_sha256`
    - fail-loud CSV contract parsing and deterministic writes
  - Added OKX integrity profile in exchange integrity suite:
    - schema/type checks
    - sequence-gap checks
    - side anomaly checks (`buy|sell`)
  - Integrated OKX into query/export paths:
    - native planner
    - aligned-1s planner
    - raw export dispatch
    - API contracts and rights gates
  - Added rights/legal artifacts:
    - `contracts/source-rights-matrix.json` (`okx` as `Hosted Allowed`)
    - `contracts/legal/okx-hosted-allowed.md`
  - Produced Slice 8 artifacts:
    - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p1-acceptance.json`
    - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p2-aligned-acceptance.json`
    - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p3-determinism.json`
    - `spec/slices/slice-8-okx-spot-trades-aligned/proof-s8-p4-source-checksums.json`
    - `spec/slices/slice-8-okx-spot-trades-aligned/baseline-fixture-2024-01-01_2024-01-02.json`
- Marked `S8-C1..S8-C4`, `S8-P1..S8-P4`, and `S8-G1..S8-G5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.52 on 7th of March, 2026
- Completed Slice 7 full Docker local-platform proof path:
  - Added root Docker stack and service images:
    - `docker-compose.yml`
    - `docker/Dockerfile.api`
    - `docker/Dockerfile.control-plane`
    - `.dockerignore`
  - Added deterministic lifecycle commands and proof runners:
    - `scripts/s7_docker_stack.sh`
    - `scripts/s7_docker_bootstrap.sh`
    - `scripts/s7_docker_local_proof.sh`
  - Added slice artifacts and deterministic baseline:
    - `spec/slices/slice-7-docker-local-platform/proof-s7-local-docker.json`
    - `spec/slices/slice-7-docker-local-platform/baseline-fixture-2017-08-17_2017-08-18.json`
    - `spec/slices/slice-7-docker-local-platform/smoke-result.json`
  - Fixed Docker proof blockers:
    - Binance integrity allows first-day `trade_id=0`
    - Dagster status parsing supports `Run` typename with strict timestamps
    - healthcheck commands aligned with container runtime tools
- Marked `S7-C1..S7-C5`, `S7-P1..S7-P4`, and `S7-G1..S7-G4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.50 on 7th of March, 2026
- Completed Slice 6 guardrail docs closeout:
  - `S6-G5` developer docs:
    - `docs/Developer/s6-fred-connector-contracts.md`
    - `docs/Developer/s6-fred-ingest-persistence.md`
    - `docs/Developer/s6-fred-guardrails-runbook.md`
  - `S6-G6` user docs and taxonomy updates:
    - `docs/fred-reference.md`
    - `docs/raw-query-reference.md`
    - `docs/aligned-reference.md`
    - `docs/data-taxonomy.md`
    - `docs/raw-export-reference.md`
  - Documentation updates include FRED dataset contract, aligned/native semantics, warning taxonomy, serving gates, and environment contracts.
- Marked `S6-G5` and `S6-G6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.49 on 7th of March, 2026
- Completed Slice 6 guardrail step `S6-G4` (FRED alerts and audit-event coverage):
  - Added immutable FRED warning audit log module:
    - `api/origo_api/fred_alert_audit.py`
    - event type: `fred_query_warning`
    - hash-chained JSONL audit log with strict validation
  - Wired FRED warning alert + audit emission into query API path:
    - `api/origo_api/main.py`
    - emits when FRED warnings are present
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s6_g4_fred_alerts_audit_proof.py`
    - `spec/slices/slice-6-fred-integration/guardrails-proof-s6-g4-fred-alerts-audit.json`
  - Added required env contract entries:
    - `ORIGO_FRED_ALERT_AUDIT_LOG_PATH`
    - `ORIGO_FRED_DISCORD_WEBHOOK_URL`
    - `ORIGO_FRED_DISCORD_TIMEOUT_SECONDS`
    - updated `.env.example`
  - Guardrail proof validations:
    - missing webhook env fails loudly
    - warning audit event is written
    - Discord webhook alert is emitted
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-G4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.48 on 7th of March, 2026
- Completed Slice 6 guardrail step `S6-G3` (shadow-then-promote gating):
  - Extended query serving state gate to include FRED dataset:
    - `api/origo_api/rights.py`
    - `ORIGO_FRED_QUERY_SERVING_STATE` is now required for `fred_series_metrics` query serving
    - allowed values: `shadow`, `promoted`
    - behavior:
      - `shadow` blocks rights resolution with `QUERY_SERVING_SHADOW_MODE`
      - `promoted` allows rights resolution to continue
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s6_g3_fred_shadow_promote_proof.py`
    - `spec/slices/slice-6-fred-integration/guardrails-proof-s6-g3-fred-shadow-promote.json`
  - Updated env contract:
    - `.env.example` now includes `ORIGO_FRED_QUERY_SERVING_STATE`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-G3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.47 on 7th of March, 2026
- Completed Slice 6 guardrail step `S6-G2` (freshness checks based on source publish timestamps):
  - Added FRED publish-freshness warnings module:
    - `api/origo_api/fred_warnings.py`
  - Added FRED publish freshness runtime env contract:
    - `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
    - updated `.env.example`
  - Wired FRED publish freshness warnings into query API warning path:
    - `api/origo_api/main.py`
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s6_g2_fred_freshness_proof.py`
    - `spec/slices/slice-6-fred-integration/guardrails-proof-s6-g2-fred-freshness.json`
  - Guardrail proof validations:
    - synthetic missing-source publish timestamp -> `FRED_SOURCE_PUBLISH_MISSING`
    - live publish age threshold breach -> `FRED_SOURCE_PUBLISH_STALE`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-G2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.46 on 7th of March, 2026
- Completed Slice 6 guardrail step `S6-G1` (rights classification and legal gating):
  - Added FRED rights classification to source rights matrix:
    - `contracts/source-rights-matrix.json`
    - source: `fred`, rights_state: `Hosted Allowed`, dataset: `fred_series_metrics`
  - Added legal signoff artifact for FRED Hosted Allowed serving:
    - `contracts/legal/fred-hosted-allowed.md`
  - Extended query rights allowlist to include FRED dataset:
    - `api/origo_api/rights.py`
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s6_g1_fred_rights_proof.py`
    - `spec/slices/slice-6-fred-integration/guardrails-proof-s6-g1-fred-rights.json`
  - Guardrail proof validations:
    - missing legal signoff -> `QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING`
    - valid legal signoff -> query rights pass
    - missing FRED classification -> `QUERY_RIGHTS_MISSING_STATE`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-G1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.45 on 7th of March, 2026
- Completed Slice 6 proof step `S6-P3` (metadata/version reproducibility for replay):
  - Added metadata/version reproducibility proof harness:
    - `origo/fred/s6_p3_proof.py`
  - Added proof artifact:
    - `spec/slices/slice-6-fred-integration/proof-s6-p3-metadata-version-reproducibility.json`
  - Replay reproducibility validated:
    - run-1 and run-2 row hashes match
    - run-1 and run-2 metadata hashes match
    - run-1 and run-2 provenance hashes match
    - run-1 and run-2 per-series row counts match
  - Live proof results:
    - `deterministic_match=true`
    - rows hash: `ae7e8f20ce8d72b18bc250a50b36168b092b1949df96e129149bd2fe5cacea4e`
    - metadata hash: `984fa3bc176c19b8131d595fe49c600fd06371923a19a63928cc1400983c39bf`
    - provenance hash: `74d0f70976290e221b3f8d282a4d2d02696d4b4698d2c6854d79a6effb40703b`
    - registry version: `2026-03-06-s6-c1`
    - registry file sha256: `3970cf51d3f8c477be344b9d87cba7ee41e851dc9396b5958065dc17d400de85`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-P3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.44 on 7th of March, 2026
- Completed Slice 6 proof step `S6-P2` (replay fixed windows and verify deterministic outputs):
  - Added determinism proof harness:
    - `origo/query/fred_s6_p2_determinism_proof.py`
  - Added proof artifact:
    - `spec/slices/slice-6-fred-integration/proof-s6-p2-determinism.json`
  - Determinism scenarios validated across run-1 vs run-2:
    - native `fred_series_metrics` fixed window
    - aligned `fred_series_metrics` fixed window
  - Live proof results:
    - `deterministic_match_all=true`
    - native rows hash: `d878b3c383dfa76ea244f2afd2b305238466fc5b26f4ee45b6bc194f727a2499`
    - aligned rows hash: `851110d60f32b6603dd725b7e92d3dba5e28b78160337bce27546caab64ad381`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-P2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.43 on 7th of March, 2026
- Completed Slice 6 proof step `S6-P1` (acceptance scenarios for FRED native and aligned query paths):
  - Added proof harness:
    - `origo/query/fred_s6_p1_acceptance_proof.py`
  - Added proof artifact:
    - `spec/slices/slice-6-fred-integration/proof-s6-p1-acceptance.json`
  - Acceptance scenarios validated:
    - native `fred_series_metrics` time-range query envelope + contract validation
    - aligned `fred_series_metrics` forward-fill time-range query envelope + contract validation
  - Live proof results:
    - native row count: `74`
    - aligned row count: `74`
    - native rows hash: `d878b3c383dfa76ea244f2afd2b305238466fc5b26f4ee45b6bc194f727a2499`
    - aligned rows hash: `851110d60f32b6603dd725b7e92d3dba5e28b78160337bce27546caab64ad381`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-P1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.42 on 7th of March, 2026
- Completed Slice 6 capability step `S6-C7` (integrate FRED into aligned-1s materialization path):
  - Added FRED aligned materialization/query module:
    - `origo/query/fred_aligned_1s.py`
  - Extended aligned planner and routing for FRED observation + forward-fill paths:
    - `origo/query/aligned_core.py`
    - `origo/query/__init__.py`
  - Extended API aligned dataset allowlist:
    - `api/origo_api/main.py`
  - Added capability proof harness + artifact:
    - `origo/query/fred_s6_c7_aligned_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c7-fred-aligned-query.json`
  - Live proof results:
    - forward-fill case rows: `74`
    - observation latest-rows case rows: `25`
    - forward-fill rows hash: `918ea6ed63d71f44451f7f74e20ff90b38f3497c3bf8cd4b93462a9dc3088b8f`
    - observation rows hash: `425694586e06f54122c2ce7442f995a813283269d2012a4813d1bab043276689`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-C7` complete in `spec/2-itemized-work-plan.md`.

## v1.2.41 on 7th of March, 2026
- Completed Slice 6 capability step `S6-C6` (expose FRED through native raw query mode):
  - Added native FRED query planner and dataset typing:
    - `origo/query/fred_native.py`
    - `origo/query/__init__.py`
    - `origo/data/_internal/generic_endpoints.py`
  - Extended API query dataset contract and mode validation:
    - `api/origo_api/schemas.py`
    - `api/origo_api/main.py`
  - Added capability proof harness + artifact:
    - `origo/query/fred_s6_c6_query_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c6-fred-native-query.json`
  - Live proof results:
    - row count: `74`
    - source coverage: `fred_cpiaucsl`, `fred_dgs10`, `fred_fedfunds`, `fred_unrate`
    - rows hash: `d878b3c383dfa76ea244f2afd2b305238466fc5b26f4ee45b6bc194f727a2499`
  - Static hard gates passed (`ruff`, `pyright`).
- Marked `S6-C6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.40 on 7th of March, 2026
- Completed Slice 6 capability step `S6-C5` (persist native FRED data to ClickHouse and raw artifacts to object store):
  - Added versioned SQL migration:
    - `control-plane/migrations/sql/0003__create_fred_series_metrics_long.sql`
  - Added FRED persistence module:
    - `origo/fred/persistence.py`
    - ClickHouse persistence: `persist_fred_long_metrics_to_clickhouse(...)`
    - raw artifact bundle + object-store persistence:
      - `build_fred_raw_bundles(...)`
      - `persist_fred_raw_bundles_to_object_store(...)`
  - Added capability proof harness + artifact:
    - `origo/fred/s6_c5_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c5-fred-persistence.json`
  - Live proof results:
    - inserted rows: `74`
    - persisted raw artifacts: `4`
    - rows hash: `ae7e8f20ce8d72b18bc250a50b36168b092b1949df96e129149bd2fe5cacea4e`
  - Exported persistence symbols in:
    - `origo/fred/__init__.py`
- Marked `S6-C5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.39 on 7th of March, 2026
- Completed Slice 6 capability steps `S6-C3` and `S6-C4`:
  - Added FRED ingest module with deterministic backfill and incremental runners:
    - `origo/fred/ingest.py`
  - Added backfill proof harness + artifact:
    - `origo/fred/s6_c3_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c3-fred-backfill.json`
  - Added incremental proof harness + artifact:
    - `origo/fred/s6_c4_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c4-fred-incremental.json`
  - Exported ingest contracts/functions through:
    - `origo/fred/__init__.py`
  - Live fixed-window proof fingerprints:
    - backfill `rows_hash_sha256=ae7e8f20ce8d72b18bc250a50b36168b092b1949df96e129149bd2fe5cacea4e`
    - incremental `rows_hash_sha256=d2665db89f639c45ae0645cc620e82d31edbce0896cf224ce20527b78c224779`
- Marked `S6-C3` and `S6-C4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.38 on 7th of March, 2026
- Completed Slice 6 capability step `S6-C2` (normalize FRED records into long-metric schema):
  - Added FRED long-metric normalization module:
    - `origo/fred/normalize.py`
  - Added capability proof harness + artifact:
    - `origo/fred/s6_c2_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c2-fred-normalize.json`
  - Proof executed against live source API with deterministic normalized output fingerprint:
    - `rows_hash_sha256=55594af4eec42413711a3d0764278dccf16f93c24810c52d0ea1a689f4d8f932`
- Updated FRED API key naming contract to remove `ORIGO_` prefix for the key:
  - Runtime key: `FRED_API_KEY`
  - Updated in:
    - `origo/fred/client.py`
    - `.env.example`
    - slice docs (`spec/slices/slice-6-fred-integration/*`)
- Marked `S6-C2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.37 on 7th of March, 2026
- Completed Slice 6 capability step `S6-C1` (FRED series connector and metadata fetch):
  - Added new FRED module:
    - `origo/fred/contracts.py`
    - `origo/fred/registry.py`
    - `origo/fred/client.py`
    - `origo/fred/__init__.py`
  - Added FRED series registry contract:
    - `contracts/fred-series-registry.json`
  - Added capability proof harness + artifact:
    - `origo/fred/s6_c1_proof.py`
    - `spec/slices/slice-6-fred-integration/capability-proof-s6-c1-fred-connector.json`
  - Proof executed against live source API with provided key; registry mappings, metadata normalization, and observation fetch path validated.
- Updated env contract for Slice 6 source settings:
  - `FRED_API_KEY`
  - `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`
- Marked `S6-C1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.36 on 6th of March, 2026
- Completed Slice 5 documentation closeout `S5-G5` and `S5-G6`:
  - Added developer reference:
    - `docs/Developer/s5-aligned-query-export-guardrails.md`
  - Added/updated user references:
    - `docs/aligned-reference.md`
    - `docs/raw-query-reference.md`
    - `docs/raw-export-reference.md`
    - `docs/data-taxonomy.md`
  - Documentation now covers aligned query/export contracts, field taxonomy, freshness metadata, warning/error semantics, rights behavior, determinism notes, env contract, and examples.
- Added Slice 5 baseline fixture artifact:
  - `spec/slices/slice-5-raw-query-aligned-1s/baseline-fixture-2017-08-17_2026-03-05-2026-03-07.json`
  - includes fixture windows, day fingerprints, run1/run2 fingerprints, deterministic match, and column-key definitions.
- Marked `S5-G5` and `S5-G6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.35 on 6th of March, 2026
- Completed Slice 5 guardrail steps `S5-G1`, `S5-G2`, `S5-G3`, `S5-G4`:
  - Added aligned strict/warning guardrail coverage in query API path.
  - Added aligned freshness metadata and stale warning generation:
    - response `freshness` payload
    - warning code `ALIGNED_FRESHNESS_STALE`
    - env contract `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - Added aligned-specific query queue/concurrency controls:
    - env contracts:
      - `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
      - `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
    - error code:
      - `ALIGNED_QUERY_QUEUE_LIMIT_REACHED`
  - Added aligned export guardrail coverage (rights + audit + mode handling):
    - `api/origo_api/rights.py`
    - `api/origo_api/main.py`
    - `control-plane/origo_control_plane/jobs/raw_export_native.py`
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s5_g1_g4_aligned_guardrails_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/guardrails-proof-s5-g1-g4.json`
    - proof validates strict warning failure, freshness warning, aligned queue limit rejection, and aligned export ingest-only rights rejection with audit event capture.
- Marked `S5-G1..S5-G4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.34 on 6th of March, 2026
- Completed Slice 5 proof stage `S5-P1`, `S5-P2`, `S5-P3`:
  - Added aligned acceptance proof harness:
    - `origo/query/aligned_s5_p1_acceptance_proof.py`
    - artifact: `spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p1-acceptance.json`
  - Added aligned determinism replay harness:
    - `origo/query/aligned_s5_p2_determinism_proof.py`
    - artifact: `spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p2-determinism.json`
  - Added forward-fill UTC day-boundary proof harness:
    - `origo/query/aligned_s5_p3_boundary_proof.py`
    - artifact: `spec/slices/slice-5-raw-query-aligned-1s/proof-s5-p3-boundary.json`
  - Proof results:
    - acceptance cases passed across Binance and ETF aligned windows
    - deterministic replay hashes matched across repeated runs
    - UTC boundary transition semantics validated for forward-fill intervals
- Marked `S5-P1`, `S5-P2`, `S5-P3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.33 on 6th of March, 2026
- Completed Slice 5 capability steps `S5-C6` and `S5-C7` (aligned exports in Parquet and CSV):
  - Extended export contracts to include aligned mode:
    - `api/origo_api/schemas.py`
    - `RawExportMode = native|aligned_1s`
    - export dataset typing expanded to query dataset universe
  - Extended export metadata parsing/status typing for aligned mode:
    - `api/origo_api/main.py`
  - Extended export rights typing and fail-closed checks:
    - `api/origo_api/rights.py`
  - Extended Dagster export execution step to dispatch aligned query path:
    - `control-plane/origo_control_plane/jobs/raw_export_native.py`
  - Added aligned export proof harness + artifact:
    - `control-plane/origo_control_plane/aligned_export_s5_06_07_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c6-c7-aligned-export.json`
    - proof validates parity for both formats (`parquet`, `csv`) across Binance and ETF aligned windows.
- Marked `S5-C6` and `S5-C7` complete in `spec/2-itemized-work-plan.md`.

## v1.2.32 on 6th of March, 2026
- Completed Slice 5 capability step `S5-C5` (aligned response envelope and schema metadata):
  - Added aligned envelope adapter:
    - `origo/data/_internal/generic_endpoints.py`
    - `query_aligned_wide_rows_envelope(...)` now emits `mode=aligned_1s`
  - Extended query API contracts:
    - `api/origo_api/schemas.py`
    - `RawQueryMode = native|aligned_1s`
    - `RawQueryRequest` now includes `mode`
    - `RawQueryResponse.mode` is typed to query modes
    - export mode typing split and kept `native`-only pending `S5-C6/S5-C7`
  - Wired `/v1/raw/query` route to dispatch by mode:
    - `native` -> native envelope path
    - `aligned_1s` -> aligned envelope path
  - Added capability proof harness + artifact:
    - `origo/query/aligned_envelope_s5_05_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c5-aligned-envelope.json`
    - proof validates aligned envelope mode/source/schema/row-count and `RawQueryResponse` contract validation for Binance + ETF aligned cases.
- Marked `S5-C5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.31 on 6th of March, 2026
- Completed Slice 5 capability step `S5-C4` (unified aligned query planner path):
  - Added unified planner module:
    - `origo/query/aligned_core.py`
  - Added planner contracts:
    - `AlignedQueryPlan`
    - `build_aligned_query_plan(...)`
    - `query_aligned_data(...)`
  - Planner dispatch paths:
    - Binance datasets -> `binance_aligned`
    - ETF `time_range`/`month_year` -> `etf_aligned_forward_fill`
    - ETF `n_rows`/`n_random` -> `etf_aligned_observation`
  - Added strict aligned projection validation with fail-loud behavior.
  - Added aligned endpoint adapter:
    - `origo/data/_internal/generic_endpoints.py` (`query_aligned(...)`)
  - Exported aligned planner symbols:
    - `origo/query/__init__.py`
  - Added capability proof harness + artifact:
    - `origo/query/aligned_planner_s5_04_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c4-aligned-planner.json`
    - proof validates planner dispatch across Binance + ETF cases, projection contract enforcement, deterministic ordering, and second-grid datetime alignment.
- Marked `S5-C4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.30 on 6th of March, 2026
- Completed Slice 5 capability step `S5-C3` (ETF logical forward-fill semantics for aligned mode):
  - Extended ETF aligned query module:
    - `origo/query/etf_aligned_1s.py`
    - added prior-state lookup before window start
    - added interval construction with deterministic bounds:
      - `valid_from_utc`
      - `valid_to_utc_exclusive`
  - Tightened prior-state value selection ordering to `(observed_at_utc, ingested_at_utc)`.
  - Exported forward-fill query symbol via:
    - `origo/query/__init__.py`
  - Added capability proof harness + artifact:
    - `origo/query/etf_forward_fill_s5_03_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c3-etf-forward-fill.json`
    - proof validates interval monotonicity/non-overlap, window carry/clip behavior, second alignment, and UTC day-boundary transitions.
- Marked `S5-C3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.29 on 6th of March, 2026
- Completed Slice 5 capability step `S5-C2` (ETF aligned-1s materialization definitions):
  - Added ETF aligned-1s materialization module:
    - `origo/query/etf_aligned_1s.py`
  - Added dataset definition for:
    - `etf_daily_metrics` -> `etf_daily_metrics_long`
  - Added aligned query SQL compiler for all window modes:
    - `month_year`, `time_range`, `n_rows`, `n_random`
  - Added deterministic 1-second aligned output shape:
    - `aligned_at_utc`, `source_id`, `metric_name`, `metric_unit`,
      `metric_value_string`, `metric_value_int`, `metric_value_float`, `metric_value_bool`,
      `dimensions_json`, `provenance_json`, `latest_ingested_at_utc`, `records_in_bucket`
  - Added deterministic merge rule per aligned bucket key:
    - value/unit/lineage columns selected via `argMax(..., ingested_at_utc)`
  - Exported ETF aligned symbols via:
    - `origo/query/__init__.py`
  - Added capability proof harness + artifact:
    - `origo/query/etf_aligned_s5_02_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c2-etf-aligned.json`
    - proof validates non-empty aligned output, second-alignment, schema contract, and full 10-source coverage in fixed ETF proof window.
- Marked `S5-C2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.28 on 6th of March, 2026
- Completed Slice 5 capability step `S5-C1` (Binance aligned-1s materialization definitions):
  - Added Binance aligned-1s materialization module:
    - `origo/query/binance_aligned_1s.py`
  - Added dataset definitions for:
    - `binance_spot_trades` -> `binance_trades`
  - Added aligned query SQL compiler for all window modes:
    - `month_year`, `time_range`, `n_rows`, `n_random`
  - Added deterministic 1-second aggregate output shape:
    - `aligned_at_utc`, `open_price`, `high_price`, `low_price`, `close_price`,
      `quantity_sum`, `quote_volume_sum`, `trade_count`
  - Exported aligned symbols via:
    - `origo/query/__init__.py`
  - Added capability proof harness + artifact:
    - `origo/query/binance_aligned_s5_01_proof.py`
    - `spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c1-binance-aligned.json`
    - proof validates Binance spot dataset returns non-empty second-aligned rows with expected schema.
- Marked `S5-C1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.27 on 6th of March, 2026
- Completed Slice 4 guardrail doc closeout steps `S4-G8` and `S4-G9`:
  - Developer docs (`S4-G8`):
    - `docs/Developer/s4-etf-adapters.md`
    - `docs/Developer/s4-etf-normalization.md`
    - `docs/Developer/s4-etf-proof-workflow.md`
    - `docs/Developer/s4-etf-guardrails-runbook.md`
    - coverage includes issuer adapter contracts, normalization/canonical-load contract, proof workflow, and guardrail operations.
  - User docs (`S4-G9`):
    - `docs/etf-reference.md`
    - updated `docs/data-taxonomy.md`
    - updated `docs/raw-query-reference.md`
    - updated `docs/scraper-reference.md`
    - coverage includes ETF dataset taxonomy, fields/units/cadence/freshness, warnings, rights/serving gates, and error semantics.
- Marked `S4-G8` and `S4-G9` complete in `spec/2-itemized-work-plan.md`.

## v1.2.26 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G7` (logs + Discord alerts for ETF ingestion anomalies):
  - Added ETF anomaly logging + Discord alert plumbing in control-plane definitions:
    - `control-plane/origo_control_plane/definitions.py`
    - new failure sensor: `origo_etf_ingest_anomaly_alert_sensor`
    - new alert helpers:
      - anomaly log append (`JSONL`)
      - Discord webhook post with strict HTTP status check
  - Added required env contract values:
    - `ORIGO_ETF_ANOMALY_LOG_PATH`
    - `ORIGO_ETF_DISCORD_WEBHOOK_URL`
    - `ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS`
    - `ORIGO_ETF_DISCORD_TIMEOUT_SECONDS`
    - documented in root `.env.example`
  - Alerting behavior:
    - sensor listens to ETF ingest run failures
    - non-ETF failures are skipped with explicit reason
    - ETF failure emits local anomaly log event and Discord webhook alert
    - webhook non-2xx responses fail loudly
  - Added guardrail proof harness + artifact:
    - `control-plane/origo_control_plane/etf_daily_alerts_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g7.json`
    - proof validates:
      - missing webhook env fails loudly
      - sensor registration and env-bound interval
      - anomaly log event write
      - Discord webhook emission path
- Marked `S4-G7` complete in `spec/2-itemized-work-plan.md`.

## v1.2.25 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G6` (warning generation for stale/missing/incomplete daily records):
  - Added ETF daily quality warning engine:
    - `api/origo_api/etf_warnings.py`
    - warning codes:
      - `ETF_DAILY_STALE_RECORDS`
      - `ETF_DAILY_MISSING_RECORDS`
      - `ETF_DAILY_INCOMPLETE_RECORDS`
    - warning checks evaluate latest ETF day freshness, expected-source presence, and mandatory metric completeness.
  - Wired ETF warning generation into query API path:
    - `api/origo_api/main.py`
    - `/v1/raw/query` now appends ETF quality warnings for `dataset=etf_daily_metrics`
    - strict mode behavior remains enforced (`strict=true` fails when warnings exist)
    - warning-runtime failures surface loudly via:
      - `QUERY_WARNING_RUNTIME_ERROR`
      - `QUERY_WARNING_BACKEND_ERROR`
      - `QUERY_WARNING_UNKNOWN_ERROR`
  - Added new required env contract:
    - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
    - documented in root `.env.example`
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s4_g6_etf_warning_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g6.json`
    - proof validates missing-only, degraded (stale+missing+incomplete), and clean snapshot warning outcomes.
- Marked `S4-G6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.24 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G5` (shadow-then-promote gating for serving enablement):
  - Added ETF query serving-state gate in:
    - `api/origo_api/rights.py`
    - required env contract: `ORIGO_ETF_QUERY_SERVING_STATE`
    - allowed values: `shadow`, `promoted`
    - fail-loud behavior:
      - missing/empty state env raises runtime contract error
      - invalid state value raises runtime contract error
      - `shadow` blocks ETF query rights with `QUERY_SERVING_SHADOW_MODE`
      - `promoted` allows ETF query rights to proceed (subject to existing rights/signoff checks)
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s4_g5_shadow_promote_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g5.json`
    - proof validates missing-env fail-loud behavior, shadow block behavior, promoted pass behavior, and invalid-value fail-loud behavior.
  - Updated root env contract:
    - `.env.example` now includes `ORIGO_ETF_QUERY_SERVING_STATE`.
- Marked `S4-G5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.23 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G4` (24h same-day retry window behavior):
  - Added ETF daily retry decision logic and run-failure sensor wiring in:
    - `control-plane/origo_control_plane/definitions.py`
    - sensor: `origo_etf_daily_retry_sensor`
    - monitored job: `origo_etf_daily_ingest_job`
  - Implemented strict retry policy behavior:
    - retry window fixed to 24 hours from failed-run creation time
    - max retry attempts enforced via env contract
    - retry run tags include attempt/origin/parent and window-end metadata
  - Added new required env contract values:
    - `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS`
    - `ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS`
    - documented in root `.env.example`
  - Added retry proof harness + artifact:
    - `control-plane/origo_control_plane/etf_daily_retry_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g4.json`
    - proof validates:
      - fail-loud on missing retry env contract
      - retry run request generation within 24h window
      - skip behavior outside window and at max attempts
      - chained retry run-key behavior by origin run.
- Marked `S4-G4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.22 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G3` (daily run schedule for ETF ingestion):
  - Added dedicated Dagster ETF daily ingest job:
    - `control-plane/origo_control_plane/jobs/etf_daily_ingest.py`
    - job name: `origo_etf_daily_ingest_job`
    - step name: `origo_etf_daily_ingest_step`
    - execution path runs all 10 issuer adapters via scraper pipeline and emits run metadata.
  - Wired job into control-plane jobs package:
    - `control-plane/origo_control_plane/jobs/__init__.py`
  - Added env-driven daily schedule in definitions:
    - `control-plane/origo_control_plane/definitions.py`
    - schedule name: `origo_etf_daily_ingest_schedule`
    - required env contract:
      - `ORIGO_ETF_DAILY_SCHEDULE_CRON`
      - `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`
  - Updated root env contract:
    - `.env.example` now includes required ETF schedule vars.
  - Added guardrail proof harness + artifact:
    - `control-plane/origo_control_plane/etf_daily_schedule_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g3.json`
    - proof validates:
      - fail-loud behavior when schedule cron env is missing
      - schedule registration/wiring to `origo_etf_daily_ingest_job`
      - cron/timezone values bound from env.
- Marked `S4-G3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.21 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G2` (rights matrix checks in ingestion and API paths):
  - Enforced query-path rights-state gating (fail-closed) in:
    - `api/origo_api/rights.py`
      - added `resolve_query_rights(dataset, auth_token)` with:
        - `QUERY_RIGHTS_MISSING_STATE` on missing dataset classification
        - `QUERY_RIGHTS_INGEST_ONLY` on ingest-only sources
        - `QUERY_RIGHTS_BYOK_REQUIRED` when BYOK token is required and missing
        - hosted-source legal-signoff validation reuse (`QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING`)
  - Updated `/v1/raw/query` route to enforce full rights checks before execution:
    - `api/origo_api/main.py`
  - Enforced ingestion source-id classification against rights matrix:
    - `origo/scraper/rights.py`
      - `resolve_scraper_rights(source_key, source_id)` now requires explicit `source_ids` coverage
      - emits `SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED` on mismatch/missing source-id contract
    - `origo/scraper/pipeline.py` now passes and audits `source_id` in rights resolution.
  - Expanded rights matrix contract for ETF serving/ingestion checks:
    - `contracts/source-rights-matrix.json`
      - mapped ETF issuer sources to dataset `etf_daily_metrics`
      - added explicit ETF issuer `source_ids` entries
      - version bumped to `2026-03-06-s4-g2`
  - Added guardrail proof harness + artifact:
    - `api/origo_api/s4_g2_rights_enforcement_proof.py`
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g2.json`
    - proof validates query/export rights-state error codes, hosted pass path, and scraper source-id fail-closed behavior.
- Marked `S4-G2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.20 on 6th of March, 2026
- Completed Slice 4 guardrail step `S4-G1` (legal sign-off artifact requirement before external serving/export):
  - Added query-path legal sign-off gating:
    - `api/origo_api/rights.py`
      - `enforce_query_legal_signoff(dataset=...)`
      - hosted sources now fail loud when legal sign-off artifact is missing/misconfigured
  - Refactored legal-signoff enforcement to shared helper for both query and export paths.
  - Wired query API to enforce legal sign-off gate before executing `/v1/raw/query`:
    - `api/origo_api/main.py`
  - Added guardrail proof harness:
    - `api/origo_api/s4_g1_legal_signoff_proof.py`
  - Added guardrail proof artifact:
    - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g1.json`
    - proof checks confirm:
      - query path fails with `QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING` when sign-off artifact is missing
      - export path fails with `EXPORT_RIGHTS_LEGAL_SIGNOFF_MISSING` when sign-off artifact is missing
      - query/export pass with valid sign-off artifact
- Marked `S4-G1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.19 on 6th of March, 2026
- Completed Slice 4 proof stage (`S4-P1..S4-P4`) for ETF ingestion/query path:
  - Added ETF proof-suite runner:
    - `origo/scraper/etf_s4_05_proof_suite.py`
  - Added proof artifact:
    - `spec/slices/slice-4-etf-use-case/proof-s4-p1-p4.json`
  - Proof results:
    - `S4-P1`: 10-issuer parity proof executed against official issuer sources.
    - `S4-P2`: parity threshold met and exceeded (`100.0%` vs required `>=99.5%`).
    - `S4-P3`: replay determinism verified (`run1` and `run2` inserted-row fingerprints match).
    - `S4-P4`: provenance/artifact reference checks passed for all proof-window rows.
  - Root-cause correction in proof runner:
    - mandatory parity metric set aligned to normalized metric output contract (excludes parser-only metadata field `row_index`).
- Marked `S4-P1..S4-P4` complete in `spec/2-itemized-work-plan.md`.

## v1.2.18 on 6th of March, 2026
- Completed Slice 4 capability step `S4-C8` (expose ETF fields via native raw query mode):
  - Added ETF native query planner:
    - `origo/query/etf_native.py`
    - dataset: `etf_daily_metrics`
    - table: `etf_daily_metrics_long`
    - time column: `observed_at_utc`
  - Extended native query core for dataset-specific time/seed columns:
    - `NativeQuerySpec` now supports `datetime_column` and `random_seed_column`.
    - existing Binance query behavior remains unchanged.
  - Added native query dispatch path for mixed dataset families:
    - `origo/data/_internal/generic_endpoints.py`
      - new `query_native` and `query_native_wide_rows_envelope`
  - Updated `/v1/raw/query` route wiring to use dataset-agnostic native dispatch:
    - `api/origo_api/main.py`
    - `api/origo_api/schemas.py` (`RawQueryRequest.dataset` now includes `etf_daily_metrics`)
  - Added capability proof script + artifact:
    - `origo/query/etf_s4_08_query_proof.py`
    - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c8-native-query.json`
  - Verified HTTP contract path with `POST /v1/raw/query` (`dataset=etf_daily_metrics`) returning `200` and expected schema/rows.
- Marked `S4-C8` complete in `spec/2-itemized-work-plan.md`.

## v1.2.17 on 6th of March, 2026
- Completed Slice 4 capability step `S4-C7` (full available-history backfill per issuer):
  - Added backfill runner:
    - `origo/scraper/etf_s4_07_backfill.py`
  - Backfill behavior:
    - iShares (`IBIT`) uses date-parameterized official source requests (`asOfDate=YYYYMMDD`) from `2024-01-01` through `2026-03-06`, weekday-filtered.
    - non-iShares issuers run official-source snapshot backfill (single current as-of day) because tested issuer endpoints remain snapshot-style in current source contracts.
    - existing `(source_id, as_of_date)` rows are deduplicated before insert.
  - Added fetch resiliency and no-data handling in backfill flow:
    - retry/backoff (`max_attempts=5`, exponential backoff from `0.5s`)
    - explicit iShares no-data day detection.
  - Fixed iShares historical parse blocker:
    - `origo/scraper/etf_adapters.py` `_parse_optional_float` now treats dash sentinels (`-`, `--`, dash variants) as null for optional numeric fields.
  - Added capability proof artifact:
    - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c7-backfill.json`
    - proof highlights:
      - iShares inserted days: `287`
      - iShares coverage in canonical table: `542` distinct as-of days (`2024-01-05` .. `2026-03-04`)
- Marked `S4-C7` complete in `spec/2-itemized-work-plan.md`.

## v1.2.16 on 6th of March, 2026
- Completed Slice 4 capability step `S4-C6` (canonical ETF daily ClickHouse load):
  - Added SQL migration:
    - `control-plane/migrations/sql/0002__create_etf_daily_metrics_long.sql`
  - Updated scraper ClickHouse persistence routing in `origo/scraper/clickhouse_staging.py`:
    - ETF daily batches (`source_id` pattern `etf_*_daily`) now insert into canonical table `etf_daily_metrics_long`
    - mixed ETF/non-ETF insert batches fail loudly
    - ETF canonical insert path enforces UTC-midnight `observed_at_utc`
  - Added capability proof harness:
    - `origo/scraper/etf_s4_06_load_proof.py`
  - Added proof artifact:
    - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c6-canonical-load.json`
    - proof run result: `total_inserted_rows=116`, `db_total_inserted_rows=116`, `row_count_match=true`
- Marked `S4-C6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.15 on 5th of March, 2026
- Completed Slice 4 capability step `S4-C1` (ETF issuer adapters 1-3):
  - Added issuer adapters in `origo/scraper/etf_adapters.py`:
    - `ISharesIBITAdapter`
    - `InvescoBTCOAdapter`
    - `BitwiseBITBAdapter`
  - Added strict source parsing for official issuer endpoints and canonicalized ETF daily payload fields.
  - Added UTC-daily normalization semantics by deriving `observed_at_utc` from source `as_of_date`.
- Added capability proof harness:
  - `origo/scraper/etf_s4_01_proof.py`
  - generated proof artifact: `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-01.json`
- Added Slice 4 working docs:
  - `spec/slices/slice-4-etf-use-case/run-notes.md`
  - `spec/slices/slice-4-etf-use-case/manifest.md`
- Updated source rights matrix with ETF issuer source keys (fail-closed but resolvable):
  - `contracts/source-rights-matrix.json` now includes `ishares`, `invesco`, `bitwise` as `Ingest Only`.
- Marked `S4-C1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.14 on 5th of March, 2026
- Completed Slice 3 Generic Scraper platform end-to-end:
  - Capability (`S3-C1..S3-C12`):
    - Added typed scraper contracts and adapter interface in `origo/scraper/contracts.py`.
    - Added HTTP + browser fetch modules:
      - `origo/scraper/http_fetch.py`
      - `origo/scraper/browser_fetch.py`
    - Added parser modules for HTML/CSV/JSON/PDF:
      - `origo/scraper/parse_html.py`
      - `origo/scraper/parse_csv.py`
      - `origo/scraper/parse_json.py`
      - `origo/scraper/parse_pdf.py`
      - `origo/scraper/parse_dispatch.py`
    - Added normalization + persistence path:
      - `origo/scraper/normalize.py`
      - `origo/scraper/object_store.py`
      - `origo/scraper/clickhouse_staging.py`
    - Added orchestrated pipeline runtime:
      - `origo/scraper/pipeline.py`
- Completed Slice 3 proofs (`S3-P1..S3-P3`):
  - `spec/slices/slice-3-generic-scraper/acceptance-proof.json`
  - `spec/slices/slice-3-generic-scraper/baseline-fixture-proof_etf_csv-2026-03-01_2026-03-02.json`
  - `spec/slices/slice-3-generic-scraper/replayability-proof.json`
- Completed Slice 3 guardrails (`S3-G1..S3-G4`):
  - Error taxonomy and typed failures in `origo/scraper/errors.py`.
  - Retry/backoff hooks in `origo/scraper/retry.py`.
  - Rights fail-closed hook in `origo/scraper/rights.py`.
  - Immutable hash-chained scraper audit events in `origo/scraper/audit.py`.
  - Guardrail proof in `spec/slices/slice-3-generic-scraper/guardrails-proof.json`.
- Completed Slice 3 docs closeout (`S3-G5`, `S3-G6`):
  - Developer docs:
    - `docs/Developer/s3-scraper-contracts.md`
    - `docs/Developer/s3-scraper-pipeline.md`
  - User docs:
    - `docs/scraper-reference.md`
    - `docs/data-taxonomy.md` updates
- Updated scraper env contract in root `.env.example`:
  - object-store vars (`ORIGO_OBJECT_STORE_*`)
  - scraper guardrail vars (`ORIGO_SCRAPER_FETCH_*`, `ORIGO_SCRAPER_AUDIT_LOG_PATH`)
- Marked all Slice 3 checkboxes complete in `spec/2-itemized-work-plan.md`.

## v1.2.13 on 5th of March, 2026
- Completed Slice 2 guardrails (`S2-G1`, `S2-G2`, `S2-G3`) and doc closeout (`S2-G4`, `S2-G5`):
  - Enforced pre-dispatch source rights gate using repo matrix contract:
    - `contracts/source-rights-matrix.json`
    - `contracts/legal/binance-hosted-allowed.md`
  - Added immutable export lifecycle audit events with append-only hash-chained sink.
  - Added export submit backpressure controls:
    - `ORIGO_EXPORT_MAX_CONCURRENCY`
    - `ORIGO_EXPORT_MAX_QUEUE`
  - Added terminal failure classification in export status payload (`error_code`, `error_message`) with failure metadata persistence from Dagster export job (`error.json`).
- Completed Slice 2 docs closeout:
  - Developer docs:
    - `docs/Developer/s2-raw-export-api.md`
    - `docs/Developer/s2-export-guardrails.md`
  - User docs:
    - `docs/raw-export-reference.md`
    - `docs/data-taxonomy.md` update
- Added Slice 2 guardrail proof artifact:
  - `spec/slices/slice-2-raw-export-native/guardrails-proof.json`
- Marked `S2-G1..S2-G5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.12 on 5th of March, 2026
- Completed Slice 2 proof step `S2-P3` (export parity vs query output):
  - Added parity artifact `spec/slices/slice-2-raw-export-native/parity-proof.json`.
  - Verified parity for both `parquet` and `csv` on fixed window:
    - row counts match,
    - columns/order match,
    - canonical row-content hashes match.
- Updated Slice 2 proof docs:
  - `spec/slices/slice-2-raw-export-native/run-notes.md`
  - `spec/slices/slice-2-raw-export-native/manifest.md`
- Marked `S2-P3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.11 on 5th of March, 2026
- Completed Slice 2 proof step `S2-P2` (deterministic export artifacts on fixed windows):
  - Added deterministic proof artifact `spec/slices/slice-2-raw-export-native/baseline-fixture-2017-08-native-export.json`.
  - Verified two-run determinism for both `parquet` and `csv` export formats:
    - matching artifact SHA256 checksums,
    - matching artifact byte sizes,
    - matching metadata row counts and column sets.
- Updated Slice 2 proof docs:
  - `spec/slices/slice-2-raw-export-native/run-notes.md`
  - `spec/slices/slice-2-raw-export-native/manifest.md`
- Marked `S2-P2` complete in `spec/2-itemized-work-plan.md`.

## v1.2.10 on 5th of March, 2026
- Implemented Slice 2 artifact metadata persistence and retrieval pointers (`S2-C6`):
  - Export job now computes `checksum_sha256` for written artifacts and persists it in `metadata.json`.
  - Export status endpoint now reads persisted metadata from `ORIGO_EXPORT_ROOT_DIR/<run_id>/metadata.json`.
  - Export status response now returns `artifact` details (`format`, absolute artifact `uri`, `row_count`, `checksum_sha256`) when available.
  - Added fail-loud behavior for metadata mismatches/missing artifact files and for succeeded runs without metadata.
- Marked `S2-C6` complete in `spec/2-itemized-work-plan.md`.

## v1.2.9 on 5th of March, 2026
- Implemented Slice 2 native export execution path in Dagster:
  - Added `control-plane/origo_control_plane/jobs/raw_export_native.py` with `origo_raw_export_native_job`.
  - Export job now parses request payload from run tags, executes native query path, and writes export artifacts.
  - Added support for both `parquet` and `csv` artifact writes.
- Rewired export dispatch plumbing:
  - `api/origo_api/dagster_graphql.py` now sends full request payload (`origo.export.request_json`) in Dagster run tags.
  - `api/origo_api/main.py` now rejects export `auth_token` input for this slice and dispatches request payload to Dagster.
  - `control-plane/origo_control_plane/definitions.py` now imports export job from dedicated jobs module.
- Updated environment contract:
  - Added `ORIGO_EXPORT_ROOT_DIR` requirement in root `.env.example`.
- Marked `S2-C4` and `S2-C5` complete in `spec/2-itemized-work-plan.md`.

## v1.2.8 on 4th of March, 2026
- Added Dagster-backed raw export dispatch plumbing for Slice 2:
  - Added `api/origo_api/dagster_graphql.py` with strict GraphQL launch/status calls, fail-loud env handling, and typed run snapshot parsing.
  - Updated `POST /v1/raw/export` to submit real Dagster runs (export id == Dagster `runId`).
  - Updated `GET /v1/raw/export/{export_id}` to read Dagster run state and map run statuses to API status contract.
- Added Dagster export contract env vars to root `.env.example`:
  - `ORIGO_DAGSTER_GRAPHQL_URL`
  - `ORIGO_DAGSTER_REPOSITORY_NAME`
  - `ORIGO_DAGSTER_LOCATION_NAME`
  - `ORIGO_DAGSTER_EXPORT_JOB_NAME`
- Added `origo_raw_export_native_job` in `control-plane/origo_control_plane/definitions.py` as the initial export dispatch target job.
- Marked `S2-C2` and `S2-C3` complete in `spec/2-itemized-work-plan.md`.

## v1.2.7 on 4th of March, 2026
- Closed Slice 1 documentation guardrails:
  - Added developer docs in `docs/Developer/s1-native-query-core.md` and `docs/Developer/s1-raw-query-api.md`.
  - Added user docs in `docs/raw-query-reference.md` and `docs/data-taxonomy.md`.
  - Marked `S1-G6` and `S1-G7` complete in `spec/2-itemized-work-plan.md`.
- Started Slice 2 capability with export API contracts:
  - Added typed `RawExport*` request/response/status schemas in `api/origo_api/schemas.py`.
  - Added live `POST /v1/raw/export` submit endpoint and `GET /v1/raw/export/{export_id}` status endpoint in `api/origo_api/main.py`.
  - Marked `S2-C1` complete in `spec/2-itemized-work-plan.md`.

## v1.2.6 on 4th of March, 2026
- Added Slice 1 native query core in `origo/query/native_core.py` (`ClickHouse SQL -> Arrow -> Polars`) with deterministic window semantics.
- Added unified Binance native planner in `origo/query/binance_native.py` covering `binance_spot_trades` with strict field allowlists.
- Added UTC millisecond canonicalization and optional ISO datetime output for query results.
- Added wide-row envelope builder with schema metadata in `origo/query/response.py`.
- Rewired `origo.data.HistoricalData` native trade methods through the new query planner path.
- Removed eager package import side effects in `origo/__init__.py` and `origo/data/__init__.py` so API/runtime imports do not require optional utility dependencies.
- Replaced swallowed ClickHouse client-close exceptions with explicit warning logs in query execution paths.
- Replaced swallowed ClickHouse disconnect exceptions in Binance ingestion assets (`daily`, `monthly`, `monthly_agg`, `monthly_futures`) and preserved traceback semantics (`raise`).
- Added FastAPI adapter scaffold in `api/origo_api` with `/v1/raw/query` + request/response contracts and `/health`.
- Added Slice 1 guardrails in API adapter:
  - static internal API key middleware,
  - status/error mapping (`200/404/409/503`),
  - structured warnings + `strict=true` fail behavior,
  - in-process concurrency and queue controls.
- Added Slice 1 proof artifacts under `spec/slices/slice-1-raw-query-native/` and marked all Slice 1 work-plan checkboxes complete.

## v1.2.5 on 4th of March, 2026
- Added monorepo env contract at root `.env.example` for required ClickHouse runtime variables.
- Enforced fail-loud env loading in control-plane via `origo_control_plane.config.env` (missing or empty required vars now raise immediately).
- Removed deployment-specific ClickHouse defaults from control-plane asset/migration/runtime paths.
- Updated ClickHouse client wiring to require explicit native (`CLICKHOUSE_PORT`) and HTTP (`CLICKHOUSE_HTTP_PORT`) ports.
- Updated compose wiring to pass required ClickHouse env vars into Dagster services without hard-coded defaults.
- Marked Slice 0 guardrail `S0-G6` complete in the itemized work plan.

## v1.2.4 on 4th of March, 2026
- Added SQL migration scaffold under `control-plane/migrations/` with ordered version files.
- Added migration runner (`status` and `migrate`) in `origo_control_plane.migrations` with strict filename, contiguous version, and checksum enforcement.
- Added migration ledger management (`schema_migrations`) and `{{DATABASE}}` placeholder rendering support.
- Added first scaffold migration `0001__create_migration_probe.sql` and validated apply/status against ClickHouse.
- Marked Slice 0 guardrail `S0-G5` complete in the itemized work plan.

## v1.2.3 on 4th of March, 2026
- Added `control-plane/uv.lock` and validated deterministic environment creation with `uv sync --frozen`.
- Switched Docker dependency install path to lockfile-based `uv sync --frozen --no-dev`.
- Added runtime dependency `dagster-webserver` and switched compose command from `dagit` to `dagster-webserver`.
- Added `control-plane/.dockerignore` to prevent large/dirty host context from entering image builds.
- Added `uv` workflow instructions to `control-plane/README.md`.
- Marked Slice 0 guardrail `S0-G4` complete in the itemized work plan.

## v1.2.2 on 4th of March, 2026
- Retroactive Slice 0 closeout record (`slice-0-bootstrap`) added in monorepo `spec/`.
- `tdw-control-plane` imported to `control-plane` and major TDW -> Origo renaming applied.
- Control-plane metadata/config updated (`pyproject`, Docker image names, Dagster code location, README).
- Enforced fail-fast ClickHouse secret wiring in compose (`CLICKHOUSE_PASSWORD` is now required).
- Fixed `create_binance_trades_table_origo` table settings for ClickHouse compatibility.
- Fixed UTC conversion bug in `daily_trades_to_origo` timestamp handling.
- Added required runtime dependencies: `clickhouse-connect`, `pyarrow`, `polars`.
- Imported client module from Limen into `origo/data` (excluding `standard_bars.py`) and remapped imports.
- Verified deterministic fixture replay for Binance daily files (`2017-08-17` to `2017-08-19`) with matching checksums/fingerprints across two runs.
