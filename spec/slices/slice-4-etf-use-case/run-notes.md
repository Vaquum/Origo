# Slice 4 ETF Use Case Run Notes

- Date (UTC): 2026-03-05
- Scope: `S4-C1` (issuer adapters 1-3)
- Issuers implemented:
  - iShares (`IBIT`)
  - Invesco (`BTCO`)
  - Bitwise (`BITB`)
- Proof command:
  - `uv run --with clickhouse-connect --with polars --with pyarrow python -m origo.scraper.etf_s4_01_proof`
- Proof artifact:
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-01.json`

## System changes made as a side effect of proof run

- External issuer endpoints were called live over HTTPS.
- Bitwise adapter dynamically resolved current Next.js `buildId` from homepage before fetching JSON data endpoint.
- No ClickHouse writes, object-store writes, or Dagster jobs were executed in this capability proof.

## Known warnings

- This run validates capability only for `S4-C1`; full Slice 4 proof targets (`S4-P1..S4-P4`) are not complete yet.
- iShares CSV includes preamble rows before holdings table; parser now explicitly locates the holdings header and as-of preamble field.
- Invesco endpoint returns `text/plain` despite JSON payload; adapter force-tags artifact format to JSON and parses content strictly.

## Deferred guardrails

- `S4-G1..S4-G9` are deferred to guardrail phase.
- Legal sign-off gating for ETF external serving/export is not part of this capability step.

## Completion confirmation

- `S4-C1` checked in `spec/2-itemized-work-plan.md`.
- New adapter capability modules added:
  - `origo/scraper/etf_adapters.py`
  - `origo/scraper/etf_s4_01_proof.py`
- Rights matrix updated for new ETF issuer source keys (`Ingest Only`):
  - `contracts/source-rights-matrix.json`

## S4-C2 run notes

- Date (UTC): 2026-03-05
- Scope: `S4-C2` (issuer adapters 4-7)
- Issuers implemented:
  - ARK 21Shares (`ARKB`)
  - VanEck (`HODL`)
  - Franklin (`EZBC`)
  - Grayscale (`GBTC`)
- Proof command:
  - `uv run --with playwright --with clickhouse-connect --with polars --with pyarrow python -m origo.scraper.etf_s4_02_proof`
- Proof artifact:
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-02.json`

### Notes

- Franklin EZBC adapter captures browser-session PDS API responses (required operations only) and stores the captured JSON payload as the artifact.
- Grayscale GBTC adapter uses browser fetch and parses escaped Next.js payload fields for daily metrics.

## S4-C3 run notes

- Date (UTC): 2026-03-05
- Scope: `S4-C3` (issuer adapters 8-10)
- Issuers implemented:
  - Fidelity (`FBTC`)
  - CoinShares (`BRRR`)
  - Hashdex (`DEFI`)
- Proof command:
  - `uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_03_proof`
- Proof artifact:
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-03.json`

### Notes

- Source-origin contract is now enforced in adapter runtime:
  - only HTTPS issuer-origin hosts are accepted
  - redirect/final URL host is validated
  - non-issuer-origin response capture fails hard
- Fidelity and CoinShares use issuer pages rendered via Playwright Firefox and are parsed from deterministic line extraction.
- Hashdex DEFI uses issuer `_payload.json` and extracts DEFI holdings metrics via strict payload signature matching.
- Decision note (2026-03-05): issuer slot #10 is explicitly pinned to Hashdex `DEFI` for this phase.
  - Context: WisdomTree `BTCW` official path was not reliably usable in this environment during S4-C3 validation.
  - Follow-up: WisdomTree can be revisited in a later slice when a stable official endpoint/path is confirmed.

## S4-C4 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-C4` (normalized ETF daily metric schema mapping)
- Capability changes:
  - Added strict ETF daily schema contract (required/allowed field sets) for parsed payloads.
  - Enforced fail-loud behavior on unknown/missing schema fields before record materialization.
  - Added ETF metric-unit mapping (`BTC`, `USD`, `PCT`, `COUNT`) on normalized long-metric output.
  - Hardened Hashdex parser against issuer payload ordering/shape drift while keeping source-origin constraints.
- Validation command:
  - `uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_03_proof`
- Validation artifact (updated):
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-03.json`

## S4-C5 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-C5` (UTC daily semantics normalization)
- Capability changes:
  - Added strict UTC-day semantic validation after ETF normalization:
    - exactly one `as_of_date` metric per source payload
    - every normalized metric row must have `observed_at_utc` at `00:00:00+00:00`
    - every normalized metric row observed day must equal `as_of_date`
- Validation command:
  - `uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_03_proof`
- Validation artifact (updated):
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-03.json`

## S4-C6 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-C6` (canonical daily load into ClickHouse long-metric table)
- Capability changes:
  - Added SQL migration:
    - `control-plane/migrations/sql/0002__create_etf_daily_metrics_long.sql`
  - Updated ClickHouse persistence routing:
    - ETF daily batches (`source_id` matches `etf_*_daily`) now load into canonical table `etf_daily_metrics_long`
    - mixed ETF and non-ETF batch inserts fail loudly
    - ETF canonical loads require UTC-midnight `observed_at_utc` on every metric row
  - Added capability proof harness:
    - `origo/scraper/etf_s4_06_load_proof.py`
- Validation commands:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run python -m origo_control_plane.migrations migrate` (from `control-plane/`)
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_06_load_proof`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c6-canonical-load.json`
- Validation result:
  - canonical table rows inserted in proof run: `116`
  - database count for same run window/source set: `116`
  - deterministic row-count match flag: `true`
- Environment contract check:
  - no new environment variables introduced in `S4-C6`
  - no deployment-specific runtime values were hard-coded in the new load path

## S4-C7 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-C7` (full available-history backfill per issuer)
- Capability changes:
  - Added backfill runner:
    - `origo/scraper/etf_s4_07_backfill.py`
  - Implemented iShares date-parameterized backfill loop (`asOfDate=YYYYMMDD`) over a bounded window with weekday filtering.
  - Implemented per-source dedupe against existing canonical `as_of_date` rows before insert.
  - Added retry/backoff around fetch calls in backfill execution (`max_attempts=5`, `initial_backoff_seconds=0.5`, multiplier `2.0`).
  - Added iShares no-data day detection for holdings CSV preamble sentinel and known no-data parse outcomes.
  - Fixed optional numeric parsing for iShares historical rows where values are emitted as dash sentinels:
    - `_parse_optional_float` now maps `-`, `--`, and dash variants to null for optional numeric fields.
- Validation command:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_07_backfill --ishares-start-date 2024-01-01 --end-date 2026-03-06`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c7-backfill.json`
- Validation result:
  - iShares request window:
    - calendar days requested: `796`
    - weekdays requested: `570`
    - inserted days: `287`
    - skipped existing days: `255`
    - skipped no-data days: `28`
  - Canonical coverage after run:
    - `etf_ishares_ibit_daily`: `542` distinct `as_of_date` values (`2024-01-05` .. `2026-03-04`)
    - all other issuers: one current snapshot day each (existing row skipped in this run)
- System changes made as a side effect of proof run:
  - Live official issuer endpoints were called over HTTPS.
  - New normalized ETF backfill rows were inserted into `origo.etf_daily_metrics_long` for iShares.
- Known warnings:
  - For non-iShares issuers, official tested source contracts remain snapshot-style; full historical pull is not available from current endpoint shapes in this capability step.
- Environment contract check:
  - no new environment variables introduced in `S4-C7`
  - no deployment-specific runtime values were hard-coded in the new backfill path
- Completion confirmation:
  - `S4-C7` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.17`
  - `control-plane/CHANGELOG.md` updated with `v1.2.17` entry
  - `.env.example` reviewed; no `S4-C7` env additions or changes required

## S4-C8 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-C8` (expose ETF fields through native raw query mode)
- Capability changes:
  - Added ETF native query planner:
    - `origo/query/etf_native.py`
    - dataset key: `etf_daily_metrics`
    - table binding: `etf_daily_metrics_long`
    - time column: `observed_at_utc`
  - Extended native core query spec for dataset-specific temporal semantics:
    - `origo/query/native_core.py`
    - new `NativeQuerySpec` fields:
      - `datetime_column`
      - `random_seed_column`
  - Added dataset-agnostic native query dispatch:
    - `origo/data/_internal/generic_endpoints.py`
    - new functions:
      - `query_native`
      - `query_native_wide_rows_envelope`
  - Updated API query route wiring:
    - `api/origo_api/main.py` now delegates `/v1/raw/query` to unified native dispatch
    - `api/origo_api/schemas.py` now includes `etf_daily_metrics` in `RawQueryRequest.dataset`
  - Added capability proof runner:
    - `origo/query/etf_s4_08_query_proof.py`
- Validation commands:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with clickhouse-connect --with polars --with pyarrow python -m origo.query.etf_s4_08_query_proof`
  - `ORIGO_INTERNAL_API_KEY=test-key ORIGO_QUERY_MAX_CONCURRENCY=4 ORIGO_QUERY_MAX_QUEUE=16 ORIGO_EXPORT_MAX_CONCURRENCY=2 ORIGO_EXPORT_MAX_QUEUE=16 ORIGO_SOURCE_RIGHTS_MATRIX_PATH=contracts/source-rights-matrix.json ORIGO_EXPORT_AUDIT_LOG_PATH=/tmp/origo-export-audit.log CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with fastapi --with httpx --with clickhouse-connect --with polars --with pyarrow python - <<'PY' ...`
- Validation artifacts/results:
  - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c8-native-query.json`
  - Internal proof query summary:
    - historical time-range query row count: `22`
    - latest rows query row count: `50`
  - HTTP adapter proof (`POST /v1/raw/query`):
    - status code: `200`
    - dataset: `etf_daily_metrics`
    - schema includes `source_id`, `metric_name`, `metric_value_float`, `observed_at_utc`
- System changes made as a side effect of proof run:
  - Live queries executed against local ClickHouse `origo.etf_daily_metrics_long`.
  - No schema/migration changes or side-effect writes were performed by `S4-C8` proofs.
- Known warnings:
  - Export path remains Binance-only in this step; ETF query capability is limited to `/v1/raw/query` native mode.
- Environment contract check:
  - no new environment variables introduced in `S4-C8`
  - no deployment-specific runtime values were hard-coded in query planner/router changes
- Completion confirmation:
  - `S4-C8` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.18`
  - `control-plane/CHANGELOG.md` updated with `v1.2.18` entry
  - `.env.example` reviewed; no `S4-C8` env additions or changes required

## S4-P1..S4-P4 run notes

- Date (UTC): 2026-03-06
- Scope:
  - `S4-P1` 10-issuer daily parity proof window against official issuer sources
  - `S4-P2` parity threshold enforcement (`>=99.5%` on mandatory metrics)
  - `S4-P3` replay determinism proof on fixed proof-window artifacts
  - `S4-P4` raw artifact/provenance reference verification
- Proof runner:
  - `origo/scraper/etf_s4_05_proof_suite.py`
- Validation command:
  - `CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo uv run --with clickhouse-connect --with playwright --with polars --with pyarrow python -m origo.scraper.etf_s4_05_proof_suite`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/proof-s4-p1-p4.json`
- Validation results:
  - `S4-P1`: parity executed for all 10 issuers with proof-window as-of days:
    - `2026-03-04`, `2026-03-05`, `2026-03-06`
  - `S4-P2`:
    - mandatory metrics matched: `70/70`
    - parity: `100.0%`
    - threshold status: `passed` (`100.0 >= 99.5`)
  - `S4-P3`:
    - run1 inserted rows: `116`
    - run2 inserted rows: `116`
    - run1 fingerprint: `a20e5ee942097a08278366c6205b5bc42aa88ee83a1ac951f66b6de46be45a0f`
    - run2 fingerprint: `a20e5ee942097a08278366c6205b5bc42aa88ee83a1ac951f66b6de46be45a0f`
    - deterministic match: `true`
  - `S4-P4`:
    - rows checked: `116`
    - missing provenance rows: `0`
    - invalid provenance rows: `0`
    - artifact-reference mismatches: `0`
    - all references valid: `true`
- System changes made as a side effect of proof run:
  - Live issuer endpoints were called during run1 fetch.
  - Canonical ETF table `origo.etf_daily_metrics_long` received:
    - run1 inserts: `116` rows
    - run2 replay inserts: `116` rows
- Known warnings:
  - Proof-window dates are source-driven snapshot dates (issuer current publish days), not a synthetic unified calendar window.
  - Non-iShares issuers remain snapshot-style at source contract level; full history for those issuers remains constrained by issuer-exposed endpoint shape.
- Environment contract check:
  - no new environment variables introduced in `S4-P1..S4-P4`
  - no deployment-specific runtime values were hard-coded in proof-runner paths
- Completion confirmation:
  - `S4-P1..S4-P4` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.19`
  - `control-plane/CHANGELOG.md` updated with `v1.2.19` entry
  - `.env.example` reviewed; no `S4-P1..S4-P4` env additions or changes required

## S4-G1 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G1` (enforce legal sign-off artifact requirement before external serving/export)
- Guardrail changes:
  - Added query-path legal sign-off enforcement in:
    - `api/origo_api/rights.py`
      - query legal-signoff gate for hosted sources (later integrated into `resolve_query_rights(...)` in `S4-G2`)
  - Refactored legal-signoff requirement to shared helper used by both query and export rights enforcement.
  - Wired `/v1/raw/query` to execute legal-signoff check before query execution:
    - `api/origo_api/main.py`
  - Added guardrail proof harness:
    - `api/origo_api/s4_g1_legal_signoff_proof.py`
- Validation command:
  - `uv run --with pydantic python -m api.origo_api.s4_g1_legal_signoff_proof`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g1.json`
- Validation results:
  - query path missing sign-off case:
    - error code: `QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING`
  - export path missing sign-off case:
    - error code: `EXPORT_RIGHTS_LEGAL_SIGNOFF_MISSING`
  - valid sign-off cases:
    - query: `passed`
    - export: `passed`
- System changes made as a side effect of proof run:
  - None. Proof harness uses temporary local matrix/artifact files only.
- Known warnings:
  - `S4-G1` enforces legal-signoff presence for hosted serving paths; full rights-state enforcement for ingestion/query API paths remains `S4-G2`.
- Environment contract check:
  - no new environment variables introduced in `S4-G1`
  - no deployment-specific runtime values were hard-coded in legal-signoff enforcement paths
- Completion confirmation:
  - `S4-G1` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.20`
  - `control-plane/CHANGELOG.md` updated with `v1.2.20` entry
  - `.env.example` reviewed; no `S4-G1` env additions or changes required

## S4-G2 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G2` (enforce rights matrix checks in ingestion and API paths)
- Guardrail changes:
  - Added full query rights-state resolution and fail-closed checks:
    - `api/origo_api/rights.py`
      - `resolve_query_rights(dataset, auth_token)`
      - query now errors on:
        - missing dataset classification (`QUERY_RIGHTS_MISSING_STATE`)
        - ingest-only serving (`QUERY_RIGHTS_INGEST_ONLY`)
        - missing BYOK token when required (`QUERY_RIGHTS_BYOK_REQUIRED`)
      - hosted-source legal sign-off gate remains enforced (`QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING`)
  - Updated query API route to enforce rights matrix checks before query execution:
    - `api/origo_api/main.py`
  - Enforced ingestion source-id classification contract in scraper rights resolution:
    - `origo/scraper/rights.py`
      - `resolve_scraper_rights(source_key, source_id)` now requires explicit `source_ids` match
      - unclassified source-id now fails with `SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED`
    - `origo/scraper/pipeline.py` now passes and audits `source_id` in rights resolution events
  - Updated rights matrix contract:
    - `contracts/source-rights-matrix.json`
      - ETF issuer sources now map dataset `etf_daily_metrics`
      - ETF issuer sources now declare explicit `source_ids`
      - matrix version bumped to `2026-03-06-s4-g2`
  - Added guardrail proof harness:
    - `api/origo_api/s4_g2_rights_enforcement_proof.py`
- Validation command:
  - `uv run --with pydantic --with fastapi --with httpx --with clickhouse-connect --with polars --with pyarrow --with playwright --with bs4 --with lxml python -m api.origo_api.s4_g2_rights_enforcement_proof`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g2.json`
- Validation results:
  - query ingest-only scenario:
    - error code: `QUERY_RIGHTS_INGEST_ONLY`
  - API ingest-only scenario (`POST /v1/raw/query`):
    - HTTP status: `409`
    - error code: `QUERY_RIGHTS_INGEST_ONLY`
  - query BYOK scenario:
    - error code: `QUERY_RIGHTS_BYOK_REQUIRED`
  - query missing-state scenario:
    - error code: `QUERY_RIGHTS_MISSING_STATE`
  - export BYOK scenario:
    - error code: `EXPORT_RIGHTS_BYOK_REQUIRED`
  - hosted-path pass checks:
    - query: `passed`
    - export: `passed`
  - ingestion source-id checks:
    - valid source-id classification: `passed`
    - invalid source-id classification: `SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED`
    - empty `source_ids` contract: `SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED`
- System changes made as a side effect of proof run:
  - None. Proof harness uses temporary local matrix/artifact files only.
- Known warnings:
  - Query serving for ETF dataset is now intentionally blocked by rights state (`Ingest Only`) until rights policy changes.
- Environment contract check:
  - no new environment variables introduced in `S4-G2`
  - no deployment-specific runtime values were hard-coded in rights checks
- Completion confirmation:
  - `S4-G2` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.21`
  - `control-plane/CHANGELOG.md` updated with `v1.2.21` entry
  - `.env.example` reviewed; no `S4-G2` env additions or changes required

## S4-G3 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G3` (add daily run schedule)
- Guardrail changes:
  - Added Dagster ETF daily ingest job:
    - `control-plane/origo_control_plane/jobs/etf_daily_ingest.py`
    - job: `origo_etf_daily_ingest_job`
    - op: `origo_etf_daily_ingest_step`
    - execution path runs all 10 ETF issuer adapters via `run_scraper_pipeline(...)`
  - Added ETF daily schedule wiring in Dagster definitions:
    - `control-plane/origo_control_plane/definitions.py`
    - schedule: `origo_etf_daily_ingest_schedule`
    - schedule target: `origo_etf_daily_ingest_job`
    - schedule config is env-driven and fail-loud:
      - `ORIGO_ETF_DAILY_SCHEDULE_CRON`
      - `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`
  - Updated jobs package exports:
    - `control-plane/origo_control_plane/jobs/__init__.py`
  - Added schedule proof harness:
    - `control-plane/origo_control_plane/etf_daily_schedule_proof.py`
  - Updated env contract:
    - `.env.example` now includes required ETF schedule env vars.
- Validation command:
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_schedule_proof` (from `control-plane/`)
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g3.json`
- Validation results:
  - missing cron env fails loudly:
    - `ORIGO_ETF_DAILY_SCHEDULE_CRON must be set and non-empty`
  - schedule is registered and targets expected job:
    - schedule: `origo_etf_daily_ingest_schedule`
    - job: `origo_etf_daily_ingest_job`
  - env-bound schedule settings resolved in proof run:
    - cron: `17 6 * * *`
    - timezone: `UTC`
- System changes made as a side effect of proof run:
  - None. Proof harness validates definition wiring only and does not execute ingestion.
- Known warnings:
  - ETF daily ingest job runtime requires full scraper runtime env and browser runtime dependencies in the execution environment.
- Environment contract check:
  - introduced and documented new required env vars:
    - `ORIGO_ETF_DAILY_SCHEDULE_CRON`
    - `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`
  - no deployment-specific values were hard-coded in schedule configuration paths
- Completion confirmation:
  - `S4-G3` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.22`
  - `control-plane/CHANGELOG.md` updated with `v1.2.22` entry
  - `.env.example` updated for `S4-G3` schedule env contract

## S4-G4 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G4` (add 24h same-day retry window behavior)
- Guardrail changes:
  - Added ETF retry decision logic + run-failure sensor:
    - `control-plane/origo_control_plane/definitions.py`
    - sensor: `origo_etf_daily_retry_sensor`
    - monitored job: `origo_etf_daily_ingest_job`
  - Retry behavior:
    - retries are eligible only within 24 hours from failed-run creation timestamp
    - retries stop when max retry attempts are reached
    - retry runs carry deterministic retry tags:
      - `origo.etf.retry_attempt`
      - `origo.etf.retry_origin_run_id`
      - `origo.etf.retry_parent_run_id`
      - `origo.etf.retry_window_end_utc`
  - Added retry proof harness:
    - `control-plane/origo_control_plane/etf_daily_retry_proof.py`
  - Updated env contract:
    - `.env.example` now includes required retry env vars.
- Validation command:
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_retry_proof` (from `control-plane/`)
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g4.json`
- Validation results:
  - missing retry max-attempt env fails loudly:
    - `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS must be set and non-empty`
  - retry request generated for in-window failure:
    - run key: `origo-etf-retry-run-1-1`
    - attempt tag: `1`
  - outside-window failures are skipped with explicit reason.
  - max-attempt failures are skipped with explicit reason.
  - chained retry run-key behavior by origin run is enforced:
    - `origo-etf-retry-origin-1-2`
- System changes made as a side effect of proof run:
  - None. Proof harness validates retry decision and sensor wiring without executing ingestion runs.
- Known warnings:
  - Retry execution requires Dagster daemon + run-failure sensor processing to be active.
- Environment contract check:
  - introduced and documented new required env vars:
    - `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS`
    - `ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS`
  - no deployment-specific values were hard-coded in retry policy paths
- Completion confirmation:
  - `S4-G4` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.23`
  - `control-plane/CHANGELOG.md` updated with `v1.2.23` entry
  - `.env.example` updated for `S4-G4` retry env contract

## S4-G5 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G5` (shadow-then-promote gating for serving enablement)
- Guardrail changes:
  - Added ETF query serving-state gating in:
    - `api/origo_api/rights.py`
  - New gate behavior for `dataset=etf_daily_metrics`:
    - required env: `ORIGO_ETF_QUERY_SERVING_STATE`
    - allowed values: `shadow`, `promoted`
    - fail-loud contract:
      - missing/empty env raises runtime contract error
      - invalid env value raises runtime contract error
      - `shadow` blocks rights resolution with `QUERY_SERVING_SHADOW_MODE`
      - `promoted` allows rights resolution to continue
  - Added guardrail proof harness:
    - `api/origo_api/s4_g5_shadow_promote_proof.py`
  - Updated env contract:
    - `.env.example` now includes `ORIGO_ETF_QUERY_SERVING_STATE`.
- Validation command:
  - `uv run --with pydantic python -m api.origo_api.s4_g5_shadow_promote_proof`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g5.json`
- Validation results:
  - missing state env fails loudly:
    - `ORIGO_ETF_QUERY_SERVING_STATE must be set and non-empty`
  - shadow state blocks ETF query serving:
    - error code: `QUERY_SERVING_SHADOW_MODE`
  - promoted state allows ETF query rights:
    - pass: `true`
  - invalid state value fails loudly:
    - env validation error includes allowed values.
- System changes made as a side effect of proof run:
  - None. Proof harness uses temporary local matrix/artifact files only.
- Known warnings:
  - Current production rights matrix keeps ETF sources as `Ingest Only`, so ETF query serving remains blocked by rights even if serving state is `promoted`.
- Environment contract check:
  - introduced and documented new required env var:
    - `ORIGO_ETF_QUERY_SERVING_STATE`
  - no deployment-specific values were hard-coded in serving-state gate paths
- Completion confirmation:
  - `S4-G5` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.24`
  - `control-plane/CHANGELOG.md` updated with `v1.2.24` entry
  - `.env.example` updated for `S4-G5` serving-state env contract

## S4-G6 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G6` (warning generation for stale/missing/incomplete daily records)
- Guardrail changes:
  - Added ETF daily quality warning module:
    - `api/origo_api/etf_warnings.py`
  - Added deterministic warning categories:
    - `ETF_DAILY_STALE_RECORDS`
    - `ETF_DAILY_MISSING_RECORDS`
    - `ETF_DAILY_INCOMPLETE_RECORDS`
  - Added quality checks for latest ETF day:
    - stale-day detection by configurable max age
    - missing-source detection against 10-issuer expected source set
    - incomplete-record detection against mandatory metric set
  - Wired ETF warning generation into `/v1/raw/query`:
    - `api/origo_api/main.py`
    - warnings are appended when `dataset=etf_daily_metrics`
    - warning-evaluation failures fail loudly with:
      - `QUERY_WARNING_RUNTIME_ERROR`
      - `QUERY_WARNING_BACKEND_ERROR`
      - `QUERY_WARNING_UNKNOWN_ERROR`
  - Updated env contract:
    - `.env.example` now includes `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`.
  - Added guardrail proof harness:
    - `api/origo_api/s4_g6_etf_warning_proof.py`
- Validation command:
  - `uv run --with pydantic --with clickhouse-connect --with polars python -m api.origo_api.s4_g6_etf_warning_proof`
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g6.json`
- Validation results:
  - missing snapshot warning set:
    - `ETF_DAILY_MISSING_RECORDS`
  - degraded snapshot warning set:
    - `ETF_DAILY_STALE_RECORDS`
    - `ETF_DAILY_MISSING_RECORDS`
    - `ETF_DAILY_INCOMPLETE_RECORDS`
  - clean snapshot warning set:
    - none
- System changes made as a side effect of proof run:
  - None. Proof harness is deterministic and does not call live endpoints or mutate storage.
- Known warnings:
  - Current rights matrix keeps ETF sources at `Ingest Only`, so ETF query path remains rights-blocked until policy changes; warning logic is in place and validated for when serving is enabled.
- Environment contract check:
  - introduced and documented new required env var:
    - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - no deployment-specific values were hard-coded in ETF warning evaluation paths
- Completion confirmation:
  - `S4-G6` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.25`
  - `control-plane/CHANGELOG.md` updated with `v1.2.25` entry
  - `.env.example` updated for `S4-G6` stale-threshold env contract

## S4-G7 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G7` (logs + Discord alerts for ETF ingestion anomalies)
- Guardrail changes:
  - Added ETF ingestion anomaly alerting in control-plane definitions:
    - `control-plane/origo_control_plane/definitions.py`
  - Added new run-failure sensor:
    - `origo_etf_ingest_anomaly_alert_sensor`
    - monitors failed runs for `origo_etf_daily_ingest_job`
    - skips non-ETF runs with explicit skip reason
  - Added strict anomaly emission path:
    - append anomaly event to JSONL log (`ORIGO_ETF_ANOMALY_LOG_PATH`)
    - POST anomaly alert to Discord webhook (`ORIGO_ETF_DISCORD_WEBHOOK_URL`)
    - non-2xx webhook responses fail loudly
  - Added env-driven alert controls:
    - `ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS`
    - `ORIGO_ETF_DISCORD_TIMEOUT_SECONDS`
  - Added guardrail proof harness:
    - `control-plane/origo_control_plane/etf_daily_alerts_proof.py`
  - Updated env contract:
    - `.env.example` now includes anomaly log + Discord alert env vars.
- Validation command:
  - `PYTHONPATH=.. uv run python -m origo_control_plane.etf_daily_alerts_proof` (from `control-plane/`)
- Validation artifact:
  - `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g7.json`
- Validation results:
  - missing webhook env fails loudly:
    - `ORIGO_ETF_DISCORD_WEBHOOK_URL must be set and non-empty`
  - alert sensor registration:
    - sensor: `origo_etf_ingest_anomaly_alert_sensor`
    - interval: `45` seconds (env-bound in proof run)
  - anomaly emission path:
    - anomaly log lines written: `1`
    - emitted anomaly code: `ETF_DAILY_INGEST_RUN_FAILURE`
    - emitted run id: `run-s4-g7-proof-1`
    - webhook call: `https://discord.example/webhook` (mocked), status `204`
- System changes made as a side effect of proof run:
  - None in production systems. Proof harness used temporary local anomaly log path and mocked webhook calls.
- Known warnings:
  - Dagster emitted a deprecation-style user warning when calling `get_sensor_def` in proof harness; this does not affect sensor registration or runtime behavior in this slice.
- Environment contract check:
  - introduced and documented new required env vars:
    - `ORIGO_ETF_ANOMALY_LOG_PATH`
    - `ORIGO_ETF_DISCORD_WEBHOOK_URL`
    - `ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS`
    - `ORIGO_ETF_DISCORD_TIMEOUT_SECONDS`
  - no deployment-specific values were hard-coded in anomaly alert paths
- Completion confirmation:
  - `S4-G7` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.26`
  - `control-plane/CHANGELOG.md` updated with `v1.2.26` entry
  - `.env.example` updated for `S4-G7` alerting env contract

## S4-G8 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G8` (developer docs closeout for Slice 4)
- Guardrail changes:
  - Added Slice 4 developer doc files:
    - `docs/Developer/s4-etf-adapters.md`
    - `docs/Developer/s4-etf-normalization.md`
    - `docs/Developer/s4-etf-proof-workflow.md`
    - `docs/Developer/s4-etf-guardrails-runbook.md`
  - Coverage added:
    - issuer adapter contracts and official-source constraints
    - normalization + canonical-load schema and failure semantics
    - proof workflow contracts and acceptance thresholds
    - guardrail runbook for rights/legal/schedule/retry/serving gate/warnings/alerts
- Validation:
  - Documentation-only step; no runtime code paths were executed.
- System changes made as a side effect of proof run:
  - None.
- Known warnings:
  - None.
- Environment contract check:
  - no new environment variables introduced in `S4-G8`
  - no deployment-specific runtime values were hard-coded
- Completion confirmation:
  - `S4-G8` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` version bumped to `1.2.27`
  - `control-plane/CHANGELOG.md` updated with `v1.2.27` entry
  - `.env.example` reviewed; no `S4-G8` env additions required

## S4-G9 run notes

- Date (UTC): 2026-03-06
- Scope: `S4-G9` (user docs closeout for Slice 4)
- Guardrail changes:
  - Added ETF user reference:
    - `docs/etf-reference.md`
  - Updated user-facing references:
    - `docs/data-taxonomy.md`
    - `docs/raw-query-reference.md`
    - `docs/scraper-reference.md`
  - Coverage added:
    - ETF dataset taxonomy and source coverage
    - field and unit semantics
    - cadence/time/freshness semantics
    - warning and error taxonomy
    - rights + serving-gate behavior
- Validation:
  - Documentation-only step; no runtime code paths were executed.
- System changes made as a side effect of proof run:
  - None.
- Known warnings:
  - None.
- Environment contract check:
  - no new environment variables introduced in `S4-G9`
  - no deployment-specific runtime values were hard-coded
- Completion confirmation:
  - `S4-G9` checked in `spec/2-itemized-work-plan.md`
  - `control-plane/pyproject.toml` remains `1.2.27` for combined `S4-G8/S4-G9` doc release
  - `control-plane/CHANGELOG.md` includes `v1.2.27` entry for both doc closeout steps
  - baseline fixture artifact added:
    - `spec/slices/slice-4-etf-use-case/baseline-fixture-2026-03-04_2026-03-06.json`
  - `.env.example` reviewed; no `S4-G9` env additions required
