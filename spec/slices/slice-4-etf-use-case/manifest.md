## What was done
- Implemented `S4-C1` issuer adapters for:
  - `ISharesIBITAdapter`
  - `InvescoBTCOAdapter`
  - `BitwiseBITBAdapter`
- Implemented strict source-specific parsing and mapped outputs to daily ETF payload with canonical keys:
  - `issuer`, `ticker`, `as_of_date`, `btc_units`, `btc_market_value_usd`, `total_net_assets_usd`, and related metrics.
- Added UTC-daily normalization path by deriving `observed_at_utc` from source `as_of_date`.
- Added capability proof harness `origo/scraper/etf_s4_01_proof.py` and generated:
  - `spec/slices/slice-4-etf-use-case/acceptance-proof-s4-01.json`
- Added rights-matrix source states for new ETF issuer keys (`Ingest Only`) so scraper rights checks are fail-closed but resolvable.

## Current state
- Slice 4 capability stage is complete; `S4-C1..S4-C8` are complete.
- Slice 4 proof stage is complete; `S4-P1..S4-P4` are complete.
- Slice 4 guardrail stage is complete; `S4-G1..S4-G9` are complete.
- Ten live issuer adapters can discover, fetch, parse, and normalize one daily record set each.
- Capability proof confirms normalized UTC-day output and deterministic fingerprints for one run:
  - iShares IBIT as-of `2026-03-03`
  - Invesco BTCO as-of `2026-03-03`
  - Bitwise BITB as-of `2026-03-04`
- ETF canonical daily metrics are now loaded to ClickHouse table:
  - `origo.etf_daily_metrics_long`
  - batch routing is strict (`etf_*_daily` only for canonical insert path)
- iShares (`IBIT`) history backfill is now materialized in canonical storage with broad daily coverage:
  - distinct as-of days: `542`
  - earliest: `2024-01-05`
  - latest: `2026-03-04`
- External serving/export paths beyond raw query are not yet extended for ETF data in this slice step.
- ETF native raw query remains implemented in `/v1/raw/query` for dataset `etf_daily_metrics`, but serving is now rights-gated fail-closed (`Ingest Only`).
- Dagster now has ETF daily schedule wiring:
  - `origo_etf_daily_ingest_schedule` -> `origo_etf_daily_ingest_job`
  - schedule settings are bound from env (`ORIGO_ETF_DAILY_SCHEDULE_CRON`, `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`).
- Dagster now has ETF run-failure retry sensor wiring:
  - `origo_etf_daily_retry_sensor` monitors `origo_etf_daily_ingest_job`
  - retries are gated to a 24h window from failed-run creation time.
- Slice baseline fixture artifact now exists for proof window replay handoff:
  - `spec/slices/slice-4-etf-use-case/baseline-fixture-2026-03-04_2026-03-06.json`

## Watch out
- Bitwise source URI depends on the current Next.js `buildId`; adapter resolves it dynamically from homepage on every source discovery.
- Invesco JSON endpoint currently responds with `Content-Type: text/plain`; adapter force-tags artifact format as JSON and fails loudly on parse mismatch.
- iShares CSV uses preamble metadata rows before holdings table; parser depends on finding:
  - `Fund Holdings as of`
  - holdings header starting with `Ticker,`
- Source-origin contract is now strict fail-closed: issuer adapters reject non-HTTPS or non-issuer-host redirects/final URLs.
- Issuer #10 decision is intentionally pinned to Hashdex `DEFI` (decision date: 2026-03-05) for Phase-1 execution; WisdomTree `BTCW` is deferred until an official stable path is validated.
- Rights states are currently `Ingest Only` for ETF issuers; query serving is now blocked by rights-state enforcement until policy changes.
- ETF canonical load requires SQL migrations to be applied before ingestion (`0002__create_etf_daily_metrics_long.sql`).
- iShares historical CSV may encode optional numeric fields as dash sentinels; parser now treats dash tokens as null only for optional numeric fields.
- Non-iShares issuers are currently snapshot-style from official endpoint contracts; full history remains constrained by source-exposed history access.
- `S4-G2` now requires explicit `source_ids` classification for scraper ingestion rights checks; new issuer source IDs must be added to matrix before pipeline runs.
- `S4-G3` schedule wiring is env-driven and fail-loud; missing schedule env values prevent definitions load by design.
- `S4-G3` scheduled ETF job requires scraper runtime env and browser runtime dependencies in the Dagster execution environment.
- `S4-G4` retry behavior depends on Dagster run-failure sensor processing in daemon runtime; no retries occur if sensor processing is not running.
- `S4-G5` query serving promotion gate is env-driven and fail-loud; `ORIGO_ETF_QUERY_SERVING_STATE` must be explicitly set (`shadow|promoted`) for ETF rights resolution paths.
- `S4-G6` warning evaluation path requires `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS` and ClickHouse availability; warning evaluation failures are surfaced as query runtime errors by design.
- `S4-G7` alerting path requires valid Discord webhook credentials and writable anomaly log path; webhook failures fail loudly by design.

## S4-C2 update
- Implemented `S4-C2` issuer adapters for:
  - `Ark21SharesARKBAdapter`
  - `VanEckHODLAdapter`
  - `FranklinEZBCAdapter`
  - `GrayscaleGBTCAdapter`
- Added `build_s4_02_issuer_adapters()` to run first seven issuers as one capability set.
- Added capability proof harness `origo/scraper/etf_s4_02_proof.py`.
- Updated rights matrix with additional ETF issuer source keys (`Ingest Only`):
  - `ark`
  - `vaneck`
  - `franklin`
  - `grayscale`

## S4-C3 update
- Implemented `S4-C3` issuer adapters for:
  - `FidelityFBTCAdapter`
  - `CoinSharesBRRRAdapter`
  - `HashdexDEFIAdapter`
- Added `build_s4_03_issuer_adapters()` to run all ten issuers as one capability set.
- Added capability proof harness `origo/scraper/etf_s4_03_proof.py`.
- Enforced strict issuer-origin checks in ETF adapter path:
  - HTTPS-only source URIs
  - issuer-host suffix allowlists
  - final/redirect URL validation on fetch artifacts
- Updated rights matrix with additional ETF issuer source keys (`Ingest Only`):
  - `fidelity`
  - `coinshares`
  - `hashdex`

## S4-C4 update
- Added strict ETF daily schema mapping contract in adapter path:
  - required fields are enforced before parsed record materialization
  - non-schema fields fail loudly
  - normalized metric names are validated against ETF schema
- Added metric-unit mapping to normalized ETF output:
  - `BTC`, `USD`, `PCT`, `COUNT`
- Reworked Hashdex DEFI parsing logic to tolerate issuer payload ordering/shape drift while preserving strict source-origin constraints.

## S4-C5 update
- Added strict UTC daily semantic enforcement on normalized ETF records:
  - one and only one `as_of_date` metric per source payload
  - all `observed_at_utc` timestamps must be UTC midnight
  - observed day must equal `as_of_date` for every metric row
- Re-ran ten-issuer capability proof after semantic enforcement with updated acceptance artifact.

## S4-C6 update
- Added versioned SQL migration for canonical ETF long-metric table:
  - `control-plane/migrations/sql/0002__create_etf_daily_metrics_long.sql`
- Updated ClickHouse persistence behavior:
  - ETF batches (`source_id` pattern `etf_*_daily`) route to `etf_daily_metrics_long`
  - mixed ETF/non-ETF insert batches fail loudly
  - ETF batch insert requires UTC-midnight `observed_at_utc` on every metric row
- Added and executed capability proof harness:
  - `origo/scraper/etf_s4_06_load_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/capability-proof-s4-c6-canonical-load.json`
  - result: inserted row count and database row count match (`116`).

## S4-C7 update
- Added full-history backfill runner:
  - `origo/scraper/etf_s4_07_backfill.py`
- Implemented iShares date-parameterized history ingestion from official source endpoint with:
  - query-date loop (`asOfDate=YYYYMMDD`)
  - weekday filtering
  - per-source `as_of_date` dedupe against canonical table
  - fetch retry/backoff
  - explicit no-data day recognition
- Fixed iShares historical parsing blocker for optional numeric dash sentinel values in:
  - `origo/scraper/etf_adapters.py`
- Executed backfill proof run and generated:
  - `spec/slices/slice-4-etf-use-case/capability-proof-s4-c7-backfill.json`
- Proof run result highlights:
  - iShares inserted days in run: `287`
  - iShares canonical coverage after run: `542` days (`2024-01-05` .. `2026-03-04`)
  - other issuers remained snapshot-backed in this run because current official endpoint contracts are single-day.

## S4-C8 update
- Added ETF native query capability in:
  - `origo/query/etf_native.py`
- Extended native query core to support non-Binance temporal columns through:
  - `datetime_column`
  - `random_seed_column`
- Added dataset-agnostic native dispatch in:
  - `origo/data/_internal/generic_endpoints.py`
- Wired API request path (`/v1/raw/query`) to unified native dispatch and expanded query dataset contract with:
  - `etf_daily_metrics`
- Added and executed capability proof runner:
  - `origo/query/etf_s4_08_query_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/capability-proof-s4-c8-native-query.json`
- Verified API-level behavior for ETF native query:
  - `POST /v1/raw/query` returns `200` for ETF dataset requests with expected schema and rows.

## S4-P1..S4-P4 update
- Added proof-suite runner for parity, threshold, replay determinism, and provenance verification:
  - `origo/scraper/etf_s4_05_proof_suite.py`
- Executed and captured proof-suite artifact:
  - `spec/slices/slice-4-etf-use-case/proof-s4-p1-p4.json`
- Proof outcomes:
  - `S4-P1`: 10-issuer parity proof executed against official issuer sources.
  - `S4-P2`: mandatory-metric parity `100.0%` (`70/70`) and threshold pass (`>=99.5%`).
  - `S4-P3`: replay determinism passed with identical run1/run2 inserted-row fingerprints.
  - `S4-P4`: provenance references valid for all run1 proof-window rows (`rows_checked=116`, no missing/invalid/mismatch rows).
- Proof-window daily coverage observed in run:
  - `2026-03-04`
  - `2026-03-05`
  - `2026-03-06`

## S4-G1 update
- Added legal sign-off gate for query serving path:
  - `api/origo_api/rights.py`
  - `api/origo_api/main.py`
- Unified legal-signoff enforcement between query and export rights checks.
- Added and executed guardrail proof harness:
  - `api/origo_api/s4_g1_legal_signoff_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g1.json`
- Guardrail proof confirms fail-loud behavior on missing sign-off artifact and pass behavior with valid sign-off artifact for both query and export paths.

## S4-G2 update
- Added fail-closed rights-state enforcement in API query path:
  - `api/origo_api/rights.py`
    - added `resolve_query_rights(dataset, auth_token)` for full rights resolution
    - query path now blocks:
      - missing classification (`QUERY_RIGHTS_MISSING_STATE`)
      - ingest-only serving (`QUERY_RIGHTS_INGEST_ONLY`)
      - missing BYOK token (`QUERY_RIGHTS_BYOK_REQUIRED`)
    - hosted-source legal-signoff enforcement remains active.
  - `api/origo_api/main.py`
    - `/v1/raw/query` now enforces `resolve_query_rights(...)` before query execution.
- Added ingestion-path source-id rights enforcement:
  - `origo/scraper/rights.py`
    - `resolve_scraper_rights(source_key, source_id)` now requires explicit `source_ids` classification
    - emits `SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED` on mismatch/missing source-id coverage
  - `origo/scraper/pipeline.py`
    - passes `source_id` into rights resolver and includes it in rights audit payload.
- Updated rights matrix contract for explicit ETF rights mapping:
  - `contracts/source-rights-matrix.json`
    - ETF issuer entries now include dataset mapping `etf_daily_metrics`
    - ETF issuer entries now include explicit `source_ids`.
- Added and executed guardrail proof harness:
  - `api/origo_api/s4_g2_rights_enforcement_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g2.json`
- Guardrail proof confirms expected fail-loud behavior for API rights and ingestion source-id classification checks.

## S4-G3 update
- Added a dedicated ETF daily ingestion Dagster job:
  - `control-plane/origo_control_plane/jobs/etf_daily_ingest.py`
  - job: `origo_etf_daily_ingest_job`
  - op: `origo_etf_daily_ingest_step`
  - execution loops all ten issuer adapters via scraper pipeline and emits deterministic run metadata.
- Added ETF daily schedule registration in control-plane definitions:
  - `control-plane/origo_control_plane/definitions.py`
  - schedule: `origo_etf_daily_ingest_schedule`
  - target job: `origo_etf_daily_ingest_job`
  - schedule settings are env-driven (`ORIGO_ETF_DAILY_SCHEDULE_CRON`, `ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE`) and fail loud when missing.
- Updated jobs package export:
  - `control-plane/origo_control_plane/jobs/__init__.py`
- Updated root env contract:
  - `.env.example` now includes required ETF schedule env vars.
- Added and executed schedule proof harness:
  - `control-plane/origo_control_plane/etf_daily_schedule_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g3.json`
- Guardrail proof confirms missing-env fail-loud behavior and correct schedule-to-job wiring.

## S4-G4 update
- Added ETF retry window decision logic and run-failure sensor:
  - `control-plane/origo_control_plane/definitions.py`
  - sensor: `origo_etf_daily_retry_sensor`
  - monitored job: `origo_etf_daily_ingest_job`
- Retry behavior is now explicit:
  - retries are allowed only within 24 hours from failed-run creation timestamp
  - retries stop when `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS` is reached
  - retry runs carry deterministic lineage tags (`attempt`, `origin`, `parent`, `window_end`).
- Updated env contract for retry policy controls:
  - `ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS`
  - `ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS`
  - documented in root `.env.example`.
- Added and executed retry proof harness:
  - `control-plane/origo_control_plane/etf_daily_retry_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g4.json`
- Guardrail proof confirms:
  - fail-loud missing-env behavior,
  - in-window retry run request generation,
  - explicit skip behavior outside window and at max attempts,
  - chained retry run-key behavior by origin run.

## S4-G5 update
- Added explicit ETF query serving promotion gate in:
  - `api/origo_api/rights.py`
- Gate contract for `dataset=etf_daily_metrics` is now strict:
  - required env var: `ORIGO_ETF_QUERY_SERVING_STATE`
  - allowed values: `shadow`, `promoted`
  - fail-loud behavior:
    - missing/empty env value raises runtime contract error
    - invalid env value raises runtime contract error
    - `shadow` blocks rights resolution with `QUERY_SERVING_SHADOW_MODE`
    - `promoted` allows rights resolution to continue (with existing rights/legal checks still enforced)
- Updated env contract:
  - `.env.example` now includes `ORIGO_ETF_QUERY_SERVING_STATE`
- Added and executed guardrail proof harness:
  - `api/origo_api/s4_g5_shadow_promote_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g5.json`
- Guardrail proof confirms missing-env fail-loud behavior, shadow block behavior, promoted pass behavior, and invalid-value fail-loud behavior.

## S4-G6 update
- Added ETF stale/missing/incomplete warning engine:
  - `api/origo_api/etf_warnings.py`
  - warning codes:
    - `ETF_DAILY_STALE_RECORDS`
    - `ETF_DAILY_MISSING_RECORDS`
    - `ETF_DAILY_INCOMPLETE_RECORDS`
- Integrated warning generation into API query path:
  - `api/origo_api/main.py`
  - warning checks are applied for `dataset=etf_daily_metrics`
  - warning computation errors fail loudly with explicit API error codes:
    - `QUERY_WARNING_RUNTIME_ERROR`
    - `QUERY_WARNING_BACKEND_ERROR`
    - `QUERY_WARNING_UNKNOWN_ERROR`
- Added stale-threshold env contract:
  - required env var: `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - documented in root `.env.example`.
- Added and executed guardrail proof harness:
  - `api/origo_api/s4_g6_etf_warning_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g6.json`
- Guardrail proof confirms deterministic warning behavior for:
  - missing-only snapshot
  - degraded snapshot (stale + missing + incomplete)
  - clean latest-day snapshot

## S4-G7 update
- Added ETF ingestion anomaly alert sensor and emit path in:
  - `control-plane/origo_control_plane/definitions.py`
  - sensor: `origo_etf_ingest_anomaly_alert_sensor`
- Added strict anomaly output channels:
  - local anomaly log append (`JSONL`)
  - Discord webhook alert post
- New alerting env contract (fail-loud when missing/invalid):
  - `ORIGO_ETF_ANOMALY_LOG_PATH`
  - `ORIGO_ETF_DISCORD_WEBHOOK_URL`
  - `ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS`
  - `ORIGO_ETF_DISCORD_TIMEOUT_SECONDS`
- Added and executed guardrail proof harness:
  - `control-plane/origo_control_plane/etf_daily_alerts_proof.py`
  - artifact: `spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g7.json`
- Guardrail proof confirms:
  - missing webhook env fail-loud behavior,
  - sensor registration with env-bound interval,
  - anomaly log write behavior,
  - Discord alert emission behavior.

## S4-G8 update
- Added Slice 4 developer documentation set:
  - `docs/Developer/s4-etf-adapters.md`
  - `docs/Developer/s4-etf-normalization.md`
  - `docs/Developer/s4-etf-proof-workflow.md`
  - `docs/Developer/s4-etf-guardrails-runbook.md`
- Developer docs now cover:
  - issuer adapter contracts and source-origin constraints
  - ETF normalization/canonical-load contract
  - parity/replay proof workflow
  - guardrail operations and env contract for rights/schedule/retry/serving/warnings/alerts

## S4-G9 update
- Added ETF user reference:
  - `docs/etf-reference.md`
- Updated user-facing references and taxonomy:
  - `docs/data-taxonomy.md`
  - `docs/raw-query-reference.md`
  - `docs/scraper-reference.md`
- User docs now include:
  - ETF source coverage and dataset field taxonomy
  - units/cadence/freshness semantics
  - warning/error semantics including ETF quality warnings
  - rights and serving-promotion gate behavior
- Added Slice 4 baseline fixture artifact for deterministic replay handoff:
  - `spec/slices/slice-4-etf-use-case/baseline-fixture-2026-03-04_2026-03-06.json`
