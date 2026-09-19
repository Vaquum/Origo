# v3.17.7

- Revert perp provisional pages to 500 trades: fapi answers limit=1000 on fromId-paged `historicalTrades` with HTTP 400 code -1130, which stalled the entire perp tail from the v3.17.4 deploy until this fix. The 500 cap is now recorded on the adapter so it cannot be re-"optimized". The earlier weight measurement was taken on the keyless path, which validates differently — validity must be proven with the authenticated request shape.

# v3.17.6

- Share canonical decimal parsing on the archive base: the three near-identical `parse_decimal` clones (spot, perp, spot-agg) collapse into one `parse_decimal(text, *, noun)` that validates, names the source in errors, and normalizes padded API decimals to trimmed archive form. Each daily adapter keeps a one-line noun-bound wrapper, so module APIs and every existing test hold; spot gains the normalization its siblings already had.

# v3.17.5

- Split the Binance REST request budget per host family, each paced at 60% of its documented IP allowance with the header-driven backstop at 80%: api at 60 weight/s with backstop 4800 (of 6000/min), fapi at 24 weight/s with backstop 1920 (of 2400/min), unknown hosts at the previous 20/s. Spot aliases (api1-4) share api's budget and circuit. A hot fapi backlog no longer paces spot traffic and a 418 on one host no longer halts the other; prod demand at full catch-up is ~900 weight/min with zero 429/418 in 30h. With the 1000-trade perp pages, a perp minute drops from ~108s toward ~40s, closing the worker's structural deficit.

# v3.17.4

- Fetch perp provisional minutes with 1000-trade REST pages instead of 500. Fapi charges a flat 200 weight per `historicalTrades` request at any limit (verified live at 100/500/1000), so doubling the page halves the paced requests per minute at zero extra weight — roughly halving the ~108s a perp minute cost the single worker and letting the tail backlog drain twice as fast.

# v3.17.3

- Close the reconcile blind spot on partition failures: reconcile now blocks on any open `PARTITION`-scoped failure instead of only `canonical`/`component`, so a failed provisional or certification attempt holds the partition until a retry succeeds. A successful canonical build or repair recovers every open failure for the partition at once via `FailureLog.recover_partition`, so one good retry self-heals instead of leaving scars other operations cannot see.

# v3.17.2

- Share the aggregate archive quirk cleaner: the sentinel/duplicate-drop machinery moves from the spot agg adapter to `BinanceArchiveDaily.clean_agg_rows`, which the spot agg `clean_rows` hook now delegates to and the futures agg source will reuse. Behavior-preserving port; the four held production days verify identical through the delegated path.

# v3.17.1

- Record `required_approving_review_count: 1` in the `Protect-Main` ruleset snapshot. The live ruleset gained the one-approval requirement as the new standard while `.github/rulesets/main.json` still carried 0, so `pr_checks_ruleset` failed every open PR on drift it had not caused. Protection itself is unchanged; the snapshot now matches live field for field.

# v3.17.0

- Promote the Binance spot aggregate-trades revisioned source to LIVE: the spec flips from CANARY, and the `mount` and `huggingface` consumers go public with uploads, retiring the `huggingface_shadow` consumer and its sensor. Production history was verified complete (2017-08-17 to 2026-09-18, 3320 days, zero open failures, quirk-cleaned row counts verified) before promotion; the first public publish creates the twelve `vaquum/binance_btcusdt_spot_agg_*` datasets.

# v3.16.2

- Drop the two observed Binance-side quirk rows from spot aggregate archives before validation: -1/zero sentinel aggregates and byte-identical duplicate lines from repackaged chunks. A `clean_rows` hook on the archive base (identity by default) lets the agg adapter clean once for both the columnar and streaming consumers; drop counts merge into the revision evidence while `csv_sha256` keeps pinning the archive as served. Anything else malformed still fails loud.

# v3.16.1

- Fix the provisional worker watchdog livelock: each built interval, each publication, and each paced REST request touches the worker heartbeat, so a slow catch-up tick proves liveness instead of tripping the 180s watchdog and retrying the same oldest minute forever. Dagster runs never set `ORIGO_WORKER_HEARTBEAT` and skip the beat.

# v3.16.0

- Promote the Binance perp trades revisioned source to LIVE: the spec flips from CANARY, and the `mount` and `huggingface` consumers go public with uploads, retiring the `huggingface_shadow` consumer and its sensor. Production history was verified complete (2019-09-08 to present, fresh tail) before promotion; the first public publish creates the twelve `vaquum/binance_btcusdt_perp_*` datasets.

# v3.15.0

- Add the Binance spot aggregate-trades revisioned source (CANARY): canonical adapter over the vision aggTrades daily zips from 2017-08-17, single-endpoint provisional adapter over the spot aggTrades REST channel, ten components through the profile factory and generic formulas, and mount plus huggingface_shadow consumers with `spot_agg_`-prefixed series. Tick bars count aggregate events and dollar bars accumulate aggregate notional (price times quantity per aggregate); first/last trade ids are `Int64`. The scaffold gains the declared parameters the aggregate layout requires, all default-preserving with existing suites green and unchanged: the archive timestamp index, the provisional single-endpoint mode, the factory/formula/consumer id-column and quote-expression parameters, and the acceptance harness aggregate row layout with a third source case.

# v3.14.5

- Parameterize the acceptance harness and comparison helper: `acceptance_cases` declares one `SourceCase` per source (spec key, fixture root, twelve-product inventory, time and quote carve-outs) with `SOURCE_CASES`, and `assert_archive_rest_equal` owns the archive-vs-REST comparison both provisional suites shared inline. Each backfill suite pins its explicit twelve-series inventory against the literal and the live series declaration; the spot suite keeps its microseconds-to-milliseconds timestamp carve-out and the perp suite its recomputed price-times-quantity quote carve-out. Zero production-code changes.

# v3.14.4

- Scaffold the profile factory and generic formulas: `ProfileDeclaration` plus `build_components` own the measure columns, projection rewrite client, raw loader, and daily/minute/imbalance builders behind per-source declarations (raw columns, rewrite names, formula prefix, imbalance module), and the spot and perp profiles shrink to those declarations with their legacy aliases and retired tables. The sixteen bar-formula modules keep their table-name constants and delegate to `generic_bars`, which holds each SQL statement and the imbalance Arrow math exactly once; the dead ClickHouse wiring the perp imbalance port already dropped is removed from the spot module. Component hashes hold with zero test changes.

# v3.14.3

- Scaffold the parameterized provisional REST base: `BinanceProvisionalBase` owns minute-partition math, the 36h/5 candidate window, the locate+page loop, the 100-page cap, and the empty-minute two-observation evidence behind declared per-source parameters (host, paths, weights, page limit, credential policy, paging backtrack) and hooks (row mapper, HTTP seam, clock seam), and the spot and perp provisional adapters shrink to those declarations. Every request weight is a named field; `get_response` and `now_utc` keep resolving through the subclass modules so all patch seams hold with zero test changes.

# v3.14.2

- Scaffold the parameterized vision archive adapter: `BinanceArchiveDaily` owns download, checksum, partition, and row-validation mechanics behind declared per-source parameters (base URL, first day, field count, header policy) and hooks (HTTP seam, table builder, row builder), and the spot and perp daily adapters shrink to those declarations. Error texts are neutralized to `Binance` with every pinned substring preserved; all digests, revision keys, and REST-vs-archive proofs hold with zero test changes. The shared consumer factory (`ConsumerDeclaration` plus mount/huggingface renderers) shrinks both consumer modules to their dataset maps, scoping flags, and rollout-state tuples, with staging ownership, series/path/env scoping, and the HfApi and formula patch seams preserved per source; and `tools/fixture_bundle.py` fetches, packs, and verifies daily-archive fixture bundles, reproducing all five existing bundles' digests including the header-inclusive selection hash.

# v3.14.1

- Wire `BINANCE_API_KEY` from repo secrets through the deploy workflow into the `dagster` and `provisional-worker` services. The perp provisional adapter requires the key and the backfill's final health check failed without it (`PROVIDER_CREDENTIAL_MISSING` on every provisional tick); the secret existed but no deploy file referenced it.

# v3.14.0

- Add the Binance perp trades revisioned source beside spot: the daily adapter serves the vision futures archives from the 2019 first day, the provisional adapter pages closed minutes through `aggTrades` plus `historicalTrades` (paging back 1,000 ids before the locator because an aggregate's open time can hide in-minute trades, recomputing the cents-rounded `quoteQty` as price times quantity, and normalizing decimals so the API's padded text matches the archive's trimmed text), the perp profile, formulas and consumers mirror spot with CANARY shadow publication, and the REST replay test asserts full row equality against the archive on all 2,325 rows of the captured minute. The legacy futures pipeline is deleted in the same slice: its tables, jobs, schedules and tests are removed and the briefing reads the revisioned source.

# v3.13.2

- Reconcile only what differs: the source reconciliation sensor requests canonical runs for partitions whose Dagster record differs from the store or whose last run failed, and no longer adds one rotating day per tick, which re-materialized every canonical day once every two days (about 1,440 `refresh_binance_spot_trades_canonical_source_job` runs a day while nothing had changed) and, before the briefing guard, launched a briefing publish for each of them.

# v3.13.1

- Keep the provisional worker alive through the `mount` render: the deploy container's memory limit is raised to 16 GiB (the render needs about 5.1 GiB of RSS at peak, which the Dagster container never bounded, and 3 GiB killed the worker on every tick), and the backfill-ownership lookup asks the run storage for one tag and matches the source client-side, because the two-tag filter takes eleven seconds on production and timed out every tick.

# v3.13.0

- Run the minute feeds as observed workers: `depth-worker` ingests, projects and publishes the depth20 and depth200 minutes with a lookback catch-up, and `provisional-worker` builds every eligible provisional interval of each source and publishes the consumers that pin provisional rows (`mount`) when the pinned state changed, unless a native backfill owns publication. Both write one receipt per minute to `origo.worker_minute_log`, report partition materializations and a live feed asset to Dagster, keep a heartbeat and are restarted by their watchdog. A failing provisional minute, or a failing publication of one pinned state, is retried with a doubling delay up to the source's `retry_delay`, at most `retry_count` times; publication of a consumer that pins provisional rows follows the same backfill-ownership rule as the consumer sensors. The six per-minute depth schedules, the source's provisional schedule, the depth Arrow run-status sensor and the `mount` sensor are removed; each live feed asset carries a five-minute freshness policy the daemon evaluates without a run or a sensor, and the jobs stay for operator use. `OrchestrationSpec` requires the one-minute provisional cadence. `docs/Developer/Monitoring.md` describes the workers, their receipts and how a stale feed is read.

# v3.12.1

- Deliver the monitor's alerts and ship the container log: `send_alert` names its sender with a `User-Agent`, because Resend's edge answers the default urllib agent with 403 (Cloudflare error 1010) before the request reaches the API, and the Vector service sets `VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION=true`, because Vector 0.58 otherwise leaves `${CLICKHOUSE_PASSWORD}` and `${COMPOSE_PROJECT_NAME}` literal in its configuration and the ClickHouse sink fails its healthcheck with 401.

# v3.12.0

- Monitor from outside Dagster: a `monitor` worker evaluates five checks on the external asset `origo_monitor` every minute (Dagster reachable, queue bounded with run and check failures, workers alive from heartbeats and receipts, collectors serving, no error logs), writes the evaluations to Dagit through the webserver, then e-mails new findings through Resend with a per-key cooldown and a daily digest. Vector ships every container's output into `origo.container_log` with the 14-day diagnostic retention. `docs/Developer/Monitoring.md` fixes the model and the investigation order and `AGENTS.md` points to it. The run queue reserves a maintenance lane so `maintain_operational_metadata_job` runs while the backfill and routine lanes are full. The processor profile log is retired in the ClickHouse configuration and rotated or retired log tables are reported with their expiry and dropped whole after retention. The briefing sensors skip days before the book projection exists.

# v3.11.0

- Delete the legacy spot pipeline: the daily ingest, bar projections, latest stack, table setup, mirror and bar-store assets, their jobs, schedules, tests and helpers are removed, and the bar and export formulas they carried live in the spot profile. The revisioned spot source serves the legacy table names as views over its components (canonical rows under the base names, provisional rows under the `_latest` names), retires the ingestion ledgers, watermarks and per-interval cut tables together with the inert parity table at setup, purges the spot rows the retired refresh wrote into the futures pipeline's `aligned_1m_exchange`, and the briefing publications read its current views. The mount consumer stages a render beside the mirror, activates it only after the canonical check so a discarded render leaves the public files untouched, and sweeps staging a render that died left behind.

# v3.10.0

- Route the spot public identities to the revisioned source: the `mount` consumer owns the Parquet mirror and the Arrow bar store and refreshes only the months whose pinned state changed, the `huggingface` consumer uploads the twelve public datasets from the canonical state, the spot source is LIVE, and the legacy mirror, bar-store and Hugging Face publisher jobs, schedule and sensors are removed. Consumers that pin provisional rows republish as those rows change; canonical-only consumers follow the canonical state.

# v3.9.6

- Publication currency and triggering follow the canonical source state: renderers that declare provisional components pin the partial-day rows present at render time, and minute-cadence provisional refreshes neither invalidate a render nor request another publication. Remove the operational metadata byte budget and its deployment variable; the health check keeps the business-data ratio.

# v3.9.5

- Remove the one-time legacy parity comparison from the source framework: reconciliation re-checks retained content under the partition lock, repair shares the maintenance fence with builders, publication readiness is component evidence plus open partition failures, and open failures with retired parity codes are closed on reconciliation. Report a rotated diagnostic table's oversized lagging partition as pending until its expected expiry, separate free-space shortfalls from capacity limits, store only Information and above in `text_log`, and disable processor profile logging.

# v3.9.4

- Run startup and deployment recovery before any captured Dagster op exists so cancelling redundant queued runs cannot deadlock event storage; bound the recovery command, the compose start and the deploy job, and print the daemon log on a failed start.

# v3.9.3

Bound duplicate scheduling, reserve native Dagster capacity for routine and backfill work, launch concurrently, and recover retired workers and redundant queued runs during deployment.

# v3.9.2

- Run source backfills as bounded parallel native daily partitions with dependent file publication; use native bulk parsing, transport and versioned content proofs while retaining legacy generation verification.

# v3.9.1

- Replace typed backfill date configuration with native Dagster asset partition selection, coverage, gaps and retries. Selected source partitions prepare, verify and publish their complete declared footprint automatically.
- Activate schedules and sensors from code for every enabled source, including CANARY; preserve source provenance and independent file publication state.
- Put the source onboarding playbook in docs/Developer and require reading it before any new-source PRD or slice; document uniform native operations and per-source acceptance evidence.
- Verify that later source data triggers every declared publisher and completed publications suppress duplicate requests.

# v3.9.0

- Prepare enabled revisioned sources and managed sensors from code at daemon startup; persist declared files on a shared volume.
- Run an inclusive period from one Jobs/Launchpad backfill: verify each day, measure capacity automatically, and complete every declared file publication before success. Preserve per-day asset state and failures in Dagster.
- Query pinned ClickHouse projections for spot files instead of copying all historical raw rows into Python.

# v3.8.9

- Correlate repository membership for Dagster sensor and schedule run queries so Automation controls do not wait behind repeated repository tag scans.

# v3.8.8

- Retain thirty completed UTC minutes of depth Arrow chunks plus the current latest target; reject expired publications and report expiry through Dagster. Retain invalid date paths with warnings without blocking publication. Recover interrupted current-minute replacements from their committed chunk before expiry.

# v3.8.7

Completed metadata scans no longer report a growing cleanup backlog solely because protected projection runs or newly aged arrivals remain. Interrupted scans and unfinished retirement batches retain the backlog alarm.

# v3.8.6

- Report diagnostic timestamp-read byte limits per part and continue checking and reclaiming other parts.
- Verify diagnostic expiry against actual event timestamps so stale ClickHouse partition dates cannot report false retention failures or repeatedly schedule completed catch-up work.

# v3.8.5

- Reduce metadata-retirement overhead by reusing SQLAlchemy engines, unlinking retired event shards without schema initialization, and refreshing cached sensor state on committed changes. Source provenance, current asset facts, and fresh retry checks remain protected.

# v3.8.4
- Defer source compaction when readers or writers hold its locks, without recording a false archive failure or blocking unrelated projection retirement.
- Bound SQLite page reclamation by the maintenance work deadline so an interrupted pass still reports its checkpoint, capacity and health.
- Allow parallel native run-history reads and event writes; source compaction retries while an active reader still needs the native shard.

- Preserve source, mixed and unclassified run provenance; losslessly compact terminal source event databases and shared JSON payloads while keeping their Dagit history readable and late writes recoverable.
- Pack small asset output values without changing their serialized bytes; retain normal Dagster input loading and expose the actual packed storage location in output metadata.
- Bound recent failed-run resolution lookups before checking complete history, keeping broad partition tags from stalling cleanup.
- Isolate source compaction failures, preserve their diagnostics across maintenance runs, and keep unrelated projection cleanup moving.
- Keep resolved projection failures resolved after successor run histories expire by checking later materializations for every failed plan.
- Retire settled projection histories after a one-minute shutdown grace (24 hours for resolved failures), preserving current asset/partition/check state and fresh execution dependencies.
- Check Dagster physical storage against 10% of ClickHouse business data and a 13 GiB ceiling; schedule maintenance every 10 minutes, enable incremental SQLite reclamation after an explicit backed-up migration, and retain terminal scheduler ticks for one/seven days. ClickHouse diagnostics retain their separate 14-day policy.

# v3.8.3

- Scan metadata history in run-ID order and batch run, retry-reference and sensor-state reads with one read-only event connection per batch; preserve per-run live revalidation before reclamation.

# v3.8.2

- Enforce a separate work deadline for operational-metadata reporting, tolerate disappearing transient files while rejecting missing storage roots, and launch deployment maintenance in the configured Dagster workspace.

# v3.8.1

- Bound Dagster execution history while preserving current partition facts, retry holds and source receipts; expose inspection, catch-up and health through one Dagit maintenance job.
- Correct SQLite job/repository history queries, maintain planner statistics, and skip Arrow rebuilds for unchanged Parquet input generations.
- Configure 14-day ClickHouse diagnostic retention and bounded backlog maintenance, with first-cleanup manifest and restore verification.

- Isolate retention locks per run, initialize unused diagnostic logs before TTL checks, and preserve approved manifests across live revalidation.
- Preserve completed first-cleanup inventories across scheduled inspection runs so backup preparation and approval remain valid.

# v3.8.0

- Add native Dagit spot-history backfills with exact legacy parity, database state reconciliation, storage admission checks, and system logs in Dagit. Deterministic verification failures wait for a state change or operator retry; transient failures and cancellations retry with bounded backoff. Native completion suppresses redundant verification, while source health checks expose unresolved failures without recursive failed runs. Reuse run-local archive bytes and seek through component rows without OFFSET scans.

# v3.7.0

- Add a dormant revisioned individual Binance spot-trade source with isolated components, fenced activation, shared failure history, and local shadow consumers.
- Prove legacy spot output parity on checksum-verified official data using the deployed ClickHouse image; preserve existing source identities and running schedules.

# v3.6.2 on September 10, 2026
- Run source-native tests against the deployed ClickHouse image and configuration; freeze existing Binance spot and futures source, Dagster, and consumer contracts before revisioned-source implementation.

# v3.6.1 on August 23, 2026
- Key every HuggingFace publish sensor run on both the materialized partition and its triggering run, so re-materializing a partition requests a fresh publish while repeated sensor evaluation of one materialization remains idempotent.

# v3.6.0 on August 20, 2026
- Add the `publish_btc_briefing_history` asset and its `btc_briefing_history/1` rolling file: one `btc_briefing_history.json` on the same `vaquum/btc_briefing_feed` dataset, republished on every daily partition, carrying 70 days of 15m bars and 61 days of daily bars rolled up from the 1m klines projection over the span that ends where the partition day begins — the two lookbacks the consuming briefing is computed over, so it reads its whole history from this dataset instead of re-fetching it from an exchange REST API on every run. The span's bar grid is anchored to its opening midnight and declared in UTC epoch seconds in the SQL itself, so every bar in the window sits on one midnight-aligned grid and none straddles a day boundary; the daily feed's completeness policy is applied across the whole span, so a span short by a bar, a bar built from fewer than its full complement of distinct 1m minutes, or a duplicated 1m source row raises instead of publishing a gap a consumer would read as real quiet. A new `publish_btc_briefing_history_sensor` fires on each daily feed materialization, so the history and that day's feed file always name the same day and compose without overlapping; a partition older than the published window is refused — before its rollup runs — rather than rolling the single file backwards.

# v3.5.1 on August 20, 2026
- Record `require_extra_approval_for_unattributed_changes` in the `Protect-Main` ruleset snapshot. GitHub added the field to the rulesets API, so the live ruleset began reporting it while `.github/rulesets/main.json` did not carry it, and `pr_checks_ruleset` failed every open PR on drift it had not caused. The snapshot now matches the live ruleset field for field, which is the only state in which that gate can tell a real out-of-band change from an API addition.

# v3.5.0 on August 18, 2026
- Publish the daily BTC briefing feed to HuggingFace on the daily cadence: `publish_btc_briefing_feed` becomes daily-partitioned and uploads each partition day's `btc_briefing/1` feed to the `vaquum/btc_briefing_feed` dataset as one JSON file per day plus a `latest.json` pointer and dataset card (reusing the existing `HF_TOKEN` credential; no new secret). A new `publish_btc_briefing_feed_sensor` fires after each daily spot klines materialization, so the first tick after deploy publishes the most recent complete UTC day and any past covered day is a manual partition launch.

# v3.4.0 on August 18, 2026
- Add the `publish_btc_briefing_feed` asset: builds and validates the daily BTC briefing feed (`btc_briefing/1`) for the last complete UTC day from the origo ClickHouse tables — 15m/1d OHLCV bars from the 1m klines projection, measured volume-at-price in exact integer satoshis split by taker side, and per-minute series, exact daily percentiles and 8h session aggregates of the depth20 1m book. Every time field is declared as UTC epoch seconds in the SQL itself, so the contract's time representation cannot drift with the server's Arrow serialization. An incomplete, short, or duplicated bar day raises instead of producing a corrupt feed. The asset computes and validates only; delivery to the consuming repository is deliberately a separate slice.

# v3.3.1 on July 21, 2026
- Add the MIT license (verbatim from Vaquum/Limen) as a root LICENSE file plus `[project]` license metadata in pyproject.toml, and rewrite README.md to the shared Vaquum module README structure: honest capability inventory backed by code on `main`, a docker-compose quickstart (table creation, one-day spot backfill, query-module read-back), and the standard boilerplate tail.

# v3.3.0 on July 10, 2026
- Make the daily spot/futures partition write atomic: the full-day row insert (millions of rows over minutes — the window a killed run left a partial day in) now builds and count-verifies a per-day staging table before the live table is touched, then promotes it with a synchronous day DELETE followed by an atomic `MOVE PARTITION` (metadata part-move, not a splittable INSERT..SELECT that can commit a partial day on cancellation). MOVE appends the day into the live month partition, preserving the month's other days. The only residual window (between DELETE and MOVE) leaves the day MISSING, never partial or duplicated, which the daily gap-repair schedule then heals (tracker #275 item 4). Unblocks run-level retries (item 21).

# v3.2.0 on July 10, 2026
- Enable Dagster run monitoring: runs stuck in STARTING fail after 300s, and any run is bounded at 26h (`max_runtime_seconds`; per-run override via the canonical `dagster/max_runtime` tag). DefaultRunLauncher has no worker health checks, so a dead STARTED worker turns red at the 26h bound, not sooner — an honest bound, not instant orphan detection. Gap repair additionally excludes partitions whose runs reached a terminal state within a 1h grace period, because a timed-out run is force-marked FAILED without confirmed worker exit (tracker #275 items 1 and 21). Run retries stay disabled until partition replacement is atomic.

# v3.1.0 on July 9, 2026
- Add hourly daily-gap-repair schedules for the spot and futures daily pipelines: ledger-absent days in a 14-day lookback (ending at today-2 to never race the regular daily ticks) whose Binance archive exists are re-requested as partition runs, keyed once per day per gap — the 2026-07-03 futures incident class now self-heals (tracker #275 item 17).

# v3.0.3 on July 6, 2026
- SECURITY: bind the deploy dagit webserver to 127.0.0.1 instead of all interfaces (it had no auth and was publicly reachable on :4000 since 2026-04-21); operator access is now via SSH tunnel. Add a tests/tools guard that fails any deploy-compose port not bound to loopback.

# v3.0.2 on July 6, 2026
- CI: sync the checked-in main ruleset snapshot with the `dismissal_restriction` field GitHub now returns on the live pull_request rule (disabled/default value; effective branch protection unchanged), unblocking the law-9 ruleset-drift gate.

# v3.0.1 on July 4, 2026
- Adopt the Origo identity in CI and deploy: repo renamed to Vaquum/Origo, deploy workflow variables `TDW_*` -> `ORIGO_*` (values unchanged; server names intentionally stay tdw), fresh `ORIGO_PRIVATE_KEY` deploy key, GHCR images `origo-dagster`/`origo-clickhouse` (legacy packages retained for rollback), slice-gate help text and ruleset fixtures updated.

# v3.0.0 on July 4, 2026
- BREAKING: rename the Python package `tdw_control_plane` to `origo` — module imports, Dagster workspace/code-location, packaging config, typing/fail-loud gate roots, and the deploy smoke test all move; the Dagster code location becomes `origo.definitions` (instigator state restarts via in-code RUNNING defaults).

# v2.1.0 on July 4, 2026
- Allow `package_root` to change in the typing and fail-loud budget-source ratchets only when the base root directory no longer exists in the head tree AND every ratchet total is identical to base (a totals-neutral rename); narrowing onto a subtree while the old root exists stays blocked, and no ratchet total can move in the same PR as a root change. Residual risk accepted and documented: a rename-shaped PR can still relocate files outside the scan surface — as any PR always could by moving files out of the root — and operator review remains the backstop for that.

# v2.0.1 on July 4, 2026
- Rebrand repo metadata to Origo: dist name, description, README, slice template prose, dev image tag, test fixtures dir and container prefix; remove the never-existing `quickstart_etl_tests` ghost path from pyproject, budgets, gates, and the lint contract; delete vestigial `dagster_cloud.yaml`.
- Pin `default_status=RUNNING` on all 12 HuggingFace asset sensors and add a regression test asserting every sensor defaults to RUNNING.

# v2.0.0 on July 2, 2026
- BREAKING: remove the legacy tdw warehouse pipeline — all tdw table/ingestion/summary assets, the tdw daily and monthly roll-forward schedules, their jobs, the tdw-only ClickHouse helpers in definitions, and the orphaned tdw utils.
- Remove the dead `query.get_binance_spot_klines` helper (read from `tdw.binance_trades_complete`) and the tdw module stub in the origo test fixture.
- Add a regression test that fails any PR reintroducing a tdw asset, job, schedule, or module.

# v1.20.1 on June 16, 2026
- Reconcile Binance spot depth20/depth200 live gaps across source history, ClickHouse projections, and Arrow chunks.

# v1.20.0 on June 15, 2026
- Auto-update Binance spot depth20 and depth200 Arrow snapshots as minute Arrow chunks after their source refresh jobs succeed.

# v1.19.0 on June 15, 2026
- Publish Binance spot depth20 and depth200 raw snapshots as mmap-ready Arrow IPC files under `/opt/arrow`.

# v1.18.1 on June 15, 2026
- Wire Binance spot depth200 service credentials into the deployment environment.

# v1.18.0 on June 15, 2026
- Add Binance spot depth200 source-native snapshots and 1-minute projection tables alongside the existing depth20 source.

# v1.17.2 on June 5, 2026
- Replace the legacy RFC issue template with a PRD issue form and keep the slice issue form loadable by removing its empty title field.

# v1.17.1 on June 5, 2026
- Fix the Arrow bar store writing multi-record-batch files for any series past polars' ~122k-row IPC batch default: a `memory_map=True` reader surfaced those batches as multiple chunks, breaking the single-batch zero-copy `ts` view the store exists to provide. Force one record batch via `record_batch_size`. Self-heals on deploy (the byte change yields a new content-hash version, so each series republishes once as a single batch).

# v1.17.0 on June 5, 2026
- Add a versioned, mmap-ready Arrow bar store: a run-status sensor rebuilds every series into a single-record-batch, uncompressed Arrow IPC file under LOCAL_ARROW_DIR (default /opt/arrow) whenever the Parquet mirror job succeeds. Measures are carried verbatim at full precision (no downcast), so the store stays bit-for-bit reproducible against the mirror; it is published with an atomic `latest` symlink swap, content-hash versioning, a monotonic freshness guard, and a few retained prior versions so in-flight mmap and pinned-version reads never break mid-swap.
- Run the Binance spot Parquet mirror every minute (was every 10 minutes) so the mirror — and the Arrow bar store it triggers on completion — track the 1-minute ClickHouse latest projections.

# v1.16.1 on June 4, 2026
- Fix Binance spot dollar-kline Hugging Face exports collapsing every timestamp to ~1970 under polars >=1.40 by emitting millisecond-precision DateTime64 so the Arrow round-trip preserves the real dates.

# v1.16.0 on June 4, 2026
- Add a 10-minute stateless job mirroring the 12 Binance spot kline series to monthly Parquet files on a local mount.

# v1.15.2 on June 4, 2026
- Route Hugging Face Binance spot time-kline exports through the Origo 1m kline projection.

# v1.15.1 on May 30, 2026
- Include latest Binance spot table creation in the scheduled latest data-source job.

# v1.15.0 on May 27, 2026
- Add rolling latest Binance spot trade, kline, and cut projections in Origo.

# v1.14.2 on May 22, 2026
- Require raw spot trades before replacing Binance spot dollar imbalance kline partitions.

# v1.14.1 on May 21, 2026
- Fix the Binance spot dollar kline base size to 1M and publish 1M, 15M, 30M, 60M, 120M, and 240M dollar snapshots.

# v1.14.0 on May 20, 2026
- Add Hugging Face publishers for BTCUSDT 100k, 2M, 4M, 8M, 16M, and 32M dollar spot kline snapshots from Origo dollar klines.

# v1.13.4 on May 20, 2026
- Add a manual Origo depth20 partition-state reconciliation job for existing ClickHouse rows.

# v1.13.3 on May 20, 2026
- Add manual Origo backfill jobs for Binance spot raw trades and depth20 snapshots plus 1m projection.

# v1.13.2 on May 20, 2026
- Decouple the Binance spot dollar klines backfill job from raw-trades ingestion and fail the dollar refresh when raw trades are absent.

# v1.13.1 on May 20, 2026
- Add a dedicated Binance spot dollar klines backfill job.

# v1.13.0 on May 20, 2026
- Add Binance spot dollar imbalance klines on Origo daily spot trades.

# v1.12.0 on May 20, 2026
- Add Binance spot tick klines on Origo daily spot trades.

# v1.11.0 on May 19, 2026
- Add Binance spot volume klines on Origo daily spot trades.

# v1.10.0 on May 19, 2026
- Add Hugging Face publishers for BTCUSDT 15-minute, 30-minute, and 2-hour spot kline snapshots from Origo daily spot trades.

# v1.9.0 on May 19, 2026
- Add Binance spot dollar klines on Origo daily spot trades.

# v1.8.0 on May 18, 2026
- Add Hugging Face publishers for BTCUSDT 1-hour and 4-hour spot kline snapshots from Origo daily spot trades.

# v1.7.1 on May 14, 2026
- Add Dagster table-creation jobs for the Binance spot depth20 source-native snapshots and 1-minute projection tables.

# v1.7.0 on May 14, 2026
- Add the Binance spot depth20 history service as an Origo source with source-native snapshots and a 1-minute source projection.

# v1.6.7 on May 1, 2026
- Add `audit_main_ruleset`, a privileged post-merge `main` workflow that audits full live parity of ruleset `5406599`, including `bypass_actors`, against `.github/rulesets/main.json`.
- Add `tools/privileged_ruleset_audit.py` and `tests/tools/test_privileged_ruleset_audit.py`, including the fail-loud contract for missing or underscoped visibility of `bypass_actors` and the live-payload snapshot on failure.
- Extend `pr_checks_ruleset` so the privileged-audit workflow and tool contract are mechanically protected by required CI before rollout.

# v1.6.6 on April 30, 2026
- Route the Hugging Face spot kline publisher through the Origo projection.

# v1.6.5 on April 28, 2026
- Move the default-running daily Binance spot Origo source schedule to `04:00 UTC` while leaving the futures schedule at `10:00 UTC`.

# v1.6.4 on April 28, 2026
- Move the default-running daily Binance Origo source schedules to `10:00 UTC` so routine automation runs after observed Binance archive publication.
- Add bounded hourly Dagster retries to the spot and futures daily archive ingest assets for late archive publication.

# v1.6.3 on April 25, 2026
- Replace the two daily Binance Origo source-template schedules with Dagster partitioned-job schedules that request the latest daily partition instead of launching non-partitioned empty-config runs.
- Start the daily spot and futures schedules enabled so Dagster owns routine daily automation while partition backfills stay on Dagster's built-in backfill path.

# v1.6.2 on April 23, 2026
- Rename the two Origo source-template schedules to `daily_binance_spot_pipeline_schedule` and `daily_binance_futures_pipeline_schedule` so both surfaces include the source prefix explicitly.
- Keep the existing spot and futures source-template jobs unchanged; this slice only renames the Dagster schedule definitions and their registration surface.

# v1.6.1 on April 23, 2026
- Add the repo-root `AGENTS.md` governance file with the operator-specified ten-law workflow contract and `zero-bang` approval authority.
- Add `tests/tools/test_agents_contract.py` and extend `pr_checks_ruleset` so the checked-in `AGENTS.md` file identity and workflow coverage are mechanically enforced in CI.

# v1.6.0 on April 23, 2026
- Complete the Binance futures Origo data-source template on top of `binance_daily_futures_trades` by adding the single-source `binance_futures_klines` projection and the shared `aligned_1m_exchange` futures path.
- Rename the generic spot schedule to `daily_spot_pipeline_schedule`, add `daily_futures_pipeline_schedule`, and wire `refresh_binance_futures_data_source_job` so spot and futures source-template automation follow the same naming law.
- Add checked-in real Binance futures daily fixtures for both the headerless and headered source shapes, plus fixture-backed futures row/schema/idempotency tests that prove `aligned_1m_exchange` can hold both `binance_spot` and `binance_futures`.

# v1.5.1 on April 23, 2026
- Correct the Origo Binance spot projection contract so `binance_spot_klines` and `aligned_1m_exchange` match the TDW 1-minute kline schema instead of the exchange-native 12-column shape.
- Replace the single-source and aligned refresh SQL so both tables materialize the TDW analytics columns (`mean`, `std`, `median`, `iqr`, maker/liquidity fields) from raw spot trades.
- Add a checked-in TDW contract fixture and replace the old exchange-native row tests with fixture-backed schema and row-parity tests for both projection tables.

# v1.5.0 on April 23, 2026
- Complete the first Binance spot Origo data-source template on top of `binance_daily_spot_trades` by adding the single-source `binance_spot_klines` projection and the shared `aligned_1m_exchange` projection layer.
- Replace the old raw-only daily Origo schedule target with `refresh_binance_spot_data_source_job`, which materializes the raw daily insert plus both projection layers for the same partition.
- Add end-to-end `tests/origo_source_native/test_origo_binance_spot_data_source_template.py` proofs for table-name contracts, exact schema, exact Binance-derived 1-minute rows, aligned dataset-source rows, and same-partition rerun idempotency.

# v1.4.1 on April 22, 2026
- Sync `tests/fixtures/github/ruleset_live_unexpected_field.json` to the current 9-context protected-check set on `main`, including `pr_checks_lint` and `pr_checks_tests`.
- Add `test_unexpected_field_fixture_preserves_required_contexts` so `pr_checks_ruleset` fails if that negative fixture ever drifts from the checked-in ruleset snapshot's required-status list.

# v1.4.0 on April 22, 2026
- Move the runtime image to Python `3.11.12` so Docker matches the package and CI interpreter contract.
- Replace the Origo path with a daily-source-native Binance spot trades template: idempotent `create_origo_database`, idempotent `create_binance_daily_spot_trades_table_origo`, and fail-loud `insert_daily_binance_spot_trades_to_origo`.
- Rename the Origo raw table surface to `binance_daily_spot_trades`, add the companion `binance_daily_spot_trades_ingestion` ledger, preserve source timestamps with `DateTime64(6)`, and record Dagster run metadata plus source checksums/counts per ingested daily file.
- Add `.github/workflows/pr_checks_tests.yml`, the ClickHouse-backed `tests/origo_source_native` suite, fixture-backed Binance daily archives plus `.CHECKSUM` files, and the checked-in ruleset snapshot change that requires `pr_checks_tests` on `main`.

# v1.3.3 on April 22, 2026
- Add `.github/workflows/pr_checks_lint.yml` so `tools` and `tests/tools` gain a required fail-loud Ruff gate on `main`, pinned to Ruff `0.15.11`.
- Extend `pr_checks_ruleset` with `tests/tools/test_lint_ci_contract.py` and a pinned Ruff install so the lint gate itself is mechanically protected by required CI.
- Remove dead Ruff ignores `ANN101` and `ANN102`, replace broad-exception handling in `tools/slice_gate.py` and `tools/typing_gate.py` with explicit fail-loud setup/read handling, and replace the remaining `RUF005` list concatenations in `tools/fail_loud_gate.py`.

# v1.3.2 on April 22, 2026
- Add `.github/rulesets/main.json`, `tools/ruleset_gate.py`, `pr_checks_ruleset`, and fixture-backed ruleset drift tests so `main` can ratchet its required PR-path contexts against a checked-in snapshot.
- Fix `tools/cc_gate.py` so linked-issue title lookup failures raise a hard setup error instead of silently skipping Conventional Commits validation.
- Remove the post-merge CHANGELOG automation workflow and `scripts/update_changelog.py` because `pr_checks_version` is now the authoritative pre-merge version/changelog gate.

# v1.3.1 on April 21, 2026
- Add `pr_checks_fail_loud` workflow and `tools/fail_loud_gate.py` ratcheting seven silent-fallback categories in the package: `bare_except`, `empty_pass`, `empty_ellipsis`, `empty_return_none`, `empty_continue_break`, `contextlib_suppress`, `errors_ignore_kwarg`. Base-vs-head protection so the budget cannot be weakened in the same PR that gates against it.
- Add `.github/fail_loud_budget.json` as the committed baseline oracle (`bare_except=4`, `empty_pass=6`, `empty_continue_break=1`, all others zero on 35 production files at introduction).
- Add `pr_checks_version` workflow and `tools/version_gate.py` enforcing six rules on every PR: pyproject.toml differs, `[project].version` advances by strict `MAJOR.MINOR.PATCH` (prerelease and build-metadata forms rejected outright, since the gate compares as integer triples and real semver precedence would be misrepresented), CHANGELOG.md differs, CHANGELOG's first `# v<X.Y.Z>` header matches the new version, the top version section has at least one non-empty non-header line of content before the next version header (so a header-only trail is rejected), and the bump level meets the minimum required by the PR's Conventional Commits type (`type!` → major, `feat` → minor, anything else → patch).
- `contextlib.suppress` detection resolves module-alias chains to a fixed point (`import contextlib as cl; mod = cl; sup = mod.suppress; sup2 = sup`) so any re-binding path to `contextlib.suppress` is counted. Same fixed-point technique already used by `typing_gate.py` for `typing.Any`.

# v1.3.0 on April 21, 2026
- Add `.github/ISSUE_TEMPLATE/slice.yml` — slice issue template. Eleven sections each carrying a blockquoted `> **Significance.**` paragraph that survives into the filed issue body.
- Add `pr_checks_slice` workflow and `tools/slice_gate.py` enforcing the PR↔slice-issue contract as eight deterministic rules: exactly one `Closes/Fixes/Resolves #N` reference, the reference resolves in the repo, resolves to an issue (not another PR), issue is OPEN, issue has the `slice` label, PR title byte-equals issue title, issue body contains every full multi-line Significance blockquote from the template verbatim (extracted at gate runtime so template and validator cannot drift apart), PR diff ⊆ issue `## Surfaces`, PR diff ∩ issue `## Out of Scope` = ∅.
- Add `pr_checks_slice_on_issue` workflow — stale-state recovery on `issues` events (edited, labeled, unlabeled, closed, reopened, deleted). Finds linked open PRs by scanning every open PR body with the same closing-keyword regex rule 1 uses, re-runs the slice gate against current state, and posts a fresh `pr_checks_slice` check-run to each PR's head SHA via the Checks API. Branch protection uses the latest check-run per name per SHA, so an issue change that breaks any rule invalidates the required check within seconds.
- Add `pr_checks_cc` workflow and `tools/cc_gate.py` enforcing Conventional Commits v1.0.0 on three surfaces: PR title, linked-issue title, and every non-merge commit in the PR range. Allowed types: `feat | fix | docs | style | refactor | perf | test | build | ci | chore | revert`.
- Use GitHub REST `pulls/:num/files` with `--paginate` for all PR file enumeration (previously `gh pr view --json files`, which caps at 100). Both slice workflows cross-check the enumerated count against the PR object's `changedFiles` field and hard-fail if they disagree, so scope rules cannot silently under-enforce on large PRs.
- Ensure branch-protection `Protect-Main` ruleset requires `pr_checks_slice` and `pr_checks_cc` in addition to `PR Checks CodeQL (python)` and `pr_checks_typing`.

# v1.2.1 on April 21, 2026
- Add `pr_checks_typing` workflow and `tools/typing_gate.py` enforcing typing discipline as a ratchet: pyright strict config audit, `pyrightconfig.json` ban, `pyright.include` identity check, regex escape-hatch ratchet, AST-based `typing.Any`-reference ratchet (covers bare `Any`, `typing.Any`, `t.Any`, aliased imports, and module-level assignment-alias chains), pyright total-error-count ratchet, `filesAnalyzed` ratchet, and a base-vs-head budget-source ratchet that blocks weakening of the oracle in the same PR it gates.
- Add `[tool.pyright]` strict configuration to `pyproject.toml` with the full `report*` matrix set to `error`.
- Add `[tool.ruff]` configuration to `pyproject.toml` selecting `E/F/I/UP/RUF/BLE/ANN`.
- Add `.github/typing_budget.json` as the committed baseline oracle (zero escape hatches, 1213 pyright-strict errors on 35 files at introduction).
- Bump `project.requires-python` to `>=3.11` to align with `tomllib` usage, `pyright.pythonVersion`, `ruff.target-version`, and CI.
- Bump `pr_checks_codeql.yml` Python from `3.10` to `3.11` to match the above.
