# Live Performance Investigation Log

Purpose: Continuous, explicit record of backfill performance/debug actions.
Format per entry: `I tried -> I found -> Progress/Next`.

## 2026-03-13

### Entry 001
- I tried: Collected live runtime snapshot from server (`docker compose ps`, Dagster GraphQL active runs, ClickHouse event-log counts).
- I found: All core services healthy, one stale Dagster run `82cf6c5e-cb4a-488a-b2a0-49fcd4106af2` stuck `STARTED` on partition `2021-05-19`, canonical stream partially populated.
- Progress/Next: Established baseline failure mode to target (stale run + partial partition).

### Entry 002
- I tried: Inspected Dagster instance config and effective coordinator details.
- I found: `dagster.yaml` has no explicit concurrency/run-monitoring config; effective coordinator defaults are implicit.
- Progress/Next: Need explicit Dagster concurrency + run monitoring configuration to prevent lingering stale `STARTED` runs.

### Entry 003
- I tried: Profiled full-day ingest stages for Binance daily file (real source fetch + parse + integrity + canonical write).
- I found: For `2021-05-24` (3.51M rows), total ~126.5s. Breakdown: download ~5.6s, parse ~35.8s, integrity ~10.7s, canonical write ~73.1s.
- Progress/Next: Bottleneck is Python processing + end-to-end canonical path, not network.

### Entry 004
- I tried: Benchmarked vectorized parse/integrity surrogate (Polars) on same daily file.
- I found: Parse ~0.25s, vectorized integrity surrogate ~0.04s versus Python parse+integrity ~46s.
- Progress/Next: Highest-value optimization is replacing row-loop parsing/integrity with chunked vectorized path.

### Entry 005
- I tried: Queried ClickHouse query_log for canonical insert latencies.
- I found: `100k` row inserts are typically ~0.4–0.95s (DB-side capacity is strong, roughly >200k rows/s).
- Progress/Next: ClickHouse is not primary bottleneck; focus on ingestion pipeline CPU/path overhead and orchestration stability.

### Entry 006
- I tried: Investigated Dagster daemon/code-server logs around long-running behavior.
- I found: Repeated warnings `No heartbeat received in 20 seconds, shutting down` from Dagster code servers.
- Progress/Next: Strong indicator that long/heavy execution and heartbeat TTL configuration are contributing to instability/stale runs.

### Entry 007
- I tried: Validated canonical timestamp correctness after local benchmark injections.
- I found: One benchmark path accidentally wrote incorrect time units for `2021-05-25` (`1970-01-01` range).
- Progress/Next: Repaired partition and revalidated to `2021-05-25` bounds; added this as a caution for any manual benchmark path.

### Entry 008
- I tried: Recomputed global canonical binance stream bounds after repair.
- I found: Global Binance canonical now spans `2017-08-17` to `2021-05-25` with `22,978,721` rows.
- Progress/Next: Dataset integrity restored for investigated benchmark side effects.

### Entry 009
- I tried: Compared Origo ingestion path to `../tdw-control-plane` path.
- I found: Origo path includes heavier canonical guarantees and richer validation/audit semantics than legacy direct-table ingestion.
- Progress/Next: Optimization must preserve canonical guarantees while reducing per-row Python overhead.

### Entry 010
- I tried: Assessed current explicit backfill concurrency controls in S34 runner.
- I found: S34 runner enforces minimum/default worker concurrency `10`, but Dagster-level run coordination is still not explicitly pinned in config.
- Progress/Next: Next implementation step should harden Dagster coordination and run monitoring explicitly, then rerun controlled concurrency sweep.

### Entry 011
- I tried: Added persistent investigation log in `spec/` and formalized entry structure.
- I found: We now have a stable, append-only debug narrative file for all subsequent attempts and findings.
- Progress/Next: Continue adding entries for each change + benchmark cycle, then implement explicit Dagster run-monitoring/concurrency config.

### Entry 012
- I tried: Added explicit Dagster controls in `control-plane/dagster.yaml` (`run_queue.max_concurrent_runs=20`, run monitoring, code server timeouts).
- I found: Configuration now pins concurrency/monitoring behavior explicitly instead of relying on implicit defaults.
- Progress/Next: Deploy and verify in server runtime (`dagster instance info` + active run behavior) to confirm stale-run handling works as intended.

### Entry 013
- I tried: Ran local Dagster instance validation.
- I found: Local command failed on `/opt/dagster-instance` permission (environment path issue), while YAML parse succeeded in project runtime environment.
- Progress/Next: Validate effective Dagster instance settings in server container after deploy.

### Entry 014
- I tried: Re-read S34 runner + Binance asset code paths to confirm active backfill execution model and write mode behavior.
- I found: S34 backfill uses `ProcessPoolExecutor` (not Dagster run queue) and forces `ORIGO_CANONICAL_FAST_INSERT_MODE=assume_new_partition` when projection mode is `deferred`.
- Progress/Next: Root-cause work must target Python ingest path and canonical write cost, not only Dagster queue tuning.

### Entry 015
- I tried: Probed ClickHouse function support for SQL-native canonical write acceleration.
- I found: CH 26.2 supports `SHA256` but does not support `generateUUIDv5`, which blocks full SQL-only deterministic event-id generation matching current UUIDv5 contract.
- Progress/Next: Keep deterministic UUIDv5 generation in Python for now; optimize parse/integrity and Python-side envelope construction.

### Entry 016
- I tried: Ran a controlled micro-benchmark on Binance day `2021-05-24` and separated parse, event/payload generation, and insert throughput.
- I found: parse ~22.9s, generation ~16.0s (3.51M rows), and remote insert benchmark ~81k rows/s at 50k-1M chunks (best ~84k/s at 500k chunk).
- Progress/Next: Implement fast path with Polars parse + vectorized integrity and remove per-row dataclass parsing overhead; then re-benchmark end-to-end on the same day.

### Entry 017
- I tried: Ran full end-to-end benchmark on the new Binance fast path (`parse_binance_spot_trade_csv_frame` + frame integrity + frame canonical write) for day `2021-05-24`.
- I found: Parse improved from ~22.9s to ~0.97s and integrity from ~10.7s to ~0.27s, but canonical write remained dominant at ~154.7s for 3.51M rows.
- Progress/Next: Parsing/integrity bottlenecks are resolved; current root bottleneck is canonical write internals (payload/UUID/hash construction and/or insert path), so next step is internal timing decomposition of write stage.

### Entry 018
- I tried: Started a decomposed write-stage benchmark that times extraction, payload JSON build, payload hash, UUID generation, and insert phases separately.
- I found: Run is in progress at the time of this entry (full-day 3.51M row measurement).
- Progress/Next: Use phase timings to implement the next targeted optimization patch on the true dominant sub-stage.

### Entry 019
- I tried: Completed decomposed full-day write-stage benchmark for Binance day `2021-05-24` (3.51M rows).
- I found: write-stage breakdown was `extract 0.40s`, `offsets 0.34s`, `payload_json 2.50s`, `payload_raw 0.21s`, `sha256 1.77s`, `uuid 8.66s`, `insert 33.25s`, total write-path core ~`47.11s` (excluding download/parse).
- Progress/Next: The primary remaining bottleneck is ClickHouse insert (~33s) followed by UUID generation (~8.7s); next optimization targets are insert-path throughput settings/chunking and optional UUID computation parallelization.

### Entry 020
- I tried: Re-ran `write_binance_spot_trade_frame_to_canonical` benchmark and compared it against decomposed stage timings.
- I found: The high ~150s writer result was an artifact of runtime-audit initialization against an existing large audit log (first-use chain validation), not core payload/insert performance.
- Progress/Next: Benchmarking and tuning must use fresh per-run runtime-audit paths (same model as S34 workers) to reflect true backfill runtime.

### Entry 021
- I tried: Re-ran the exact writer path with a fresh runtime-audit file path (`/tmp/origo-runtime-audit-perf.json`) and reset runtime-audit singleton.
- I found: Full writer time dropped to ~49.54s for 3.51M rows, matching the decomposed write-stage estimate.
- Progress/Next: True per-day baseline is now ~57s including download/parse/integrity; next optimization focus is insert throughput and UUID generation, plus controlled concurrency sweep on server.

### Entry 022
- I tried: Benchmarked deterministic UUIDv5 generation alternatives (`uuid.uuid5` vs byte-level SHA1 UUIDv5-equivalent implementation) on 1M offsets.
- I found: Byte-level implementation is output-identical and ~9.6% faster (`~603k/s` vs `~548k/s`), but absolute gain is small relative to total partition runtime.
- Progress/Next: Keep this as a secondary optimization option; primary runtime lever remains insert throughput + controlled concurrency on server.

### Entry 023
- I tried: Removed redundant partition-list materialization in frame writer and switched partition consistency checks to vectorized DataFrame checks.
- I found: This reduces Python-side allocation overhead in the writer preflight path and keeps fail-loud partition mismatch checks intact.
- Progress/Next: Continue with server-side controlled concurrency sweep using the new parse/integrity/writer path.

### Entry 024
- I tried: Ran targeted guardrail tests after optimization patch (`tests/integrity/test_exchange_integrity.py` and S34 backfill contract/orchestration tests).
- I found: All targeted tests passed (`10 passed` integrity suite; `9 passed` S34 contract suite).
- Progress/Next: Optimization patch is contract-safe on covered paths; proceed with end-to-end timing confirmation and then server concurrency sweep.

### Entry 025
- I tried: Re-ran full day-path timing (`download -> parse -> integrity -> canonical write`) with fresh runtime-audit path and temp canonical table.
- I found: For day `2021-05-24` (3.51M rows): `download 6.38s`, `unzip 0.31s`, `parse 1.08s`, `integrity 0.39s`, `write 55.26s`, total `63.42s`.
- Progress/Next: Daily partition path is now materially faster and stable; next step is controlled server concurrency sweep (15/20/30) with CPU/RAM/network/CH writes telemetry to pick the operational setting.

### Entry 026
- I tried: Applied UUIDv5-equivalent fast path in `origo/events/writer.py` (byte-level SHA1 + RFC4122 v5 bit layout), replacing direct `uuid.uuid5` call while preserving deterministic output.
- I found: Contract tests stayed green (`test_canonical_event_writer_contract` + ingest-state contract), confirming event-id compatibility.
- Progress/Next: Re-benchmark canonical write phase to quantify real end-to-end gain.

### Entry 027
- I tried: Re-ran canonical writer benchmark after UUID optimization with fresh runtime-audit path and temp canonical table.
- I found: `write` dropped from ~55.26s to ~50.15s for 3.51M rows (~9.2% improvement in canonical write phase).
- Progress/Next: Current realistic daily baseline is now near ~58s end-to-end; next step is server concurrency sweep (`15/20/30`) to select operational throughput setting.

### Entry 028
- I tried: Hardened CSV header detection by stripping optional UTF-8 BOM before header checks.
- I found: This keeps fast parser behavior stable across header files that may include BOM-prefixed `id`/`trade_id`.
- Progress/Next: Continue with operational sweep/rollout using the optimized parser-writer path.

### Entry 029
- I tried: Re-ran targeted regression suite after parser/writer/UUID updates.
- I found: `26/26` tests passed across exchange integrity, canonical writer contracts, and S34 runner/orchestration contracts.
- Progress/Next: Code path is contract-clean for this optimization set; next step is run/deploy and concurrency telemetry sweep on server.

### Entry 030
- I tried: Ran multi-date end-to-end benchmark (`download -> parse -> integrity -> canonical write`) on optimized path for `2017-08-17`, `2019-06-01`, and `2021-05-24`.
- I found: Runtime scales with row volume and remained stable: 
  - `2017-08-17`: `3,427` rows, total `1.29s`
  - `2019-06-01`: `348,375` rows, total `7.16s`
  - `2021-05-24`: `3,510,578` rows, total `53.36s` (`write 45.62s`)
- Progress/Next: Worst-case daily partition is now ~53s in this benchmark; next step is server concurrency sweep (`15/20/30`) and live resource telemetry to set operational backfill concurrency.

### Entry 031
- I tried: Ran official ClickHouse docs pass focused on bulk inserts, async inserts, and insert strategy constraints.
- I found: The docs emphasize large client-side batches and avoiding excessive insert overhead, with async inserts intended mainly for high-frequency small insert patterns.
- Progress/Next: Validate our runtime against these assumptions using live system metrics before applying more code-level changes.

### Entry 032
- I tried: Queried live ClickHouse runtime metrics/settings (`system.disks`, `system.settings`, `system.parts`, `system.metrics`) on the currently targeted host.
- I found: Current benchmark target is local ClickHouse (`CLICKHOUSE_HOST=localhost`) with severe constraints (`max_threads=auto(5)`, only ~`2.11 GiB` free of `58.37 GiB`, no async insert active), which invalidates expectations for high-throughput server benchmarking.
- Progress/Next: Stop treating local benchmarks as production throughput truth; benchmark and tune on the server ClickHouse runtime (where hardware/disk layout is intended for backfill scale).

### Entry 033
- I tried: Studied current official ClickHouse docs on bulk inserts and async inserts, plus settings references for insert-block formation and insert-thread behavior.
- I found: Core guidance is to minimize insert-query count and maximize batch size (`bulk-inserts`), with async inserts recommended when clients cannot batch; `wait_for_async_insert=1` is the safe production mode. Also, `max_insert_threads` only affects `INSERT ... SELECT` execution parallelism, not arbitrary client row serialization bottlenecks.
- Progress/Next: Treat Python-side serialization as primary suspect for our path; benchmark server-side SQL transformation path that shifts row shaping/UUID/hash work into ClickHouse execution.

### Entry 034
- I tried: Implemented a server-side transform path for Binance fast-mode canonical writes (`assume_new_partition`) that inserts only source columns into a temporary ClickHouse stage table, then performs `INSERT ... SELECT` into `canonical_event_log` with payload JSON, payload SHA256, and UUIDv5-compatible event-id generated in ClickHouse SQL.
- I found: This removes the dominant Python row-shaping bottleneck and drastically reduces client-side serialization load.
- Progress/Next: Validate contract parity (event-id and payload shape) and run integrity/contract gates before using this path in backfill operations.

### Entry 035
- I tried: Ran high-volume benchmark (`2021-05-24`, 3,510,578 rows) after switching fast-mode writer to server-side transform.
- I found: Stage timings were `download 6.35s`, `unzip 0.30s`, `parse 1.04s`, `integrity 0.24s`, `canonical write 10.57s` (previous fast-path write baseline was ~50s). This is ~4.8x faster in canonical write phase.
- Progress/Next: Run contract/integrity tests and then validate on server runtime with concurrency sweeps.

### Entry 036
- I tried: Ran gate-level validation after introducing SQL-transform fast path (`pyright`, `ruff`, and targeted contract/integrity suites including S34 backfill contracts and canonical writer contracts).
- I found: Type/style checks passed and targeted suites stayed green (`37 passed` across integrity + contract subsets run in this cycle).
- Progress/Next: Push this optimization path through server deployment, then run server concurrency sweep (`10/15/20`) with CPU/RAM/network/rows-per-second telemetry.

### Entry 037
- I tried: Verified UUIDv5 parity strategy in SQL by switching namespace input from `reinterpretAsString(toUUID(...))` to `unhex(namespace_hex)` and applying RFC4122 version/variant bit masking inside ClickHouse.
- I found: SQL-generated UUIDs now match Python deterministic UUIDv5 output for canonical idempotency keys.
- Progress/Next: Keep this implementation as the canonical SQL event-id path for fast Binance backfill mode.

### Entry 038
- I tried: Connected directly to the live host using available SSH access and validated active deployment paths/services.
- I found: Live stack is running at `/opt/origo/deploy` (not legacy tdw path), with healthy `api`, `clickhouse`, `dagster-webserver`, and `dagster-daemon`; deploy env currently pins `ORIGO_BACKFILL_PROJECTION_MODE=deferred`, `ORIGO_CANONICAL_FAST_INSERT_MODE=assume_new_partition`, `ORIGO_CANONICAL_RUNTIME_AUDIT_MODE=summary`.
- Progress/Next: Run concurrency sweeps against this live stack only; no local dry-run assumptions.

### Entry 039
- I tried: Executed S34 runner with custom backfill state/manifests from host shell env.
- I found: `docker compose exec` did not inherit host env overrides automatically; runner ignored intended run-state and restarted at `2017-08-17`, triggering fail-loud partition-nonempty errors.
- Progress/Next: Pass path envs explicitly with `docker compose exec -e ...` for every benchmark/backfill invocation.

### Entry 040
- I tried: Seeded benchmark run-state/manifests on host filesystem under `/opt/origo/deploy/storage/...`.
- I found: Containers read `/workspace/storage` volume, so host-seeded files were invisible inside `dagster-webserver`.
- Progress/Next: Seed and read benchmark artifacts strictly inside container-visible paths (`/workspace/storage/audit/...`).

### Entry 041
- I tried: Ran sanity benchmark (`concurrency=10`, partitions `2021-05-26..2021-05-27`) with isolated run-state and clean window.
- I found: Run completed successfully in `20s` for `4,534,501` rows (~`226,725` rows/s end-to-end), confirming corrected runner path + env wiring.
- Progress/Next: Move to multi-partition controlled sweeps.

### Entry 042
- I tried: Swept `concurrency=10,15,20,30` on a 10-partition window (`2021-05-26..2021-06-04`, `19,986,874` rows).
- I found: Durations were `31.949s (c10)`, `30.516s (c15)`, `30.499s (c20)`, `30.544s (c30)`.
- Progress/Next: Above `c10`, gains were flat because only `10` partitions were scheduled; run larger partition count to expose real concurrency scaling.

### Entry 043
- I tried: Re-ran scaling test on 40 partitions (`2021-05-26..2021-07-04`, `73,384,135` rows).
- I found:
  - `c10`: `106.713s` (~`687,675` rows/s)
  - `c15`: `95.379s` (~`769,380` rows/s)
  - `c20`: `97.014s` (~`756,417` rows/s)
  - `c30`: `99.472s` (~`737,743` rows/s)
  Real throughput peak is at `c15`; higher concurrency regressed slightly.
- Progress/Next: Set operational S34 exchange backfill concurrency default to `15` and keep runner overrideable for controlled experiments.

### Entry 044
- I tried: Captured live telemetry during sweeps (CPU/RAM/network + ClickHouse counters) at multiple snapshots.
- I found: Host memory remained safe throughout (typically `<20GB` actively used with `~230GB+` cache/available), network deltas tracked Binance download bursts, and ClickHouse memory tracking stayed well below system limits while insert workloads progressed.
- Progress/Next: Promote `c15` as default, then continue with full S34 backfill execution using this setting and keep telemetry checks running during long windows.

### Entry 045
- I tried: Interpreted ClickHouse `system.events.InsertedRows` during fast-path benchmarks as direct canonical row throughput.
- I found: `InsertedRows` includes both temporary stage-table inserts and canonical table inserts in the SQL-transform path, so event counter deltas are roughly `~2x` the canonical rows written.
- Progress/Next: Use manifest duration + canonical table row counts for throughput truth; use `InsertedRows` only as relative load indicator.

### Entry 046
- I tried: Shifted S34 execution focus to non-exchange datasets and launched live ETF (`python -m origo.scraper.etf_s4_07_backfill --ishares-start-date 2024-01-11 --end-date 2026-03-12`) plus FRED (`python /tmp/fred_backfill_live.py`) backfills in `deploy-dagster-webserver-1`.
- I found: Both processes are actively running and canonical partitions are advancing (`etf/etf_daily_metrics` and `fred/fred_series_metrics`), but these runs are ad-hoc and currently do not update `canonical_ingest_cursor_state` for resume control.
- Progress/Next: Keep monitoring live partition advancement, then move ETF/FRED into a contract-compliant S34 runner path so resume state and manifests are uniformly canonical.

### Entry 047
- I tried: Took timed live snapshots of canonical progression for ETF/FRED while the two non-exchange backfill processes were running.
- I found: Over ~70 seconds, ETF advanced `18 -> 20` canonical rows (max partition still `2024-01-12`) and FRED advanced `14 -> 16` rows (max partition `1948-02-01 -> 1948-04-01`), confirming ongoing ingestion but at slow visible partition progression.
- Progress/Next: Keep these runs active for now, continue short-interval monitoring, and prioritize moving ETF/FRED execution onto a canonical-cursor runner path for deterministic resume/progress accounting.

### Entry 048
- I tried: Replaced the previous slow FRED live process with a batch canonical-writer run (`/tmp/fred_backfill_batch_live.py`) that accumulates event inputs and commits through one `writer.write_events(...)` call.
- I found: The new batch FRED process is running in `deploy-dagster-webserver-1` while ETF backfill continues unchanged; FRED canonical counts are expected to stay flat until the batch commit point.
- Progress/Next: Wait for batch completion signal in `/workspace/storage/audit/fred-backfill-batch-live.log`, then verify step-change in canonical FRED coverage and resume with ETF monitoring.

### Entry 049
- I tried: Monitored post-switch canonical coverage while batch FRED and ETF runs were active.
- I found: FRED coverage jumped to `19,495` canonical rows with max partition `2026-03-11` (from low double-digit row counts previously), confirming the batch-writer path materially changed ingestion throughput; ETF continued forward progression (`max partition` advanced into `2024-01-17`).
- Progress/Next: Keep both runs active, wait for FRED process exit/final summary emission, and continue ETF progression monitoring before deciding whether ETF also needs an explicit batch-writer override path.

### Entry 050
- I tried: Validated FRED completion state directly from canonical events by grouping on `dimensions.series_id`, then terminated the lingering batch process once all four registry series showed full historical coverage.
- I found: FRED now has complete per-series ranges in canonical (`CPIAUCSL`, `DGS10`, `FEDFUNDS`, `UNRATE`) through expected latest partitions, and only ETF backfill remains actively running.
- Progress/Next: Treat FRED backfill as complete for S34-C6 data coverage, keep ETF running/monitored, and revisit whether ETF needs the same explicit batch-writer acceleration path.

### Entry 051
- I tried: Parallelized ETF backfill with five date-range shards using the existing `etf_s4_07_backfill` flow.
- I found: Shards failed with `WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT` because ETF payload provenance fields are run-volatile, while ETF persistence currently writes canonical events directly (and existing-day checks rely on `etf_daily_metrics_long`, which is not authoritative in deferred projection mode).
- Progress/Next: Avoid broad rerun collisions by switching to an iShares-only shard runner with canonical metric-id prechecks per partition.

### Entry 052
- I tried: Built and launched an iShares-only shard runner (`/tmp/etf_ishares_shard_backfill.py`) with canonical precheck logic and five non-overlapping date shards.
- I found: ETF canonical coverage resumed forward movement without immediate fail-loud identity conflicts; snapshot moved from `81 rows / 9 distinct days / max 2026-01-02` to `101 rows / 13 distinct days / max 2026-01-05` over roughly two minutes while shards were active.
- Progress/Next: Keep shard runners active, monitor distinct partition growth and shard completion artifacts, and then reconcile remaining ETF-day gaps.

### Entry 053
- I tried: Kept five iShares shard runners active and sampled ETF canonical coverage deltas during runtime.
- I found: ETF continued to advance under shard mode (`111 -> 131` rows, `13 -> 14` distinct partitions over ~2 minutes), with all five shard workers still active.
- Progress/Next: Continue runtime monitoring until shards finish and proof artifacts are emitted, then compute exact missing partition set for any final reconciliation pass.

### Entry 054
- I tried: Implemented a permanent ETF canonical idempotency fix by removing run-volatile provenance timestamps (`fetched_at_utc`, `parsed_at_utc`, `normalized_at_utc`) from ETF canonical payload construction in `origo/scraper/etf_canonical_event_ingest.py`.
- I found: This makes canonical payload hash stable for identical source-event identity across replay runs, aligning ETF behavior with strict exactly-once identity semantics.
- Progress/Next: Validate with targeted contract tests and then move this fix through PR/deploy so server backfills no longer need runtime precheck workarounds.

### Entry 055
- I tried: Added/ran ETF contract proof for timestamp-insensitive payload determinism (`tests/contract/test_etf_canonical_event_ingest_contract.py`) using uv-run isolated deps.
- I found: Targeted contract suite passed (`3 passed`), including new assertion that payload remains identical when provenance runtime timestamps differ.
- Progress/Next: Prepare PR for permanent ETF canonical idempotency fix and then deploy; keep live ETF shards running meanwhile.

### Entry 056
- I tried: Validated permanent ETF idempotency fix locally with style/type and targeted contract checks (`ruff`, `pyright`, and ETF canonical ingest contract tests).
- I found: Checks passed cleanly (`ruff: all checks passed`, `pyright: 0 errors`, `pytest: 3 passed`), confirming the canonical payload contract update and replay-stability assertion are consistent.
- Progress/Next: Keep ETF live shard backfill running until completion while preparing the permanent fix for PR/deploy, so future runs do not require runtime precheck workarounds.

### Entry 057
- I tried: Re-checked live deployment state and active ETF shard workers (`docker compose ps`, process scan in `deploy-dagster-webserver-1`, canonical counts from `origo.canonical_event_log`).
- I found: Runtime stack is healthy; five ETF shard workers are still active; ETF canonical coverage is still moving (`etf` rows advanced to `334`, partitions `33`, max partition `2026-01-09`), while FRED remains complete at `19,495` rows through `2026-03-11`.
- Progress/Next: Keep ETF progress under watch while shipping the permanent idempotency fix/deploy so reruns stop hitting payload-hash conflicts.

### Entry 058
- I tried: Inspected shard runner logs and process telemetry to verify whether shard failures were transient or ongoing.
- I found: Each shard log contains `WRITER_IDENTITY_PAYLOAD_HASH_CONFLICT`; logs are not actively appending, but worker CPU time is still high and canonical ETF rows keep increasing slowly, indicating expensive conflict-prone execution under old image code.
- Progress/Next: Prioritize deploy of the permanent ETF payload stabilization change and stop relying on ad-hoc shard precheck behavior for correctness/performance.

### Entry 059
- I tried: Completed code-level migration away from file-based backfill resume state in active runner path by switching S34 resume source to `canonical_ingest_cursor_state` and removing deploy/env wiring for `ORIGO_BACKFILL_RUN_STATE_PATH`.
- I found: Active runner contract now reads last completed partition from canonical cursor state, reports `resume_state_source=canonical_ingest_cursor_state` in dry-run output, and no longer mutates JSON run-state in runtime flow.
- Progress/Next: Keep legacy run-state helper only for historical contract-proof scaffolding until full cleanup slice, then enforce canonical-cursor-only resume everywhere.

### Entry 060
- I tried: Ran full local validation for changed files after resume-source + ETF/FRED canonical-writer updates (`ruff`, `pyright`, and targeted contract suites for ETF canonical ingest and S34 runner/orchestrator contracts).
- I found: Gates passed (`ruff clean`, `pyright 0 errors`, `13/13 contract tests passed`), including new proofs for canonical resume lookup and ETF payload determinism under runtime provenance timestamp drift.
- Progress/Next: Package these changes into PR/deploy so server execution uses canonical-cursor resume and stable ETF canonical payload semantics.

### Entry 061
- I tried: Took a timed live ETF throughput sample over 60 seconds against `origo.canonical_event_log` while five iShares shard workers remained active on server.
- I found: ETF canonical rows increased `405 -> 413` in one minute (`+8 rows/min`), confirming continued forward movement but materially below target operational speed.
- Progress/Next: Keep current workers running until deploy change is merged, then restart ETF backfill on the patched runtime to remove conflict drag and re-measure throughput.

### Entry 062
- I tried: Applied PR-review fixes from `zero-bang` and `copilot` by (1) adding missing canonical-cursor helper tests (happy path + earliest-boundary rejection), (2) updating dry-run CLI semantics/help to state live ClickHouse dependency, and (3) removing stale “Run-state” wording from partition-planner error text.
- I found: Updated contract tests pass (`12/12`) and style checks on touched files remain clean.
- Progress/Next: Push review-fix commit, re-request review, and proceed through merge cycle once approval is granted.

### Entry 063
- I tried: Started post-deploy OKX S34 run with canonical-cursor resume and observed immediate fail-loud stop (`canonical_ingest_cursor_state max(partition_id) must not be empty`).
- I found: Root cause is query semantics, not corrupted data: ClickHouse `max(String)` on empty result set returns `''` (empty string), which tripped the non-empty cursor guard for fresh datasets.
- Progress/Next: Patch cursor lookup to `maxOrNull(partition_id)`, add regression test for query contract, deploy, and re-run OKX backfill.

### Entry 064
- I tried: Re-ran OKX S34 start after deploying the `maxOrNull` cursor fix.
- I found: Backfill progressed past cursor load and failed at canonical fast insert guard because one OKX daily file produced two UTC event-day partitions (`2021-08-31`, `2021-09-01`) while fast mode requires one canonical partition per batch.
- Progress/Next: Make OKX daily-file canonical partition deterministic by pinning canonical partition id to the selected source day (`partition_date_str`) during write and projection dispatch; keep event timestamps untouched.
