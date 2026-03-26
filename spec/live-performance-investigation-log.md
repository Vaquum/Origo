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

### Entry 065
- I tried: Started Bybit S34 smoke on live runtime after OKX was unblocked and captured the first hard failure payload.
- I found: Root cause is parser contract drift, not infra: Bybit historical CSV can contain UUID `trdMatchID` values (for example `08ff9568-cb50-55d6-b497-13727eec09dc`), while ingest parser accepted only `m-<digits>`.
- Progress/Next: Patch Bybit parser to accept both first-party source formats (`m-<digits>` and canonical UUID), keep fail-loud behavior for anything else, and add contract tests before redeploying and re-running Bybit smoke.

### Entry 066
- I tried: Implemented Bybit parser compatibility patch and executed targeted local contract verification (`ruff`, new Bybit parser contract tests, Bybit historical API contract subset, and fast-insert guardrail contract tests).
- I found: Verification passed (`ruff clean`, parser contract `3/3`, fast-insert contract `7/7`, historical Bybit contract subset `3/3`); parser now handles UUID `trdMatchID` deterministically instead of failing on valid source rows.
- Progress/Next: Ship patch through PR/deploy pipeline, then re-run live Bybit S34 smoke and continue full exchange backfill progression.

### Entry 067
- I tried: Processed review feedback on PR #59 and traced the reported correctness risk through canonical idempotency-key construction.
- I found: Reviewer concern is valid: deriving canonical source offset from `trade_id` can collide between UUID and `m-<digits>` rows when `trade_id` overlaps, which can corrupt exactly-once semantics.
- Progress/Next: Fix canonical identity by using raw `trd_match_id` as `source_offset_or_equivalent`, switch Bybit native projector fetch order to `ingested_event_id`, add a contract test proving mixed-ID rows generate distinct canonical identities, and re-run gates.

### Entry 068
- I tried: Replaced S34 live backfill correctness state with ClickHouse-backed proof/manifests/quarantine tables and removed file-backed run-state from the active runner path.
- I found: Canonical correctness is now expressed in one place: source manifests plus partition proofs in ClickHouse. Fresh partition execution can fail closed on `proved_complete`, `quarantined`, `canonical_written_unproved`, or raw canonical rows without terminal proof.
- Progress/Next: Finish moving runner execution off `execute_in_process` and onto Dagster-submitted partition runs so orchestration no longer depends on a custom worker path.

### Entry 069
- I tried: Audited the new proof path against live exchange source contracts while wiring Dagster-run tags into daily exchange assets.
- I found: There was a correctness bug in Bybit proofing: source-proof identity used numeric `trade_id` with `offset_ordering='numeric'`, while canonical ingest correctly used raw `trd_match_id`. That would have produced false proof mismatches and invalid no-miss claims.
- Progress/Next: Keep `trd_match_id` as the canonical Bybit offset, treat Bybit as `lexicographic` ordering for proof purposes, and lock this with a contract test.

### Entry 070
- I tried: Replaced the S34 runner execution path so it now only submits/polls Dagster partition runs and validates terminal ClickHouse proof state per partition.
- I found: This preserves explicit active-run concurrency while removing the last direct execution path (`execute_in_process`) from exchange backfill orchestration. Exchange assets now read `projection_mode`, `execution_mode`, and `runtime_audit_mode` from Dagster run tags instead of per-run environment mutation.
- Progress/Next: Run the broader contract suite around the new state machine and then close out the slice proof/guardrail artifacts once docs and env contracts are aligned.

### Entry 071
- I tried: Added an explicit reconcile execution path to the S34 Dagster submit/poll runner instead of overloading the normal resume planner.
- I found: The original runner could label a run as `reconcile` but still only plan from `last_completed_partition`, which meant a drifted partition could not actually be targeted for repair. The runner now requires explicit `--partition-id` selection for reconcile mode and refuses reconcile on clean partitions.
- Progress/Next: Keep reconcile as a first-class repair mode only, and make exchange assets enforce the same state-machine rules so manual Dagster runs cannot bypass them.

### Entry 072
- I tried: Moved the proof-state gate into the Binance, OKX, and Bybit daily Dagster assets instead of trusting runner preflight alone.
- I found: This closes a real bypass risk. A manual Dagster partition run could previously have skipped the runner checks and still attempted to write. The assets now fail-loud on completed partitions, fail-loud on ambiguous partitions, and stamp `reconcile_required` before raising when backfill hits a repair-only state.
- Progress/Next: Validate the new reconcile/state-machine behavior with contract tests and then align the deploy/spec layer with the now tag-driven execution contract.

### Entry 073
- I tried: Cleaned the active deploy/spec contract away from the old env-driven fast-insert/projection model and reran the targeted Slice 34 validation suite.
- I found: Active truth is now coherent: runtime semantics are driven by Dagster run tags, not deploy env switches. Targeted validation passed with `ruff`, `py_compile`, strict-scope `pyright`, and the S34 contract suite (`30 passed`).
- Progress/Next: The hardening tranche is now materially in place; the remaining Slice 34 work is the actual full-history backfills, projection rebuild, end-to-end serving proofs, and slice closeout artifacts/docs.

### Entry 074
- I tried: Closed `S34-C2f` by proof-gating projector runtime start on terminal partition proofs and by making promoted ETF/FRED serving verify exact partition-set equality between terminal proof coverage and both native/aligned projector watermark coverage.
- I found: This removed the last soft spot in the Slice 34 hardening tranche. Projection rebuild can no longer advance on non-terminal partitions, and serving promotion is no longer just an env flip. Validation passed with repo-wide `pyright` (`0 errors`), targeted `ruff`, focused historical/API contract suites (`85 passed`), and the broader Slice 34 regression suite (`46 passed`).
- Progress/Next: The hardening tranche is now complete through `S34-C2f`. The next active frontier is executing the real full-history backfills in order, starting with `S34-C3` for Binance under the hardened runner.

### Entry 075
- I tried: Processed PR #63 review findings and the failed `style-gate` by fixing the `rights.py` disconnect exception masking bug, tightening numeric offset ordering in source/canonical proof generation, explicitly pinning the private-API Dagster dependency contract to `1.12.17`, and cleaning the exported import surface ordering in `origo/events/__init__.py`.
- I found: The review surfaced two genuine correctness issues and one explicit dependency-risk contract gap. All are now covered: disconnect errors no longer mask `RightsGateError`, numeric proof digests/first-last/gap checks are computed in true numeric order, and the private Dagster API dependency is now documented and version-pinned instead of implicit. Validation passed with repo-wide `ruff`, repo-wide `pyright`, historical/API contract suite (`77 passed`), and Slice 34 regression suite (`49 passed`).
- Progress/Next: Push the review-fix commit, answer the PR threads with the exact changes, and wait for fresh CI/re-review so the hardened runtime can deploy before Binance full-history backfill starts.

### Entry 076
- I tried: Started the first real `S34-C3` Binance full-history run on the deployed hardened runtime (`cd31b7c8b9ddc222d6419bc0fb7f313d522da1e7`) with `projection_mode=deferred`, `runtime_audit_mode=summary`, and reconcile-first execution against the live server stack.
- I found: Normal backfill failed closed immediately because live Binance canonical data already existed without any terminal proof rows. The live footprint is `1,763` Binance partitions in `canonical_event_log` (`2017-08-17 -> 2026-03-12`) with zero prior proof rows, so the correct next action was explicit reconcile, not blind backfill. The first reconcile batch (100 partitions, concurrency `20`) is now live and healthy: Dagster accepted the runs, the daemon is saturating the configured active-run ceiling of `20`, and ClickHouse proof state has started advancing (`source_manifested=24`, `canonical_written_unproved=4`, `proved_complete=4` at the latest sample). Live resource snapshot during the heavy 2021 partitions: ClickHouse `650.62%` CPU / `7.1 GiB` RAM, Dagster daemon `793.45%` CPU / `97.58 GiB` RAM.
- Progress/Next: Let the first 100-partition reconcile batch finish, then continue reconciling the remaining ambiguous Binance partitions in batches until terminal proof coverage exists for the full legacy canonical footprint. Only after that should normal Binance backfill resume from terminal proof state.

### Entry 077
- I tried: Audited the live proof-state anomaly directly against ClickHouse by querying `canonical_backfill_partition_proofs` in revision order for Binance partitions under active reconcile, then compared that with latest-state aggregates and active Dagster run tags.
- I found: Terminal proof writing is working correctly. Live partitions do progress `source_manifested -> canonical_written_unproved -> proved_complete`, and the earlier “missing terminal proof” signal was a misleading partial snapshot taken while many partitions were still mid-flight. At the current sample, Binance latest proof states are `proved_complete=66` and only `14` non-terminal partitions remain, all of them in the active frontier (`2021-07-21 -> 2021-08-05`). Active Dagster runs are consistent with that frontier and all belong to the same reconcile control run `s34-reconcile-binance-2017-08-17-2021-08-22`.
- Progress/Next: Keep the current reconcile batch running to completion, then re-sample terminal proof coverage and decide whether to launch the next explicit reconcile batch or resume normal backfill if ambiguity is fully cleared for the legacy footprint in scope.

### Entry 078
- I tried: Took a fresh live status sample for the Binance reconcile run, querying latest proof-state counts, terminal coverage range, active Dagster runs, and current resource usage on the server.
- I found: The first reconcile batch is still progressing cleanly. Latest Binance proof states are now `proved_complete=80`, `source_manifested=13`, and `canonical_written_unproved=1`. Terminal proof coverage has advanced contiguously through `2021-08-04`, and the remaining visible in-flight frontier is `2021-07-26`, `2021-07-28`, and `2021-08-05 -> 2021-08-16`. Dagster still has `20` active Binance runs, including `2021-08-21` and one queued `2021-08-22`, all under the same reconcile control run `s34-reconcile-binance-2017-08-17-2021-08-22`. Resource snapshot: Dagster daemon `927.06%` CPU / `76.01 GiB` RAM, Dagster webserver `163.59%` CPU / `791.2 MiB` RAM, ClickHouse `459.97%` CPU / `11.24 GiB` RAM.
- Progress/Next: Let this reconcile batch complete, then check whether a terminal range proof was recorded. If it was not, continue with the next explicit reconcile batch; if the full targeted ambiguity window is cleared, switch back to normal backfill from terminal proof state.

### Entry 079
- I tried: Measured per-daily-file runtime from live Binance proof timestamps by taking the delta between `source_manifested` and terminal proof (`proved_complete`) for completed partitions in the current reconcile wave.
- I found: Current live daily-file runtime is still measured in minutes, not seconds. Across `53` completed daily partitions from the active Binance run, the proof-based durations are: average `326.5s` (`5.4 min`), median `297s` (`5.0 min`), `p90=414s` (`6.9 min`), minimum `216s` (`3.6 min`), and maximum `660s` (`11.0 min`). Recent examples: `2021-08-22=234s`, `2021-08-21=279s`, `2021-08-20=275s`, `2021-08-06=464s`, `2021-07-26=660s`.
- Progress/Next: Treat the current live run as functionally correct but still materially off the target throughput bar. The next optimization push has to attack the remaining per-file minutes by focusing on Dagster orchestration overhead and ClickHouse insert/write-path throughput together, not correctness logic.

### Entry 080
- I tried: Ran a formal server-side perf decomposition for Binance using three lenses together: (1) live run/proof timing from the active reconcile wave, (2) ClickHouse `system.query_log` analysis on the live writer path, and (3) a scratch benchmark on `2021-08-06` (`2,223,356` rows) that executed the real source fetch/parse/integrity path plus both fresh-insert and duplicate-writer canonical paths against an isolated scratch database.
- I found: The bottlenecks are decisively in our Python path, not in ClickHouse. On the live reconcile wave, average `pre-write` time (run start -> `source_manifested`) is about `64.9s`, and average `source_manifested -> canonical_written_unproved` is about `326.6s`; note that `proved_complete` currently reuses the pre-proof timestamp, so proof-tail time is hidden inside the run tail rather than visible as a separate proof timestamp delta. The live ClickHouse query log over the last 30 minutes shows `41,318` `writer_fetch_existing` selects totaling `3,490.449s` (average `84.478ms` each), while proof-state reads/writes are negligible. The scratch benchmark on `2021-08-06` showed: checksum `0.281s`, download `3.79s`, extract `0.637s`, parse `1.211s`, integrity `0.369s`, and `build_binance_partition_source_proof` alone `28.75s`. Fresh fast insert took `37.831s` at `58,770.8 rows/s`, but ClickHouse query log for that same fast path shows the actual database work was only `1.89s` for stage insert plus `2.243s` for canonical insert-select. That means roughly `33.7s` of the fast path is Python-side frame materialization / preparation before ClickHouse ever becomes the bottleneck. For the isolated duplicate path, ClickHouse logged `1,483` duplicate-fetch selects totaling `75.773s` (average `51.094ms`), and query-log timestamps show duplicate-mode wall time of about `176.7s` even without live Dagster contention. The remaining roughly `100s` is Python-side row construction, duplicate classification, and audit work.
- Progress/Next: The optimization target is now clear and rooted in code, not infra tuning. Next changes should be: `1.` Reconcile fast path must skip the canonical writer entirely when canonical rows already exist and prove directly against `canonical_event_log`; `2.` source-proof construction must move off Python per-row hashing into a SQL/vectorized path over staged source data; `3.` fresh insert must stop converting Polars columns into giant Python lists before stage load and instead feed staged source data directly into ClickHouse. After those three changes, rerun the same formal perf benchmark and hold the line until daily partitions are back in the seconds range.

### Entry 081
- I tried: Implemented the first optimization tranche directly in the runtime code instead of doing more speculative tuning. The Binance daily asset now stages raw CSV into ClickHouse via the existing HTTP client path, builds source proof from staged SQL material instead of Python row loops, skips duplicate-writer replay entirely when reconcile sees existing canonical rows, and uses staged insert-select for the fast empty-partition path. In parallel, `CanonicalBackfillStateStore.compute_canonical_partition_proof_or_raise` was moved from Python row fetch/sort/hash into ClickHouse-side proof aggregation so reconcile proof itself no longer drags millions of rows back through Python.
- I found: This closes the three concrete bottlenecks identified in Entry 080 at the code-path level: source proof, duplicate reconcile, and canonical proof are no longer fundamentally Python-bound on the hot Binance path. The implementation also kept the fail-closed proof contract intact: normal backfill still requires empty partitions for fast insert, reconcile still never blindly rewrites canonical truth, and terminal proof still comes only from source-vs-canonical evidence. Local validation on the touched files passed with `ruff` and strict `pyright` (`0 errors, 0 warnings, 0 informations`).
- Progress/Next: Run the formal live/server-side benchmark again on the same Binance path, measure fresh-insert and reconcile timing end-to-end, and compare directly against the Entry 080 baseline. If the live runtime is still not in the seconds range, keep iterating on the remaining insert-path cost until it is.

### Entry 082
- I tried: Extended the new reconcile proof-only behavior to the OKX and Bybit daily exchange assets as well, so all three exchange backfill assets now stop routing already-populated partitions through duplicate-writer replay. I also reran lint/type validation over the touched exchange assets plus the shared backfill/proof modules.
- I found: Exchange behavior is now consistent: Binance gets the staged/raw fast path plus SQL proofing, while Binance/OKX/Bybit all share the same reconcile rule that existing canonical rows are proved directly instead of re-written. Validation stayed clean after the expansion: `ruff` passed and strict `pyright` reported `0 errors, 0 warnings, 0 informations` on the touched exchange/backfill files.
- Progress/Next: Move to live/server benchmarking immediately and sample the real Binance reconcile path first, because that is where the largest measured loss was. If the live reconcile time collapses as expected, then benchmark the fresh empty-partition fast path next.

### Entry 083
- I tried: Ran the most relevant local proof bundle for the optimization tranche before pushing toward live benchmarking: `tests/contract/test_backfill_numeric_ordering_contract.py`, `tests/contract/test_exchange_fast_insert_guardrails_contract.py`, and `tests/contract/test_s34_backfill_orchestration_contract.py`.
- I found: One contract test needed to be updated because the fake ClickHouse client was still emulating the old row-by-row canonical proof query instead of the new SQL-aggregated proof row. After updating the fake to compute the aggregated proof result deterministically, the targeted proof bundle passed cleanly (`18 passed`). This means the new SQL-side proof path is now covered rather than silently bypassed in tests.
- Progress/Next: Push to the live server path and benchmark the deployed Binance reconcile/fresh-insert runtime against the Entry 080 baseline. The next decision point is purely measured: either the real runtime collapses into the seconds range, or we keep optimizing the remaining insert path.

### Entry 084
- I tried: Ran one broader validation pass because the optimization changed a shared proof primitive in `origo/events/backfill_state.py`, not just the Binance asset. The checks were repo-scoped strict `pyright` plus the backfill contract cluster: `tests/contract/test_backfill_runtime_contract.py`, `tests/contract/test_backfill_projection_coverage_contract.py`, and `tests/contract/test_s34_backfill_contract.py`.
- I found: The shared proof/runtime surface stayed stable after the optimization tranche. Strict `pyright` passed repo-wide (`0 errors, 0 warnings, 0 informations`), and the broader backfill contract cluster passed cleanly (`9 passed`). That gives enough confidence to stop iterating locally and move to the deployment/live-benchmark step.
- Progress/Next: Commit the optimization tranche, open the PR, get it through review/merge, and then measure the deployed Binance backfill path on the server against the Entry 080 baseline.

### Entry 085
- I tried: Deployed PR `#64` to the live server (`7e80b2f667a2a65d9c209d9a8d9c4b4d9b66bd45`), verified the running container images, and re-audited the live Binance canonical/proof frontier directly in ClickHouse before starting the new perf proof pass.
- I found: The deploy succeeded cleanly and the live data footprint is stable. `origo.canonical_event_log` holds `5,265,530,776` Binance spot-trade events covering `2017-08-17 -> 2026-03-12` across `1,763` daily partitions. Terminal proof coverage is contiguous for the first `100` Binance partitions only, from `2017-08-17 -> 2021-08-22`, and the first missing-proof legacy partition is `2021-08-23` with `2,143,309` canonical rows. Binance daily source files also exist upstream for `2026-03-13`, `2026-03-14`, and `2026-03-25`; `2026-03-26` was not yet published at the time of the check.
- Progress/Next: Use `2021-08-23+` for the live reconcile proof run, and use `2026-03-13` only in an isolated scratch write benchmark so the live contiguous proof frontier is not broken by a far-ahead proved partition.

### Entry 086
- I tried: Ran the formal live reconcile perf proof on the deployed runtime against real legacy Binance partitions with existing canonical rows and no terminal proof state: `2021-08-23`, `2021-08-24`, and then a 5-partition batch `2021-08-25 -> 2021-08-29`, all through the Dagster-native S34 runner with `execution_mode=reconcile`, `projection_mode=deferred`, and `runtime_audit_mode=summary`.
- I found: Reconcile is no longer in the minutes range. Single-partition live runs completed in `26s` wall (`2021-08-23`) and `24s` wall (`2021-08-24`), but the proof rows show that the actual source-manifest and terminal-proof writes happen inside the same second once the Dagster run is executing (`2021-08-23`: `12:39:29.898 -> 12:39:29.969`, `2021-08-24`: `12:40:43.493 -> 12:40:43.550`). ClickHouse query log shows the heavy proof queries themselves at only `~1.5-2.0s` each (`source_proof`: `1696ms` / `1508ms`, `canonical_proof`: `2024ms` / `1583ms`) with row counts exactly matching canonical truth and `gap_count=0`, `duplicate_count=0`. The operationally meaningful number came from the 5-partition batch: `2021-08-25 -> 2021-08-29` finished in `33s` wall, or about `6.6s/day` including runner startup, Dagster submission/poll, download, source proof, canonical proof, and range proof. During that batch, live resource snapshot was: Dagster daemon `493.68%` CPU / `2.952 GiB` RAM, ClickHouse `10.19%` CPU / `9.917 GiB` RAM, and the 3-minute ClickHouse query-log rollup was only `73` selects totaling `7300ms` plus `12` inserts totaling `1156ms`.
- Progress/Next: Treat reconcile as back in the target class operationally and shift the remaining optimization focus to the fresh empty-partition write path, where the next bottleneck must be outside ClickHouse because live reconcile no longer shows database-bound behavior.

### Entry 087
- I tried: Ran a real server-side fresh-write benchmark against an isolated scratch database (`origo_perf_bench`) for Binance day `2026-03-13` so the live proof frontier would not be disturbed. The benchmark used the real source artifact, real parse/integrity path, real staged source-proof path, and real staged `INSERT ... SELECT` canonical writer path against a cloned `canonical_event_log` schema.
- I found: Fresh write improved substantially, but it still has one major remaining tail outside ClickHouse. For `5,312,490` rows on `2026-03-13`, the measured stages were: checksum fetch `2.263s`, zip download `4.219s`, zip SHA `0.071s`, extract `0.978s`, CSV SHA `0.719s`, parse `2.720s`, integrity `0.702s`, stage insert `1.523s`, source proof `4.940s`, and `canonical_write_seconds=39.905s`. The scratch target row count matched exactly (`5,312,490`). ClickHouse query log proves the database is not spending `39.9s` on the write: stage `INSERT ... FORMAT CSV` took `1201-1238ms`, source-proof SQL took `4939-4996ms`, canonical `INSERT ... SELECT` took `4110-4203ms`, and the post-write min/max offset query took `36ms`. That leaves roughly `35s` of the reported `canonical_write_seconds` outside ClickHouse. Inspection of `write_staged_binance_spot_trade_csv_to_canonical()` shows the only meaningful post-insert work left in Python is the canonical runtime-audit append, and `ImmutableAuditLog.append_events()` still re-validates the entire existing audit chain on every append by replaying the file from disk under lock. That full-chain validation is now the leading suspected root cause for the remaining fresh-write tail.
- Progress/Next: The next optimization tranche should attack the immutable runtime-audit append path directly. The measured target is now explicit: keep the staged source proof and ClickHouse insert path as-is, and eliminate the `~35s` audit-chain validation overhead from fresh writes without weakening the audit immutability contract.

### Entry 088
- I tried: Reworked canonical runtime-audit storage from one monolithic shared immutable chain into partition-scoped immutable chains under the configured audit root, while leaving `ImmutableAuditLog` itself unchanged. The runtime audit now resolves one bounded hash-chained JSONL sink per `(source_id, stream_id, partition_id)` scope, and both direct contract readers and the Slice-14 runtime-audit proof were updated to read the scoped file explicitly.
- I found: This preserves the critical audit guarantees on the hot path that actually matters to backfill correctness: audit append is still immutable, fail-loud, JSONL, and cryptographically chained, but the chain being revalidated on append is now the scoped partition chain instead of the entire global history. Local validation stayed clean after the contract shift: `ruff check .` passed, strict repo `pyright` passed (`0 errors, 0 warnings, 0 informations`), and the affected runtime-audit / writer / backfill contract cluster passed (`28 passed`) including `test_canonical_event_writer_query_chunking_contract.py`, `test_exchange_fast_insert_guardrails_contract.py`, `test_s34_exchange_backfill_runner_contract.py`, `test_s34_backfill_contract.py`, `test_backfill_runtime_contract.py`, `test_canonical_runtime_audit_contract.py`, and `test_immutable_audit_log.py`.
- Progress/Next: Push this optimization through CI/deploy and rerun the exact same live Binance fresh-write benchmark on `2026-03-13` to see whether the remaining `~35s` tail disappears on the server.
