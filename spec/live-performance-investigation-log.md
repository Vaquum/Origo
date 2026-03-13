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
