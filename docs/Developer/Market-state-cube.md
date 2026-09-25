# Market state cube delivery

[PRD-0022](https://github.com/Vaquum/Origo/issues/462) is the locked product specification.
Review concerns implementation correctness, performance, resource use and proof.
Only an explicit operator amendment changes L01–L17.

## Base projection — slice #466

`origo.sources.profiles.market_state` supplies two component declarations over one
logical base projection. Slice #469 registers both in the live spot source,
with applicability from 2021-01-01 and a persisted `market_state` activation group.
The base projection and its operational lifecycle do not yet deliver the query
API, Arrow file lifecycle or whole-history performance acceptance.

The builder reads its build-owned, validated `raw` or `raw_latest` table and writes
the corresponding precreated `market_state` or `market_state_latest` table.
The source runtime owns staging, component validation and activation. Failed
build output must never be activated or reused as a completed attempt.

| Column | Type | Meaning |
| --- | --- | --- |
| `time_index` | UInt64 | Floor of normalized UTC microseconds since 2021-01-01 divided by 56,250,000 |
| `price_index` | UInt64 | Floor of recorded price divided by 125 USDT |
| `first_trade_at` | DateTime64(6) | Earliest actual constituent trade timestamp |
| `volume` | Float64 | Sum of recorded `quote_quantity` in USDT |
| `trade_count` | UInt32 | Individual trade count |
| `taker_buy_volume` | Float64 | USDT volume where `is_buyer_maker = 0` |
| `taker_buy_trade_count` | UInt32 | Individual taker-buy count |

The primary key is `(time_index, price_index)`. All trades before the history
start are excluded. The recorded provider integer timestamp is not used for
membership because its unit changes between archives. Source normalization owns
that conversion; this builder uses the normalized datetime only.

A global cell can begin before its owning provisional minute. `first_trade_at`
is inside the owning interval and therefore remains the component's validation
time column. Existing source revision, build and partition metadata identifies
the contribution. Adjacent accepted minute contributions to the same global key
must be summed once; canonical replacement must supersede them. Both physical
component tables store the same base resolution. No coarser grids or raw copies
are introduced by this module.

Counts use ClickHouse `accurateCast(..., 'UInt32')`, rejecting overflow before
narrowing. Post-build count sums explicitly widen to UInt64 and must equal both
the eligible raw count and the eligible raw taker-buy count.

## Numerical proof

The base aggregation uses `sumKahan` and `sumKahanIf`, with one aggregation thread
and input ordered by normalized datetime and trade ID. Results retain Float64
roundoff; different build/block layouts do not promise identical final bits.
Stored component hashes still validate the exact retained contents.

Before implementation, slice #466 fixed independent volume comparison to
`math.fsum` over the existing Float64 quote quantities, with absolute tolerance
**1e-8 USDT** and relative tolerance **1e-12**. Compensated summation reduces
accumulation error; these thresholds allow low-order differences between direct
and fragment reductions on the authentic fixture magnitudes. Keys, counts,
taker side and eligibility permit no tolerance. Whole-history acceptance must
separately justify its thresholds before its first benchmark; these fixture
checks cannot establish whole-history numerical or performance acceptance.

Tests use checksum-verified, unchanged Binance CSV/REST records already committed
in the repository. Independent keys use original timestamp units and Decimal
prices, including authentic exact 125-USDT edges. Scalar conversion-limit probes
test UInt32 arithmetic without fabricating market records. Both legacy content
hashes and current binary content hashes remain supported; schema_version stays 1.

Run the first slice's engine checks with:

```sh
pytest tests/origo_source_native/test_market_state_projection.py -q
```

## Registration and operational rollout — slice #469

Deployment creates both revisions tables and current views while the persisted
component group remains off. Every build reads the flag under the shared heavy
maintenance fence; the enable operation holds that fence exclusively. Deployment
recreates Dagster, Dagit and the spot provisional worker, checks that their former
containers (and child writers) retired, then runs `origo.sources.rollout`.
No operator toggle, source configuration or separate projection launch is required.
The rollout marker is stored in `source_component_rollout_log`; a missing row is off.

This release is the binary rollback floor after activation. Pre-compatible images
reject expanded component inventories and must not be deployed after enablement.
Deployment enforces the floor: before any container is replaced, the new image runs
`python -m origo.sources.compatibility`, which fails when production's activations
name a source or component the image does not declare.
Native rollback to a retained legacy generation remains supported by this release:
old product hashes are accepted and the cube view hides the unselected addition.

Existing accepted days and minutes gain only their missing projection. The upgrade
holds the existing partition and maintenance fences, validates retained products,
reads a private view over retained raw, and preserves the original revision,
build identity, product rows and hashes. It adds a generation selecting the new
component hash. No provider refetch or second raw copy is required.
Completed, validated additions are reused after interruption; only never-activated
incomplete additions may be removed and rebuilt. The current view requires the
component hash in the selected activation, so staged additions stay invisible.

Native canonical reconciliation selects accepted days missing enabled components
in the batch slots that changed or failed partitions leave, so the history upgrade
never delays a repair; Dagster backfill, selected gaps and retries use the same lifecycle. The provisional
worker handles fresh minutes first, then a bounded batch of missing accepted
minutes. Existing consumer readiness remains independent of incomplete cube
coverage. Upgrade failures are visible as `component_upgrade` failures with scope
`NONE`, leaving valid existing products available. Capacity evidence is keyed to
the enabled component footprint, and admission/build share the maintenance fence.

Cube-only activation updates publication metadata without rebuilding unchanged
bar exports. Mount and Hugging Face renderers retain the full source-state tokens
for monitoring and also fingerprint the component inputs they actually consume.
Matching inputs preserve monthly Parquet files, Arrow links and Hugging Face
version directories/upload evidence. A legacy manifest can establish equality
only through an actual retained activation whose full token matches its evidence.
Missing proof or changed inputs takes the normal render path. Publication checks
canonical inputs again before commit and retains the original pinned provisional
records; it cannot claim minutes that arrived during rendering.

The existing monitor evaluates **M1** (current cube proof, raw/trade and taker-count
agreement, 180-second freshness) and **M2** (calendar coverage from 2021-01-01).
The same law tape, Sources catalog, `/law` view and alert path carry these results.
Historical gaps remain visible even while current ingestion is healthy; deployment
and registration alone do not establish complete historical coverage.

Run the real-engine operational checks with:

```sh
pytest tests/origo_source_native/test_market_state_registration.py -q
```

Native GUI acceptance on 2026-09-24 used the committed, checksum-proven
2025-01-01 capture in a disposable ClickHouse/Dagster environment. Selecting the
retained day in the native partition bar and launching one run upgraded generation
1 to 2; native **Re-execute all** kept generation 2. Both runs succeeded in about
six seconds. SQL confirmed all seven original hashes and the build/revision were
unchanged, with one raw build and one cube receipt: 15 cells accounted for all
12,000 captured trades and 5,310 taker buys. Capacity sampling was controlled test
evidence; this does not establish production capacity or complete historical coverage.

## Local query service — slice #474

`python -m origo.workers.market_state_api` runs as the Compose service `market-state` on
`127.0.0.1:8486`. The consumer contract (request, response, files, expiry and errors) is in
[Market state cube queries](../Reference/Market-state-cube-queries.md).

- **One pinned state per request.** `origo.query.market_state.pin` reads
  `source_current_partitions`, the SQL form of `SourceStore.records()` (0.19 s instead of
  3.66 s in production). Cube coverage starts at 2021-01-01 and ends at the first day
  without `market_state` or minute without `market_state_latest`.
  - The query reads only those `(partition_key, revision, build_id)` identities, passed as
    clickhouse-connect external tables.
  - It then takes the shared `heavy` fence and checks `source_cleanup_log` for the builds it
    read. A hit discards the result.
  - The fence is never held during a read, so cleanup, rollback and component enablement
    never wait for a query.
- **Result storage.** `origo.query.market_state_results.ResultStore` records each result in
  `lifecycle.sqlite` before its staging directory exists.
  - It publishes by an fsynced rename.
  - After a restart it rolls interrupted steps back or forward.
  - It retires a file 24 hours after its last read through the cube reader.
  - Admission keeps free disk above the largest source capacity reserve plus 8 GiB, and
    results within 64 GiB.
- **Monitoring.** One cleanup receipt and at most one aggregated query receipt per minute go
  to `worker_minute_log` (feed `market_state_api`). The live asset
  `market_state_query_service` is in Dagit, and the monitor expects the service's heartbeat
  from first start.

Run its real-engine checks with:

```sh
pytest tests/origo_source_native/test_market_state_query.py tests/origo_source_native/test_market_state_results.py tests/origo_source_native/test_market_state_api.py -q
```

The tests read three committed, checksum-verified captures of official archive rows:

- 2021-01-01 00:57:11–01:00:00, 2,366 trades. This includes column 62, row 231, the earliest
  taker-free base cell in the production cube.
- The first 3 minutes of 2021-01-02, 3,243 trades.
- The first minute of 2021-01-03, 1,830 trades.

The test source anchors at 2021-01-01, the fixed history start. Their volume tolerances hold
only for these captures.

No authentic equal-volume POC tie was found. On 2026-09-25 a search of the full history
covered both volume measures, row resolutions up to 1000 USDT and windows of up to 64
columns. The lower-row rule is therefore checked against an independent reference, not an
observed tie.

## Measured acceptance — slice #476

Exploratory runs against the deployed service on 2026-09-25 set the tuning:
- **Query settings.** Every statement runs with 4 threads, 4 GiB and 60 s. The full-history
  base cells statement needs 0.7–1.3 GiB and takes 1.3 s on 4 threads (1.9 s on 2).
  - Nothing spills to disk: a statement that outgrows its memory fails, so temporary data
    never takes the disk that result admission keeps for ingestion.
  - The pin and the admission floor run under the same settings, and each carries
    `log_comment` = the result ID. Only the source's shared-mount check, inside the source
    lifecycle, stays unattributed.
- **Exact streaming sums.** Row sums and totals are summed exactly while the cells stream:
  integer mantissas per (row, exponent), rounded once. That is `math.fsum`'s result, and the
  memory no longer grows with the result. On a real 4.3 M-cell result it took 0.19 s where
  the lists of Python floats took 0.74 s.
- **Health probe.** The self-probe reads the whole answer, so the service no longer logs its
  own probes as disconnected clients.
- **Phase logs.** Each published query logs its pin, price-extent, SQL, write, validation and
  publication times, and the service's lifetime peak RSS.

The protocol is frozen in #476 and in `tools/benchmark_market_state.py`;
`test_frozen_protocol_matches_the_slice` holds every constant, the generated SQL and the Dagit
read to it.
- **Corpus and stages.** 16 cases (C01–C16) in stages:
  - A: cold, one request per case;
  - B: 5 warm rounds;
  - C: two concurrent streams of 3 rounds, in ID order and in reverse, each in its own process;
  - D: two full-history base requests started together, three times.
- **Verdicts.** `verdict` prints `INTERIM PASS`, `FAIL <criteria>` or `VOID <causes>`.
  `finalize` adds recovery and expiry and prints the final `PASS`. PRD-0022 closes only on a
  final `PASS`.
- **A void run is repeated and reported. Only an external cause voids it:**
  - a deploy overlapping the run or its baseline, evidenced by `deploy_on_merge`;
  - another consumer's query in the window;
  - cleanup or component rollout for the source;
  - a native Dagster backfill overlapping the run or its baseline;
  - a feed worker or ClickHouse start within the hour before the run.

  Any other restart of a container during the run fails criterion `S`.

### Acceptance run

The run needs these conditions:
- on `37.27.112.167`, as root;
- after the deploy, and at least 60 minutes after ClickHouse and every feed worker last started;
- outside 00:00–01:30 UTC;
- a quiet window announced to downstream consumers.

The client container needs no ClickHouse credentials. The raw reference and the evidence
run through `clickhouse-client` in the ClickHouse container, and the reference reads with
direct I/O, so the other services keep their page cache. Set `SHA` to the deployed merge SHA.

```sh
IMAGE="ghcr.io/vaquum/origo-dagster:$SHA"
VOLUME=/var/lib/docker/volumes/tdw-control-plane_market-state/_data
RUN="$HOME/market-state-acceptance/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
host_facts() {
  echo "captured_at=$(date -u +%FT%T.%6NZ)"
  for name in clickhouse dagster market-state provisional-worker provisional-binance-spot-aggtrades \
      provisional-binance-perp-aggtrades provisional-binance-perp-trades depth-worker; do
    docker inspect -f "${name}_started={{.State.StartedAt}}
${name}_image={{.Image}}
${name}_restarts={{.RestartCount}}
${name}_oom_killed={{.State.OOMKilled}}
${name}_exit_code={{.State.ExitCode}}" "tdw-control-plane-$name-1"
  done
  echo "cpu=$(lscpu | sed -n 's/^Model name: *//p')"
  echo "cpus=$(nproc)"
  echo "memory_bytes=$(free -b | awk '/^Mem:/ {print $2}')"
  echo "kernel=$(uname -r)"
  lsblk -dn -o NAME,SIZE,MODEL | sed 's/^/disk=/'
  echo "root_filesystem=$(df -B1 --output=size,used,avail / | tail -1)"
  echo "results_volume_bytes=$(du -sb "$VOLUME" | cut -f1)"
}
consumer() {
  docker run --rm --pull never --network host -v tdw-control-plane_market-state:/mnt/cube:ro \
    -v "$RUN":/acceptance "$IMAGE" python tools/benchmark_market_state.py "$@" --run /acceptance --mount /mnt/cube
}
```

0. **Fresh service.** `docker restart -t 0 tdw-control-plane-market-state-1`, then wait for
   `healthy`. The run then starts with a new process, so the peak RSS it reports belongs to the
   run. The client refuses the frozen corpus between 00:00 and 01:30 UTC.
1. `host_facts > "$RUN/host_before.txt" && consumer client` runs stages A–D, about five minutes.
2. The raw reference takes about ten minutes on 2 threads; this is an estimate.

   ```sh
   docker exec -i tdw-control-plane-clickhouse-1 clickhouse-client --format ArrowStream < "$RUN/reference.sql" > "$RUN/reference.arrow"
   ```
3. The evidence:

   ```sh
   docker exec -i tdw-control-plane-clickhouse-1 clickhouse-client < "$RUN/evidence.sql" > "$RUN/evidence.jsonl"
   ```

   Run it at least 15 minutes after step 1 ends, which the reference normally covers, so late
   depth minutes have landed.
4. The service log and the backfill figures:

   ```sh
   docker logs tdw-control-plane-market-state-1 > "$RUN/service.log" 2>&1
   docker exec tdw-control-plane-dagster-1 python tools/benchmark_market_state.py backfill \
     --since "$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['started_at'])" "$RUN/run.json")" > "$RUN/backfill.json"
   ```

   Then, from a checkout with `gh`, record every deploy that could overlap the run and copy
   the file into `$RUN/deploys.json`:

   ```sh
   gh run list --repo Vaquum/Origo --workflow deploy_on_merge.yml --limit 20 --json createdAt,updatedAt,headSha,conclusion > deploys.json
   ```
5. `host_facts > "$RUN/host_after.txt"`
6. `consumer verdict` writes `report.json`, `report.md` and `report.part<N>.md`, and prints the
   interim verdict last. Then record the expiry anchor, the last access of the run's results:

   ```sh
   docker exec -i tdw-control-plane-market-state-1 python tools/benchmark_market_state.py expiry < "$RUN/result_ids.txt" > "$RUN/expiry_due.txt"
   ```
7. **Recovery.** Tell the operator first: this raises the monitor's `receipt_failed` and
   `error_logs:market-state` alerts once each. The script:
   - waits for a registered result to appear in `staging/`, then kills the service at once with
     SIGKILL, because `python` runs as PID 1 and ignores SIGTERM;
   - records the interrupted result ID;
   - after one tick, records the log line, `staging/`, the lifecycle rows, the latest query
     receipt and a follow-up one-day request.

   ```sh
   before="$(ls "$VOLUME/staging")"
   docker run --rm --pull never --network host "$IMAGE" python -c "from origo.query.market_state_reader import query; query()" > "$RUN/recovery_client.txt" 2>&1 &
   client=$!
   for attempt in $(seq 200); do
     interrupted="$(comm -13 <(echo "$before") <(ls "$VOLUME/staging") | head -1)"
     [ -n "$interrupted" ] && break
     sleep 0.05
   done
   [ -n "$interrupted" ] && docker restart -t 0 tdw-control-plane-market-state-1
   wait $client; echo "client_exit=$?" > "$RUN/recovery.txt"
   echo "interrupted=$interrupted" >> "$RUN/recovery.txt"
   sleep 90
   echo "interrupted_logged=$(docker logs --since 3m tdw-control-plane-market-state-1 2>&1 | grep -c 'interrupted by the previous process: 1')" >> "$RUN/recovery.txt"
   echo "staging_after=$(ls "$VOLUME/staging" | wc -l)" >> "$RUN/recovery.txt"
   echo "$interrupted" | docker exec -i tdw-control-plane-market-state-1 python tools/benchmark_market_state.py expiry | sed -n 's/^lifecycle_rows=/lifecycle_rows=/p' >> "$RUN/recovery.txt"
   echo "receipt=$(docker exec -i tdw-control-plane-clickhouse-1 clickhouse-client --query "SELECT concat(status, ' ', error_code, ' ', error) FROM origo.worker_minute_log WHERE feed = 'market_state_api' AND series = 'binance_spot_trades:query' ORDER BY recorded_at DESC LIMIT 1")" >> "$RUN/recovery.txt"
   echo "follow_up_status=$(docker run --rm --pull never --network host "$IMAGE" python -c "from origo.query.market_state_reader import query; query(t1='2026-09-01T00:00:00Z', t2='2026-09-02T00:00:00Z'); print(200)")" >> "$RUN/recovery.txt"
   ```

   If no registered result appeared, `finalize` reports the step as not performed, and it is
   repeated. It is never read as a service failure.
8. **Expiry.** At the anchor's `last_access` plus 24 hours and 2 minutes, run:

   ```sh
   docker exec -i tdw-control-plane-market-state-1 python tools/benchmark_market_state.py expiry < "$RUN/result_ids.txt" > "$RUN/expiry.txt"
   ```

   Nothing may read the run's results in between. The recovery step's results are not in
   `result_ids.txt`, and each keeps its own clock.
9. `docker run --rm --pull never -v "$RUN":/acceptance "$IMAGE" python tools/benchmark_market_state.py finalize --run /acceptance`
   writes `report-final.*` and prints the final verdict last. Post the `report-final.part<N>.md`
   files on #462 in order, unedited. Each part opens with the SHA-256 of the whole
   `report-final.md`.

The run directory stays on the host.
