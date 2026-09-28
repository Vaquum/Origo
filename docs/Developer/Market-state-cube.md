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
  - The HTTP client opens no ClickHouse session. The statements share no state, and ClickHouse
    releases a session only after its answer is sent. A statement sent at once after the
    previous answer failed `SESSION_IS_LOCKED` in 3 of 800 back-to-back pairs on 25.3.2.39,
    and in 2 of 12 local runs of the acceptance tests, answering the query `500`.
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

  Any other restart of a container during the run, or a container missing from either host
  snapshot, fails criterion `S`. A `503 busy` answer fails `Q3`.

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

## Detail component — slice #480

[PRD-0023](https://github.com/Vaquum/Origo/issues/478) adds trade-by-trade path length and
dwell, base volume and trade prices per base cell. They live in their own component pair,
`market_state_detail` and `market_state_detail_latest`, in the activation group
`market_state_detail`, applicable from 2021-01-01. The PRD-0022 components, their statements
and hashes are unchanged. Amendment A02 of #462 records the changed locked rows.

`build_market_state_detail` reads the same private `raw` or `raw_latest` table as the cube:

| Column | Type | Meaning |
| --- | --- | --- |
| `time_index`, `price_index` | UInt64 | The cube's base cell, from exactly the cube builder's expressions |
| `first_event_at` | DateTime64(6) | The earliest trade, move or held instant of the cell inside its partition; the validation time column |
| `trade_count` | UInt32 | Trades in the cell |
| `base_volume` | UInt64 | Satoshis, `round(quantity × 1e8)` |
| `path_length` | UInt64 | Cents the trade path travelled inside the cell |
| `dwell` | UInt32 | Microseconds the price held inside the cell |
| `high`, `low` | Float64 | The cell's extreme trade prices as stored |
| `first_trade_at/id/price`, `last_trade_at/id/price` | DateTime64(6), UInt64, Float64 | The cell's first and last trade by trade ID |

A cell without trades stores zeros for its counts, volume and prices, and `first_event_at` as
its trade times; the query reads prices only from cells with trades.

- **Path.** Prices convert to cents by `round(price × 100)`. Each move between consecutive
  trades, in (datetime, trade ID) order, belongs to the later trade's column and is split
  across the half-open 125-USDT rows it passes.
- **Dwell.** Each price holds from its trade to the next. The partition's last price holds
  until its end, and its first trade's price also covers its start. Every base column's dwell
  therefore sums to its time inside the partition. A partition without trades has no dwell.
- **One pass.** Each trade aggregates into its own cell. A move or hold that leaves the cell
  is kept as a descriptor and expanded into the other cells afterwards, so the work per trade
  stays constant. The builder runs with 1 thread and 2 GiB, as the cube's does.
- **Identities.** After the insert the build fails as `COMPONENT_CONTENT_INVALID` if:
  - a price is more than 0.01 from whole cents, or a quantity from whole satoshis, or a
    trade's row from its price disagrees with its row in cents;
  - a trade ID does not increase with (datetime, trade ID) order;
  - the cells' trade counts or satoshis differ from the raw partition's;
  - their path differs from Σ |Δ cents| of the raw trades;
  - any base column's dwell differs from its time inside the partition.

A provisional minute is measured from its own trades in `raw_latest`, which keeps REST's
milliseconds. Until its day replaces it, it lacks the move into its first trade, credits its
start to its first trade's row, has no dwell without trades, and its times are milliseconds.

**Rollout.** Deployment creates both detail revisions tables and current views when the new
image starts, and `origo.sources.rollout` enables the group on every deploy, so there is no
off switch: a builder failure is fixed forward. Every fresh partition then builds the detail
component, and native reconciliation attaches it to accepted days and minutes in the slots
repairs leave, as for #469: about 2,092 days at four a minute, about nine hours. Each attach
activates the same revision and build at the next generation, so while it runs every result's
`state_token` and pins change although no product does. Capacity evidence is keyed to the
enabled groups, so the first backfill after deployment runs with the capacity probe.

**Rollback floor.** This release is the binary rollback floor once a detail key is activated:
`python -m origo.sources.compatibility` refuses every image that does not declare the detail
components, and such an image's `enabled_groups()` fails on the enabled group. Native rollback
to a retained older generation stays supported by this image, but reconciliation attaches the
missing detail again, so it is no off switch either.

**Monitoring.** Each detail component has the generic projection observation in `/law` and
Dagit, `UNKNOWN` with `component_proof_missing` until the partition it reads holds the key.
The laws accept the detail keys as optional on both lanes; M1 and M2 keep judging the
PRD-0022 cube, and no law is added. Protocol v1 of #476 counts every physical `market_state`
table in R4, so R4 fails once this release has created the detail tables.

**Queries.** A request with `measures` pins only partitions holding both components, runs
PRD-0022's cells statement unchanged beside a statement over the detail component, and merges
the two cell streams. With `path_length` or `dwell`, its automatic price extent comes from the
detail rows. Detail sums stay integers until the service divides each once, correctly rounded
above 2⁵³. Results with measures carry `schema_version` 2; requests without them run exactly
the v3.27.2 statements and write the v3.27.2 schemas and metadata.

The new captures are contiguous row ranges of official archives, 0-based and stop-exclusive:
2023-03-24 [587331, 589331) around the 9,157 s halt; 2021-05-19 [2855000, 2861000), the
−2,023 USDT move and a row eight moves crossed without a trade; 2025-10-10 [7426500, 7464072),
a minute boundary where the row changes and a burst of 24,433 trades at one timestamp with a
+3,005 USDT move. Inside its traded span each yields exactly its whole day's moved-through
cells.

```sh
pytest tests/origo_source_native/test_market_state_detail.py -q
pytest tests/origo_source_native/test_market_state_query.py -q -k "measures or column_prices or divide_exactly or keep_their"
pytest tests/origo_source_native/test_market_state_registration.py -q -k detail
```

Pre-merge evidence on 2026-09-28, in a disposable ClickHouse 25.3.2.39. The lifecycle built
2026-02-05, the busiest day (15,364,010 trades), from its official archive, served unchanged with
its checksum sidecar. Memory is the query log's `memory_usage`, under the builder's 1 thread
and 2 GiB:

- **Fresh**, both groups enabled: the whole build took 97 s. The detail insert took 3.2 s at
  8.1 MiB, and its checks 1.8 s at 4.0 MiB.
- **Native upgrade** of the day built with the cube only: 28 s, the same revision and build at
  generation 2, with no archive request. The detail insert took 5.3 s at 99.6 MiB, and its
  checks 4.2 s at 95.4 MiB.
- **Busiest minute**, 20:15 (42,387 trades), as a provisional partition: the detail insert took
  100 ms at 4.2 MiB.

At production size, a request without measures runs the v3.27.2 statements at the same cost.
Over ten alternating runs of each, history to 2024-06-29 at the finest grid (4.3 million cells)
took a median 2.28 s against 2.33 s on v3.27.2. The whole history at 3,600 s × 1,000 USDT took
1.36 s against 1.40 s. With every measure, the request to 2024-06-29 took 5.7 s against 1.7 s
without measures in the same process, and wrote 526 MB against 207 MB. Its detail statement
peaked at 1.90 GiB of the 4 GiB limit.

Native GUI acceptance on 2026-09-28 used the committed, checksum-proven 2025-01-01 capture in a
disposable ClickHouse/Dagster environment:

- v3.27.2 retained the day at generation 1 with the cube.
- This image's compatibility check, bootstrap and rollout then enabled the detail group.
- Every sensor and schedule was stopped on the Automation page, so that only the operator acted.
- Selecting the day in the native partition bar and launching one run attached the detail
  component at generation 2.
- Native **Re-execute all** kept generation 2.

Both runs succeeded in about 4.5 seconds, and both read only the archive's checksum sidecar.
SQL confirmed all eight earlier hashes, the revision and the build unchanged, with one raw build
and one receipt per component. The 1,539 detail cells accounted for all 12,000 captured trades,
their 123.48987 BTC and 667.76 USDT of path, and 1,536 columns held 86,400 s of dwell. The cube's
15 cells kept their 12,000 trades and 5,310 taker buys. This does not establish production
capacity or complete historical coverage.
