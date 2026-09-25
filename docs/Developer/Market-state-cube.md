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
- **Query settings.** Every statement runs with 4 threads, 4 GiB, a spill to disk at 2 GiB
  for aggregation and sorting, and 60 s. The full-history base cells statement needs
  0.7–1.3 GiB and takes 1.3 s on 4 threads (1.9 s on 2). The pin statement runs under the
  same settings, and every statement's `log_comment` is its result ID.
- **Exact streaming sums.** Row sums and totals are summed exactly while the cells stream:
  integer mantissas per (row, exponent), rounded once. That is `math.fsum`'s result, and the
  memory no longer grows with the result. On a real 4.3 M-cell result it took 0.19 s where
  the lists of Python floats took 0.74 s.
- **Health probe.** The self-probe reads the whole answer, so the service no longer logs its
  own probes as disconnected clients.
- **Phase logs.** Each published query logs its pin, price-extent, SQL, write, validation and
  publication times, and the service's peak RSS.

The protocol is frozen in #476 and in `tools/benchmark_market_state.py` (`cases()`,
`STAGES`, `PAIRS`, `THRESHOLDS`, `TOLERANCE`, `NUMERIC_CASES`, `CONTENTION_SERIES`);
`test_frozen_protocol_matches_the_slice` holds them to the issue.
- **Corpus.** 16 cases cover the current tail, a day, a month, a year, two years, the PRD's
  original rectangle and full history, at base and coarser independent resolutions, with
  explicit and omitted bounds and one empty rectangle.
- **Stages.** A (cold), B (5 warm rounds), C (two concurrent streams of 3 rounds) and D (two
  full-history base requests at once, three times).
- **Criteria.**
  - Latency: the nearest-rank p90 of all A–C samples ≤ 3 s, a failure counting as infinite.
    Full history at base: a median ≤ 5 s and a maximum ≤ 10 s. No request fails.
  - Numerical: the cube against a raw-trade reference over the same pinned builds.
    - Cell sets and counts are exact.
    - Volumes are within max(1e-8, 1e-12 × ref) USDT.
    - Totals and POCs are bit-exact against `math.fsum` from `cells.arrow`.
  - Resources: the service's peak RSS ≤ 1 GiB, no spill, and `staging/` empty.
  - Contention: every minute of the listed live series lands during the run, with a p50
    landing lag at most 2 s above the hour before.
  - The run's queries are visible in the receipts and in Dagit.

### Acceptance run

On `37.27.112.167` after the deploy, as root. The client container needs no ClickHouse
credentials. The raw reference and the evidence run through `clickhouse-client` in the
ClickHouse container, and the reference reads with direct I/O so it leaves the page cache to
the other services. Set `SHA` to the deployed merge SHA.

```sh
IMAGE="ghcr.io/vaquum/origo-dagster:$SHA"
RUN="$HOME/market-state-acceptance/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN"
host_facts() {
  echo "captured_at=$(date -u +%FT%T.%6NZ)"
  for name in clickhouse market-state; do
    echo "${name//-/_}_started=$(docker inspect -f '{{.State.StartedAt}}' "tdw-control-plane-$name-1")"
  done
  echo "image=$(docker inspect -f '{{.Config.Image}}' tdw-control-plane-market-state-1)"
  echo "cpu=$(lscpu | sed -n 's/^Model name: *//p')"
  echo "cpus=$(nproc)"
  echo "memory_bytes=$(free -b | awk '/^Mem:/ {print $2}')"
  echo "kernel=$(uname -r)"
  lsblk -dn -o NAME,SIZE,MODEL | sed 's/^/disk=/'
  echo "root_filesystem=$(df -B1 --output=size,used,avail / | tail -1)"
  echo "results_volume_bytes=$(du -sb /var/lib/docker/volumes/tdw-control-plane_market-state/_data | cut -f1)"
}
consumer() {
  docker run --rm --pull never --network host -v tdw-control-plane_market-state:/mnt/cube:ro \
    -v "$RUN":/acceptance "$IMAGE" python tools/benchmark_market_state.py "$@" --run /acceptance --mount /mnt/cube
}
```

1. `host_facts > "$RUN/host_before.txt" && consumer client` runs stages A–D, about five minutes.
2. `docker exec -i tdw-control-plane-clickhouse-1 clickhouse-client --format ArrowStream < "$RUN/reference.sql" > "$RUN/reference.arrow"`
   is the raw reference, about ten minutes on 2 threads.
3. `docker exec -i tdw-control-plane-clickhouse-1 clickhouse-client < "$RUN/evidence.sql" > "$RUN/evidence.jsonl"`
4. `docker logs tdw-control-plane-market-state-1 > "$RUN/service.log" 2>&1 && docker exec tdw-control-plane-dagster-1 python tools/benchmark_market_state.py backfill > "$RUN/backfill.json"`
5. `host_facts > "$RUN/host_after.txt"`
6. `consumer verdict` writes `report.json` and `report.md` and prints `PASS` or
   `FAIL <criteria>` last. Post `report.md` on #462 unedited.
7. **Recovery.** Tell the operator first: this raises the monitor's receipt-failure and
   error-log alerts once each.
   - Run: `docker run --rm --pull never --network host "$IMAGE" python -c "from origo.query.market_state_reader import query; query()" & sleep 1; docker restart tdw-control-plane-market-state-1; wait`
   - Expected:
     - the client fails with a connection error;
     - `docker logs --since 2m tdw-control-plane-market-state-1 2>&1 | grep -c 'interrupted by the previous process: 1'` prints `1`;
     - `ls /var/lib/docker/volumes/tdw-control-plane_market-state/_data/staging | wc -l` prints `0`.
   - After the next tick, the latest `binance_spot_trades:query` receipt is `FAILED` with
     `EXPORT_INTERRUPTED`, and a one-day request returns 200.
8. **Expiry.** 24 hours and 2 minutes after the verdict, the following prints `0 0`: no
   result of the run remains on disk or in `lifecycle.sqlite`.

   ```sh
   echo "$(ls /var/lib/docker/volumes/tdw-control-plane_market-state/_data/results | grep -c -F -f "$RUN/result_ids.txt") $(docker exec -i tdw-control-plane-market-state-1 python -c "import sqlite3, sys; ids = set(sys.stdin.read().split()); print(sum(1 for (r,) in sqlite3.connect('file:/opt/origo/market-state/lifecycle.sqlite?mode=ro', uri=True).execute('SELECT result_id FROM results') if r in ids))" < "$RUN/result_ids.txt")"
   ```

A run is void, and repeated, if ClickHouse or `market-state` starts inside it. The verdict's
`V` criterion checks this. The run directory stays on the host.
