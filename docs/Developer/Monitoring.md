# Monitoring

Origo has one detector, the monitor worker (`origo.workers.monitor`), one pane, Dagit,
and one place where each fact lives. This document is the contract for all three; the
tests in `tests/origo_source_native/test_monitor.py` hold its headings and rules.

## One truth, one pane, one detector

- **One truth.** Every operational fact has exactly one authoritative store. Dagster's
  own storage holds runs, materializations, observations, sensor and schedule state and
  asset check evaluations. ClickHouse holds worker receipts (`origo.worker_minute_log`),
  source failures (`origo.source_failure_log`) and the container log
  (`origo.container_log`). The workers' heartbeat files hold liveness. The monitor's append-only law tape records its evaluated evidence,
  with original identities and times; it does not replace those authoritative stores.
- **One pane.** Dagit is where an operator looks first. The external asset
  `origo_monitor` carries the seven checks the monitor evaluates every minute; the live
  feed assets (`<source>_provisional_feed`) carry a
  five-minute freshness policy the daemon evaluates without a run. If a fact is not
  visible in Dagit, it is visible in ClickHouse; the monitor's e-mail says which.
- **One detector.** The monitor runs outside the Dagster process, as its own Compose
  service with its own heartbeat, so it keeps working when the daemon, the queue or the
  webserver is the failure. Its operational state is a cursor for notification and historical-import positions. It writes every finding into
  Dagit as a check evaluation before it sends an e-mail, and the e-mail says whether that
  write succeeded. The authorized `/law` summary reads only the monitor tape; there is no second detector or alert path.

## The feed workers

The minute paths run outside the Dagster queue as Compose services with heartbeats,
one tick per minute, restarted by the watchdog when a tick hangs:

- Each enabled source has its own provisional worker (`origo.workers.provisional`),
  selected by the deployment's `ORIGO_PROVISIONAL_SOURCE`. Each process has its own
  watchdog and `provisional_<source>.heartbeat`, so slow work in one source cannot
  prevent another source's next tick. The worker admits the newest closed minute
  before historical catch-up and continues repairing the first coverage gap. It then
  publishes every consumer that pins provisional rows (`mount`) when the
  pinned state changed, unless a backfill owns publication (the same rule as the
  consumer sensors: an active backfill holds publication; a terminal verdict never
  does, and canonical readiness is checked next). A failing minute, and a failing
  publication of one pinned state, is
  retried with a doubling delay from one minute up to the source's `retry_delay`.
  Required minute and worker mount work remains automatically retryable after repeated
  failures; failures stay visible in receipts. Late daily archives do not stop
  provisional coverage across midnight; canonical activation replaces those minutes.
  Canonical-only consumers
  (`huggingface`) keep their sensors. Mount sensors admit only an open `RENDER_DEFERRED`
  failure through the dedicated publication job, with full-history permission and the
  existing retry budget keyed to canonical state and the last recovered failure event.
  Repeated failures retain that budget; a new deferral after recovery gets a new budget. The worker yields that consumer while
  its job is outstanding and never waits on a held consumer lock.

The monitor expects a heartbeat for every enabled provisional source, including one
that has never started, and ignores the retired shared `provisional.heartbeat`.

Raw-perp capture is an independently observed collector (`perp-capture`,
`origo.workers.perp_capture`). The monitor expects `perp_capture.heartbeat` before
first start. `workers_alive` reports a missing/stale process; `collectors_serving`
reads `/opt/origo/heartbeats/perp_capture.status.json` once per tick, capped at
16 KiB, with no spool scan, database read or provider call. It distinguishes absent
or malformed status, stale durable capture, missing overlap, provider backoff and
storage pressure. Status publication or a fresh heartbeat cannot make stale durable
capture healthy. Capture status is the authoritative local acquisition fact; the
existing R1/data_current reader law remains independent and red while readers lag.
The status file lives on the shared heartbeat volume; the monitor never mounts the
capture spool. Capture failures use the existing Dagit checks, logs and alert path.

The superseded native books, legacy depth worker and obsolete daily BTC publishers
are retired. Their jobs, assets and source declarations are absent. Deployment
refuses active old book work and any reappearing old worker container. The monitor
ignores their five heartbeat names and makes no request to old depth endpoints.
Historical law frames, source activation/component/failure records, worker receipts
and existing evidence retention (including the14-day container-log TTL) remain intact.
Retired condition groups do not carry forward into current incidents.

The final Spot depth20 archive is verified locally before the retirement deployment.
Existing source setup removes exactly the36 obsolete table/view objects. After
verification, `deploy/retire_legacy_book_files.sh` removes only the old Spot/perp
spools, depth20/depth200 Arrow directories, five heartbeats and two capture status
files. It preserves the entire `render-top20-v1` cache, new Spot Arrow and bar files.
No depth200 or perpetual history is archived.

Render top20 sources run as `provisional-binance-spot-top20` and
`provisional-binance-perp-top20`, using the existing provisional worker. They import the newest
closed minute before bounded cursor catch-up; hourly jobs reconcile retained A/B
originals. The monitor makes one authenticated `/status` request per replica per
tick, concurrently, with five-second deadlines and16-KiB response bounds. Each
market must be durably ready with original receive, durable observation, exchange
and commit clocks less than five seconds old and no more than one second ahead.
Fresh HTTP delivery does not refresh those clocks. Replica/market failures use
the existing `collectors_serving` check, law tape and notification path; the
independent replica remains separately observed. These probes make no Binance
requests. Disabled definitions perform no external status I/O.

`deploy/render_top20_deploy.py` captures retained worker identities, restart counts,
start times and accepted frontiers before deployment. It requires both qualified
top20 workers LIVE and running. Native-only deployment replaces only Dagster, Dagit,
monitor and law; the13 retained workers, including both top20 workers, keep their
identities and advance. The two old native book frontiers must remain exactly frozen.
Warm-up rollback is removed. After verification, the obsolete floor directory and
its image tag are removed. Later top20 adapter updates use deliberate full releases;
ordinary native-only deployments retain their worker image bindings.

The market state query service (`origo.workers.market_state_api`, Compose service
`market-state`) is an observed worker of the same kind:
- **Heartbeat.** It touches `market_state_api.heartbeat` at start and after every tick. The
  monitor expects that heartbeat from first start, so the first deploy can raise one
  `heartbeat_stale:market_state_api` before the container starts.
- **Tick.** Each tick expires result files idle for 24 hours and probes the service's own
  `GET /healthz`. A server that stops answering exits for a restart.
- **Receipts.** Each tick writes one `binance_spot_trades:cleanup` receipt. When queries
  ended since the last tick, it also writes one aggregated `binance_spot_trades:query`
  receipt. That receipt:
  - is `FAILED` if any export failed or was interrupted, else `REJECTED` if any was refused
    (busy, source maintenance, client gone), else `OK`;
  - carries the counts per code in `error`.
- **Dagit.** Every tick materializes `market_state_query_service`.
- **Logs.** Each published query logs two INFO lines:
  - `market state result <id>` with its cells, bytes and the pin, price-extent, SQL, write
    and validation times;
  - `market state query <id> published` with the publication time, the total time and the
    service's peak RSS.

  Every ClickHouse statement of the query carries `log_comment` = `<id>` in
  `system.query_log`.

Receipt reconciliation is scoped to the source: another source's long-running attempt
cannot be marked dead by a faster worker.

Each processed minute and each publication writes one row to `origo.worker_minute_log`
(`feed`, `series`, `minute`, `rows`, `sha256`, `duration_ms`, `status`, `error_code`,
`error`); a publication row's `rows` is the number of pinned partitions and its `sha256`
the pinned state token. Every tick reports the feed's live asset to Dagit with the tick
minute, the counts and the worker's memory, so the asset's freshness policy is the
feed's liveness in the pane and the monitor's `workers_alive` is what alerts.

## Where each fact lives

| Fact | Where | Read it with |
| --- | --- | --- |
| Is Dagster up, are the daemons healthy, how deep is the queue | Dagster instance | `origo_monitor:dagster_reachable`, `origo_monitor:queue_bounded` in Dagit; GraphQL `instance.daemonHealth` |
| Did a run fail, which partition | Dagster run storage | Runs view in Dagit; the monitor's `run_failure:<job>` finding names the partitions and runs |
| Did an asset check fail | Dagster event log | The asset's Checks tab; the monitor's `check_failed:<asset>:<check>` finding |
| Is a worker alive | `/opt/origo/heartbeats/<feed>.heartbeat` | `origo_monitor:workers_alive`; `python -m origo.workers.<feed> --check` |
| Is a feed current | Dagster event log (the live feed asset's freshness state) | The asset's freshness in Dagit; `origo_monitor:workers_alive` for the worker behind it |
| Is every public consumer publishing the current state | The consumer manifests under `/opt/origo/shadow` against `origo.source_active_partitions` | `origo_monitor:publication_current`; a stale consumer names its published and state ends |
| What did a worker do for a minute | `origo.worker_minute_log` | `SELECT * FROM origo.worker_minute_log WHERE minute = ...` |
| Did market state queries succeed, and were idle results reclaimed | `origo.worker_minute_log` (feed `market_state_api`) and the service's `lifecycle.sqlite` | `market_state_query_service` in Dagit; `origo_monitor:workers_alive` for `FAILED` receipts |
| Why did a source build or publication fail | `origo.source_failure_log` | `binance_spot_trades_failure_sensor` output in Dagit; the table itself |
| What did a container print | `origo.container_log` (14 days) | `SELECT * FROM origo.container_log WHERE service = ... ORDER BY timestamp` |
| Are the Render collectors serving | Each replica's authenticated `/status` | `origo_monitor:collectors_serving` |
| What did the monitor send and when | `origo_monitor` check metadata and the e-mail | The asset's Checks tab; the mailbox |

## Investigation order

1. **Dagit first.** Open the `origo_monitor` asset: its seven checks name the failing area
   and the finding keys. Open the failed run or the failed check it names. For a source,
   read `binance_spot_trades_failure_sensor` and the source's reconciliation state.
2. **ClickHouse second.** Read `origo.worker_minute_log` for the minute, then
   `origo.source_failure_log` for the partition, then `origo.container_log` for the
   service around the timestamp. The detail is there; Dagit keeps the summary.
3. **Docker third**, only when the container log holds no rows for the service: the
   container did not start, or Vector is down. `docker compose ps` and `docker compose
   logs --tail 200 <service>` on the host.
4. **The external collectors last.** `origo_monitor:collectors_serving` distinguishes a
   silent collector from a silent worker; probe the collector's `/history` endpoint for
   the last completed minute before touching the worker.

Production trade ingestion sets `ORIGO_TRADE_MAINTENANCE=1`. Separate processes of
`origo.workers.provisional --role publication` own spot/perpetual file publication;
`--role repair` owns perpetual bridge repair, historical-minute fallback and spool
reclamation on .144. Capture retains .140. Local/default workers keep inline behavior.
The existing monitor requires `trade_publication_binance_spot_trades`,
`trade_publication_binance_perp_trades` and `perp_repair_binance_perp_trades` heartbeats
before their first start. Their receipt feeds are `trade_publication` and `perp_repair`;
only ingestors report the live-feed asset and whole `source:tick` ingestion duration.
An exclusive spool read lock defers ingestion with `CAPTURE_BUSY`, before a build starts.
Bridge rejection retries preserve witnesses, revalidate fresh bytes and retain at most
eight rejected responses of at most 1 MiB each. A cleared error alone is never proof.
File paths, schemas, Arrow single-batch format and Parquet outputs stay unchanged.

### Trade maintenance rollback

Before restoring the previous image **and Compose file**, retire only the three
maintenance services. Run from the deployment directory with its existing environment
and `PROJECT_NAME`; stop/remove failure refuses rollback. Preserve capture, outputs,
spool, limiter and all named volumes. Retire their heartbeats only after every process
is gone, so the previous monitor cannot discover permanently stale role members.

```bash
set -euo pipefail
for service in publication-binance-spot-trades publication-binance-perp-trades perp-repair; do
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$ids" ]; then
    docker stop $ids </dev/null
    docker rm $ids </dev/null
  fi
  remaining="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$remaining" ]; then
    echo "Maintenance retirement incomplete: $service; rollback refused" >&2
    exit 1
  fi
done
docker compose -p "$PROJECT_NAME" -f docker-compose.deploy.yml run --rm --no-deps --entrypoint rm monitor -f /opt/origo/heartbeats/trade_publication_binance_spot_trades.heartbeat /opt/origo/heartbeats/trade_publication_binance_perp_trades.heartbeat /opt/origo/heartbeats/perp_repair_binance_perp_trades.heartbeat
```

The live feed asset is materialized at the end of every tick whatever the minutes did,
so its freshness is the worker's tick and its path to the webserver, not the minutes'
success: failing or skipped minutes keep the feed fresh and show as `FAILED` receipts,
which `origo_monitor:workers_alive` reports. A source-level tick exception writes a
`<source>:tick` FAILED receipt and leaves that source's feed stale; other source workers
continue independently. A stale feed with a fresh heartbeat means
the tick raised outside the per-minute handler (the ERROR line is in
`origo.container_log`) or the webserver refused the report. A stale heartbeat means the
container is down or stuck: `docker compose ps` shows the healthcheck, and the
watchdog's exit is in `origo.container_log`.

## Alerts and the daily digest

- The monitor evaluates `collectors_serving`, `dagster_reachable`, `no_error_logs`,
  `publication_current`, `queue_bounded`, `workers_alive` and `data_current` every minute, writes the
  seven evaluations to Dagit through the webserver's report endpoint, then sends one
  HTML/text summary through Resend when lifecycle changes or today's digest are due.
  All notifications share a literal one-hour distinct-dispatch budget; repeats alone
  do not trigger another message.
- Delivery: `RESEND_API_KEY`, `ORIGO_ALERT_EMAIL_TO` and `ORIGO_ALERT_EMAIL_FROM` use
  the existing deployment secret/variables. Partial required settings are a visible
  configuration error. Absent settings disable sends while detection and evidence continue.
- The daily digest is due at or after `ORIGO_ALERT_DIGEST_HOUR_UTC` (07:00 by default),
  merged with pending changes and constrained by the global budget. Late starts catch
  up today only. No lifetime tick/finding counters appear in the summary.
- Mail outages retain immutable retry state and appear in check metadata/WARNING logs;
  they do not interrupt independent detector cursors or create recursive ERROR alerts.

## Rules that must not change

- Dagit is the investigation pane. `/law` is the authorized read-only summary; no other dashboard or alert path.
- The monitor keeps notification/import cursors and the law observation tape. Authoritative facts stay in their existing stores.
- Every finding is written to Dagit before it is e-mailed, and the e-mail states whether
  the write succeeded.
- Per-minute work is provenance in `origo.worker_minute_log` and observations in Dagit,
  never one Dagster run per minute.
- Investigate in the order above. Do not start from `docker logs`.


## Reader law page

`/law` opens on Overview: fresh sources, authoritative archive readiness, historical calendar
coverage, depth minutes and market state cube freshness/history. Compact secondary figures show exact-current
outputs, worker heartbeats, queue, collectors, observed incidents and the core-law
clear window. Operational checks are separate from data health; healthy heartbeats
do not prove successful work. Every figure opens its original evidence.

Sources shows each declared source and projection with its consumer outputs. Laws
shows only the named data laws over 30 UTC days: live sources are fresh, authoritative
archives are ready, historical coverage is complete, depth minutes are complete, market state cube is fresh, and market state history is complete.
Required law membership comes from the frozen evaluator inventory through the
catalog's `law_gate_ids`; missing members remain UNKNOWN. The separate inventory
guard remains mandatory. Friendly headings are page copy: original descriptors,
conditions, thresholds, definition versions and code provenance are unchanged.
`view=gates` remains an alias; runtime gate links explain their operational locator.
The bounded `/law/gates.json` and `/law/history.json` APIs retain runtime forensic
evidence, captured by the monitor and replayed by `law_page.TapeCache`.

Recovery shows trade delay and signed one-hour change, and depth missing-minute
history with its actual checked window. An absent comparison is labelled, not zero.
Late daily archives remain distinct from live-reader freshness. Publication token
mismatches within existing coverage-lag policy are pending, never exact-current;
known data failures take precedence. Equal-frontier revision mismatch has no
revision-age timeout in the existing policy. Dagit destinations are linked only
when an operator-facing URL is recorded; otherwise the exact asset/check is text.
Source/projection inventory comes from registered source declarations, never UI lists.

`data_current` evaluates R1 (readable selected minute and reader end), C1 (latest due
canonical activation), C2 (older canonical calendar), and the separate cube laws below.
Worker liveness cannot make missing reader data green. Spot/perp freshness budgets
are180/300seconds. Missing daily trade archives are NOT_DUE before04:30/10:30UTC.
Historical reports retain D1 and the retired native book descriptors; current
inventory contains only the four trade sources and two qualified top20 sources.
Enabled `binance_spot_top20_v1` and `binance_perp_top20_v1` use the same reader
laws with up to60actual snapshots and one last-observed-quote metric per nonempty
minute. A partial or empty interval closes only after both historical imports have
crossed its beginning and end plus five minutes. Tail downloads and expired cursor
jumps cannot establish that coverage. Its original input evidence binds an exact
60-bit observed-second mask per minute, both import witnesses and the selected
bytes; absent seconds remain absent. An accepted minute leaves the retry candidates.
C1 requires the latest due UTC hour's validated canonical proofs. C2 checks every
older finalized hour from2026-10-24T13:00Z and reports recorded missing seconds
separately. Neither predicate treats a verified closed gap as an unprocessed hour.
Missing/invalid proofs and physical rows that disagree with the masks still fail;
R1 still requires an actual current observation within its freshness budget.
Micro reports actual observed book milliseconds independently of trade coverage.
Arrow retains sparse original rows and reports expected/missing seconds in its
manifest. No interpolation, carry-forward, zero-valued book or new clock fills a gap.
R1 measures book age from the genuine last exchange clock, including trailing
missing seconds, and fails on an empty current minute. C1/C2 may pass a finalized
empty interval; its metric is absent. A current empty Arrow file is a valid delivered
artifact, with zero rows and explicit missing coverage. Its `active_through` is the
processed interval boundary; quote clocks remain in the actual rows. Publication
currency measures delivery against selected inputs, while R1 judges live observations.
The existing deployment readiness check deliberately still requires a recent full
60-second minute and metric; accepting a historical gap does not relax rollout readiness.
Exact gap evidence is in the monitor's existing raw law reports and C1/C2 gate events,
served by `/law.json` and `/law/history.json` and linked from Dagit's monitor
checks. Browser evidence uses comma-separated, fixed-width 15-digit hexadecimal
observed-second masks, earliest minute first and bit0 representing second0. This
preserves all60bits; internal revision evidence retains UInt64 numeric masks and
its SHA-256 binding. Notification-frame measurement allowlists are unchanged.
Sources lists canonical and provisional depth20 and minute metrics,
with activation hash, build identity and data-through evidence. Recovery displays
hourly book and daily trade canonical readiness and calendar coverage.

Projection activation is not formal certification approval or a physical full-history audit.

The BTCUSDT market state cube has separate M1/M2 predicates under
`binance_spot_trades`. M1 requires an activated cube component for the selected
reader generation, applies the 180-second spot freshness budget, and compares
trade and taker-buy counts against retained raw rows in the at-most-112.5-second
base-cell window intersecting the selected closed minute. Both sides use the same
partition, revision and build; UInt32 cube counts widen before summation.
M2 checks every canonical day from **2021-01-01 UTC**, through yesterday once its
archive exists or its delivery deadline passes. It reports expected, valid,
missing and unknown day counts plus the first invalid day and its reason using the
existing bounded proof read; it never scans historical raw/cube rows. Any missing
day fails M2 as `cube_history_incomplete`, one stable reason while an upgrade sweep
is still covering history. The source's original
2017 anchor and R1/C1/C2 remain unchanged. Valid legacy generations preserve their
existing product health during upgrade. Cube-only proof damage is reported by
M1/M2 without changing otherwise valid R1/C1/C2. Staged but unactivated cube receipts do
not satisfy M1/M2. Cube nodes and gates appear in Sources, Overview and Laws from
the same declarations and monitor tape. The cube's detail components (PRD-0023) appear
there as projections of their own, `UNKNOWN` until the partition they read holds the key;
M1 and M2 judge only the cube. Missing registration, proof or physical
count evidence is never green. These probes retain the five-second statement,
512 MiB, single-thread and 20-second whole-pass budgets; timeout is UNKNOWN.

The monitor appends one unheld whole-inventory report per distinct minute before Dagit
or Resend. `monitor.data_current` is stored only in that committed sample; gate
history reads its verdict there, never from a separate event write. Once committed, a
repeat tick in that minute resumes an existing private delivery intent before skipping
probes, checks and sample writes. With no pending intent it preserves the existing skip.
R1/C1/D1/M1 eligibility still requires five consecutive failing slots with the same
original detector key. C2/M2, UNKNOWN and evidence/tape/page faults remain unheld.
Grouping never changes these predicates, keys or holds. Reason changes can therefore
reset held eligibility even within one stable operator incident.

Dashboard and mail use `origo.observatory` for the same recorded facts. The email
opens with current affected sources and checks, delay, reader and file-publication
limits, and the next Dagit investigation. Its compact current evidence is retained
before historical incident detail is trimmed, within the existing 16 KiB summary.
Healthy counts, publication currency, normal archive waits and recorded historical
gaps follow. HTML and plain text carry the same brief, observed/prepared times,
verification limits and omitted historical groups. The dashboard retains its cards.
Each body is capped at 64 KiB. The optional `ORIGO_ALERT_DASHBOARD_URL` must be
an operator-facing HTTP(S) `/law` URL without credentials or fragments; invalid configuration
omits the link and explains why. The private health URL is never used as an email link.

Conditions group by source/predicate or component identity, independently of reasons.
Repeated minute observations increase counts without triggering mail. New eligible
condition failures, condition verification changes, confirmed recovery (two consecutive
passes), reopening and a new recorded failure-event episode trigger summaries.
Normal waits and historical-event verification changes remain visible in the daily
digest; they do not trigger another email. Historical quiet never proves recovery. Missing/UNKNOWN evidence resets confirmation;
one passing sample is recovery pending. Event streams retain historical failures,
become historical after 60 minutes and do not claim resolution from a quiet read.
An event episode restarts only after 24 hours of fully observed quiet. Trend comparisons
use compatible recorded endpoints exactly one hour apart. Reduced lag is point-to-point;
rolling depth-gap changes cannot establish whether repair or window ageing caused them.

All changes and today's digest share one global distinct-dispatch budget, at least
3,600 seconds apart regardless of legacy cooldown configuration. The daily digest is
due on the first allowed tick at or after its configured UTC hour that same day;
a late restart catches up today only. Numeric changes/repetitions alone never trigger
an additional non-digest message. There is no eight-minute acceptance guarantee under
this hourly budget, nor a guarantee about provider processing or inbox-arrival times.

The existing `monitor.cursor.json` holds one immutable pending batch: exact JSON request
bytes, recipient/sender, endpoint, hash, idempotency key and fixed preparation-plus-one-hour
expiry. Before **every** POST, the cursor reserves dispatch quarantine through request
start + 10 + 3,600 seconds; completion extends it to at least completion + 3,600.
Retries keep identical payload/key/endpoint, respect Retry-After and wait at least a
minute. No request starts during the snapshot's final ten seconds. A nonempty provider
acknowledgement is required to advance delivery watermarks. Timeouts and ambiguous
responses remain uncertain. Expiry or terminal retirement keeps one receipt and up to
32 coalesced loss intervals; acknowledged disclosures clear only what they included.
Evidence older than 24 hours is disclosed as lost, never marked delivered.

Cursor writes use a mode-0600 temporary file, fsync, atomic replacement and directory
fsync. Failed durable reservation prohibits POST. Process restarts after any prior dispatch
reserve another conservative hour (wall time alone cannot prove elapsed real time); legacy migration is persisted once.
An attempted pending batch also waits its recorded retry delay again after restart, at least
60 seconds; its original expiry remains fixed. Within a process, monotonic elapsed time
preserves the remaining dispatch and retry waits across every forward clock correction;
a clock discrepancy above 120 seconds resets the conservative hour. Mail failures
are WARNING/check metadata, so they do not create recursive ERROR-log alerts.
Detector cursors advance independently of mail success. The worker-heartbeats volume
holds private intent and is shared by workers, but is absent from the law page.

`notification-observations-YYYY-MM-DD.jsonl` records compact typed frames from existing
detector reads even when a law sample fails. It preserves original detector keys and
actual sampled eligibility, original lifecycle transitions, scalar values, read bounds/counts/caps and omissions.
Transitions are recorded at capture time; moving the 24-hour replay boundary cannot
turn an ongoing condition into a new notification.
Unavailable measurements remain unavailable; no extra queries or probes reconstruct them.
Frames use bounded lossless zlib/base64 encoding: 8 KiB per line, at most 64 groups and
128 KiB decoded. This keeps repetitive typed field names from consuming the observation
budget. Replay is capped at 1 MiB and one second per tick with a 16 MiB compact index.
Segments retain 30 days; closeout evidence is separate. `operator-summary.json` is a
replaceable page derivative capped at 16 KiB, containing no recipient or mail payload.

Only the monitor mounts `/var/lib/origo-law` writable. The page has a read-only
mount, no database/exchange/mail credentials and no backend query path. Page requests
read bounded caches/tape only; API history is catalog-ID restricted, at most 30 days
and 1,000 original events per page. Missing, corrupt or 120-second-old evidence
is UNKNOWN. HTTP health means the page serves, independently of its data verdict;
red data never blocks deployment or recovery. Health probes use an unpublished internal
port (8485), independently of request slots. The page runs without root privileges.
Compose publishes port 8484 on host loopback only (`127.0.0.1:8484`); Caddy from Vaquum/Portal
serves it at https://origo.vaquum.fi behind the Portal login.
Historical gaps remain not observed.

The dedicated `law_reader` profile permits only reads, one concurrent single-threaded
query, 512 MiB and five seconds per statement. Transport has a wall deadline; the core
pass has 20 seconds total. Additional catalog observations and one historical import
page each have five seconds, yield to current checks and stop on pressure/timeouts.
No retries, ingestion locks or new exchange calls are allowed. Page CPU/memory are
capped independently. Timeout/cap failures become UNKNOWN on the ordinary next tick.

UTC-day tape segments retain 30 days and retained catalog definitions; selected
72-hour closeout evidence is retained separately under `closeout/`, outside segment pruning. A clear window requires 4,321
consecutive distinct minute slots spanning at least 72 hours, with every applicable
core predicate PASS (C1 NOT_DUE identified separately). Missing/corrupt slots or
incompatible predicate definitions break the window; deployment SHA or line changes alone do not. Display-only inactive nodes do not affect it.
PRD #442 remains open until a genuine production window and three contemporaneous
SQL cross-checks are attached; CI and historical reconstruction cannot substitute.

Current JSON excludes the 30-day grid and repeated catalog. The browser fetches the
catalog once per version and loads compact law history when Laws is shown. Report and catalog versions must match; a refresh race remains UNKNOWN until a consistent refresh. Minute
samples omit unobserved gate placeholders; the catalog supplies their explicit
UNKNOWN/NOT_EVALUATED display. Event-driven gates retain the last original outcome
and timestamp separately from current blocking state. Corrupt oversized history
records mark the history limited while later valid evidence remains readable.
Historical definition lookups keep their byte/time caps; omitted definitions are
explicitly incomplete, never a complete replay without the original thresholds/code.

Historical receipt/proof attribution starts at the current monitor activation.
Restart or rollback never reuses a catalog file's old creation time as deployment
evidence; earlier unimported events remain unobserved. Already taped events retain
their original catalog, identity and timestamp.

Provider circuit failures record their actual host in the existing error message.
Historical import parses that owner format; old hostless failures cannot be assigned
to a host gate. No host is inferred from a source name.


## Overview evidence and resource bounds

Overview adds no database queries, collector probes or exchange calls. Per-tick
scalar counts reuse the monitor's existing reads and reset to unknown on failed
reads. The worker denominator includes enabled provisional workers, the single
raw-perp capture and already-discovered heartbeat members; missing required
members stay unknown. Queue zero is never inferred from unreachable Dagster.

Error lines and failed receipts show 60-minute observed totals, with 24-hour detail.
Their recorded intervals are the effective whole-second UTC SQL bounds. Tape commit
precedes cursor persistence, so overlapping intervals can occur: replay excludes
ambiguous overlaps and boundary-crossing intervals, never prorates, and reports
covered seconds and observed/expected slots. Missing fields, history gaps/loading,
read caps and incomplete coverage make the number a lower bound, or UNKNOWN when
nothing is usable. Exactly 1,000 rows is a capped lower bound. No historical counts
are reconstructed from finding totals.

The page caches at most 1,441 compact recent operational samples within an additional
1 MiB, separate from the unchanged 96 MiB core-history budget. Overview reuses cached
aggregates without per-request tape scans or browser history fanout. Current refresh
remains independent of history, every 30 seconds with a five-second deadline; the
sample expires at 120 seconds. All lazy history shares at most two concurrent
requests, bounded queues and five-second deadlines, including pagination. Selection
changes cancel superseded work. The eight-entry cache, 5,000 client events, 1,000
server events per page, 30-day range, 1 MiB record limit and four request slots remain.

The recorded six-source/54-projection baseline is 126,790 compact JSON bytes. Its
Overview additions must stay within 8 KiB and the result within 160 KiB. Local replay
checks keep process RSS below 256 MiB and a warmed current response below one second;
production is never load-tested for dashboard acceptance. These are bounded costs,
not zero-cost monitoring.
