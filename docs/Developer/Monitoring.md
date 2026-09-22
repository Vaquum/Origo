# Monitoring

Origo has one detector, the monitor worker (`origo.workers.monitor`), one pane, Dagit,
and one place where each fact lives. This document is the contract for all three; the
tests in `tests/origo_source_native/test_monitor.py` hold its headings and rules.

## One truth, one pane, one detector

- **One truth.** Every operational fact has exactly one authoritative store. Dagster's
  own storage holds runs, materializations, observations, sensor and schedule state and
  asset check evaluations. ClickHouse holds worker receipts (`origo.worker_minute_log`),
  source failures (`origo.source_failure_log`) and the container log
  (`origo.container_log`). The workers' heartbeat files hold liveness. Nothing is copied
  from one store into another.
- **One pane.** Dagit is where an operator looks first. The external asset
  `origo_monitor` carries the six checks the monitor evaluates every minute; the live
  feed assets (`binance_spot_depth_live_feed`, `<source>_provisional_feed`) carry a
  five-minute freshness policy the daemon evaluates without a run. If a fact is not
  visible in Dagit, it is visible in ClickHouse; the monitor's e-mail says which.
- **One detector.** The monitor runs outside the Dagster process, as its own Compose
  service with its own heartbeat, so it keeps working when the daemon, the queue or the
  webserver is the failure. It stores only a cursor file. It writes every finding into
  Dagit as a check evaluation before it sends an e-mail, and the e-mail says whether that
  write succeeded. No second dashboard, no second alert path.

## The feed workers

The minute paths run outside the Dagster queue as Compose services with heartbeats,
one tick per minute, restarted by the watchdog when a tick hangs:

- `depth-worker` (`origo.workers.depth`) brings every minute of the last fifteen to
  completion for depth20 and depth200: raw snapshots from the collector, the 1m
  projection row and the Arrow chunk, each only when absent. A minute the collector does
  not serve yet is left for the next tick; a minute that fails stays a candidate for the
  rest of the lookback and its receipt names the error. The per-minute Dagster jobs stay
  for operators (backfills, repairs) and nothing schedules them.
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
| Why did a source build or publication fail | `origo.source_failure_log` | `binance_spot_trades_failure_sensor` output in Dagit; the table itself |
| What did a container print | `origo.container_log` (14 days) | `SELECT * FROM origo.container_log WHERE service = ... ORDER BY timestamp` |
| Are the depth collectors serving | The collectors' history endpoints | `origo_monitor:collectors_serving` |
| What did the monitor send and when | `origo_monitor` check metadata and the e-mail | The asset's Checks tab; the mailbox |

## Investigation order

1. **Dagit first.** Open the `origo_monitor` asset: its six checks name the failing area
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
  `publication_current`, `queue_bounded` and `workers_alive` every minute, writes the
  six evaluations to Dagit through the webserver's report endpoint, then sends one
  e-mail through Resend listing every new finding key. A key repeats inside the
  cooldown (six hours by default) without a second e-mail; a queue backlog is one key.
- Delivery: `RESEND_API_KEY` (repository secret), `ORIGO_ALERT_EMAIL_TO` (repository
  variable) and `ORIGO_ALERT_EMAIL_FROM` (a sender on a domain verified in the Resend
  account; the default `onboarding@resend.dev` reaches only the account owner). A
  partial `ORIGO_ALERT_*` set is a configuration error; no set at all disables alerts
  with a visible log line, which is the development default.
- The daily digest goes out once at `ORIGO_ALERT_DIGEST_HOUR_UTC` (07:00 by default)
  with the tick, finding and feed counts.
- A mail outage fails the monitor tick visibly and is retried the next minute; a Dagit
  outage is itself the `dagster_reachable` finding, still one e-mail within two minutes.

## Rules that must not change

- Dagit is the pane. Do not add a second dashboard or a second alert path.
- The monitor keeps only a cursor. Do not make it a store of truth.
- Every finding is written to Dagit before it is e-mailed, and the e-mail states whether
  the write succeeded.
- Per-minute work is provenance in `origo.worker_minute_log` and observations in Dagit,
  never one Dagster run per minute.
- Investigate in the order above. Do not start from `docker logs`.
