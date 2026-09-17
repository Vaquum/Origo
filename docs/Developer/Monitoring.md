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
  `origo_monitor` carries the five checks the monitor evaluates every minute; the live
  feed assets carry freshness. If a fact is not visible in Dagit, it is visible in
  ClickHouse; the monitor's e-mail says which.
- **One detector.** The monitor runs outside the Dagster process, as its own Compose
  service with its own heartbeat, so it keeps working when the daemon, the queue or the
  webserver is the failure. It stores only a cursor file. It writes every finding into
  Dagit as a check evaluation before it sends an e-mail, and the e-mail says whether that
  write succeeded. No second dashboard, no second alert path.

## Where each fact lives

| Fact | Where | Read it with |
| --- | --- | --- |
| Is Dagster up, are the daemons healthy, how deep is the queue | Dagster instance | `origo_monitor:dagster_reachable`, `origo_monitor:queue_bounded` in Dagit; GraphQL `instance.daemonHealth` |
| Did a run fail, which partition | Dagster run storage | Runs view in Dagit; the monitor's `run_failure:<job>:<partition>` finding |
| Did an asset check fail | Dagster event log | The asset's Checks tab; the monitor's `check_failed:<asset>:<check>` finding |
| Is a worker alive | `/opt/origo/heartbeats/<feed>.heartbeat` | `origo_monitor:workers_alive`; `python -m origo.workers.<feed> --check` |
| What did a worker do for a minute | `origo.worker_minute_log` | `SELECT * FROM origo.worker_minute_log WHERE minute = ...` |
| Why did a source build or publication fail | `origo.source_failure_log` | `binance_spot_trades_failure_sensor` output in Dagit; the table itself |
| What did a container print | `origo.container_log` (14 days) | `SELECT * FROM origo.container_log WHERE service = ... ORDER BY timestamp` |
| Are the depth collectors serving | The collectors' history endpoints | `origo_monitor:collectors_serving` |
| What did the monitor send and when | `origo_monitor` check metadata and the e-mail | The asset's Checks tab; the mailbox |

## Investigation order

1. **Dagit first.** Open the `origo_monitor` asset: its five checks name the failing area
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

## Alerts and the daily digest

- The monitor evaluates `collectors_serving`, `dagster_reachable`, `no_error_logs`,
  `queue_bounded` and `workers_alive` every minute, writes the five evaluations to Dagit
  through the webserver's report endpoint, then sends one e-mail through Resend listing
  every new finding key. A key repeats inside the cooldown (six hours by default)
  without a second e-mail; a queue backlog is one key.
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
