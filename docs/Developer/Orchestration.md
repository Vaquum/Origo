# Production orchestration

Every run uses the native Dagster queue, including scheduled jobs, sensors, manual
launches, asset backfills and native retries. `dagster.yaml` installs the shared
`OrigoQueuedRunCoordinator` and `OrigoRunLauncher`; source authors do not configure
queues or activate automation in Dagit.

## Admission and capacity

- Nineteen total run slots: ten historical/backfill runs, eight routine runs and one
  maintenance run. Each class has its own native tag limit, so routine priority cannot
  consume the backfill allocation and `maintain_operational_metadata_job` (workload
  `maintenance`, priority 300) always has a lane while the other two are full. All
  sources share the historical allocation; registering another source does not
  multiply the server's concurrency budget. Each native backfill, or bulk job run
  outside one, is one share of that lane, queued by start-time fair queuing: a run's
  place is the later of the service frontier and one past its share's newest queued
  place, and its priority is minus that place. The launcher records the frontier, the
  place after the highest one ever launched, in Dagster's cursor store, so it only moves
  forward with real service. A newcomer ties with an older backlog's next place and
  queues after it, so concurrent backfills alternate, a stream of new backfills cannot
  starve an older one, and none holds another source's fills back. Deployment recovery keeps the places admission gave and continues after them.
- Native dequeue uses nineteen launch threads and a one-second poll. Canonical
  source operations retain their registered eight-worker pool. Ten backfill run
  slots preserve the existing launch capacity around those eight operations;
  worker startup and teardown must not reduce source concurrency. Run limits
  and operation pools both apply.
- Routine runs have higher queue priority; daily partitions (including briefing
  publication) precede minute catch-up work. Each routine job has at most two
  active runs. One stalled job cannot occupy every routine slot.
- Admission permits one queued copy of an execution identity. The identity
  includes job definition, partition/range, execution configuration, asset/check/
  step selection, source event/state identity, minute input tags and native
  backfill ID. Scheduler timestamps and run keys alone do not identify work.
- One queued follow-up may coexist with an active run. This preserves updates
  received after the active run read its input. Identical work is serialized by
  the native identity tag limit. Distinct dates, depth chunk minutes, changed
  source revisions and separate backfill receipts remain distinct work.
- Run history loads as a Dagster backfill of `backfill_<key>_source_job`: only that job name switches reconcile to the `complete` operation, so asset-direct backfills bypass the completion barrier and the source backfill log.
- The minute feeds are not runs. The depth worker and the provisional worker
  (`origo.workers.depth`, `origo.workers.provisional`) process each minute outside the
  queue, write receipts to `origo.worker_minute_log` and report materializations of
  their live feed assets; see [Monitoring.md](Monitoring.md). The per-minute depth jobs
  stay for operator backfills and repairs and nothing schedules them. Current-state
  mirror, audit and metadata schedules skip outstanding work. The central coordinator also catches concurrent submissions from multiple
  schedules/sensors/UI processes under a shared filesystem admission lock.

A canceled duplicate retains its Dagster record and identifies the retained run.
No source rows, source receipts, partitions or completed provenance are deleted.
A real source outage can accumulate unique missing partitions; it must never
multiply queued copies of the same partition or take the other workload's slots.

## Failure and deployment recovery

The launcher records worker container ownership and lets native Dagster monitoring
query the existing gRPC server for its current runs. A responding server that no
longer owns a run is positive evidence of worker loss. An unreachable server is
reported as unknown, not proof that the worker died.

Short routine operations have a thirty-minute runtime bound. Daily retry envelopes
and backfill/publication runs retain the twenty-six-hour bound; no generated
backfill runs are unlimited. Automatic source requests honor the declared attempt
limit and retry delay. Terminal failures remain visible and support native retry.

Deployment performs two recovery passes, each recorded as a Dagster run:

1. `python -m origo.sources.bootstrap` classifies outstanding runs with the native
   limits and cancels redundant queued copies before the daemon starts, then records
   the counts in `prepare_revisioned_sources_job`.
2. The deployment workflow captures old container identities, replaces the app,
   and checks Docker for positive confirmation that those containers stopped or
   were removed. `python -m origo.orchestration.recovery` then fails runs owned by
   those retired workers, releases their concurrency claims, and records the counts
   in `recover_orchestration_job`. Legacy untagged local gRPC runs are covered by a
   timestamp only when **both** old app containers are confirmed retired. Unknown
   ownership and current workers are preserved.

Both passes run in the entry point's `main()` before the recording run exists, never
inside a captured op. Reporting on a run whose event shard is not yet initialized
logs through Alembic while the storage lock is held; with root python-log capture
that log re-enters the same storage and startup deadlocks. The recovery command
stops with a named phase after `--deadline-seconds` (900 in deployment). The
container healthcheck passes on state persisted by the previous deployment, so the
workflow waits for the exec into `dagster-daemon` (at most 900 seconds) before
running recovery; a bootstrap that has not finished by then fails the deploy with
the daemon log attached. The compose start waits at most 600 seconds, the deploy
job at most 45 minutes, and a failed start prints the last 200 daemon log lines. A
recovery error before the recording run exists appears in that log excerpt, not as
a Dagster run.

Recovery and launching share a short admission/claim lock. A cancellation marker
prevents a dequeuer that selected a duplicate just before recovery from starting
it afterwards. The lock is released before the worker-launch RPC, preserving
parallel dispatch. Recovery is repeatable and reports its counts/errors through
Dagster events and logs. No operator text input or manual automation toggles are
part of deployment or recovery.

## Required validation

`pytest tests/origo_source_native/test_orchestration.py -q` covers concurrent
admission, distinct source inputs, native backfill receipts, workload allocation,
noisy-job isolation, recovery/launch races, worker health and container retirement.
The full `tests/origo_source_native` suite remains required.

Run `tools/benchmark_orchestration.py` with checksum-verified official archives.
It owns an isolated ClickHouse and Dagster instance and uses native gRPC workers
and the real queued-run daemon, not a replacement worker pool. Compare identical
source ingestion/projection/parity work under baseline and bounded queue settings,
with and without routine ingestion and Parquet publication. Retain exact component
proofs, source rows/second and routine queue/completion latency. Do not substitute
run-count assertions for this mixed-workload measurement.

New sources inherit these controls. Their onboarding evidence must still check
source-specific memory, runtime and shared-hardware contention. Queue isolation
reserves execution slots; it does not manufacture CPU, RAM, disk or provider quota.
