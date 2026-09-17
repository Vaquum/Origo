# Revisioned sources

Before drafting or implementing any PRD or slice that adds a data source, read the
mandatory [source onboarding playbook](../../docs/Developer/Source-onboarding.md).
It defines the shared native Dagster workflow, required source footprint,
automation, projections, publications and acceptance evidence.

## Operational metadata maintenance

`maintain_operational_metadata_job` is the Dagit entry point. The
`operational_metadata_maintenance_schedule` runs every 10 minutes; deployment launches
this same job through the configured project workspace. Deployments start in inspection mode. The configured SQLite adapters
fix the Jobs-page repository query and preserve current materialization/observation
IDs, tags, data versions and partition facts after their old run details expire.
ClickHouse's native 14-day diagnostic TTL operates independently of this schedule.
Source tables, reconciliation state, accepted revisions and published data are outside
this retention policy.

Source ingestion, mixed ingestion/projection jobs and unknown jobs retain their run
records indefinitely. The explicit policy is `origo/maintenance/roles.py`;
`origo_source_key` also marks source work. Run deletion itself checks this policy
atomically, so a source tag added after inventory still prevents retirement.
Source provenance includes the full event history, acceptance/failure evidence,
source generation and validation references. After one hour without shard activity,
terminal source event databases are losslessly compressed into
`operational-maintenance/source-archive.sqlite`. Dagit reads the original database
schema from memory, preserving event IDs, timestamps, payloads and pagination.
The shared run/event indexes also compress source JSON payloads losslessly;
query keys, statuses, timestamps, tags and every historical entry remain intact.
Both Origo adapters decode these versioned, checksummed payloads transparently.
Direct SQLite inspection must use `origo.maintenance.sqlite.connection` to obtain
decoded rows. Keep the compatible adapters when rolling back application code;
an older adapter cannot read the packed format. Captured logs are retained.
A late event restores the shard durably under its writer lock before appending; interrupted transitions prefer the committed,
checksum-verified archive. Corrupt images fail visibly rather than creating empty
history. Images above 64 MiB remain unmodified and appear as an inventory exclusion.
Dagster schema upgrades migrate one packed database at a time during an outage.

Explicit projection jobs include bars, derived depth tables, Arrow and published
files. Successful histories are eligible after a one-minute shutdown grace; resolved failed/canceled
histories after 24 hours. Current asset facts keep their original IDs, tags, input
versions and partitions after a projection run expires. Active runs, active/unknown
backfills, retries, current checks, unresolved failures and unconsumed sensor events
remain protected. Later materializations for every planned asset can resolve an older failure even after the successful run records expire; that run’s own materializations cannot clear its failure. Fresh checks precede each deletion. Unchanged Parquet inputs do
not launch another Arrow build. Terminal successful/skipped sensor and schedule
ticks expire after one day; failed ticks after seven days. Instigator cursors and
source run evidence are independent of those operational tick logs.

### First inspection and cleanup

1. In Dagit, launch `maintain_operational_metadata_job` with the configuration below.
   The deployment default permits at most ten minutes, in batches of at most 500 runs.
   Scan, reclamation and compaction use a separate work deadline. Reporting gets
   10% of runtime, bounded to 15–300 seconds and at most half the usable runtime
   for short invocations. Exhausting the work window reports the last durable
   checkpoint and resumes on the next invocation. Disk measurement counts allocated
   blocks without following symlinks, counts hard links once, and logs paths that disappear during
   traversal. Missing configured roots, permission and I/O errors still fail the run,
   as does exhausting the absolute reporting deadline.
   Inventory reads at most 500 run records in run-ID order, without sorting the
   remaining backlog. Retry references and sensor state are loaded once per batch;
   one read-only event connection is scoped to the batch. Reclamation still checks
   fresh dependencies for each run under its writer lock.
   Each completed batch checkpoints its cursor. Repeat until the result says
   `inventory_complete: true`; an incomplete inventory is not a deletion approval.
   Once an eligible first manifest has a complete inventory, scheduled and deploy
   dry runs preserve its inventory totals, retained floor and manifest through
   backup preparation and review. Health, current disk usage and diagnostic checks
   continue. Rotated or retired system log tables are reported under `retired` with
   a `retired_drop:<table>:<date>` pending entry and dropped whole after retention;
   the monitor worker (`docs/Developer/Monitoring.md`) alerts on a failed run. Inventories without eligible runs continue scanning; first apply or a
   retention-policy change permits a new inventory cycle.
2. Read `maintenance` in the health check metadata and its `journal_path`. The one
   journal contains the exact first manifest, artifact paths/allocated bytes,
   exclusions, `archive`/`retire` actions and SHA-256. Health requires Dagster
   metadata to stay strictly below 10% of ClickHouse active business-data bytes.
   Shared databases, source archives, live shards, logs and maintenance state all
   count. Diagnostic tables never inflate the denominator. An instance above that
   ratio may perform approved cleanup, but its check remains failed until actual
   physical usage meets the ratio. The old retained-floor estimate includes reusable
   SQLite pages and is not an admission barrier.
3. Before first apply, take a consistent snapshot of **the entire Dagster instance**
   and compute logs using the storage platform's atomic snapshot facility, or a
   controlled writer-quiesced copy. Copying separate live SQLite files is not a
   consistent instance backup. Restore it on a separate filesystem outside the
   production reserve; rewrite its `dagster.yaml` paths to that restored directory.
   Keep the original run-storage instance ID and both configured Origo adapters.
4. From the deployed application environment, verify that restored instance using
   `python -m origo.maintenance.worker --config '<inspection JSON>'
   --verify-restored-home /mnt/off-volume/origo-restore --snapshot-id '<snapshot ID>'`.
   Save the returned JSON as the backup receipt. Verification checks database
   integrity, instance identity and the manifest's run/shard evidence. The operator
   supplies the snapshot's consistency guarantee; this command does not create a
   backup or turn a live file copy into one.
5. Review the exact manifest. In Dagit set `dry_run: false`, `backup_receipt` to
   the readable receipt path and `approved_manifest_sha256` to
   that exact manifest hash. The first batch requires a matching, unexpired receipt;
   later batches use the durable first-apply checkpoint and revalidate each run.
   Failure/timeout is a failed run and check, with progress retained for another run.
6. After the first batch's state and physical release have been reconciled, enable
   recurring apply with repository variables `ORIGO_METADATA_DRY_RUN=false` and
   `ORIGO_METADATA_MAX_RUNTIME_SECONDS=600`, then deploy. Repeat bounded Dagit
   invocations during catch-up until eligible backlog declines faster than ingress.

```yaml
ops:
  maintain_operational_metadata:
    config:
      dry_run: true
      projection_success_minutes: 1
      projection_failure_hours: 24
      source_archive_after_hours: 1
      diagnostic_retention_days: 14
      max_runs_per_batch: 500
      max_runtime_seconds: 600
      lock_wait_seconds: 1
```

The first manifest stays available across inspection batches. Its initial eligibility
reason is immutable; live revalidation exclusions are separate progress fields and
do not invalidate the approved hash or backup receipt. Policy changes reset
inspection; finish an interrupted deletion before changing policy. The journal holds
at most 500 candidates and 32 aggregate reports from the last 30 days. The backup
receipt expires after 30 days. Configure the external backup service to expire these
maintenance snapshots after 30 days; Origo does not delete external backups.

### Reading the result

All worker output and exceptions flow into Dagit logs. The blocking
`operational_metadata_health` check reports candidate/protected/deleted/archived counts,
exclusion reasons, age-eligible projection backlog (including protected projections), inferred eligible
arrival rate, cleanup rate, query p95, business-data ratio, last healthy invocation
and free filesystem bytes. Arrival rate uses the observed backlog change plus deletions;
concurrent manual deletion can understate arrivals. Active cleanup throughput and
between-invocation cleanup rate are separate measurements. Required state is never
deleted to force a check to pass.

`reclaimed_bytes` includes removed projection artifacts, net source-archive savings
and measured shared-database release. `shared_sqlite.*.reusable_bytes` is still
allocated until page reclamation returns it to the filesystem. Initial conversion
requires a controlled outage with all Dagster/Dagit writers quiesced and a verified
backup. Run `python tools/metadata_compact_sqlite.py --instance-home <home>
--backup-receipt <receipt.json> --writers-quiesced --max-runtime-seconds 900` under
an independent timeout that always resumes both services. It copies no rows between
logical histories: SQLite VACUUM preserves records and enables incremental vacuum.
A failed conversion leaves the original committed database recoverable. Subsequent
maintenance reclaims free pages in bounded transactions and checkpoints the WAL;
`sqlite_compaction_not_initialized` identifies databases still needing conversion.
Do not run initial VACUUM against active production writers. Preserve the full
maintenance output and attach it to the Dagit maintenance run after services resume.
 ClickHouse reports active, inactive and
expired part bytes separately from actual allocated blocks and net physical release.
A scheduled TTL operation is not claimed as reclaimed space.

ClickHouse catch-up first flushes configured system logs, creating empty tables for
unused logs such as crash/backup logs before auditing their TTLs. It then validates
the explicit diagnostic table allowlist and archived numeric schema incarnations. It drops only parts whose actual maximum event time is
older than 14 days. Mixed-age partitions use at most one asynchronous TTL mutation,
with a 4 GiB partition limit and at least 20 GiB plus twice the partition's bytes free.
Concurrent merges/mutations exclude their tables. Inventory limits (500 system
MergeTree tables, 5,000 diagnostic parts) fail visibly instead of applying an
incomplete inventory. TTL drift, lag over the configured allowance, failed mutations,
unknown growing system tables and insufficient catch-up capacity fail the health
check. Native TTL may release inactive parts later; repeated measurement establishes
physical recovery. The OpenTelemetry timestamp is microseconds and is converted to
seconds before applying the same retention window.

Production acceptance remains a deployment step: run the unchanged setup/Arrow
GraphQL job-history queries 20 times, require p95 ≤250 ms after restart and under
normal ingestion, inspect the Jobs page, then reconcile the reviewed cleanup batch.
Only measured recovered space counts toward the backfill reserve.

Migration acceptance additionally requires `python tools/metadata_capacity_report.py
--evidence <measured-evidence.json>`. Its schema is `CapacityEvidence`: every source
identity and history must match, all retained captured logs must be verified, current
Dagit state must match, and an actual replay at ten times observed ingress must leave
no growing projection backlog. Missing proof or a failed storage/latency condition
exits nonzero. Compression samples and lower-bound capacity estimates do not certify
production. Source retention grows with authoritative coverage; if required evidence
cannot fit the capacity limits, the check fails instead of discarding it.


Small asset outputs (up to 64 KiB in Dagster's existing serialized form) share
`storage/.origo-outputs.sqlite`. Their bytes and checksums are preserved; normal
Dagster input loads, partition paths and overwrite behavior use the packed IO
manager. New handled-output metadata identifies the database and its storage key.
Larger values and run-scoped op outputs keep their filesystem representation.
Both services select `origo.maintenance.io_manager.packed_io_manager` through
Dagster's default IO-manager environment settings; silent fallback is disabled.

After every writer uses that manager, migrate existing asset outputs with
`python tools/metadata_pack_outputs.py --instance-home <home> --backup-receipt
<receipt.json> --all-writers-use-packed-io --max-runtime-seconds 600`. It uses the
same per-output locks as live writes, commits at most 100 values together, verifies
all committed bytes, and only then unlinks their original files. Interrupted runs
resume from remaining files. A conflicting raw/packed value fails visibly and
retains both. Preserve this migration's output with the maintenance run. Rollback
must retain the packed IO manager as well as both compatible SQLite adapters.

A source run whose history is still being read or written is deferred to the next
inventory. Lock contention is reported as `source_in_use`, leaves the source
history intact, and creates no compaction error; unrelated projection retirement
continues. Genuine snapshot, integrity and I/O failures retain their error status
until compaction succeeds.

Source event transitions lock only the affected run. A slow archive or historical
read cannot take a global lock away from unrelated runs' live logging. Packed
reads release the transition lock after obtaining their immutable image. Database
upgrades and instance wipes retain Dagster's quiesced-instance requirement.

Local run-status sensor dependencies are matched by repository as well as job name.
A cursor left by a renamed code location cannot pin a different repository's new
projection runs. Current repository cursors still protect unconsumed events. Failure
protection tests for a newer matching run directly, without sorting all historical
runs to find the latest one.

A source shard that cannot be compacted does not stop unrelated projection
retirement. Its original history remains available; the full exception reaches
Dagit logs and a per-run error stays in the archive database. The health check
continues to fail with the unresolved count and a bounded sample of errors across
later invocations. Successful compaction clears that run's error. A failure after
archive commit still retries its unfinished shared-JSON compaction. Work deadlines
remain resumable checkpoints; a broken shared storage backend still fails the job.

Metadata cleanup reuses database engines while opening a fresh connection and
transaction for each storage operation. Retired native event shards are unlinked
through the existing guarded artifact phase without initializing or migrating
their discarded schema. Live retry references are queried at each retirement;
sensor state is reused only while the scheduler database's `data_version` is
unchanged on one autocommit connection. A commit during a sensor-state read causes
a fresh read before the candidate can be retired. The work deadline still applies.
