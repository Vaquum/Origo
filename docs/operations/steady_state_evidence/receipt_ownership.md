# S439: worker attempt ownership and receipt migration

This is implementation evidence for the ownership part of M05 and for the receipt
schema/rollback proof, not a declaration that S439, SS-06 or production acceptance is
complete. Nothing here was run against production; every measurement below comes from
the pinned ClickHouse 25.3.2.39 image on an owned local container.

## Ownership

Both feed workers hold a shared-filesystem lifetime `flock` on an owner marker
(`<lock root>/worker-owners/<feed>/<epoch>.json`) for a unique owner UUID. Each
admitted unit records a separate attempt UUID, stable work identity, intended
publication token and prerequisite family; STARTED and terminal events share that
identity. Source activation and mount-manifest commit check the active execution
owner. Native Dagster execution has no feed-owner context and retains its existing
generation/partition fencing.

Death is proven only by acquiring the marker's lock from another process; a durable
`.retired` fence is then published and recovery (`recover_confirmed_dead`) requires
that fence. A progressing live process cannot be declared dead because an observer's
clock advanced past 300 seconds, and an unknown or unreadable marker is reported as
UNKNOWN, never treated as dead. A retired epoch can no longer record STARTED or OK
(`WORKER_OWNER_FENCED`); only recovery appends its FAILED/WORKER_DIED event, carrying
the attempt's own identity and intended token.

Marker creation, fencing and pruning serialize on one per-feed registry lock.
Retention is bounded by `prune_released_owners`: a marker whose lifetime lock is free
and whose epoch has no open attempt in the receipt table is removed together with its
fence. An owner named as outstanding keeps its evidence until recovery has failed its
attempts. Nothing is deleted on age.

## Schema: additive, old writers untouched

`worker_minute_log` keeps its eleven insertable columns and its sorting key
`(feed, series, minute, recorded_at)`. One ALTER adds:

```sql
ALTER TABLE origo.worker_minute_log
  ADD COLUMN _attempt String EPHEMERAL '',
  ADD COLUMN work_id String MATERIALIZED JSONExtractString(_attempt, 'work_id'),
  ADD COLUMN attempt_id UUID MATERIALIZED toUUIDOrZero(JSONExtractString(_attempt, 'attempt_id')),
  ADD COLUMN owner_epoch String MATERIALIZED JSONExtractString(_attempt, 'owner_epoch'),
  ADD COLUMN state_token String MATERIALIZED JSONExtractString(_attempt, 'state_token'),
  ADD COLUMN prerequisite_key String MATERIALIZED JSONExtractString(_attempt, 'prerequisite_key'),
  ADD COLUMN event_id UUID MATERIALIZED toUUIDOrZero(JSONExtractString(_attempt, 'event_id'))
```

Verified on the pinned image (`tests/origo_source_native/test_steady_state_recovery.py`):

- An old binary's eleven-value positional `INSERT ... VALUES` and `SELECT *` keep
  exactly eleven fields before and after the upgrade; column-subset inserts also work.
- Rows written without `_attempt` read an empty/zero identity; no historical row is
  rewritten and no identity is invented for old attempts.
- Modern writes name the eleven base columns plus `_attempt` (a JSON object). The
  materialized identities survive `OPTIMIZE ... FINAL` into a single active part.
- Rollback: the old writer inserting after the upgrade, then the newer binary
  restarting, leaves every row and identity intact. Preparation is idempotent and
  refuses any other shape (`RECEIPT_SCHEMA_PARTIAL`), including the physical-column
  layout with an extended sorting key that an earlier draft used.

Because the sorting key is unchanged, ReplacingMergeTree still collapses rows that
share `(feed, series, minute, recorded_at)`. Every modern write of one triple is
serialized under one of 64 fixed hash-shard locks (`<lock root>/worker_receipts/
shard_NN.lock`) and reads the triple's stored maximum `recorded_at`; the row's
`recorded_at` is the real clock, truncated to the stored millisecond, strictly after
that maximum. A clock that has not passed the stored millisecond is waited on for at
most two seconds, then the write fails `RECEIPT_CLOCK_SKEW`; a stored stamp further
ahead fails immediately. No historical timestamp is invented. Old writers do not take
the shard lock; the deployment stops both old workers before the new image starts,
which is why only modern writers ever share a triple concurrently.

Event identity is `uuid5(attempt_id, status)`. A repeated write of an existing event
compares status, error code, work, owner, token, prerequisite, sha256 and rows and the
event's row count; identical content is a no-op, anything else is
`RECEIPT_IDENTITY_CONFLICT`. A new event for an attempt that already has a terminal
event is `WORKER_ATTEMPT_TERMINAL`. A publication receipt's `sha256` must equal the
attempt's intended token, so `failed_attempts(token=...)` keeps counting recovered
publication failures under the original token.

## Legacy ambiguity

Identity-less STARTED rows older than the horizon without a later terminal row of the
same `(feed, series, minute)` unit are the pre-identity unresolved set. They are read
with keyset pagination (`legacy_unresolved_page`, 1,000 rows per page, cursor on
series/minute/millisecond) and counted across at most ten pages
(`legacy_unresolved_count`); a count that stopped at the bound is reported as "at
least". `reconcile_died_receipts` logs that count as UNKNOWN ownership every tick and
never fails those rows. No native resolution exists yet: a resolving operator action
would have to record a terminal receipt for the unit with its own provenance, which is
not implemented and not claimed.

## Tests executed here

`tests/origo_source_native/test_steady_state_recovery.py` (5 tests): real spawned
child owner terminated by SIGTERM; no death on an observer clock one hour ahead;
retirement and single idempotent recovery with preserved token/attempt/owner;
stale-owner STARTED/OK rejected; live same-minute attempt untouched; pruning of the
recovered owner only; schema upgrade/merge/rollback with old writers; forced
same-millisecond collisions under a controlled clock; conflicting retries; bounded
clock-collision handling; 2,300-row legacy pagination with identical timestamps.
Also executed unchanged: `test_provisional_worker.py`, `test_depth_worker.py`,
`test_monitor.py` receipt cases and `tests/tools/test_deploy_receipt_migration.py`.
A mutation removing the `recorded_at` floor makes the collision test fail (7 rows
collapse to 4 after `OPTIMIZE FINAL`).

The six-hour fault/load trial and the 72-hour production window have not been run.
