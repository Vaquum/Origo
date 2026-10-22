# Retire hourly vendor book history

This retirement removes only CryptoHFTData hourly authority. The independent
depth20/depth200 collectors, native spot/perpetual capture, accepted local minutes,
book spool seals, current-reader interfaces and unrelated sources are protected.
Historical changelog and incident records remain audit evidence.

## Order of operations

The owning agent completes these operations; no operator configuration is needed.

1. Record Dagit checks, the exact eight book instigator states, nonterminal book
   runs/backfills, collector/worker identities and current selected outputs. Pin a
   genuine completed minute from each independent depth feed separately from the
   six registered revisioned sources.
2. Stop only the two book sources' canonical/audit schedules and failure/reconciliation
   sensors. Drain their existing jobs and backfills; leave live collection and
   provisional workers running. Recheck this boundary before every deletion.
3. Use the reviewed release source and its unchanged runtime dependencies in an
   isolated command container sharing the existing database, instance and lock
   volumes. Do not replace a live service to run cleanup. Dry-run
   `python -m tools.retire_cryptohftdata --evidence /opt/origo/locks/retirement-608.jsonl`
   first. The tool refuses nonlocal canonical specs, active book automation/work,
   unfinished mutations, unexpected source namespaces and unbounded protection rows.
4. Invoke the same command with `--apply` one batch at a time. Each invocation
   appends private, durable evidence, removes at most one table/month and rechecks
   exact count/SHA256 proofs of immutable native provisional minutes. Stop on any
   error, unverified mutation or change in protected live operation. Do not retry
   an unfinished database mutation blindly. A dry-run with `statement: null` marks
   completion of this database phase.
5. Remove only the provider's isolated `book_vendor_spot` and `book_vendor_perp`
   cache directories and `binance_rest_budget.api_cryptohftdata_com.state`. Keep
   source domain markers, other quota ledgers, all native spool files and all other
   volumes. Remove the retired credential from deployment secrets and local
   deployment configuration after the release no longer references it. Delete
   terminal vendor Dagster runs by verified run identity; never wipe an entire
   shared asset or source history.
6. Merge and deploy the validated release only after the obsolete hourly metadata
   has been removed. Bootstrap re-enables managed book automation, so deploying
   daily code first would allow daily audit/reconciliation to encounter retired
   hourly keys. The release's native-only pin preserves live collector/worker
   identities while updating orchestration and monitoring.
7. Verify zero vendor database generations/cache objects and no provider requests;
   verify the original native/depth minute proofs and advancing live minutes,
   worker identity/restart counts and unrelated publication state. Report existing
   gaps honestly. A removed provider history is not repaired native history.

## Deletion boundaries

`tools/retire_cryptohftdata.py` selects only `binance_spot_book` and
`binance_perp_book`. Revision-bearing tables require the exact
`cryptohftdata-v1:` or `cryptohftdata-v2:` prefix. Shared records without a revision
use verified vendor build/run identities or the retired canonical hourly key and
operation. Vendor activations are removed before their proofs and physical rows,
exposing retained local minutes through the existing views. Canonical tables can
also contain local revisions and therefore stay in place. No `_latest_revisions`
table is a deletion target.

Protection hashes cover bounded accepted provisional minutes; they do not scan
canonical trade days or claim an independent rehash of every historical row.
Read/mutation admission stays at one query thread, 256 MiB and a ten-second query
deadline. Every apply is synchronous; a timed-out mutation remains unverified and
blocks the next invocation until inspected.

## Native retention prerequisite

The existing local daily adapter requires every captured minute. A day with a
capture gap cannot become canonical, and its payloads would have no whole-day
cleanup acknowledgment after hourly authority is retired. A native retention
change requires explicit scope approval and real captured-data preservation proof.
Until that is resolved, retirement is incomplete; available disk space alone does
not establish indefinite live collection safety.
