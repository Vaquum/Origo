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
   `python /opt/app/tools/retire_cryptohftdata.py --evidence /opt/origo/locks/retirement-608.jsonl`
   first. The tool refuses nonlocal canonical specs, active book automation/work,
   unfinished mutations, unexpected source namespaces and unbounded protection rows.
   Its durable ownership manifest records exact vendor builds, referenced runs and
   events before deleting provenance. It refuses any remaining scratch database
   whose exact name matches a recorded vendor build. Resolve only those verified
   names before continuing; never delete databases by a broad prefix. Referenced
   observer runs can also contain native events and are not exclusively vendor-owned.
4. Invoke the same command with `--apply` one batch at a time. Each invocation
   appends private, durable evidence, removes at most one table/month and rechecks
   exact count/SHA256 proofs of immutable native provisional minutes. The owning
   agent admits no manual book work during this window. Stopped instigators and
   source-wide run/backfill checks are rechecked before every mutation; the tool's
   own lock serializes retirement commands, not deployed source writers. It never
   holds the shared heavy fence that live minute ingestion needs. Stop on any
   error, unverified mutation or change in protected live operation. Do not retry
   an unfinished database mutation blindly. A dry-run with `statement: null` marks
   completion of this database phase. An unmatched or failed apply blocks later
   operations against the same evidence ledger. Use `--reconcile --evidence FILE`
   for a read-only recheck of that original operation's pins and exact row outcome;
   it accepts only zero remaining rows or the original unchanged count. Partial
   outcomes and changed protected data stay blocked, with no waiver/new baseline.
   A partial Dagster wipe requires investigation before its complete outcome can
   be proved. Keep the same private evidence file throughout the retirement.
5. Remove only the provider's isolated `book_vendor_spot` and `book_vendor_perp`
   cache directories and `binance_rest_budget.api_cryptohftdata_com.state`. Keep
   source domain markers, other quota ledgers, all native spool files and all other
   volumes. Remove the retired credential from deployment secrets and local
   deployment configuration after the release no longer references it. Delete
   no shared Dagster run history: vendor completion runs can also contain native
   receipts. After the database phase has no candidate, run the same command with
   `--dagster` to inventory the two canonical assets. `--dagster --apply` uses the
   supported exact-asset wipe only after every materialization is proven to have
   a vendor revision and an hourly key, and every planned event has an hourly key.
   Native daily facts, observations or unknown events refuse the wipe. Separate
   provisional/feed assets and their tags remain. Physical historical audit tags
   may remain behind Dagster's wipe boundary; public APIs must expose no vendor facts.
6. Merge and deploy the validated release only after the obsolete hourly metadata
   has been removed. Bootstrap re-enables managed book automation, so deploying
   daily code first would allow daily audit/reconciliation to encounter retired
   hourly keys. The release also executes `tools/book_deploy_preflight.py` before
   compatibility/bootstrap/container replacement; it refuses retired metadata and
   hourly canonical Dagster facts using bounded read-only checks. The release's native-only pin preserves live collector/worker
   identities while updating orchestration and monitoring.
7. Verify zero vendor database generations/cache objects and no provider requests;
   verify the original native/depth minute proofs and advancing live minutes,
   worker identity/restart counts and unrelated publication state. Report existing
   gaps honestly. A removed provider history is not repaired native history.
8. Briefing caches live responses for 20/60 seconds and historical responses for
   at most one hour from response construction. Cache hits do not extend expiry.
   After that hour, a fresh book request prunes expired server entries. Already
   loaded historical pages need a reload. No native-book file consumers or export
   directories were found; preserve the original depth Arrow series.

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
Initiating queries use one thread, 256 MiB and a ten-second query deadline. These
settings do not bound the background work that rewrites database parts. Qualify
one affected-part mutation alongside the live feeds before proceeding. Every apply
waits synchronously; a timed-out mutation remains unverified and blocks the next
invocation until inspected.

## Native retention prerequisite

The existing local daily adapter requires every captured minute. A day with a
capture gap cannot become canonical, and its payloads would have no whole-day
cleanup acknowledgment after hourly authority is retired. The operator approved
reclaiming temporary copies only after their live-collected data is safely stored.
The existing native audit checks at most 64 payloads and eight old calendar days
per invocation, only for incomplete days older than two days. It requires the exact
seal-derived native revision and all four physical component proofs, retains the
seal, and leaves stored rows unchanged. Complete live-captured days remain eligible
for the existing daily build; no imported provider history is preserved by this
rule. These live captures predate CryptoHFTData and are explicitly protected.
