# Native book hourly authority

`binance_spot_book` and `binance_perp_book` use the existing revisioned source
framework. Sealed WebSocket minutes publish provisional depth20 (100 ms), depth200
(one second), and the two minute metric projections. CryptoHFTData supplies hourly
authority for the same four projections. This is the declared `hour` interval of
the shared canonical contract; trades and aggregate trades retain `day`.

## Delivery and activation

The canonical schedules run at minute 15. To reconstruct exchange hour H, they
need H's receive-time file and H+1's file: closing exchange updates can arrive in
the latter. At 12:15 UTC the candidate is therefore 10:00–11:00, using the 10Z and
11Z files. Native calendars exclude the still-ineligible final hour. This adds
about one hour to authoritative delivery compared with single-file acceptance;
WebSocket minutes remain provisional and readable during that delay.
The existing audit runs at :05, :20, :35 and :50, keeping its 15-minute cadence
without launching simultaneously with the :15 canonical refresh. It retries up to five requested inactive
hours, prioritizing the latest and recent delayed hours before least-recently
checked historical requests; persistent failures cannot permanently occupy all five slots;
revision checks cover two recent hours and rotate through 50 older active hours.
Both sources keep their own canonical pool, bounded to two builds per market.

Discovery appends provider availability and original observation time to the
existing source observation log. A fresh 404 for the latest closed hour is waiting,
not a discovery failure. C1 reports the requested exchange hour, required closing-file hour and last check time;
stale availability evidence becomes UNKNOWN after 20 minutes. Once availability
is observed, activation has five minutes. An observed available file that remains
unactivated then fails. R1 independently judges actual reader freshness and missing
tail minutes; C2 independently counts historical missing hours. Waiting for HCD
cannot hide either failure. Before the first observation, the existing initial
20-minute window applies; absence of evidence after that fails.

A build downloads the original current and following vendor files, retains their
SHA-256 values, replays native update IDs and actual exchange clocks, and requires
every grid sample in the hour. An actual exchange event must witness the final
100-ms sample. A checkpoint cannot skip missing updates inside the requested hour;
a tail shorter than the five-second freshness allowance still fails without that
witness. Only complete activation masks overlapping provisional minutes.
The ordinary component checks, dependency revalidation and atomic activation follow.
Failure preserves the existing active/provisional state and native failure evidence.

The REST file names are `binance_spot/YYYY-MM-DD/HH/BTCUSDT_orderbook.parquet`
and `binance_futures/YYYY-MM-DD/HH/BTCUSDT_orderbook.parquet`. The provider's
[order-book format](https://www.cryptohftdata.com/docs/rest-orderbook) carries
snapshots and original diff-depth fields. Current files preserve receive order; legacy merged collectors preserve native ID order and their original clocks. Replay verifies
spot `U/u` and perpetual `pu`, rejects stale/crossed/insufficient-depth states, and
requires a real vendor checkpoint at or before the start. Historical files may require preceding vendor hours. A missing checkpoint fails visibly; no
Binance seed or invented state repairs it. Complete hours have 36,000 depth20 rows,
3,600 depth200 rows, and 60 rows in each minute projection. Canonical activation
masks the overlapping provisional minutes in the existing current-reader views.

Both vendor sources share the existing host-keyed transport ledger, limited to
30 requests/minute, separately from Binance host-family quotas. Discovery,
backfill, replay, revalidation and monitoring make zero Binance requests. Capture
seed/connection limits and weighted Binance allowances are unchanged.

## Operator workflow and coverage

Deployment activates the generated schedules, sensors and provisional workers.
No manual enablement or source configuration in Dagit is required. The Jobs page
exposes `backfill_binance_spot_book_source_job` and
`backfill_binance_perp_book_source_job` with native hourly partition selection.
Select hours, missing/failed partitions, or retries; every launch builds all four
canonical projections and runs ordinary completion checks.

Canonical keys use `YYYY-MM-DDTHHZ`, distinct from the shared second-qualified
provisional minute keys, so activation and failure recovery cannot collide.

Native calendars begin at **2025-06-28 07:00 UTC for spot** and
**2025-06-28 06:00 UTC for perpetual**, the first whole recorded hours after the
provider's partial first files. File availability does not certify completeness:
the first perpetual hour passes full replay; the first spot hour fails because its
recording cannot prove depth200 throughout the hour. Such gaps remain failed native
partitions and missing hours in C2. Historical yield is not assumed to be 100%.

Deployment admits only the explicitly declared former book anchor,
2026-10-04 00:00 UTC, and changes it to the registered hourly start under ordinary
source setup under the existing exclusive maintenance fence. It records the previous/new anchor in the existing observation log,
verifies the synchronized update, and preserves every active generation. All other
anchor changes fail. Repeated setup is idempotent. Rolling back code requires an
explicit corresponding calendar migration; old code rejects the expanded anchor.
Audit retires requests before the exact first hour and former daily keys by appending
recovery evidence; it does not delete them. C2 remains red for unfilled history.
A full day of accepted canonical hours permits local spool payload cleanup after
two days; incomplete days retain unacknowledged input and immutable seals remain.

The existing monitor owns the mandatory R1/C1/C2 checks for both CANARY books.
`/law` exposes source/projection evidence, reader minute counts and age, the latest
authoritative hour/deadline, and expected/validated/missing older hours. A current
reader cannot hide absent authoritative history. Capture health remains an
independent operational check in the same monitor. Existing Render collectors and
consumers remain active until a separate reviewed routing/promotion change proves
the replacement and required consumer/history coverage. Do not retire them merely
because this authority extension is deployed.

## Real-data acceptance

Original spot/perpetual provider files for **2026-10-04 09:00–10:00 UTC** prove
complete grids and native activation. Inputs stay private; CI prepares them from
the provider without Binance calls, and no vendor market data is committed or
published. Provider unavailability fails acceptance rather than substituting data.

Historical files before August 19, 2026 can have an outer Zstandard wrapper and
fragmented duplicate-collector messages. The adapter streams decompression, groups
actual rows by native ID, and uses the matched update's exchange clock for legacy
REST snapshots. Updates precede their same-ID snapshots even when collector
arrival order differs; a REST collector clock cannot close an exchange hour.
Same-ID snapshots must agree with all proven prices, including observed quantities
and deletions outside the complete interval, and extend
known depth without discarding older proven prices. Inputs and market timestamps
are never fabricated, sorted by a future clock, or shifted.

Prelude search is bounded to 24 preceding hours, 256 MiB per expanded file and
1 GiB combined replay input. Original and expanded inputs plus the grid coexist in
private scratch during a build. Worst-case dependency discovery/revalidation uses
26 metadata reads and download uses 26 files; the shared 30/minute HCD ledger paces
all requests. Binance demand is zero. Modern hours use the current and following
files; historical hours can also require preceding inputs.
The existing capacity monitor measures mounted storage; the bounded pool limits
concurrent scratch. Full-range import remains a native operator selection.

A revision binds the current object plus every ordered preceding and following
ETag and original input hash. The v2 revision prefix makes ordinary audit recheck
previous single-file generations against this completeness policy. Bounded private dependency metadata lets ordinary discovery and audit
notice a changed predecessor; revalidation rejects it before activation. Losing
that derived metadata can cause a rebuild, never accept changed bytes under an old
identity. Missing files, sequence links, freshness, depth proof, bounds or scratch
remain explicit failures.

Native GUI acceptance on October 5, 2026 used the registered jobs and a real
ClickHouse instance on a 4 GiB APFS volume. All-history selection exposed the
expanded calendars; the ordinary date picker and partition bar selected hours,
without typed dates, configuration or identifiers. Final-policy run
`e1bbc9a9-0e4e-4fe5-bf6e-b4af65946dd4` rebuilt perpetual `2025-06-28T06Z`
as generation 2, build `25c9f5d5-d80b-4cd8-a41b-1df50782e651`, using original
05Z/06Z/07Z files. Its four products contain 36,000/3,600/60/60 rows. The terminal
exchange event is 07:00:00.093 UTC, proving the final 06:59:59.900 sample;
source content hash is
`0b1fb41b5178039785f08dd3ba8fea92097a56ef3f473e00371ba3a6037c1e57`.

That run activated its data in a 35-second run but its completion step rejected a
retained earlier audit failure. Native audit retry
`1d245530-0d26-47b7-ad3d-dffd9132b5fe` succeeded; native Re-execute all
`a3afba2e-9736-4e98-9f06-2400e2e4dbad` then completed in 20 seconds,
retaining generation 2. This distinguishes activation from successful completion.
The earlier 06Z/14Z runs predate the closing-file policy and are not final-policy
acceptance. Spot `2025-06-28T07Z` failed depth proof; native Re-execute reproduced
the failure while retaining already accepted generations.

On an Apple M1 Max with 64 GiB RAM, the final historical rebuild's ordinary
capacity receipts measured a peak 117,448,704-byte increase on the shared mounted
volume, including original/expanded files and replay grids. These paths share one
volume; their receipts must not be summed. Scratch lives under the existing source
lock mount so the monitor observes it. A separate preceding-file local replay
measured about 294 MB peak RSS; it predates the closing-file addition and is not a
final process-memory bound. Neither finite measurement is a universal bound.

Observed single-generation hours used up to about 5.2 MB compressed for spot and
6.5 MB for perpetual, with 52.24 MB uncompressed per market-hour. Extrapolating
these samples over approximately 11,140 hours per market suggests about 130 GB
compressed (1.16 TB uncompressed) for both markets combined. This is a planning
estimate, not proof of full-range yield or production throughput. Retained
revisions, concurrent scratch, indexes, logs and ordinary capacity reserves add
space. Existing admission checks remain mandatory for a full native backfill.
