# Native book hourly authority

`binance_spot_book` and `binance_perp_book` use the existing revisioned source
framework. Sealed WebSocket minutes publish provisional depth20 (100 ms), depth200
(one second), and the two minute metric projections. CryptoHFTData supplies hourly
authority for the same four projections. This is the declared `hour` interval of
the shared canonical contract; trades and aggregate trades retain `day`.

## Delivery and activation

The canonical schedules run at minute 15 of each UTC hour and select the previous
closed hour. The existing audit retries up to five requested unactivated partitions
and checks revisions through the provider object ETag. Discovery uses a one-byte
range request; a build downloads one vendor Parquet file, retains its input SHA-256,
replays native update IDs and exchange timestamps through the capture's sampler,
and proves a complete hour before the ordinary atomic activation. The same native
component/hash validation and revision revalidation precede activation. Failure
preserves the previous active/provisional state and appends native source evidence.

The REST file names are `binance_spot/YYYY-MM-DD/HH/BTCUSDT_orderbook.parquet`
and `binance_futures/YYYY-MM-DD/HH/BTCUSDT_orderbook.parquet`. The provider's
[order-book format](https://www.cryptohftdata.com/docs/rest-orderbook) carries
snapshots and original diff-depth fields. Replay preserves receive order, verifies
spot `U/u` and perpetual `pu`, rejects stale/crossed/insufficient-depth states, and
requires an initial vendor checkpoint. A missing checkpoint fails visibly; no
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

The immutable source/history anchor is **2026-10-04 00:00 UTC**. Older vendor
history is not admitted by this calendar. The audit ignores requests before that
anchor and appends a recovery reason to their prior failures, retaining the events.
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
