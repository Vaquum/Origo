# Raw-perp request efficiency and egress evidence

Implements [PRD448](https://github.com/Vaquum/Origo/issues/448) / [S449](https://github.com/Vaquum/Origo/issues/449).
This file records evidence and deployment boundaries; setup is owned by versioned
code and requires no operator preparation commands.

## Baseline

Main `055302c49ae6eaf91a1c5c53465f28cd08a26bdc` used one shared FAPI budget at
24 weight/second, with a 1,920 used-weight backstop. The September 23 05:47 UTC
audit measured about 38 contiguous raw-perp minutes/hour against 60 arrivals.
Daily canonical files arrive later; their replacement is excluded from REST gains.

## Verified provider cost

At 2026-09-23T07:27:16.453243Z, an authenticated production read of
`/fapi/v1/historicalTrades?symbol=BTCUSDT&fromId=8086999063&limit=500`
returned HTTP 200 and 497 rows. A time request followed by that page, under the
existing FAPI lock, produced used-weight headers **201 then 401**: page cost 200.
Both network responses took 0.635 seconds; the local limiter charged 201 for both.
The response hash was
`462bcc55a03c353cd1ece16e355854a415ebd7963935c253662d1a3de5a9e59c`,
identical to committed real fixture `page-01.json`.

The original September 18 capture records seven successive +200 header deltas.
[Current Binance documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#old-trades-lookup)
also specifies 200 and a 500-row maximum. Older SDK metadata saying 20 is stale.
No credential or arbitrary response headers are retained here.

## Paging evidence

The immutable September 16 20:00 UTC corpus contains 2,325 rows, including three
before locator raw ID `f`. Original pages 00–06 cost 1,420 weight including the
locator. Starting at `f-500` reuses exact captured pages 01–06 at 1,220 weight,
a 14.08% reduction, while returning identical archive rows/hashes. This is one
real minute, not a universal throughput benchmark. Recorded-input fault cases
prove bounded recovery and rejection; they are not new provider captures.

First-page pre-minute proof, ordered raw IDs/times, actual end-boundary evidence,
500-row requests and the total 100-page bound remain. A short page is not EOF.
The predecessor lookup and f-64 approach were removed from this intervention.

A six-hour September 23 sample had 260 completed observation records averaging
8.792 historical pages. Raw-completion gaps across 2,285 successive requests had
p50=8.342s and p90=10.023s; within-minute gaps had p50=33.233s. These timestamps
mix pacing, concurrency and network. They do not isolate every provider latency.

## Deployment and failure boundaries

Automatic raw-perp minutes use `.140`/`.144` by absolute-minute modulo over the
configured one/two-address pool. Other sources and native repairs keep `.167`.
The same address controls socket binding and the persistent host/IP limiter;
pool changes preserve existing cooldown/circuit files. No automatic failover.
A lost address can block the contiguous frontier and occupy the oldest-gap slots;
explicit reviewed singleton configuration is the supported recovery path.

Deployment validates a temporary copy of the actual host Netplan configuration,
then adds secondary addresses without replacing primary routes. Candidate checks
are local binding/default-route checks; provider/dependency outages do not block
pre-up deployment. Containers detach stdin from the SSH-delivered script.
Rollback uses a revert PR, without a separate saved-state mechanism.

Host networking applies only to this worker. It uses existing loopback services
and a deployment diagnostic hostname; no new listener is introduced. Any future
listener requires explicit loopback binding review. These identifiers do not
implement the ownership/lease work proposed in #439.

If Binance's account restricts API-key source IPs, zero-bang owns adding the two
addresses in the account security controls. This does not weaken the allowlist.
Provider acceptance remains an observed post-deployment requirement.

Future books under #399/#400 use `.167` with the same spot/FAPI budgets as its
other callers; collector implementation and reconnect load are not certified here.
S449 lands before overlapping #439 limiter changes, which must retain per-IP state.

## Acceptance status

Implementation validation results are recorded in the PR. Production egress and
sustained improvement are **pending deployment**; requested-IP metadata alone is
not public-egress proof. Post-deploy evidence must name the image/configuration,
bound sockets/routes, provider acceptance, per-IP progress, recovery frequency,
weight/completed minute, mount duration and other-feed freshness.

The qualifying two-hour window needs backlog, unchanged canonical coverage,
at least 144 new distinct provisional minutes, contiguous frontier advance >120 minutes,
falling missing-minute debt, recovery <50%, no new 429/418 and no new freshness
failure in previously current feeds. Keep failed windows visible. Modelled gains
(~1.25–1.60× arrivals on mean demand) are not measured production acceptance.
