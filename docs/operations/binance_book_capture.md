# Native Binance book capture

`binance_spot_book` and `binance_perp_book` start as CANARY sources beside the
existing trade and Render book pipelines. Each source declares depth20 snapshots
at 100 ms, depth200 snapshots at one second, and the corresponding minute metrics.
Each component has a provisional `_latest` counterpart. There are no aliases,
retired tables or public file consumers in this slice.

## Acquisition and API demand

One BTCUSDT diff-depth WebSocket and one working book per market serve both depths.
Spot initialization requests depth 5000 (weight 250); perpetual initialization
requests depth 1000 (weight 20). Every seed uses the existing weighted Binance
adapter and `egress_ip=None`, which shares the primary `.167` host-family ledger.
Both capture services mount the same writable `source-locks` volume as existing
callers. The `.140` recent-trade capture and `.144` historical-repair routes and
all existing host-family allowances are unchanged.

Seeds are permitted at initialization, proven sequence loss, or exhaustion of the
seeded known depth region. At most three attempts per market per rolling hour are
durably reserved before transport; failed and uncertain attempts consume capacity.
Only one seed per market may be in flight. Connection attempts are capped at five
per market per rolling five minutes, with at most two overlapping connections.
Process replacement retains these reservations and the shared provider cooldown.
429 responses retain Retry-After pacing; 418 responses retain the provider circuit.
Exponential reconnect backoff adds jitter and reaches 60 seconds. Exhaustion leaves
a visible gap until the applicable limit expires.

A planned rotation starts after 23 hours, before the provider's 24-hour connection
limit. The initialized book transfers only after the new stream bridges its last
update ID; a verified handover makes no REST request. Sampling, local `/top20`
readers, native minute/day builds, retries, backfills, health and monitoring make
zero Binance REST requests. None increases the shared API allowance.

## Time, completeness and native operations

Rows use UTC grids and the last fully applied exchange event `E` at or before each
grid timestamp. Spot checks `U/u` continuity; perpetual checks its initial covering
frame and subsequent `pu`. Unverified, crossed, stale or insufficiently known books
cannot emit samples. No interpolation fills a missing minute.

A minute becomes eligible only after its event-time watermark closes and all 600
and 60 samples are sealed. Immutable minute metadata binds compressed local input
by SHA-256. The spool allows 16 GiB per market and refuses to discard unacknowledged
input when full. Accepted canonical days retain minute metadata; their payloads
can be removed after two days. Forced rebuilds after payload removal fail visibly.
CryptoHFTData history/gap reconstruction belongs to the separate vendor slice;
this implementation does not claim that recovery is already connected.

The source's `available` policy exposes verified minutes beyond a gap. Trade
sources retain their existing `contiguous` policy. Daily canonical activation still
requires all 1,440 sealed minutes. Each registered source inherits native Dagster
setup, schedules, sensors, failure records and retry controls. Its independent
provisional worker builds all four declared projections automatically.

The native jobs are `backfill_binance_spot_book_source_job` and
`backfill_binance_perp_book_source_job`. Use Jobs and native partition selection for
all available days, missing/failed days or selected gaps. One launch builds every
canonical component; retries reuse verified generations. No operator configuration
text or separate projection launch is needed. The first partition date is the first
UTC day after the planned deployment and must be checked against the merge date.

## Local endpoint and monitoring

The spot service exposes authenticated `GET /top20` on host loopback port 8088.
`ORIGO_BOOK_TOP20_TOKEN` is supplied through the deployment secret. Requests need
`Authorization: Bearer <token>`. Missing/wrong credentials return 401; stale or
unverified books return 503. Valid responses contain `t` (exchange event ms) and
`d` (`lastUpdateId`, `bids`, `asks`) with decimal strings and `Cache-Control: no-store`.
It uses the existing working book; each reader creates no subscription or seed.
Perpetual capture has no listener. Consumer repointing remains a separate change.

The existing monitor reads `book_capture_<market>.status.json` on the shared
heartbeat volume, at most 16 KiB per market per tick. It observes acquisition age,
seal age, spool use, seed count/charged weight and connection count. It makes no
provider call and mounts no spool. Findings appear in Dagit's existing
`origo_monitor` checks; minute build receipts and source failures keep their native
stores. Investigate Dagit, ClickHouse, Docker, then external collectors.

## Verification and promotion

Pre-merge verification uses short, original Binance recordings dated 2026-10-02.
The entire acquisition made four REST requests: two spot and two perpetual, charged
500 and 40 weight respectively through the shared limiter. Perpetual reconstruction
matched an independent REST seed; spot reconstruction matched over 1,000 independent
partial-depth20 WebSocket states at identical update IDs. Fixtures include endpoint,
receive times, original frames, snapshots and SHA-256 provenance. The recordings
stopped; no multi-day test process remains running.

The tests exercise real minute grids, both projections, immutable retries, missing
canonical-day failure, gaps, seeded-depth exhaustion, handover, authentication,
request budgets and shared cooldown persistence. Wide component hashing uses
2,048-row pages; a full page of original depth-200 event states measured 67 MiB
peak query memory (2,245 states, two pages, 65 ms total on the local test instance).
The regression ceiling is 256 MiB. Trade hash chunk sizes and identities are unchanged. They do not simulate a complete
UTC day by inventing market rows or moving timestamps. A positive complete-day
canonical/backfill run uses ordinary accumulated CANARY input after deployment.

Promotion and Render retirement require a separate reviewed change and production
evidence: full-day native GUI success, capture restart and handover, minute
publication p95/max, seed/connection demand, 429/418 attribution and existing-feed
freshness/gap debt against comparable workload. Replay must add zero REST requests.
Before deployment, record a finite overlap end and candidate stop/retirement policy;
a failed comparison keeps Render authoritative and never expands Binance quota.
