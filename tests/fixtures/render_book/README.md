# Real regional top20 recordings

Four gzip tar bundles contain the original immutable NDJSON segments and JSON
manifests from A Frankfurt and B Singapore, for BTCUSDT Spot and USD-M perpetuals.
The selected exchange interval is 2026-10-24 13:00:00–14:00:00 UTC, end exclusive.
Each bundle contains 61 arrival segments, including the boundary observations
needed to select the complete exchange hour. No market values or clocks changed.

`provenance.json` records source URLs, capture date and bundle SHA256 values.
Every original manifest retains its own exact payload count, byte length, SHA256
and original clock bounds. No credentials are included.

Tests remove segment availability, interrupt transfer/acknowledgement and delete
an isolated database minute. The conflicting-identity test deliberately duplicates
one genuine observation with a changed receipt clock, updating the test transport
manifest; it is a named protocol fault, never accepted market history.

The tests qualify the adapter/cache, canonical input pin, top20/minute components,
registered hourly recovery, retained-content cleanup and Spot Arrow publication
through the existing source runtime. Production routing, deployment, GUI behavior
and shared-worker headroom require separate qualification.

`status-A.json` and `status-B.json` are original authenticated `/status` responses.
`status-provenance.json` records their capture times, URLs and exact SHA256 values.
Monitor tests replay them and identify deliberate clock/readiness protocol faults.
