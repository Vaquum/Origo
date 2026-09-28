# Recorded raw-trade regression fixtures

`2026-09-21/` preserves genuine recent-endpoint responses and the historical
crosscheck from commit `63bc1c3` (`tests/fixtures/steady_state/perp_recent/`).
The original provenance and response bytes are unchanged. The official daily ZIP
was downloaded on 2026-09-28 and verified against its published checksum before
extracting the two complete captured minutes: 9,948 and 6,647 trades. Their full
archive parity and the separate 499-row historical crosscheck run offline in CI.

The busy processing regression uses the existing historical-response fixture in
`../high_volume_trades/` as labeled offline input, with its original trade IDs,
timestamps and archive comparison. It is not a recording of the recent endpoint
under busy market conditions. Missing or corrupt committed fixtures fail tests.

Live recent-endpoint throughput, repair cost and production freshness remain
validation work on PRD #460. CI has no capture schedule, future-archive dependency,
acceptance manifest or saved implementation-hash attestation.
