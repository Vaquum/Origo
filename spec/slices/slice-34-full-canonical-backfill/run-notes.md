## Run metadata
- Date: 2026-03-28
- Scope: Slice 34 ETF sub-slices `S34-C5f` live runtime proof, `S34-C5e` live archive-only replay proof, `S34-C5g` local archive-revision-selection proof, `S34-C5h` local issuer-specific history-contract proof, and `S34-C5i` local iShares holiday/no-data proof.
- Fixture window:
  - `S34-C5f`: 2026-03-28 local runtime-proof replay (`run 1` uncached image build, `run 2` cached rebuild).
  - `S34-C5e`: 2026-03-28 local archive-replay validation (`run 1` and `run 2` on the focused ETF archive-replay contract bundle).
  - `S34-C5g`: 2026-03-28 local archive-revision-selection validation (`run 1` and `run 2` on the focused ETF revision-selection contract bundle).
  - `S34-C5h`: 2026-03-28 local issuer-specific ETF history-contract validation (`run 1` and `run 2` on the focused ETF archive bootstrap + history-contract bundle).
  - `S34-C5i`: 2026-03-28 local iShares holiday/no-data validation (`run 1` and `run 2` on the focused ETF holiday-contract bundle).
- Runtime environment:
  - Local workspace: `/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo`
  - Branches:
    - `codex/s34-etf-playwright-runtime`
    - `codex/s34-etf-artifact-replay`
    - `codex/s34-etf-archive-revisions`
    - `codex/s34-etf-history-contract`
    - `codex/s34-etf-ishares-holiday-contract`
  - Local host: macOS workstation
  - Docker target: `docker/Dockerfile.control-plane`
  - Live runtime target for ETF proofs: `deploy-dagster-daemon-1` on the production host after `main` deploy

## System changes made as proof side effects
- Refreshed `control-plane/uv.lock` after adding `playwright` to the control-plane package metadata.
- Built local Docker image `origo-control-plane:playwright-proof-fixed` twice; the resulting image id is `sha256:d5bcb93c91764e241f7e6d003d733cd561872e4915800086033c77582a1b45a8`.
- Ran `docker builder prune -f` locally to clear stale builder cache pressure before the successful proof build.
- Queried the live ETF object-store archive inventory from `deploy-dagster-daemon-1` and confirmed the currently persisted ETF raw-artifact manifests are sparse and recent-run scoped rather than full-history complete.
- Inspected the live Bitwise and Grayscale raw artifacts directly from object storage and replayed the Bitwise pair through the real adapter code to isolate the archive-selection bug to future/out-of-scope revision noise.
- Audited the live ETF object-store archive inventory against live ETF canonical partitions and confirmed the currently persisted historical footprint is effectively iShares-only plus sparse recent captures for the snapshot-only issuers.
- Added a dedicated repo-native iShares archive bootstrap runner so first-party historical raw artifacts can be persisted to object storage before the ETF replay job claims those days.
- Live iShares archive bootstrap proved out to `2026-03-26` and exposed the next honest contract issue: official holiday no-data responses must be persisted and consumed as negative evidence instead of being treated as missing historical partitions.

## Known warnings and disposition
- Local Docker proof required one cache cleanup because the builder’s apt archive area was exhausted. Acceptable for the proof run; this was a local builder-state issue, not a repo/runtime contract issue.
- `S34-C5f` is now live-proven and should be marked complete.
- `S34-C5e` is now live-proven: the ETF backfill path is archive-only on `main`.
- `S34-C5g` remains open until the revision-selection path is merged, deployed, and rerun live against the real ETF archive inventory.
- `S34-C5h` is locally proven but not yet live-proven. The merged runtime still needs the real iShares archive bootstrap run plus a live ETF rerun before this sub-slice can be marked complete.
- `S34-C5i` is locally proven but not yet live-proven. The merged runtime still needs the holiday/no-data replay path deployed so official iShares holidays disappear from the live missing-partition list.

## Deferred guardrails
- None inside `S34-C5f` / `S34-C5e` / `S34-C5g` scope. Remaining ETF historical completeness work is now about acquiring or validating enough first-party archive coverage, not about replay/runtime fallbacks.

## Closeout confirmation
- Work-plan checkboxes updated: partially. `S34-C5f` is complete, `S34-C5e` is live-proven but not yet checked off on this branch, and `S34-C5g` / `S34-C5h` / `S34-C5i` remain open pending merge/deploy/live ETF archive proof.
- Version updated: yes (`origo_control_plane 1.2.73`).
- Changelog updated: yes (`CHANGELOG.md` and `control-plane/CHANGELOG.md`).
- `.env.example` reviewed against slice changes: yes; no new environment variables were introduced by this ETF history-contract update.
- Slice artifacts created: yes (`manifest.md`, `run-notes.md`, `baseline-fixture-2026-03-28.json`, `baseline-fixture-2026-03-28-etf-archive-replay.json`, `baseline-fixture-2026-03-28-etf-archive-revision-selection.json`, `baseline-fixture-2026-03-28-etf-history-contract.json`, `baseline-fixture-2026-03-28-etf-holiday-contract.json`).
