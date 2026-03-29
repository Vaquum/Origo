## Run metadata
- Date: 2026-03-28 to 2026-03-29
- Scope: Slice 34 ETF sub-slices `S34-C5f` live runtime proof, `S34-C5e` live archive-only replay proof, `S34-C5g` local archive-revision-selection proof, `S34-C5h` live issuer-specific history-contract proof, `S34-C5i` live iShares holiday/no-data proof, `S34-C5j` local zero-history snapshot-boundary proof, and `S34-C6f` local FRED live-safe vintage-window env-contract proof.
- Fixture window:
  - `S34-C5f`: 2026-03-28 local runtime-proof replay (`run 1` uncached image build, `run 2` cached rebuild).
  - `S34-C5e`: 2026-03-28 local archive-replay validation (`run 1` and `run 2` on the focused ETF archive-replay contract bundle).
  - `S34-C5g`: 2026-03-28 local archive-revision-selection validation (`run 1` and `run 2` on the focused ETF revision-selection contract bundle).
  - `S34-C5h`: 2026-03-28 live issuer-specific ETF history-contract validation (iShares bootstrap to `2026-03-26` plus live ETF rerun after deploy).
  - `S34-C5i`: 2026-03-28 live iShares holiday/no-data validation (bootstrap persisted `22` official no-data days followed by live ETF rerun with no holiday-gap failure).
  - `S34-C5j`: 2026-03-28 local zero-history snapshot-boundary validation (`run 1` and `run 2` on the focused ETF zero-history bundle).
  - `S34-C6f`: 2026-03-29 local FRED env-contract validation (`run 1` and `run 2` on the focused FRED client/backfill contract bundle plus `ruff` and `pyright`).
- Runtime environment:
  - Local workspace: `/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo`
  - Branches:
    - `codex/s34-etf-playwright-runtime`
    - `codex/s34-etf-artifact-replay`
    - `codex/s34-etf-archive-revisions`
    - `codex/s34-etf-history-contract`
    - `codex/s34-etf-ishares-holiday-contract`
    - `codex/s34-etf-zero-history-boundary`
    - `codex/s34-fred-live-post-104`
    - `codex/s34-fred-vintage-window-env`
  - Local host: macOS workstation
  - Docker target: `docker/Dockerfile.control-plane`
  - Live runtime target for ETF proofs: `deploy-dagster-daemon-1` on the production host after `main` deploy
- Additional 2026-03-29 scope:
  - Slice 34 cross-cutting sub-slice `S34-C2l` local proof for append-only ETF/FRED reconcile reset boundaries.
  - Live evidence source for the blocker: Dagster run `affcd2a9-3538-4a54-a638-996da5027e74` plus ClickHouse mutation `mutation_24566.txt` after merged `#104`.

## System changes made as proof side effects
- Refreshed `control-plane/uv.lock` after adding `playwright` to the control-plane package metadata.
- Built local Docker image `origo-control-plane:playwright-proof-fixed` twice; the resulting image id is `sha256:d5bcb93c91764e241f7e6d003d733cd561872e4915800086033c77582a1b45a8`.
- Ran `docker builder prune -f` locally to clear stale builder cache pressure before the successful proof build.
- Queried the live ETF object-store archive inventory from `deploy-dagster-daemon-1` and confirmed the currently persisted ETF raw-artifact manifests are sparse and recent-run scoped rather than full-history complete.
- Inspected the live Bitwise and Grayscale raw artifacts directly from object storage and replayed the Bitwise pair through the real adapter code to isolate the archive-selection bug to future/out-of-scope revision noise.
- Audited the live ETF object-store archive inventory against live ETF canonical partitions and confirmed the currently persisted historical footprint is effectively iShares-only plus sparse recent captures for the snapshot-only issuers.
- Added a dedicated repo-native iShares archive bootstrap runner so first-party historical raw artifacts can be persisted to object storage before the ETF replay job claims those days.
- Live iShares archive bootstrap proved out to `2026-03-26` and exposed the next honest contract issue: official holiday no-data responses must be persisted and consumed as negative evidence instead of being treated as missing historical partitions.
- Deployed `#89`, reran the live iShares bootstrap, persisted `22` official no-data days, and reran ETF live to verify the holiday-gap issue was gone before isolating the next zero-history snapshot-only issuer boundary.
- Added ClickHouse migrations `0045__create_canonical_partition_reset_boundaries.sql` and `0046__create_canonical_event_log_active_v1.sql`.
- Captured live FRED evidence showing the old proof-mismatch blocker was fixed but the reset path stalled on a huge synchronous `canonical_event_log` delete mutation against one March-2026 part.
- Rewired live canonical proof/planner/projector/writer-identity reads to use `canonical_event_log_active_v1` and patched ETF/FRED reconcile reset paths to leave `canonical_event_log` append-only.
- Added required env `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST` and rewired the FRED revision-history chunk planner to read the initial live-safe window from runtime config instead of a hidden code constant.

## Known warnings and disposition
- Local Docker proof required one cache cleanup because the builder’s apt archive area was exhausted. Acceptable for the proof run; this was a local builder-state issue, not a repo/runtime contract issue.
- `S34-C5f` is now live-proven and should be marked complete.
- `S34-C5e` is now live-proven: the ETF backfill path is archive-only on `main`.
- `S34-C5g` remains open until the revision-selection path is merged, deployed, and rerun live against the real ETF archive inventory.
- `S34-C5h` is now live-proven: issuer-specific ETF historical boundaries are the active live contract.
- `S34-C5i` is now live-proven: official iShares holidays are no longer treated as missing historical partitions once the no-data evidence is archived.
- `S34-C5j` is locally proven but not yet live-proven. The merged runtime still needs the zero-history snapshot-only issuer contract deployed so those sources stop blocking ETF replay as fake incomplete coverage.
- `S34-C2l` is locally proven but not yet live-proven. The merged runtime still needs the append-only reset-boundary patch deployed so FRED can finish the reset-and-rewrite phase without opening another giant `canonical_event_log` mutation.
- `S34-C6f` is locally proven but not yet live-proven. The next merged FRED rerun must confirm the deployed env file carries the new vintage-window variable and that Dagster no longer depends on a hidden code default.

## Deferred guardrails
- None inside `S34-C5f` / `S34-C5e` / `S34-C5g` scope. Remaining ETF historical completeness work is now about acquiring or validating enough first-party archive coverage, not about replay/runtime fallbacks.
- Remaining work after `S34-C2l` is live proof, not local contract work: merge/deploy the patch, rerun the same FRED reconcile window, and verify terminal proof without destructive canonical deletes.
- Remaining work after `S34-C6f` is live proof, not local transport logic: merge/deploy the env contract, rerun the same bounded FRED tranche, and treat Dagster logs as the source of truth for whatever blocker remains.

## Closeout confirmation
- Work-plan checkboxes updated: partially. `S34-C2l`, `S34-C5e`, `S34-C5h`, `S34-C5i`, and `S34-C6f` are now checked off locally/live as noted above, while `S34-C5g`, `S34-C5j`, and the remaining live Slice-34 dataset closures are still open pending merge/deploy/live proof.
- Version updated: yes (`origo_control_plane 1.2.81`).
- Changelog updated: yes (`CHANGELOG.md` and `control-plane/CHANGELOG.md`).
- `.env.example` reviewed against slice changes: yes; added `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST` and removed the hidden code default for that runtime contract.
- Slice artifacts created: yes (`manifest.md`, `run-notes.md`, `baseline-fixture-2026-03-28.json`, `baseline-fixture-2026-03-28-etf-archive-replay.json`, `baseline-fixture-2026-03-28-etf-archive-revision-selection.json`, `baseline-fixture-2026-03-28-etf-history-contract.json`, `baseline-fixture-2026-03-28-etf-holiday-contract.json`, `baseline-fixture-2026-03-28-etf-zero-history-boundary.json`, `baseline-fixture-2026-03-29-fred-append-only-reset-boundary.json`, `baseline-fixture-2026-03-29-fred-vintage-window-env.json`).
