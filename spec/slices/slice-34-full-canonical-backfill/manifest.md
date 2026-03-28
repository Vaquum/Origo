## What was done
- Added the `playwright` Python runtime dependency to the control-plane package and refreshed `control-plane/uv.lock` at version `1.2.69`.
- Patched [docker/Dockerfile.control-plane](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/docker/Dockerfile.control-plane) to install the Debian/Chromium runtime libraries required by Playwright on the deployed control-plane image and to run `python -m playwright install chromium` during image build.
- Added [tests/contract/test_s34_etf_browser_runtime_contract.py](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/tests/contract/test_s34_etf_browser_runtime_contract.py) so Slice 34 fails loudly if the control-plane dependency set, Docker runtime, or deploy workflow drops browser support again.
- Updated the Slice 34 work plan, top-level plan, changelogs, developer docs, and investigation log so the ETF browser runtime is an explicit hardened-runtime contract instead of tribal knowledge.
- Rewired [control-plane/origo_control_plane/jobs/etf_daily_ingest.py](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/control-plane/origo_control_plane/jobs/etf_daily_ingest.py) so historical ETF backfill replays only archived issuer-source artifacts from object storage instead of issuing fresh live issuer requests during the Slice-34 backfill path.
- Added focused archive-replay contract coverage in [tests/contract/test_etf_daily_backfill_job_contract.py](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/tests/contract/test_etf_daily_backfill_job_contract.py) for missing coverage, exact duplicate artifact deduplication, and fail-loud invalid archive behavior.

## Current state
- Local proof shows the control-plane image now builds successfully with Playwright and Chromium installed.
- Deterministic local replay passed twice with the same validation summaries and the same Docker image id `sha256:d5bcb93c91764e241f7e6d003d733cd561872e4915800086033c77582a1b45a8`.
- `S34-C5f` is live-proven on `main`: the deployed ETF Dagster path now gets past the missing-Playwright failure and reaches real issuer payload parsing.
- `S34-C5e` is implemented on the current branch but still needs merge, deploy, and live rerun proof. The next honest live ETF failure should be archive completeness or invalid archived payloads, not a fallback live issuer request.
- The current server archive remains sparse. Historical ETF completeness cannot be claimed beyond the archived issuer artifacts already stored under `raw-artifacts/`.

## Watch out
- Slice 34 as a whole is still open; these artifacts only close the local proof leg for `S34-C5f`.
- The live ETF rerun for `S34-C5e` must happen on the deployed server Dagster runtime; local proof alone cannot validate archive inventory or claimed coverage.
- Invalid archived issuer artifacts are not acceptable historical coverage. If the stored raw artifact is a security checkpoint or other interstitial payload, the backfill must fail loudly rather than counting the blob as “archive present”.
- ETF historical completeness still depends on building out real archive coverage for every required issuer/day. This branch only makes the runtime honest about that boundary.
