## What was done
- Added the `playwright` Python runtime dependency to the control-plane package and refreshed `control-plane/uv.lock` at version `1.2.69`.
- Patched [docker/Dockerfile.control-plane](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/docker/Dockerfile.control-plane) to install the Debian/Chromium runtime libraries required by Playwright on the deployed control-plane image and to run `python -m playwright install chromium` during image build.
- Added [tests/contract/test_s34_etf_browser_runtime_contract.py](/Users/mikkokotila/Library/Mobile%20Documents/com~apple~CloudDocs/WIP/projects/Origo/tests/contract/test_s34_etf_browser_runtime_contract.py) so Slice 34 fails loudly if the control-plane dependency set, Docker runtime, or deploy workflow drops browser support again.
- Updated the Slice 34 work plan, top-level plan, changelogs, developer docs, and investigation log so the ETF browser runtime is an explicit hardened-runtime contract instead of tribal knowledge.

## Current state
- Local proof shows the control-plane image now builds successfully with Playwright and Chromium installed.
- Deterministic local replay passed twice with the same validation summaries and the same Docker image id `sha256:d5bcb93c91764e241f7e6d003d733cd561872e4915800086033c77582a1b45a8`.
- `S34-C5f` is implemented on branch but is not complete until the fix is merged, deployed to `main`, and the live ETF Dagster path is rerun without the missing-Playwright runtime failure.

## Watch out
- Slice 34 as a whole is still open; these artifacts only close the local proof leg for `S34-C5f`.
- The live ETF rerun must happen on the deployed image, not on a local container, because the real gate is the hardened server Dagster runtime.
- `S34-C5e` still remains after this runtime fix: ETF historical completeness must be limited to archived issuer artifacts, with explicit failure on archive coverage gaps.
