## What was done
- Retrospective closeout completed after-the-fact for Slice 10 using preserved GitHub Actions evidence.
- Added deploy capability/proof/guardrail evidence references for:
  - merge-triggered build-and-push with commit-pinned tags
  - remote bootstrap + image-only deploy apply path
  - replay deploy verification on the same commit
- Added required slice artifacts for handoff:
  - `spec/slices/slice-10-image-based-server-deploy/baseline-fixture-2026-03-08_2026-03-10.json`
  - `spec/slices/slice-10-image-based-server-deploy/run-notes.md`

## Current state
- Deployment is handled by `.github/workflows/deploy-on-merge.yml` on push to `main`.
- Runtime server apply remains image-only:
  - API image tag: `ghcr.io/vaquum/origo-api:<commit-sha>`
  - control-plane image tag: `ghcr.io/vaquum/origo-control-plane:<commit-sha>`
- Required deployment secrets remain fail-loud:
  - `ORIGO_SERVER_IP`
  - `ORIGO_SERVER_USER`
  - `ORIGO_SERVER_PASSWORD`
  - `ORIGO_SERVER_KNOWN_HOSTS`
  - `ORIGO_SERVER_ENV_B64` (required when `/opt/origo/deploy/.env` is missing)
- Latest replay proof evidence in this closeout uses run `22892668220` attempts `1` and `2` with matching verified runtime service image tags.

## Watch out
- This closeout was reconstructed retrospectively; evidence is complete for run metadata and outcomes, but some low-level first-run branch-path details are partially inferential from preserved logs.
- Replay evidence is currently tag-level/runtime-state deterministic. Build output digests differed between reruns of the same commit, so strict digest-level reproducibility is not proven by this artifact set.
- If digest-level reproducibility is required later, the deploy proof contract should be tightened to pin and compare image digests, not only commit tags.
