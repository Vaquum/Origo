# Slice 10 Developer: Bootstrap and Apply Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S10 (`S10-P1`, `S10-P2`, `S10-P3`)
- Version reference: API `v0.1.5`, control-plane `v1.2.55`

## Purpose and scope
- Operational runbook for first-run host bootstrap and repeat deploy apply behavior.

## Inputs and outputs with contract shape
- Inputs:
  - SSH credentials + known-host pin
  - encoded deploy artifacts and env payload from workflow
  - registry auth token
- Outputs:
  - host dependencies installed if missing (`docker`, `compose`, required OS tools)
  - deploy manifests and config files synced to `/opt/origo`
  - services restarted with target image tags

## Data definitions (field names, types, units, timezone, nullability)
- `run_id`: GitHub workflow run identifier.
- `run_attempt`: GitHub rerun attempt integer.
- `service_count`: number of services verified post-deploy.
- `verified_services_hash_sha256`: deterministic hash of verified service-image pairs.

## Source/provenance and freshness semantics
- Run evidence is sourced from workflow logs and GitHub run metadata.
- Bootstrap/install-if-missing behavior is idempotent and safe to re-run.

## Failure modes, warnings, and error codes
- Unsupported distro for bootstrap package install fails deployment.
- Missing runtime env values in server env file fails deployment.
- Missing compose container IDs after restart fails deployment.

## Determinism/replay notes
- Compare `run_1_fingerprints` and `run_2_fingerprints` in Slice 10 baseline fixture.
- Service image tag verification is mandatory for replay proof acceptance.

## Environment variables and required config
- First-run critical key: `ORIGO_SERVER_ENV_B64`.
- Deploy routing key: `ORIGO_DEPLOY_API_PORT`.
- Image keys: `ORIGO_API_IMAGE`, `ORIGO_CONTROL_PLANE_IMAGE`.

## Minimal examples
- Evidence files:
  - `spec/slices/slice-10-image-based-server-deploy/run-notes.md`
  - `spec/slices/slice-10-image-based-server-deploy/baseline-fixture-2026-03-08_2026-03-10.json`
