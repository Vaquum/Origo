# Deployment Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice/version reference: S10 + S32 (platform deploy contract + env-drift fix)

## Purpose and scope
- User-facing reference for how Origo server deployments are applied after code merges.

## Inputs and outputs with contract shape
- Trigger: merge to `main`.
- Output:
  - fresh API/control-plane images for merged commit
  - migration-first apply
  - running server stack with updated services

## Data definitions (fields, types, units, timezone, nullability)
- Deployment image tag format:
  - `ghcr.io/vaquum/origo-api:<commit-sha>`
  - `ghcr.io/vaquum/origo-control-plane:<commit-sha>`
- Runtime services after successful deploy:
  - `api`
  - `dagster-webserver`
  - `dagster-daemon`
  - `clickhouse`

## Source/provenance and freshness semantics
- Deploy provenance is tied to GitHub Actions run id + commit SHA.
- Runtime image verification is done during deploy and surfaced in workflow logs.

## Failure modes, warnings, and error semantics
- Missing deploy secrets/env contracts fail deployment immediately.
- Missing/invalid `ORIGO_AUDIT_LOG_RETENTION_DAYS` fails deployment before compose service startup.
- Migration failure fails deploy and prevents partial silent rollout.
- Service image mismatch after apply fails deployment.

## Determinism/replay notes
- Same-commit deploy reruns are validated by comparing runtime verified service-image fingerprints.
- Reference artifact:
  - `spec/slices/slice-10-image-based-server-deploy/baseline-fixture-2026-03-08_2026-03-10.json`

## Environment variables and required config
- Deploy is controlled by repository secrets and server env contract.
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` is sourced from root `.env.example` and synchronized into `/opt/origo/deploy/.env` on deploy.
- No deployment-specific runtime values are hard-coded in code paths.

## Minimal examples
- Verify live API endpoint after deploy:
  - `curl -sS http://origo.vaquum.fi/health`
- Query one dataset after deploy:
  - `curl -sS -X POST 'http://origo.vaquum.fi/v1/raw/query' -H 'Content-Type: application/json' -H 'X-API-Key: <internal-key>' --data '{"mode":"native","sources":["binance_spot_trades"],"time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"],"strict":false}'`
