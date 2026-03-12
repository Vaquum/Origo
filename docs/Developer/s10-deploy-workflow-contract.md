# Slice 10 Developer: Deploy Workflow Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-12
- Slice reference: S10 (`S10-C1`, `S10-C2`, `S10-C3`, `S10-C4`, `S10-G1`, `S10-G2`, `S10-G3`)
- Version reference: API `v0.1.23`, control-plane `v1.2.55`

## Purpose and scope
- Defines the CI deployment contract for image-based server apply on merge to `main`.
- Scope covers build, publish, bootstrap, migration, and compose restart behavior.

## Inputs and outputs with contract shape
- Inputs:
  - workflow: `.github/workflows/deploy-on-merge.yml`
  - Dockerfiles: `docker/Dockerfile.api`, `docker/Dockerfile.control-plane`
  - deploy compose contract: `deploy/docker-compose.server.yml`
  - runtime env contract: `/opt/origo/deploy/.env` on server
- Outputs:
  - `ghcr.io/vaquum/origo-api:<commit-sha>`
  - `ghcr.io/vaquum/origo-control-plane:<commit-sha>`
  - migrated ClickHouse schema before service restart
  - running services: `api`, `dagster-webserver`, `dagster-daemon`, `clickhouse`

## Data definitions (field names, types, units, timezone, nullability)
- `ORIGO_API_IMAGE`: string image reference (`ghcr.io/...:<sha>`).
- `ORIGO_CONTROL_PLANE_IMAGE`: string image reference (`ghcr.io/...:<sha>`).
- `ORIGO_DEPLOY_API_PORT`: integer string (`1..65535`).
- `ORIGO_AUDIT_LOG_RETENTION_DAYS`: integer string (`>=365`) sourced from root `.env.example` and written to server runtime env on deploy.
- `ORIGO_SERVER_ENV_B64`: base64-encoded env file content string (required when server env file is missing).
- Verified service image tuple: `service` (string) + `image` (string).

## Source/provenance and freshness semantics
- Build provenance is tied to GitHub run metadata and commit SHA tags.
- Deploy apply path always uses prebuilt registry images (no server-side docker build).
- Server runtime env is treated as durable contract in `/opt/origo/deploy/.env`.
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` is enforced from root `.env.example` during deploy to keep runtime policy contract in sync.

## Failure modes, warnings, and error codes
- Missing required secrets or env values fails workflow immediately.
- Missing `/opt/origo/deploy/.env` without `ORIGO_SERVER_ENV_B64` fails first-run bootstrap.
- Migration failure fails deploy before full stack restart.
- Service image mismatch after apply fails deploy and prints compose status.

## Determinism/replay notes
- Replay proof compares same-commit deploy attempts and verifies service image tags and compose runtime state.
- Baseline evidence artifact:
  - `spec/slices/slice-10-image-based-server-deploy/baseline-fixture-2026-03-08_2026-03-10.json`

## Environment variables and required config
- Required GitHub secrets:
  - `ORIGO_SERVER_IP`
  - `ORIGO_SERVER_USER`
  - `ORIGO_SERVER_PASSWORD`
  - `ORIGO_SERVER_KNOWN_HOSTS`
  - `ORIGO_SERVER_ENV_B64` (conditional first-run requirement)
- Required runtime env keys on server:
  - `ORIGO_API_IMAGE`
  - `ORIGO_CONTROL_PLANE_IMAGE`
  - `ORIGO_DOCKER_API_PORT`
  - `ORIGO_AUDIT_LOG_RETENTION_DAYS` (`>=365`)
  - `ORIGO_BITCOIN_CORE_*`
  - existing query/export/runtime env contract keys

## Minimal examples
- Trigger deploy on merge:
  - merge PR to `main`.
- Manual replay deploy for same commit:
  - rerun `deploy-on-merge` workflow in Actions UI for the same run.
