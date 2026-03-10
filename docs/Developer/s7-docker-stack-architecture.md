# Slice 7 Developer: Docker Stack Architecture

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice reference: S7 (`S7-C1`, `S7-C2`, `S7-C4`)
- Version reference: API `v0.1.2`, control-plane `v1.2.52`

## Purpose and scope
- Defines the local Docker runtime architecture for Origo end-to-end proof runs.
- Scope covers service topology, network wiring, persistent storage, and runtime contracts.

## Inputs and outputs with contract shape
- Inputs:
  - `docker-compose.yml`
  - `docker/Dockerfile.api`
  - `docker/Dockerfile.control-plane`
  - root `.env` values required by compose variable expansion
- Outputs:
  - five running services: `clickhouse`, `bitcoin-core`, `dagster-webserver`, `dagster-daemon`, `api`
  - API available on `http://localhost:${ORIGO_DOCKER_API_PORT}`
  - Dagster webserver available on `http://localhost:${ORIGO_DOCKER_DAGSTER_PORT}`

## Data definitions (field names, types, units, timezone, nullability)
- Service names: string identifiers (`clickhouse`, `bitcoin-core`, `dagster-webserver`, `dagster-daemon`, `api`).
- Port vars: integer strings (`ORIGO_DOCKER_DAGSTER_PORT`, `ORIGO_DOCKER_API_PORT`).
- ClickHouse credentials: non-empty strings (`ORIGO_DOCKER_CLICKHOUSE_USER`, `ORIGO_DOCKER_CLICKHOUSE_PASSWORD`).
- Runtime URLs: absolute HTTP URL strings (`ORIGO_DOCKER_DAGSTER_GRAPHQL_URL`, `ORIGO_DOCKER_API_BASE_URL`).

## Source/provenance and freshness semantics
- Docker images are built from monorepo source at current git commit.
- ClickHouse and Dagster metadata persist through named volumes:
  - `clickhouse-data`
  - `bitcoin-core-data`
  - `dagster-instance`
  - `origo-storage`
- Freshness semantics are unchanged from source-level ingestion/query contracts.

## Failure modes, warnings, and error codes
- Missing or empty compose env variable fails at container startup (`${VAR:?message}`).
- Missing runtime env in scripts fails loudly with explicit variable name.
- Service healthcheck failures fail proof steps and emit logs to slice artifacts.

## Determinism/replay notes
- Determinism proof for Docker stack is captured in:
  - `spec/slices/slice-7-docker-local-platform/proof-s7-local-docker.json`
  - `spec/slices/slice-7-docker-local-platform/baseline-fixture-2017-08-17_2017-08-18.json`

## Environment variables and required config
- Required Docker runtime mapping vars:
  - `ORIGO_DOCKER_CLICKHOUSE_HOST`
  - `ORIGO_DOCKER_CLICKHOUSE_PORT`
  - `ORIGO_DOCKER_CLICKHOUSE_HTTP_PORT`
  - `ORIGO_DOCKER_CLICKHOUSE_USER`
  - `ORIGO_DOCKER_CLICKHOUSE_PASSWORD`
  - `ORIGO_DOCKER_CLICKHOUSE_DATABASE`
  - `ORIGO_DOCKER_DAGSTER_HOST`
  - `ORIGO_DOCKER_DAGSTER_PORT`
  - `ORIGO_DOCKER_API_PORT`
  - `ORIGO_DOCKER_EXPORT_ROOT_DIR`
  - `ORIGO_DOCKER_ETF_ANOMALY_LOG_PATH`
- Required Bitcoin Core node vars:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`

## Minimal examples
- Build and start stack:
  - `scripts/s7_docker_stack.sh up`
- Stop stack:
  - `scripts/s7_docker_stack.sh down`
- Tail stack logs:
  - `scripts/s7_docker_stack.sh logs`
