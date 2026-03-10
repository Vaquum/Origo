# Docker Local Environment Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice/version reference: S7 (API `v0.1.2`, control-plane `v1.2.52`)

## Purpose and scope
- User-facing reference for local Docker environment variables required to run Origo services and proofs.

## Inputs and outputs with contract shape
- Input: `.env` values.
- Output: successful compose startup and proof execution.

## Data definitions (fields, types, units, timezone, nullability)
- Host ports are integer strings:
  - `ORIGO_DOCKER_DAGSTER_PORT` (default local example: `14000`)
  - `ORIGO_DOCKER_API_PORT` (default local example: `18000`)
- URL fields are absolute HTTP URLs:
  - `ORIGO_DOCKER_DAGSTER_GRAPHQL_URL`
  - `ORIGO_DOCKER_API_BASE_URL`

## Source/provenance and freshness semantics
- `.env.example` is the single source of truth for required local config.
- Runtime code and scripts fail loudly on missing or empty required values.

## Failure modes, warnings, and error codes
- Empty or missing value causes immediate runtime failure with variable name.
- No alias/fallback variable names are accepted.

## Determinism/replay notes
- Stable env values are required for replay comparability between runs.
- Changing ports/paths between runs can invalidate comparability of artifacts.

## Environment variables and required config
- Docker service mapping:
  - `ORIGO_DOCKER_CLICKHOUSE_HOST`
  - `ORIGO_DOCKER_CLICKHOUSE_PORT`
  - `ORIGO_DOCKER_CLICKHOUSE_HTTP_PORT`
  - `ORIGO_DOCKER_CLICKHOUSE_USER`
  - `ORIGO_DOCKER_CLICKHOUSE_PASSWORD`
  - `ORIGO_DOCKER_CLICKHOUSE_DATABASE`
  - `ORIGO_DOCKER_DAGSTER_HOST`
  - `ORIGO_DOCKER_DAGSTER_PORT`
  - `ORIGO_DOCKER_API_PORT`
  - `ORIGO_DOCKER_DAGSTER_GRAPHQL_URL`
  - `ORIGO_DOCKER_API_BASE_URL`
  - `ORIGO_DOCKER_EXPORT_ROOT_DIR`
  - `ORIGO_DOCKER_ETF_ANOMALY_LOG_PATH`
- API and export runtime:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_DAGSTER_REPOSITORY_NAME`
  - `ORIGO_DAGSTER_LOCATION_NAME`
  - `ORIGO_DAGSTER_EXPORT_JOB_NAME`
  - `ORIGO_EXPORT_MAX_CONCURRENCY`
  - `ORIGO_EXPORT_MAX_QUEUE`
- Bitcoin Core node runtime:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`
- Core ClickHouse contract:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`

## Minimal examples
- Validate env file exists and is non-empty:
  - `test -s .env`
- Run proof with env contract:
  - `scripts/s7_docker_stack.sh proof`
