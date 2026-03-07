# Slice 6 Developer: FRED Ingest and Persistence

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice reference: S6 (`S6-C3`, `S6-C4`, `S6-C5`, `S6-C6`, `S6-C7`)
- Version reference: `control-plane v1.2.49`

## Purpose and scope
- Define the FRED ingest execution path from backfill/update through persistence and query exposure.
- Scope includes ClickHouse + raw-artifact persistence and query-mode availability (`native`, `aligned_1s`).

## Inputs and outputs
- Inputs:
  - connector-normalized rows from `origo.fred`
  - ingest window (`start_date`, `end_date`, `as_of_date`)
  - ClickHouse/object-store runtime env contracts
- Outputs:
  - rows in `origo.fred_series_metrics_long`
  - raw source payload artifacts in object store
  - queryable dataset `fred_series_metrics` in both modes

## Data definitions
- ClickHouse table:
  - `origo.fred_series_metrics_long` (migration `0003__create_fred_series_metrics_long.sql`)
- Query dataset key:
  - `fred_series_metrics`
- Aligned execution paths:
  - `fred_aligned_forward_fill`
  - `fred_aligned_observation`

## Source, provenance, and freshness semantics
- Canonical source remains direct FRED API.
- Persisted provenance carries series metadata and `last_updated_utc`.
- Native query returns source-native row shape from persisted long-metric table.
- Aligned query applies deterministic 1-second alignment logic over persisted rows.

## Failure modes, warnings, and error semantics
- Missing ClickHouse/object-store env vars fail loudly.
- ClickHouse migration or insert failures fail loudly.
- Object-store write failures fail loudly.
- Query contract failures map to typed API error codes (`QUERY_CONTRACT_ERROR`, `QUERY_BACKEND_ERROR`, `QUERY_RUNTIME_ERROR`).

## Determinism and replay notes
- Determinism for fixed windows validated in:
  - `proof-s6-p1-acceptance.json`
  - `proof-s6-p2-determinism.json`
  - `proof-s6-p3-metadata-version-reproducibility.json`
- Native/aligned row hashes are used as replay fingerprints in proof artifacts.

## Environment variables
- ClickHouse:
  - `CLICKHOUSE_HOST`
  - `CLICKHOUSE_PORT`
  - `CLICKHOUSE_HTTP_PORT`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `CLICKHOUSE_DATABASE`
- Object store:
  - `ORIGO_OBJECT_STORE_ENDPOINT_URL`
  - `ORIGO_OBJECT_STORE_ACCESS_KEY_ID`
  - `ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY`
  - `ORIGO_OBJECT_STORE_BUCKET`
  - `ORIGO_OBJECT_STORE_REGION`
- FRED:
  - `FRED_API_KEY`
  - `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`

## Minimal examples
- Backfill proof:
  - `set -a; source .env; set +a; ORIGO_FRED_HTTP_TIMEOUT_SECONDS=20 uv run python -m origo.fred.s6_c3_proof`
- Incremental proof:
  - `set -a; source .env; set +a; ORIGO_FRED_HTTP_TIMEOUT_SECONDS=20 uv run python -m origo.fred.s6_c4_proof`
- Persistence proof:
  - `set -a; source .env; set +a; CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=9000 CLICKHOUSE_HTTP_PORT=8123 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=origo CLICKHOUSE_DATABASE=origo ORIGO_OBJECT_STORE_ENDPOINT_URL=http://localhost:19000 ORIGO_OBJECT_STORE_ACCESS_KEY_ID=origo ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY=origo-secret ORIGO_OBJECT_STORE_BUCKET=origo-raw-artifacts ORIGO_OBJECT_STORE_REGION=us-east-1 ORIGO_FRED_HTTP_TIMEOUT_SECONDS=20 uv run --with clickhouse-connect --with boto3 --with polars --with pyarrow python -m origo.fred.s6_c5_proof`
