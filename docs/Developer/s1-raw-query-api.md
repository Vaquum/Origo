# Slice 1 Developer: Raw Query API Adapter

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-04
- Slice reference: S1 (`S1-C7`, `S1-G1` to `S1-G5`)

## Purpose and scope
- Defines FastAPI adapter for native raw query: `POST /v1/raw/query`.
- Adapter validates request contract and delegates to proven internal native query core.

## Inputs and outputs
- Input model: `RawQueryRequest` in `api/origo_api/schemas.py`.
- Output model: `RawQueryResponse` in `api/origo_api/schemas.py`.
- Header contract: `X-API-Key` required for internal auth.

## Data definitions
- `sources`: query source list (current capability requires exactly one):
  - `spot_trades | spot_agg_trades | futures_trades | etf_daily_metrics | fred_series_metrics`.
- Window mode: at most one of `time_range | n_rows | n_random`.
- No selector defaults to full available history (`earliest -> now`).
- `filters`: optional projection filter clauses (`eq|ne|gt|gte|lt|lte|in|not_in`).
- `warnings`: structured warning list with `code`, `message`.
- `rows`: wide-row payload with schema metadata.

## Source, provenance, freshness
- API data is query-time read from ClickHouse native tables.
- Freshness is bounded by ingestion completion for each source table.

## Failure modes and status mapping
- `404`: no rows for query window (`QUERY_NO_DATA`).
- `409`: contract/auth/strict failures (`QUERY_CONTRACT_ERROR`, `AUTH_INVALID_API_KEY`, `STRICT_MODE_WARNING_FAILURE`).
- `503`: runtime/backend/queue failures (`QUERY_RUNTIME_ERROR`, `QUERY_BACKEND_ERROR`, `QUERY_QUEUE_LIMIT_REACHED`).

## Determinism and replay notes
- Determinism follows query kernel semantics and fixed ordering rules.
- Replay checks are recorded in `spec/slices/slice-1-raw-query-native/run-notes.md`.

## Environment and required config
- `ORIGO_INTERNAL_API_KEY`
- `ORIGO_QUERY_MAX_CONCURRENCY`
- `ORIGO_QUERY_MAX_QUEUE`

## Minimal example
- Request:
  - `POST /v1/raw/query`
  - `X-API-Key: <internal key>`
  - body includes one source + zero-or-one window selector
- Result:
  - `200` with schema + rows
  - or mapped error code/status above
