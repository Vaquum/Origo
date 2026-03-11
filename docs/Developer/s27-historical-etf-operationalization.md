# S27 Historical ETF Operationalization

## Metadata
- Owner: Origo API
- Last updated: 2026-03-11
- Slice reference: S27 (API v0.1.18)

## Purpose and scope
- Operationalize ETF historical access on both internal Python and HTTP surfaces.
- Scope is dataset `etf_daily_metrics` only.
- Keep shared historical contract semantics from S25.

## Capability delivered
- Added Python method:
  - `HistoricalData.get_etf_daily_metrics`
- Added HTTP endpoint:
  - `POST /v1/historical/etf/daily_metrics`
- Added shared historical query helper:
  - `query_etf_daily_metrics_data(...)`
- Enabled both historical modes:
  - `native`
  - `aligned_1s`
- Date-window execution uses strict UTC day bounds:
  - `start_date` inclusive at `00:00:00Z`
  - `end_date` inclusive through next-day exclusive bound

## Contract behavior
- Shared request params:
  - `mode`, `start_date`, `end_date`, `n_latest_rows`, `n_random_rows`, `fields`, `filters`, `strict`
- Window mode is fail-loud:
  - at most one of date-window, `n_latest_rows`, `n_random_rows`
  - no selector means full available history
- Filter then projection execution order is preserved.

## Guardrails
- Auth:
  - `X-API-Key` required
- Rights:
  - rights gate + rights metadata parity with raw query
- ETF quality warnings:
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
- Strict mode:
  - `strict=true` returns `409` when any warning exists
- Error taxonomy:
  - `200`, `404`, `409`, `503`

## Key implementation files
- `origo/data/_internal/generic_endpoints.py`
- `origo/data/historical_data.py`
- `api/origo_api/schemas.py`
- `api/origo_api/main.py`
- `tests/contract/test_historical_spot_api_contract.py`
- `tests/replay/test_historical_exchange_replay.py`
- `tests/replay/test_native_query_replay.py`

## Notes
- ETF historical rights checks require `ORIGO_ETF_QUERY_SERVING_STATE=promoted`.
- Full contract/integrity suites that import `origo_control_plane` require that package available on `PYTHONPATH` in the executing environment.
