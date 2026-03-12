# S25 Historical Contract Normalization

## Metadata
- Owner: Origo API
- Last updated: 2026-03-11
- Slice reference: S25 (API v0.1.16)

## Purpose and scope
- Normalize historical request semantics across Python `HistoricalData` and historical HTTP routes.
- Normalize raw query selector behavior to match target optional-window semantics.
- Scope in this slice is contract normalization only; historical `aligned_1s` execution remains deferred to S26.

## Inputs and outputs with contract shape
- Historical shared parameter set on routes/models/methods:
  - `mode`
  - `start_date`
  - `end_date`
  - `n_latest_rows`
  - `n_random_rows`
  - `fields`
  - `filters`
  - `strict`
- Historical route-specific extras:
  - trades: `include_datetime_col`
  - klines: `kline_size`
- Raw query selector contract (`POST /v1/raw/query`):
  - at most one selector: `time_range | n_rows | n_random`
  - no selector: full available history (`earliest -> now`)
- Historical selector contract (`/v1/historical/*`):
  - at most one selector: `date-window | n_latest_rows | n_random_rows`
  - no selector: full available history (`earliest -> now`)

## Data definitions (fields, types, units, timezone, nullability)
- `start_date` / `end_date` are strict UTC day values in `YYYY-MM-DD`.
- Date-window execution uses UTC bounds:
  - start inclusive (`00:00:00Z`)
  - end inclusive via next-day exclusive bound
- `fields` is a projection list; unknown columns fail loudly.
- `filters` uses `{field, op, value}` with ops `eq|ne|gt|gte|lt|lte|in|not_in`.

## Source/provenance and freshness semantics
- This slice does not change source ingest or provenance contracts.
- Historical routes continue to read from canonical exchange projection tables.

## Failure modes, warnings, and error codes
- Multiple selectors in one request fail with contract error (`409`).
- Invalid date formats/values fail with contract error (`409`).
- Unknown projection/filter fields fail loudly with contract error (`409`).
- Historical `mode=aligned_1s` requests were fail-loud in S25; execution support is added for exchange historical spot routes in S26.
- `strict=true` continues to escalate warning-bearing mutable/sample windows.

## Determinism/replay notes
- No-selector windows are deterministic:
  - native path orders by canonical event time and source identity key.
- Replay/contract coverage includes bounded and unbounded windows in:
  - `tests/contract/test_api_contracts.py`
  - `tests/contract/test_historical_spot_api_contract.py`

## Environment variables and required config
- No new environment variables in this slice.
- Existing API env contract in root `.env.example` remains authoritative.

## Minimal examples
- Raw query full history:
  - `{ "mode": "native", "sources": ["binance_spot_trades"], "strict": false }`
- Historical trades full history:
  - `{ "mode": "native", "fields": ["trade_id", "price"], "strict": false }`
- Historical date-window query:
  - `{ "mode": "native", "start_date": "2024-01-01", "end_date": "2024-01-01", "strict": false }`
