# Historical ETF API Reference

- Last updated: 2026-03-28
- Scope: ETF historical daily metrics dataset (`etf_daily_metrics`)

## Endpoint

- `POST /v1/historical/etf/daily_metrics`

All requests require `X-API-Key`.

## Request contract

- `mode: native | aligned_1s` (default `native`)
- `start_date: YYYY-MM-DD | null`
- `end_date: YYYY-MM-DD | null`
- `n_latest_rows: int | null`
- `n_random_rows: int | null`
- `fields: string[] | null`
- `filters: object[] | null` (`eq|ne|gt|gte|lt|lte|in|not_in`)
- `strict: bool` (default `false`)

Window selector rules:
- At most one selector mode is allowed:
  - date-window (`start_date` and/or `end_date`)
  - `n_latest_rows`
  - `n_random_rows`
- If no selector is provided, full available history is returned (`earliest -> now`).

Date semantics:
- `start_date` is inclusive at `00:00:00Z`.
- `end_date` is inclusive via next-day exclusive bound.
- Invalid dates (for example `2022-02-30`) return contract error.

## Historical availability semantics

- ETF historical availability is issuer-specific and proof-gated.
- `etf_ishares_ibit_daily`:
  - official historical source with date-parameter support
  - can claim business-day history from `2024-01-11` forward once the raw-artifact archive and canonical proof boundary cover that day
  - official market-closure days are excluded from required history only when Origo has archived the first-party no-data response for that `asOfDate`
- Snapshot-only ETF issuers:
  - `etf_ark_arkb_daily`
  - `etf_bitwise_bitb_daily`
  - `etf_coinshares_brrr_daily`
  - `etf_fidelity_fbtc_daily`
  - `etf_franklin_ezbc_daily`
  - `etf_grayscale_gbtc_daily`
  - `etf_hashdex_defi_daily`
  - `etf_invesco_btco_daily`
  - `etf_vaneck_hodl_daily`
- Snapshot-only issuer history begins at the first valid archived raw artifact day and does not extend backward from later canonical leftovers.
- No-selector ETF requests return the full currently proved history that satisfies those issuer-specific availability boundaries.

## Response contract

Response fields:
- `mode`
- `source`
- `sources`
- `row_count`
- `schema`
- `warnings`
- `rows`
- `rights_state`
- `rights_provisional`

## Schema taxonomy

Native rows:
- `metric_id`
- `source_id`
- `metric_name`
- `metric_unit`
- `metric_value_string`
- `metric_value_int`
- `metric_value_float`
- `metric_value_bool`
- `observed_at_utc`
- `dimensions_json`
- `provenance_json`
- `ingested_at_utc`
- `datetime`

Aligned rows:
- `aligned_at_utc`
- `source_id`
- `metric_name`
- `metric_unit`
- `metric_value_string`
- `metric_value_int`
- `metric_value_float`
- `metric_value_bool`
- `dimensions_json`
- `provenance_json`
- `latest_ingested_at_utc`
- `records_in_bucket`
- `valid_from_utc`
- `valid_to_utc_exclusive`

## Warnings and strict behavior

Window warnings:
- `WINDOW_LATEST_ROWS_MUTABLE`
- `WINDOW_RANDOM_SAMPLE`

ETF quality warnings:
- `ETF_DAILY_STALE_RECORDS`
- `ETF_DAILY_MISSING_RECORDS`
- `ETF_DAILY_INCOMPLETE_RECORDS`

`strict=true` escalates warnings to `409` (`STRICT_MODE_WARNING_FAILURE`).

## Errors

- `200`: success
- `404`: no data in selected window
- `409`: contract/auth/strict-warning failures
- `503`: runtime/backend/warning-evaluation failures
