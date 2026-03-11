# Historical Contract Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-11
- Slice/version reference: S25 (API v0.1.16)

## Purpose and scope
- User reference for shared historical request behavior on Origo historical dataset routes.
- Covers normalized parameter naming and selector semantics.

## Shared request contract
- `mode`
- `start_date`
- `end_date`
- `n_latest_rows`
- `n_random_rows`
- `fields`
- `filters`
- `strict`

Route-specific additions:
- trades routes: `include_datetime_col`
- klines routes: `kline_size`

## Window behavior
- At most one window selector can be supplied.
- Valid selectors:
  - `start_date` / `end_date` (date-window)
  - `n_latest_rows`
  - `n_random_rows`
- If no selector is supplied, query defaults to full available history (`earliest -> now`).
- Date format is strict UTC `YYYY-MM-DD`.

## Response shape
- `mode`
- `source`
- `sources`
- `row_count`
- `schema`
- `warnings`
- `rows`
- `rights_state`
- `rights_provisional`

## Error behavior
- `200` success
- `404` no rows found
- `409` contract/auth/strict failures
- `503` runtime/backend failure

## Current mode availability
- Historical route contracts accept `mode=native|aligned_1s`.
- Historical route execution in current slice supports `native` only.
- `aligned_1s` requests fail loudly with `409` until S26 completes aligned historical parity.
