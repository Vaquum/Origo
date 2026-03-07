# Data Taxonomy

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-07
- Slice/version reference: S1-S6 (platform v0.1)

## Purpose and scope
- Canonical user reference for all currently exposed sources, fields, modes, and status taxonomies.
- Scope includes query datasets, scraper source keys, warning/error taxonomies, and export lifecycle taxonomy.

## Inputs and outputs with contract shape
- This document is a reference artifact; it does not define an endpoint.
- It is consumed by users when building requests to:
  - `POST /v1/raw/query`
  - `POST /v1/raw/export`
  - `GET /v1/raw/export/{export_id}`
- Query contract currently uses `sources` (single item today), `fields`, `time_range|n_rows|n_random`, `filters`, `strict`.

## Data definitions (fields, types, units, timezone, nullability)
- Query source keys:
  - `spot_trades`
  - `spot_agg_trades`
  - `futures_trades`
  - `etf_daily_metrics`
  - `fred_series_metrics`
- Core cross-dataset fields:
  - `datetime`: UTC timestamp
  - `timestamp`: epoch integer
  - `observed_at_utc`: UTC observation timestamp
  - `aligned_at_utc`: UTC aligned 1-second timestamp
  - `valid_from_utc`: UTC interval start (inclusive)
  - `valid_to_utc_exclusive`: UTC interval end (exclusive)
- ETF source taxonomy:
  - `etf_ishares_ibit_daily`
  - `etf_invesco_btco_daily`
  - `etf_bitwise_bitb_daily`
  - `etf_ark_arkb_daily`
  - `etf_vaneck_hodl_daily`
  - `etf_franklin_ezbc_daily`
  - `etf_grayscale_gbtc_daily`
  - `etf_fidelity_fbtc_daily`
  - `etf_coinshares_brrr_daily`
  - `etf_hashdex_defi_daily`
- FRED source taxonomy:
  - `fred_fedfunds`
  - `fred_cpiaucsl`
  - `fred_unrate`
  - `fred_dgs10`

## Source/provenance and freshness semantics
- Binance canonical historical truth source is exchange-hosted daily/monthly files.
- ETF canonical source hierarchy is issuer official sources.
- FRED canonical source is direct API calls to FRED.
- Provenance fields (`provenance_json`, checksums, source keys) are replay-critical.

## Failure modes, warnings, and error codes
- Query warning taxonomy:
  - `WINDOW_LATEST_ROWS_MUTABLE`
  - `WINDOW_RANDOM_SAMPLE`
  - `ALIGNED_FRESHNESS_STALE`
  - `ETF_DAILY_STALE_RECORDS`
  - `ETF_DAILY_MISSING_RECORDS`
  - `ETF_DAILY_INCOMPLETE_RECORDS`
  - `FRED_SOURCE_PUBLISH_MISSING`
  - `FRED_SOURCE_PUBLISH_STALE`
- Query status taxonomy:
  - `200`, `404`, `409`, `503`
- Export submit status taxonomy:
  - `202`
- Export lifecycle taxonomy:
  - `queued`, `running`, `succeeded`, `failed`
- Rights state taxonomy:
  - `Hosted Allowed`
  - `BYOK Required`
  - `Ingest Only`

## Determinism/replay notes
- Deterministic replay artifacts are maintained under `spec/slices/`.
- Baseline fixtures record source checksums and repeated-run fingerprints.
- Query and export determinism is validated slice-by-slice before guardrail closeout.

## Environment variables and required config
- API/runtime:
  - `ORIGO_INTERNAL_API_KEY`
  - `ORIGO_QUERY_MAX_CONCURRENCY`
  - `ORIGO_QUERY_MAX_QUEUE`
  - `ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY`
  - `ORIGO_ALIGNED_QUERY_MAX_QUEUE`
- Freshness/rights:
  - `ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS`
  - `ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS`
  - `ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS`
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- Storage:
  - `CLICKHOUSE_*`
  - `ORIGO_OBJECT_STORE_*`

## Minimal examples
- Query source list example:
  - `sources=["spot_trades"]`
- Query filter clause example:
  - `{ "field":"price", "op":"gt", "value":1000 }`
- Export status artifact fields:
  - `uri`, `row_count`, `checksum_sha256`
