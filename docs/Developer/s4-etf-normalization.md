# Slice 4 Developer: ETF Normalization and Canonical Load

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-06
- Slice reference: S4 (`S4-C4` to `S4-C7`)

## Purpose and scope
- Defines ETF normalized metric contract and canonical ClickHouse load behavior.
- Scope includes schema mapping, unit mapping, UTC semantics, and canonical persistence.

## Inputs and outputs
- Input: issuer parsed ETF daily payloads.
- Normalization output fields:
  - `metric_id`
  - `source_id`
  - `metric_name`
  - `metric_unit`
  - `metric_value_string|int|float|bool`
  - `observed_at_utc`
  - `dimensions_json`
  - `provenance_json`
- Canonical table output:
  - `origo.etf_daily_metrics_long`

## Data definitions
- Mandatory ETF daily metrics:
  - `issuer`
  - `ticker`
  - `as_of_date`
  - `btc_units`
  - `btc_market_value_usd`
  - `total_net_assets_usd`
  - `holdings_row_count`
- Unit mapping examples:
  - `btc_units` -> `BTC`
  - `btc_market_value_usd` -> `USD`
  - `btc_weight_pct` -> `PCT`
  - `holdings_row_count` -> `COUNT`

## Source, provenance, freshness
- Canonical long-metric rows are append-only for raw source truth.
- Provenance includes source URI, artifact hash, parser identifiers, and processing timestamps.
- Freshness is measured from source-published daily observations.

## Failure modes and error semantics
- Missing required metrics fail normalization.
- Non-UTC-midnight `observed_at_utc` fails canonical load for ETF batch.
- Mixed ETF/non-ETF source IDs in one batch fail canonical insert path.
- Missing ClickHouse schema/migration fails load path.

## Determinism and replay notes
- Deterministic metric IDs and normalized output fingerprints are validated in proof artifacts.
- Canonical table parity and replay checks are in Slice 4 proof suite artifacts.

## Environment and required config
- `CLICKHOUSE_*`
- `ORIGO_OBJECT_STORE_*`
- `ORIGO_SCRAPER_*`

## Minimal example
- Run canonical load proof:
  - `uv run --with clickhouse-connect --with playwright --with polars python -m origo.scraper.etf_s4_06_load_proof`
- Validate inserted rows match expected run payload count in proof artifact.
