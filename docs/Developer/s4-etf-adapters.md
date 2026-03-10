# Slice 4 Developer: ETF Issuer Adapters

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S4 (`S4-C1` to `S4-C3`)

## Purpose and scope
- Documents adapter contracts and issuer coverage for the ETF daily ingestion capability.
- Scope is official-source discovery/fetch/parse behavior for the 10 issuer adapters.

## Inputs and outputs
- Entry builders:
  - `build_s4_01_issuer_adapters`
  - `build_s4_02_issuer_adapters`
  - `build_s4_03_issuer_adapters`
- Runtime adapter contract (`ScraperAdapter`):
  - `discover_sources(run_context)` -> `SourceDescriptor[]`
  - `fetch(source, run_context)` -> `RawArtifact`
  - `parse(artifact, source, run_context)` -> parsed records
  - `normalize(parsed_records, source, run_context)` -> normalized metric rows
- Output is normalized long-metric records with source lineage.

## Data definitions
- Canonical source IDs:
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
- Source-origin contract:
  - HTTPS-only source URIs.
  - Issuer host validation on effective fetch URL.
  - Non-issuer origin fails hard.

## Source, provenance, freshness
- Sources are issuer-official endpoints only.
- Every normalized row carries provenance metadata (`source_uri`, artifact hash/id, parser metadata, pipeline timestamps).
- Canonical cadence is daily and normalized to UTC day semantics.

## Failure modes and error semantics
- Adapter-level contract breaks raise runtime errors and stop the run.
- Pipeline-level errors are surfaced as typed `SCRAPER_*` errors.
- Common fail-loud classes:
  - issuer payload shape drift
  - missing required daily metrics
  - source-origin mismatch
  - parser/value coercion failures

## Determinism and replay notes
- Adapters use deterministic parsing rules (no runtime LLM parse path).
- Replay artifacts and proofs are stored under `spec/slices/slice-4-etf-use-case/`.
- Capability/proof artifacts provide repeatable fingerprints and parity checks.

## Environment and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_OBJECT_STORE_*`
- `ORIGO_SCRAPER_FETCH_*`
- `ORIGO_SCRAPER_AUDIT_LOG_PATH`
- `ORIGO_AUDIT_LOG_RETENTION_DAYS` (must be `>=365`)
- `CLICKHOUSE_*`

## Minimal example
- Build adapters: `adapters = build_s4_03_issuer_adapters()`
- Execute one adapter through `run_scraper_pipeline(adapter=..., run_context=...)`.
- Inspect `PipelineRunResult.source_results` for source IDs, row counts, and persisted artifact metadata.
