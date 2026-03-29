# Slice 6 Developer: FRED Connector Contracts

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-29
- Slice reference: S6 (`S6-C1`, `S6-C2`), S34 (`S34-C6f`)
- Version reference: `control-plane v1.2.82`

## Purpose and scope
- Define strict contracts for FRED source registry loading, metadata fetch, and normalization into Origo long-metric rows.
- Scope is connector/runtime capability only. Ingest orchestration and guardrails are documented separately.

## Inputs and outputs
- Inputs:
  - Registry file: `contracts/fred-series-registry.json`
  - Runtime env: `FRED_API_KEY`, `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`, `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST`
  - FRED endpoints: `/fred/series`, `/fred/series/observations`
- Outputs:
  - typed series metadata records
  - typed observation records
  - normalized long-metric rows for `fred_series_metrics_long`

## Data definitions
- Registry entry keys:
  - `source_id`: internal stable source key (for example `fred_fedfunds`)
  - `series_id`: FRED series code (for example `FEDFUNDS`)
  - `metric_name`, `metric_unit`
  - `dimensions_json`
- Normalized row keys:
  - `source_id`
  - `metric_name`
  - `metric_unit`
  - `metric_value_float`
  - `metric_value_string`
  - `observed_at_utc` (UTC)
  - `dimensions_json`
  - `provenance_json`
  - `ingested_at_utc` (UTC)

## Source, provenance, and freshness semantics
- Source of truth is the FRED API (direct calls, no third-party wrapper/provider).
- `provenance_json.last_updated_utc` is sourced from FRED series metadata and drives freshness checks.
- Normalized timestamps are UTC and deterministic for fixed observation windows.

## Failure modes, warnings, and error semantics
- Missing/empty `FRED_API_KEY` fails loudly at runtime.
- Missing/empty timeout env fails loudly.
- Missing/empty revision-history window env fails loudly.
- A deployed runtime missing `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST` is also a hard failure; deploy must synchronize required FRED env keys from root `.env.example` into `/opt/origo/deploy/.env`.
- Invalid registry contract fails loudly during registry load.
- Invalid/partial FRED payloads fail loudly during decode/normalize.
- No swallowed exceptions; parse/fetch errors propagate as runtime failures.

## Determinism and replay notes
- Fixed windows normalize deterministically (proof artifacts in `spec/slices/slice-6-fred-integration/`).
- Replay stability is validated in:
  - `proof-s6-p2-determinism.json`
  - `proof-s6-p3-metadata-version-reproducibility.json`

## Environment variables
- `FRED_API_KEY`
- `ORIGO_FRED_HTTP_TIMEOUT_SECONDS`
- `ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST`

## Minimal examples
- Connector proof:
  - `set -a; source .env; set +a; ORIGO_FRED_HTTP_TIMEOUT_SECONDS=20 uv run python -m origo.fred.s6_c1_proof`
- Normalize proof:
  - `set -a; source .env; set +a; ORIGO_FRED_HTTP_TIMEOUT_SECONDS=20 uv run python -m origo.fred.s6_c2_proof`
