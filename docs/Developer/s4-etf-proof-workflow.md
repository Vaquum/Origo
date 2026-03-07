# Slice 4 Developer: ETF Proof Workflow

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-06
- Slice reference: S4 (`S4-P1` to `S4-P4`)

## Purpose and scope
- Documents deterministic proof workflow for ETF capability acceptance, parity, and replay.
- Scope is Slice 4 proof runner inputs, outputs, and acceptance thresholds.

## Inputs and outputs
- Proof runner:
  - `origo.scraper.etf_s4_05_proof_suite`
- Inputs:
  - official issuer source payloads
  - canonical table state
  - fixed proof-window dates
- Outputs:
  - parity summary
  - replay fingerprint comparison
  - provenance/reference integrity checks
  - proof artifact JSON in `spec/slices/slice-4-etf-use-case/`

## Data definitions
- Parity threshold: `>= 99.5%` on mandatory metrics.
- Mandatory metric set:
  - `issuer`, `ticker`, `as_of_date`, `btc_units`, `btc_market_value_usd`, `total_net_assets_usd`, `holdings_row_count`
- Replay fingerprint dimensions:
  - row counts
  - deterministic hash of inserted records
  - provenance completeness counters

## Source, provenance, freshness
- Proof compares source-derived mandatory metrics against canonical table rows.
- Provenance checks verify required keys and artifact linkage for replayability.

## Failure modes and error semantics
- Parity below threshold fails proof.
- Replay hash mismatch fails proof.
- Missing/invalid provenance references fail proof.

## Determinism and replay notes
- Run1 and run2 of same proof window must produce identical inserted-row fingerprint.
- Proof artifacts are immutable references for slice completion validation.

## Environment and required config
- `CLICKHOUSE_*`
- scraper/object-store env contract used by pipeline execution

## Minimal example
- Execute proof suite:
  - `uv run --with clickhouse-connect --with playwright --with polars --with pyarrow python -m origo.scraper.etf_s4_05_proof_suite`
- Inspect artifact:
  - `spec/slices/slice-4-etf-use-case/proof-s4-p1-p4.json`
