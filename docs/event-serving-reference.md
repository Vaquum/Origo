# Event-Serving Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice/version reference: S14, S15, S16, S17, S18, S19, S20, S21 (API v0.1.14)

## Purpose and scope
- User-facing reference for the event-driven serving semantics introduced in Slice 14 and expanded in S15/S16/S17/S18/S19.
- Scope includes `view_id`/`view_version`, rights metadata fields, and canonical guarantee semantics used by Raw Query/Export.

## Inputs and outputs with contract shape
- Query request extensions:
  - `view_id`: optional string
  - `view_version`: optional integer (`>0`)
  - rule: both must be set together or both omitted
- Query response extensions:
  - `sources`: requested source list
  - `view_id`
  - `view_version`
  - `rights_state`
  - `rights_provisional`
- Export submit/status response extensions:
  - `rights_state`
  - `rights_provisional`
  - `source`
  - `view_id`
  - `view_version`

## Data definitions (fields, types, units, timezone, nullability)
- `rights_state`:
  - `Hosted Allowed`
  - `BYOK Required`
  - `Ingest Only`
- `rights_provisional`: boolean (`true` when temporarily hosted under legal transition).
- `view_id`: logical projection/view identifier string.
- `view_version`: positive integer identifying immutable view contract version.
- Canonical aligned serving sink:
  - table: `canonical_aligned_1s_aggregates`
  - required for Binance, OKX, Bybit, ETF, FRED, and Bitcoin-derived aligned serving paths in current scope.

## Source/provenance and freshness semantics
- Served rows are projection outputs derived from immutable canonical events.
- `payload_raw` and `payload_sha256_raw` remain the byte-level source provenance anchor.
- Freshness semantics remain source-driven and warning-aware per endpoint docs.

## Failure modes, warnings, and error codes
- Missing rights metadata in export tags is fail-loud (`EXPORT_STATUS_METADATA_ERROR` path).
- Missing rights metadata in query response contract is fail-loud (response validation error).
- Unsupported/missing view metadata combinations are rejected by request contract validation.
- Binance aligned serving storage contract violations are fail-loud (missing table / schema drift).
- OKX aligned serving storage contract violations are fail-loud (missing table / schema drift).
- ETF aligned serving storage contract violations are fail-loud (missing table / schema drift).
- FRED aligned serving storage contract violations are fail-loud (missing table / schema drift).
- Bybit aligned serving storage contract violations are fail-loud (missing table / schema drift).
- Bitcoin aligned serving storage contract violations are fail-loud (missing table / schema drift).

## Determinism/replay notes
- Determinism and parity proofs for event serving live under:
  - `spec/slices/slice-14-event-sourcing-core/`
  - `spec/slices/slice-15-binance-event-sourcing-port/`
  - `spec/slices/slice-16-etf-event-sourcing-port/`
  - `spec/slices/slice-17-fred-event-sourcing-port/`
  - `spec/slices/slice-18-okx-event-sourcing-port/`
  - `spec/slices/slice-19-bybit-event-sourcing-port/`
  - `spec/slices/slice-20-bitcoin-event-sourcing-port/`

## Environment variables and required config
- `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
- `ORIGO_INTERNAL_API_KEY`

## Minimal examples
- Query with view metadata:
  - `{ "mode":"native", "sources":["spot_trades"], "view_id":"aligned_1s_raw", "view_version":1, "n_rows":100, "strict":false }`
- Export status rights metadata shape:
  - `{ "rights_state":"Hosted Allowed", "rights_provisional":false, "source":"binance" }`
