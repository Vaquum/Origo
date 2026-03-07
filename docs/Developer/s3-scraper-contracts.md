# Slice 3 Developer: Scraper Contracts

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-05
- Slice reference: S3 (`S3-C1`, `S3-C2`, `S3-C3`)

## Purpose and scope
- Defines typed contracts for scraper adapters and canonical data handoff objects.
- Scope is source-agnostic interfaces and long-metric normalization contract.

## Inputs and outputs
- Adapter interface: `origo.scraper.ScraperAdapter`.
- Input contracts:
  - `ScrapeRunContext`
  - `SourceDescriptor`
  - `RawArtifact`
  - `ParsedRecord`
- Output contracts:
  - `NormalizedMetricRecord`
  - `ProvenanceMetadata`
  - `PersistedRawArtifact`

## Data definitions
- `SourceDescriptor`: source identity/URI + discovery metadata.
- `RawArtifact`: immutable fetched payload with `content_sha256`, `fetch_method`, and `artifact_format`.
- `ParsedRecord`: parser output row with parser name/version and typed payload map.
- `NormalizedMetricRecord`: long-metric shape (`metric_name`, `metric_value`, `observed_at_utc`, `dimensions`, provenance).
- `ProvenanceMetadata`: replay-critical linkage from normalized metric back to source/artifact/parser.

## Source, provenance, freshness
- Provenance is mandatory in normalized output path.
- Parser metadata (`parser_name`, `parser_version`) is carried into provenance.
- Freshness is represented by source publish/fetch timestamps and observed metric timestamps.

## Failure modes and error codes
- Contract validation failures raise explicit `ValueError` or `RuntimeError`.
- Pipeline-level failures are wrapped as `ScraperError` with typed codes (`SCRAPER_*`).

## Determinism and replay notes
- Artifact IDs are content/source derived (`source_id`, `source_uri`, `content_sha256`) and not run-id dependent.
- Metric IDs are deterministic hashes over source, record, metric name, and metric value.

## Environment and required config
- No additional env vars for the pure contracts layer.
- Runtime modules consuming these contracts require scraper env vars documented in pipeline docs.

## Minimal example
- Implement adapter methods:
  - `discover_sources` -> list of `SourceDescriptor`
  - `fetch` -> `RawArtifact`
  - `parse` -> `list[ParsedRecord]`
  - `normalize` -> `list[NormalizedMetricRecord]`
