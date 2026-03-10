## Run metadata
- Date: 2026-03-10
- Scope: Slice 18 (`okx_spot_trades`) capability/proof/guardrails/docs closeout
- Fixture window:
  - `2024-01-04T00:00:00Z` -> `2024-01-04T00:00:03Z` (`baseline-fixture-2024-01-04_2024-01-04.json`)
- Runtime environment:
  - local proof runners against remote ClickHouse via SSH tunnel
    - local `39000 -> remote clickhouse:9000`
    - local `38123 -> remote clickhouse:8123`
  - proof env explicitly set:
    - `CLICKHOUSE_HOST=127.0.0.1`
    - `CLICKHOUSE_PORT=39000`
    - `CLICKHOUSE_HTTP_PORT=38123`
    - `CLICKHOUSE_USER=default`
    - `CLICKHOUSE_PASSWORD=replace-with-secret`
    - `CLICKHOUSE_DATABASE=origo`
    - `ORIGO_AUDIT_LOG_RETENTION_DAYS=365`
    - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH=./storage/audit/s18-runtime-audit.jsonl`
    - `ORIGO_STREAM_QUARANTINE_STATE_PATH=./storage/audit/s18-quarantine.json`

## System changes made as proof side effects
- Added/updated OKX Slice-18 proof modules:
  - `control-plane/origo_control_plane/s18_p1_p3_okx_serving_proofs.py`
  - `control-plane/origo_control_plane/s18_p4_exactly_once_ingest_proof.py`
  - `control-plane/origo_control_plane/s18_p5_no_miss_completeness_proof.py`
  - `control-plane/origo_control_plane/s18_p6_raw_fidelity_precision_proof.py`
  - `control-plane/origo_control_plane/s18_g1_exchange_integrity_guardrail_proof.py`
  - `control-plane/origo_control_plane/s18_g3_reconciliation_guardrail_proof.py`
  - `control-plane/origo_control_plane/s18_g4_precision_guardrail_proof.py`
  - `api/origo_api/s18_g2_okx_api_guardrails_proof.py`
- Added OKX canonical serving runtime modules:
  - `control-plane/origo_control_plane/utils/okx_canonical_event_ingest.py`
  - `control-plane/origo_control_plane/utils/okx_native_projector.py`
  - `control-plane/origo_control_plane/utils/okx_aligned_projector.py`
  - migration `control-plane/migrations/sql/0031__create_canonical_okx_spot_trades_native_v1.sql`
- OKX query runtime moved to canonical serving path:
  - `origo/query/okx_native.py` reads `canonical_okx_spot_trades_native_v1`
  - `origo/query/okx_aligned_1s.py` reads `canonical_aligned_1s_aggregates`
- Runtime core alignment fix applied during S18 execution:
  - `origo/events/aligned_projector.py` now buckets by `source_event_time_utc` (fail-loud if missing)
- Proof artifacts written under this slice directory:
  - `proof-s18-p1-acceptance.json`
  - `proof-s18-p2-parity.json`
  - `proof-s18-p3-determinism.json`
  - `proof-s18-p4-exactly-once-ingest.json`
  - `proof-s18-p5-no-miss-completeness.json`
  - `proof-s18-p6-raw-fidelity-precision.json`
  - `proof-s18-g1-exchange-integrity.json`
  - `proof-s18-g2-api-guardrails.json`
  - `proof-s18-g3-reconciliation-quarantine.json`
  - `proof-s18-g4-raw-fidelity-precision.json`

## Known warnings and disposition
- `S18-P5` intentionally injects source-offset gaps to prove fail-loud no-miss/quarantine semantics; resulting quarantine behavior is expected.
- `strict=true` warning escalation behavior in API guardrail proof is expected and validated.
- No unresolved guardrail warnings remain from the Slice 18 proof suite.

## Deferred guardrails
- None for Slice 18. Guardrail scope `S18-G1..S18-G6` is closed in this slice.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for `S18-C1..S18-C4`, `S18-P1..S18-P6`, and `S18-G1..S18-G6`.
- Version bumped:
  - `pyproject.toml` -> `0.1.12`
  - `control-plane/pyproject.toml` -> `1.2.61`
  - `api/origo_api/main.py` -> `0.1.12`
- Slice summary appended to `CHANGELOG.md`.
- `.env.example` reviewed against Slice 18 changes; no new environment variables were introduced in this slice, and required runtime vars remain explicit with fail-loud semantics.
