## Run metadata
- Date: 2026-03-10
- Scope: Slice 19 (`bybit_spot_trades`) capability/proof/guardrails/docs closeout
- Fixture window:
  - `2024-01-04T00:00:00Z` -> `2024-01-04T00:00:03Z` (`baseline-fixture-2024-01-04_2024-01-04.json`)
- Runtime environment:
  - local proof runners against local ClickHouse docker runtime
  - proof env explicitly set:
    - `CLICKHOUSE_HOST=127.0.0.1`
    - `CLICKHOUSE_PORT=9000`
    - `CLICKHOUSE_HTTP_PORT=8123`
    - `CLICKHOUSE_USER=default`
    - `CLICKHOUSE_PASSWORD=origo`
    - `CLICKHOUSE_DATABASE=origo`
    - `ORIGO_AUDIT_LOG_RETENTION_DAYS=365`
    - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH=/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo/storage/audit/canonical-runtime-events.jsonl`
    - `ORIGO_STREAM_QUARANTINE_STATE_PATH=/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo/storage/audit/stream-quarantine-state.json`

## System changes made as proof side effects
- Added/updated Bybit Slice-19 proof modules:
  - `control-plane/origo_control_plane/s19_p1_p3_bybit_serving_proofs.py`
  - `control-plane/origo_control_plane/s19_p4_exactly_once_ingest_proof.py`
  - `control-plane/origo_control_plane/s19_p5_no_miss_completeness_proof.py`
  - `control-plane/origo_control_plane/s19_p6_raw_fidelity_precision_proof.py`
  - `control-plane/origo_control_plane/s19_g1_exchange_integrity_guardrail_proof.py`
  - `control-plane/origo_control_plane/s19_g3_reconciliation_guardrail_proof.py`
  - `control-plane/origo_control_plane/s19_g4_precision_guardrail_proof.py`
  - `api/origo_api/s19_g2_bybit_api_guardrails_proof.py`
- Added Bybit canonical serving runtime modules:
  - `control-plane/origo_control_plane/utils/bybit_canonical_event_ingest.py`
  - `control-plane/origo_control_plane/utils/bybit_native_projector.py`
  - `control-plane/origo_control_plane/utils/bybit_aligned_projector.py`
  - migration `control-plane/migrations/sql/0032__create_canonical_bybit_spot_trades_native_v1.sql`
- Bybit query runtime moved to canonical serving path:
  - `origo/query/bybit_native.py` reads `canonical_bybit_spot_trades_native_v1`
  - `origo/query/bybit_aligned_1s.py` reads `canonical_aligned_1s_aggregates`
- Identity mapping correction applied during S19 execution:
  - `bybit_canonical_event_ingest` now parses `trade_id` and canonical source offsets from `trd_match_id` (`m-<digits>`) to enforce real source no-miss checks
- Proof artifact path correction applied during S19 execution:
  - S19 proof scripts now resolve `_SLICE_DIR` from repo root via `Path(__file__).resolve().parents[2]`
- Proof artifacts written under this slice directory:
  - `proof-s19-p1-acceptance.json`
  - `proof-s19-p2-parity.json`
  - `proof-s19-p3-determinism.json`
  - `proof-s19-p4-exactly-once-ingest.json`
  - `proof-s19-p5-no-miss-completeness.json`
  - `proof-s19-p6-raw-fidelity-precision.json`
  - `proof-s19-g1-exchange-integrity.json`
  - `proof-s19-g2-api-guardrails.json`
  - `proof-s19-g3-reconciliation-quarantine.json`
  - `proof-s19-g4-raw-fidelity-precision.json`

## Known warnings and disposition
- `S19-P5` intentionally injects source-offset gaps to prove fail-loud no-miss/quarantine semantics; resulting quarantine behavior is expected.
- `strict=true` warning escalation behavior in API guardrail proof is expected and validated.
- No unresolved guardrail warnings remain from the Slice 19 proof suite.

## Deferred guardrails
- None for Slice 19. Guardrail scope `S19-G1..S19-G6` is closed in this slice.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for `S19-C1..S19-C4`, `S19-P1..S19-P6`, and `S19-G1..S19-G6`.
- Version bumped:
  - `pyproject.toml` -> `0.1.13`
  - `control-plane/pyproject.toml` -> `1.2.62`
  - `api/origo_api/main.py` -> `0.1.13`
- Slice summary appended to `CHANGELOG.md`.
- `.env.example` reviewed against Slice 19 changes; no new environment variables were introduced in this slice, and required runtime vars remain explicit with fail-loud semantics.
