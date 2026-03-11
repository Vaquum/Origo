## Run metadata
- Date: 2026-03-10
- Scope: Slice 20 (`bitcoin_block_*`, `bitcoin_mempool_state`, and derived Bitcoin datasets) capability/proof/guardrails/docs closeout
- Fixture window:
  - `2024-04-20T00:00:00Z` -> `2024-04-20T00:00:03Z` (`baseline-fixture-2024-04-20_2024-04-20.json`)
- Runtime environment:
  - local proof runners against local ClickHouse runtime
  - `S20-P7` executed against server Bitcoin node via local SSH tunnel (`127.0.0.1:18332 -> server:127.0.0.1:8332`)
  - proof env explicitly set:
    - `CLICKHOUSE_HOST=localhost`
    - `CLICKHOUSE_PORT=9000`
    - `CLICKHOUSE_HTTP_PORT=8123`
    - `CLICKHOUSE_USER=default`
    - `CLICKHOUSE_PASSWORD=origo`
    - `CLICKHOUSE_DATABASE=origo`
    - `ORIGO_AUDIT_LOG_RETENTION_DAYS=365`
    - `ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH=/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo/storage/audit/canonical-runtime-events.jsonl`
    - `ORIGO_STREAM_QUARANTINE_STATE_PATH=/Users/mikkokotila/Library/Mobile Documents/com~apple~CloudDocs/WIP/projects/Origo/storage/audit/stream-quarantine-state.json`

## System changes made as proof side effects
- Added/updated Slice-20 proof modules:
  - `control-plane/origo_control_plane/s20_bitcoin_proof_common.py`
  - `control-plane/origo_control_plane/s20_p1_p3_bitcoin_serving_proofs.py`
  - `control-plane/origo_control_plane/s20_p4_exactly_once_ingest_proof.py`
  - `control-plane/origo_control_plane/s20_p5_no_miss_completeness_proof.py`
  - `control-plane/origo_control_plane/s20_p6_raw_fidelity_precision_proof.py`
  - `control-plane/origo_control_plane/s20_p7_live_node_proof.py`
  - `control-plane/origo_control_plane/s20_g1_bitcoin_integrity_guardrail_proof.py`
  - `control-plane/origo_control_plane/s20_g3_reconciliation_guardrail_proof.py`
  - `control-plane/origo_control_plane/s20_g4_precision_guardrail_proof.py`
  - `api/origo_api/s20_g2_g5_bitcoin_api_guardrails_proof.py`
- Corrected S20 proof harness payload contract bug:
  - `s20_p5_no_miss_completeness_proof.py` now mutates header `height` instead of `block_height`.
- Corrected Bitcoin native projection ordering:
  - `control-plane/origo_control_plane/utils/bitcoin_native_projector.py` now sorts projection rows by deterministic stream keys before integrity checks/inserts.
- Proof artifacts written in this slice directory:
  - `proof-s20-p1-acceptance.json`
  - `proof-s20-p2-parity.json`
  - `proof-s20-p3-determinism.json`
  - `proof-s20-p4-exactly-once-ingest.json`
  - `proof-s20-p5-no-miss-completeness.json`
  - `proof-s20-p6-raw-fidelity-precision.json`
  - `proof-s20-g1-bitcoin-integrity.json`
  - `proof-s20-g2-g5-api-guardrails.json`
  - `proof-s20-g3-reconciliation-quarantine.json`
  - `proof-s20-g4-raw-fidelity-precision.json`
  - `proof-s20-p7-live-node-gate.json`
  - `baseline-fixture-2024-04-20_2024-04-20.json`

## Known warnings and disposition
- `S20-P5` intentionally injects source-offset gaps to prove fail-loud no-miss/quarantine semantics; resulting quarantine behavior is expected.
- `S20-G2` strict warning escalation behavior (`STRICT_MODE_WARNING_FAILURE`) is expected and validated.
- Running guardrail proof scripts in parallel against the same runtime audit path can race on shared temporary state files; serial runs are used for deterministic local validation.

## Deferred guardrails
- None for Slice 20. Guardrail/proof scope `S20-P1..S20-P7` and `S20-G1..S20-G7` is closed.

## Closeout confirmation
- Work plan checkboxes updated in `spec/2-itemized-work-plan.md` for:
  - `S20-P1..S20-P7`
  - `S20-G1..S20-G7`
- Version bumped:
  - `pyproject.toml` -> `0.1.14`
  - `control-plane/pyproject.toml` -> `1.2.63`
  - `api/origo_api/main.py` -> `0.1.14`
- Slice summary appended to `CHANGELOG.md`.
- `.env.example` reviewed against Slice 20 changes; no new environment variable names were introduced in this slice and required env contract remains explicit/fail-loud.
