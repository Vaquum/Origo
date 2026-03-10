# Slice 13 Developer: Bitcoin Guardrails + Proof Runbook

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-08
- Slice reference: S13 (`S13-G1`, `S13-G2`, `S13-G3`, `S13-G4`)
- Version reference: `Origo API v0.1.5`, `origo-control-plane v1.2.55`

## Purpose and scope
- Defines S13 guardrail implementation and proof workflow.
- Scope covers rights/legal gating, integrity suites, aligned-mode contract guards, fixture-based proof artifact generation, and live-node proof artifact generation.

## Inputs and outputs with contract shape
- Inputs:
  - rights matrix (`contracts/source-rights-matrix.json`)
  - legal signoff artifact (`contracts/legal/bitcoin-core-hosted-allowed.md`)
  - S13 deterministic fixture generator script.
  - S13 live-node proof generator script.
- Outputs:
  - guardrail-enforced runtime behavior in query/export and ingest assets.
  - proof artifacts under `spec/slices/slice-13-bitcoin-core-signals/`.

## Data definitions (field names, types, units, timezone, nullability)
- Rights classification fields:
  - `rights_state`
  - `datasets`
  - `legal_signoff_artifact`
- Integrity report fields:
  - `dataset`, `rows_checked`, `anomaly_checks_performed`, `min_height`, `max_height`
- Baseline fixture fields:
  - `source_checksums`, run fingerprints, `deterministic_match`, and `column_key`.

## Source/provenance and freshness semantics
- Rights and legal provenance:
  - `bitcoin_core` source is classified as `Hosted Allowed`.
- Fixture source provenance:
  - baseline `zip_sha256`/`csv_sha256` are mapped to canonicalized node fixture payload checksums (documented in `source_note`).

## Failure modes, warnings, and error codes
- Rights fail-closed:
  - missing source classification
  - missing legal artifact for hosted serving/export
- Integrity fail-closed:
  - stream and derived invariant checks raise immediately before writes.
- Aligned contract fail-closed:
  - unsupported dataset in `aligned_1s` query/export rejects with contract error.

## Determinism/replay notes
- Artifact generators:
  - `scripts/s13_generate_proof_artifacts.py`
  - `scripts/s13_generate_live_node_proof_artifacts.py`
- Produced fixture artifacts:
  - `proof-s13-p1-acceptance.json`
  - `proof-s13-p2-derived-native-aligned-acceptance.json`
  - `proof-s13-p3-determinism.json`
  - `proof-s13-p4-parity.json`
  - `baseline-fixture-2024-04-20_2024-04-21.json`
- Produced live-node artifacts:
  - `proof-s13-live-node-p1-acceptance.json`
  - `proof-s13-live-node-p2-derived-native-aligned-acceptance.json`
  - `proof-s13-live-node-p3-determinism.json`
  - `proof-s13-live-node-p4-parity.json`
  - `baseline-fixture-live-node-<day-start>_<day-end>.json`
- Deterministic replay requires unchanged fixture payloads and formulas.

## Environment variables and required config
- Rights and API:
  - `ORIGO_SOURCE_RIGHTS_MATRIX_PATH`
  - `ORIGO_INTERNAL_API_KEY`
- ClickHouse/runtime:
  - `CLICKHOUSE_*`
- Bitcoin Core source contract:
  - `ORIGO_BITCOIN_CORE_RPC_URL`
  - `ORIGO_BITCOIN_CORE_RPC_USER`
  - `ORIGO_BITCOIN_CORE_RPC_PASSWORD`
  - `ORIGO_BITCOIN_CORE_NETWORK`
  - `ORIGO_BITCOIN_CORE_RPC_TIMEOUT_SECONDS`
  - `ORIGO_BITCOIN_CORE_HEADERS_START_HEIGHT`
  - `ORIGO_BITCOIN_CORE_HEADERS_END_HEIGHT`

## Minimal examples
- Generate S13 proof artifacts locally:
  - `PYTHONPATH=.:control-plane .ci-tests-venv/bin/python scripts/s13_generate_proof_artifacts.py`
- Generate S13 live-node proof artifacts locally:
  - `PYTHONPATH=.:control-plane .ci-tests-venv/bin/python scripts/s13_generate_live_node_proof_artifacts.py`
- Run quality gates:
  - style/type/contract/replay/integrity gate commands as defined in `.github/workflows/*.yml`
