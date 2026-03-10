## What was done
- Manifest structure was corrected after-the-fact to the required three-section contract.
- Slice 4 capability/proof/guardrail work delivered ETF ingest and query coverage for all 10 configured issuers, including canonical UTC-daily normalization, rights/legal gates, schedule/retry/serving-state controls, warnings, and alerting.
- Slice 4 artifacts remain in this folder (`capability-proof-*`, `proof-*`, `guardrails-proof-*`, baseline fixture, and run-notes) and are the evidence set for this slice.

## Current state
- `etf_daily_metrics` is onboarded and queryable through the raw query path, with rights/legal behavior enforced fail-loud.
- ETF ingestion and normalization contract is active with provenance retention and deterministic replay artifacts.
- Slice 4 developer and user docs are present and remain the current reference set for ETF adapter contracts and user-facing taxonomy.

## Watch out
- This manifest was normalized retrospectively; earlier appended section-by-section notes were collapsed into this contract shape without changing recorded proof artifacts.
- ETF issuer payload structures can drift and require adapter maintenance; source-origin checks are intentionally strict and fail-closed.
- Serving remains controlled by rights/legal/promotion-state gates; policy flips without corresponding artifacts/config will fail loudly.
