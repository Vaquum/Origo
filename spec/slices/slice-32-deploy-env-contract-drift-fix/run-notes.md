## Run metadata
- Date: 2026-03-12
- Scope: Slice 32 deploy env-contract drift fix (`ORIGO_AUDIT_LOG_RETENTION_DAYS`) + live validation closure.
- Fixture window: deploy contract proof + live API smoke (no source-time fixture window; latest-row/no-data smoke only).
- Runtime environment:
  - Local: macOS workspace, branch `codex/s31-deploy-env-contract-fix`
  - CI proof: GitHub Actions `deploy-on-merge`, run `22995788913`, job `66767544363`
  - Remote target: `origo.vaquum.fi`

## System changes made as proof side effects
- Remote deploy updated running API/control-plane images to branch commit-tagged images.
- Remote `/opt/origo/deploy/.env` was rewritten by deploy workflow with synchronized `ORIGO_AUDIT_LOG_RETENTION_DAYS` value.
- Two raw-export smoke jobs were submitted and completed with zero-row artifacts:
  - `0e38c773-33de-409d-9a3d-48da85e71adf` (`mode=native`)
  - `b7febdb3-f90d-44f7-8113-3df4e799f5e9` (`mode=aligned_1s`)

## Known warnings and disposition
- `QUERY_NO_DATA` / `HISTORICAL_NO_DATA` responses across most datasets: acceptable for this slice (fresh server, no data-population proof in scope).
- `QUERY_RIGHTS_INGEST_ONLY` (ETF) and `QUERY_SERVING_SHADOW_MODE` (FRED): acceptable and expected guardrail behavior.
- GitHub Actions Node.js 20 deprecation annotation: noted; deferred to CI maintenance slice.

## Deferred guardrails
- None in Slice 32 scope.

## Closeout confirmation
- Work-plan checkboxes updated: yes (`S32-C1..C2`, `S32-P1..P2`, `S32-G1..G2`).
- Version updated: yes (`Origo API v0.1.23`).
- Changelog updated: yes (2026-03-12 Slice 32 entry).
- `.env.example` reviewed against slice changes: yes (retention key remains canonical and unchanged).
- Slice artifacts created: yes (`manifest.md`, `run-notes.md`, `baseline-fixture-2026-03-12.json`).
