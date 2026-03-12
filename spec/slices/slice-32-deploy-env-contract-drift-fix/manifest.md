## What was done
- Patched `.github/workflows/deploy-on-merge.yml` to source `ORIGO_AUDIT_LOG_RETENTION_DAYS` from root `.env.example`, validate integer bounds (`>=365`), and write it into `/opt/origo/deploy/.env` on deploy.
- Patched `deploy/docker-compose.server.yml` API service env contract to require `ORIGO_AUDIT_LOG_RETENTION_DAYS` at compose expansion time.
- Updated deployment contract/troubleshooting docs for the new fail-loud behavior and updated API version/changelog references.
- Executed branch deploy proof run `22995788913` (workflow_dispatch on `codex/s31-deploy-env-contract-fix`) and validated remote health/API smoke responses.

## Current state
- Deploy workflow now fail-loud validates `ORIGO_AUDIT_LOG_RETENTION_DAYS` before remote compose apply.
- Server runtime env file `/opt/origo/deploy/.env` is synchronized with `ORIGO_AUDIT_LOG_RETENTION_DAYS` during deploy runs.
- API compose contract fails early if retention policy key is missing.
- Live domain health checks pass on both `http://origo.vaquum.fi/health` and `https://origo.vaquum.fi/health`.

## Watch out
- Fresh server runtime currently returns `QUERY_NO_DATA` / `HISTORICAL_NO_DATA` for most datasets; this is expected until ingest backfill/population runs are executed.
- FRED/ETF guardrail states are still active (`QUERY_SERVING_SHADOW_MODE`, `QUERY_RIGHTS_INGEST_ONLY`) and will continue to return guardrail errors by design.
- GitHub Actions deprecation warning remains for Node.js 20-based actions; this is outside Slice 32 scope but should be addressed in a future CI maintenance slice.
