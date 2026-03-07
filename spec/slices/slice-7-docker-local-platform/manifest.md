## What was done
- Completed S7 capability, proof, and guardrails end-to-end.
- Added Docker platform packaging for local full-stack runtime:
  - `docker-compose.yml`
  - `docker/Dockerfile.api`
  - `docker/Dockerfile.control-plane`
  - `.dockerignore`
  - `scripts/s7_docker_stack.sh`
  - `scripts/s7_docker_bootstrap.sh`
  - `scripts/s7_docker_local_proof.sh`
- Wired strict Docker env contract with fail-loud compose/runtime checks and updated `.env.example`.
- Fixed root causes discovered during S7 proof:
  - query/export healthcheck commands switched to supported binaries in containers
  - Binance integrity suite corrected to allow `trade_id=0` on first historical day
  - Dagster run-status parsing updated to support `Run` payloads and strict timestamp parsing
  - Dagster repository/location dispatch defaults corrected for current workspace
- Produced deterministic S7 proof artifacts including replay and restart persistence evidence under `spec/slices/slice-7-docker-local-platform/`.

## Current state
- Origo can now be bootstrapped and proven locally via Docker using one command path:
  - `scripts/s7_docker_stack.sh proof`
- Full stack services (`clickhouse`, `dagster-webserver`, `dagster-daemon`, `api`) start and pass health checks from clean state.
- Fixed-window Binance seed ingest (`2017-08-17`, `2017-08-18`) is deterministic and replay-stable in Docker.
- Raw query and raw export API lifecycles succeed against Docker API with deterministic fingerprints and persisted artifacts.
- ClickHouse data and Dagster SQLite metadata persist across service restarts.

## Watch out
- Local proof depends on Docker availability and network access to Binance daily files.
- Proof artifacts include absolute in-container artifact paths under `/workspace/storage/exports`; host inspection requires volume access through Docker.
- The S7 stack intentionally targets local proof parity, not production hardening; TLS and production deployment concerns remain out of scope for this slice.
