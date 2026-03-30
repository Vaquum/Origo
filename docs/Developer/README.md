# Developer Docs Index

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30

## Purpose
- This folder is the engineering reference for Origo implementation slices.
- Governance routing lives in `AGENTS.md`.
- Machine-readable governance authority lives in `contracts/governance/`.
- Governance authority applies to the whole Origo system and all future development, not only to the currently open slices.
- Developer docs may explain governance, but they do not govern it.

## Canonical governance routing
- Routing surface:
  - `AGENTS.md`
- Canonical machine governance contracts:
  - `contracts/governance/governance-authority.json`
  - `contracts/governance/execution-doctrine.json`
  - `contracts/governance/static-analysis.json`
  - `contracts/governance/work-plan-discipline.json`
  - `contracts/governance/documentation-contract.json`
  - `contracts/governance/slice-closeout.json`
  - `contracts/governance/pr-review-routing.json`
  - `contracts/governance/task-types.json`
  - `contracts/governance/task-decomposition.json`
  - `contracts/governance/task-admission.json`
  - `contracts/governance/request-task-coverage.json`
  - `contracts/governance/contract-applicability.json`
  - `contracts/governance/dagster-authority.json`
  - `contracts/governance/ingest-throughput.json`
  - `contracts/governance/storage-sql-discipline.json`
  - `contracts/governance/serving-error-safety.json`
  - `contracts/governance/integrity-durability.json`
  - `contracts/governance/rights-secrets.json`
  - `contracts/governance/runtime-recovery.json`
  - `contracts/governance/ingestion-guarantees.json`
  - `contracts/governance/raw-fidelity.json`
  - `contracts/governance/event-sourcing.json`
  - `contracts/governance/historical-surface.json`
  - `contracts/governance/source-onboarding.json`
  - `contracts/governance/source-migration.json`

## Live runtime contracts
- Runtime API contract:
  - `docs/raw-query-reference.md`
  - `docs/raw-export-reference.md`
  - `docs/aligned-reference.md`
  - `docs/data-taxonomy.md`
- Event/runtime contract:
  - `docs/event-serving-reference.md`
  - `docs/Developer/s14-canonical-event-runtime.md`
  - `docs/Developer/s21-canonical-aligned-storage-contract.md`
  - `docs/Developer/s34-canonical-backfill-runtime.md`
  - `docs/Developer/s34-bitcoin-height-window-contract.md`

## Governance reference docs (non-authoritative)
- `docs/Developer/pr-review-routing-contract.md`
- `docs/Developer/dagster-authority-contract.md`
- `docs/Developer/ingest-throughput-contract.md`
- `docs/Developer/task-type-contract.md`
- `docs/Developer/task-admission-contract.md`
- `docs/Developer/request-task-coverage-contract.md`

## Historical slice docs
- All `s*.md` files outside the live runtime contracts list above are historical slice records only.
- Version metadata in those files is a slice snapshot, not necessarily the current runtime version.
- Any CLI examples, runner names, or controller names in those historical docs are provenance only and must not be treated as live write-entrypoint authority.
- When there is any mismatch, follow `AGENTS.md` plus `contracts/governance/*.json` for governance and the live runtime contracts above for runtime behavior.

## Editing rule
- If governance changes, update `AGENTS.md` routing first and then update the affected machine contracts in `contracts/governance/`.
- If runtime behavior changes, update the live runtime docs first.
- Developer docs may then be updated as reference material, but they must not introduce parallel governance authority.
