# Slice 14 Run Notes

## Run metadata
- Date: 2026-03-10
- Scope: Slice 14 (`S14-C1`..`S14-C9`, `S14-P1`..`S14-P6`, `S14-G1`..`S14-G7`)
- Fixture windows:
  - Canonical hash/exactly-once/no-miss/fidelity proof fixtures under `spec/slices/slice-14-event-sourcing-core/`
  - Pilot cutover fixture window centered on `2024-01-01T00:00:00Z` (`binance_spot_trades` synthetic deterministic sample)
- Runtime environment:
  - local Docker/ClickHouse development environment
  - API/control-plane Python runtimes with repo env contract
  - proof scripts executed from `control-plane/origo_control_plane/` and `api/origo_api/`

## System changes made as proof side effects
- Applied Slice-14 migration tranche (`0017`..`0026`) for canonical event/log/projection tables.
- Created/used Slice-14 proof databases and emitted proof artifacts in `spec/slices/slice-14-event-sourcing-core/`.
- Emitted runtime audit events and quarantine state files through configured env paths during guardrail proofs.

## Known warnings and disposition
- No unresolved runtime warnings were accepted for slice closeout.
- Pilot fixture coverage is synthetic by design; this is accepted for Slice-14 core-contract proofing and is called out explicitly in manifest/baseline notes.

## Deferred guardrails
- None inside Slice 14 guardrail scope.

## Closeout confirmation
- `spec/2-itemized-work-plan.md` checkboxes updated for all completed Slice-14 items, including `S14-G6` and `S14-G7`.
- Version bumped: yes (`Origo API 0.1.7`, `control-plane 1.2.57`).
- `CHANGELOG.md` updated with Slice-14 summary.
- `.env.example` updated with Slice-14 required env vars (`ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH`, `ORIGO_STREAM_QUARANTINE_STATE_PATH`).
- Baseline fixture artifact written: `baseline-fixture-2024-01-01_2024-01-01.json`.
