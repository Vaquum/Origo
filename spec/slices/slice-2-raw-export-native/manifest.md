## What was done
- Completed Slice 2 guardrails `S2-G1` to `S2-G3`:
  - Added source rights matrix contract and legal signoff artifact:
    - `contracts/source-rights-matrix.json`
    - `contracts/legal/binance-hosted-allowed.md`
  - Enforced pre-dispatch export rights gate in API (`S2-G1`).
  - Added append-only hash-chained export audit log with lifecycle transition events (`S2-G2`).
  - Added export queue backpressure controls and explicit failure classification (`S2-G3`).
  - Persisted export failure metadata (`error.json`) from Dagster export job failures.
- Completed Slice 2 documentation closeout `S2-G4` and `S2-G5`:
  - Developer docs:
    - `docs/Developer/s2-raw-export-api.md`
    - `docs/Developer/s2-export-guardrails.md`
  - User docs:
    - `docs/raw-export-reference.md`
    - `docs/data-taxonomy.md` updates.
- Added guardrail proof artifact:
  - `spec/slices/slice-2-raw-export-native/guardrails-proof.json`

## Current state
- Slice 2 is complete (`S2-C1..S2-C6`, `S2-P1..S2-P3`, `S2-G1..S2-G5` all checked).
- Export submit path now enforces:
  - rights lookup from matrix,
  - legal-signoff requirement for `Hosted Allowed`,
  - queue backpressure (`ORIGO_EXPORT_MAX_CONCURRENCY`, `ORIGO_EXPORT_MAX_QUEUE`),
  - immutable audit event writes (`ORIGO_EXPORT_AUDIT_LOG_PATH`).
- Export status path now surfaces classified terminal failures:
  - persisted `error_code`/`error_message` from `ORIGO_EXPORT_ROOT_DIR/<run_id>/error.json`,
  - fallback codes for uncategorized Dagster failures/cancellations.
- Export job writes either:
  - success metadata (`metadata.json` + artifact), or
  - failure metadata (`error.json`) and then re-raises failure.

## Watch out
- Audit chain validation is intentionally strict; any tampering or invalid line blocks further writes and returns `EXPORT_AUDIT_WRITE_ERROR`.
- Rights matrix is fail-closed; missing/invalid dataset classification blocks export submit.
- Current export path still rejects `auth_token` dispatch for this slice; BYOK execution path remains future scope.
