# Slice 10 Developer: Deploy Troubleshooting

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-10
- Slice reference: S10 (`S10-G1`, `S10-G2`, `S10-G3`)
- Version reference: API `v0.1.5`, control-plane `v1.2.55`

## Purpose and scope
- Quick troubleshooting map for common deploy-on-merge failures.

## Inputs and outputs with contract shape
- Input: failed `deploy-on-merge` run logs.
- Output: root-cause classification and required fix path.

## Data definitions (field names, types, units, timezone, nullability)
- `step`: workflow step name where failure occurred.
- `error_signature`: exact fail-loud log string.
- `fix_path`: concrete remediation action.

## Source/provenance and freshness semantics
- Source of truth is the failing GitHub run logs and server compose state output.

## Failure modes, warnings, and error codes
- `ERROR: <VAR> must be set and non-empty.`
  - fix: add missing secret/env contract value.
- `ERROR: /opt/origo/deploy/.env must be present and non-empty.`
  - fix: provide valid `ORIGO_SERVER_ENV_B64` for first run or repair server env file.
- `ERROR: service <name> image mismatch`.
  - fix: inspect image tag propagation in compose env and rerun deploy.
- `ERROR: clickhouse container id missing after compose up.`
  - fix: inspect compose config and container startup logs.

## Determinism/replay notes
- Validate same-commit replay using run-attempt fingerprint comparison in Slice 10 baseline fixture.

## Environment variables and required config
- See `.env.example` and workflow secret contract.

## Minimal examples
- Find recent deploy runs:
  - `gh run list --workflow "deploy-on-merge" --limit 20`
- Inspect one run:
  - `gh run view <run-id> --log`
