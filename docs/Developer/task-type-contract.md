# Task-Type Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Governance/version reference: whole-system Origo governance / `origo_control_plane v1.2.90`

## Canonical source
- Routing surface: `AGENTS.md`
- Canonical machine contracts:
  - `contracts/governance/request-task-coverage.json`
  - `contracts/governance/task-types.json`
  - `contracts/governance/task-decomposition.json`
  - `contracts/governance/task-admission.json`
  - `contracts/governance/contract-applicability.json`

## Purpose
- Reference note for the allowed task-type taxonomy and the decomposition rules that force one primary task type per work unit.
- Use the machine contracts for exact request-family coverage, allowed task types, fail-closed admission rules, scope boundaries, mixed-type prohibitions, and shared-contract coverage across tasks.

## Authority note
- This document is reference-only for governance.
- If this file and the machine contracts ever differ, the machine contracts win.
