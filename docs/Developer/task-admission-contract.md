# Task Admission Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Governance/version reference: whole-system Origo governance / `origo_control_plane v1.2.90`

## Canonical source
- Routing surface: `AGENTS.md`
- Canonical machine contract: `contracts/governance/task-admission.json`
- Request-family coverage support: `contracts/governance/request-task-coverage.json`
- Coverage/reference support: `contracts/governance/contract-applicability.json`

## Purpose
- Reference note for fail-closed task admission.
- Use the machine contract for the exact rules that forbid unroutable, mixed-type, under-contracted, or newly uncovered recurring work from proceeding without governance extension first.

## Authority note
- This document is reference-only for governance.
- If this file and the machine contracts ever differ, the machine contracts win.
