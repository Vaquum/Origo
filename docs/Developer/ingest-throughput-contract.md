# Ingest Throughput Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Governance/version reference: whole-system Origo governance / `origo_control_plane v1.2.90`

## Canonical source
- Routing surface: `AGENTS.md`
- Canonical machine contract: `contracts/governance/ingest-throughput.json`

## Purpose
- Reference note for the daily-file ingest throughput law, covered modules, and explicit exception handling.
- Use the machine contract for exact hot-path and concurrency requirements wherever the covered daily-file ingest classes appear in future Origo work.

## Authority note
- This document is reference-only for governance.
- If this file and the machine contract ever differ, the machine contract wins.
