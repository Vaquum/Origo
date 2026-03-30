# Developer Docs Index

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30

## Purpose
- This folder is the engineering reference for Origo implementation slices.
- It has two doc types:
  - live canonical contracts
  - historical slice closeout records

## Live canonical contracts (use these first)
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
  - `docs/Developer/s34-daily-tranche-controller.md`
  - `docs/Developer/s34-exchange-sequence-controller.md`
  - `docs/Developer/s34-etf-backfill-runner.md`
  - `docs/Developer/s34-etf-ishares-archive-bootstrap-runner.md`
  - `docs/Developer/s34-bitcoin-height-window-contract.md`
  - `docs/Developer/s34-bitcoin-backfill-runner.md`
- Engineering workflow contract:
  - `docs/Developer/pr-review-routing-contract.md`

## Historical slice docs
- All `s*.md` files capture what was true at slice closeout time.
- Version metadata in those files is a slice snapshot, not necessarily the current runtime version.
- When there is any mismatch, treat the live canonical contracts above as source of truth.

## Editing rule
- If behavior changes in code, update live canonical docs first.
- Then update the relevant slice doc with an explicit note that it was adjusted after the original slice closeout.
