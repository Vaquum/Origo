# Backfill Status Reference

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-26
- Slice/version reference: S34 prep (platform v0.1.28 branch state)

## Purpose and scope
- User-facing reference for what Origo means by historical availability while full canonical backfill is in progress.
- Scope covers raw query, raw export, and historical HTTP/Python surfaces.

## Inputs and outputs with contract shape
- This document is a reference artifact; it does not define a standalone endpoint.
- It applies to:
  - `POST /v1/raw/query`
  - `POST /v1/raw/export`
  - `POST /v1/historical/*`
  - `HistoricalData` Python methods

## Data definitions (field names, types, units, timezone, nullability)
- `terminal proof boundary`:
  - the latest partition/window that has passed canonical proof and is eligible for serving
- `historical availability`:
  - the portion of dataset history that is currently queryable because it is both ingested and terminally proved
- `full available history`:
  - for no-selector requests, the full currently available proved history, not speculative vendor history beyond the proof boundary
- `in-progress backfill`:
  - backfill work is ongoing and the served historical boundary may advance over time
- `ambiguous partition`:
  - canonical rows exist but the partition is not yet terminally proved
- `quarantined partition`:
  - proof failed and the partition is blocked pending explicit reconcile

## Source/provenance and freshness semantics
- Origo serves only from projection ranges that are backed by terminally proved canonical partitions.
- During Slice 34, backfill status is dataset-specific and may differ by source.
- Source existence alone does not make data queryable; proof completion is the serving gate.
- No-selector requests resolve to `earliest -> latest proved boundary` for that dataset.

## Failure modes, warnings, and error codes
- `404`:
  - no proved rows exist in the requested window
- `409`:
  - request contract failure
  - auth/rights failure
  - strict-mode rejection when warnings are present
- `503`:
  - runtime/backend failure
- Origo does not silently serve beyond the proved boundary.

## Determinism/replay notes
- Historical surfaces are replayable only on the terminally proved portion of history.
- As backfill advances, later windows become available without changing the replay contract for already-proved windows.
- Slice 34 closeout is the point where full-history availability claims become complete for each dataset.

## Environment variables and required config
- HTTP/API access:
  - `X-API-Key`
- Python access:
  - working Origo runtime config for the target environment

## Minimal examples
- No-selector historical request:
  - returns the full currently proved history for that dataset
- Bounded request beyond the proved boundary:
  - returns `404` or a fail-loud runtime error rather than partial silent fill
