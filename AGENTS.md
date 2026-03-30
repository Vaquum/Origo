---
alwaysApply: true
---

## Non-negotiables

- No workarounds. Find the root cause, fix it.
- No fallbacks. Let things break and make noise.
- No silent failures. If something goes wrong, surface it.
- No swallowed exceptions. If it's caught, it's handled or re-raised.
- Nothing deployment-specific is ever hard-coded. If a value changes between local, CI, and production, it is an environment variable. No exceptions.
- Never replace real execution. No unplanned dry-runs.

## Before every slice

Read `spec/1-top-level-plan.md` and `spec/2-itemized-work-plan.md` in full. No exceptions.

## Execution order

Every slice is `Capability → Proof → Guardrails`. In that order. No skipping ahead.

## Pull Request Governance

- `zero-bang` must never be used as the git user, PR author, or implementation identity.
- Every PR must request review from `zero-bang` and wait for the review outcome before merge.
- If `zero-bang` leaves review comments or requested changes, resolve every conversation and then re-request `zero-bang` review.
- Merge only after `zero-bang` approves the final PR state.

## After every slice

A slice is not done until all of the following are complete. Do not report done before then.

1. Check off all completed items in `spec/2-itemized-work-plan.md`.
2. Bump the version in the appropriate config file.
3. Append the slice summary to `CHANGELOG.md`.
4. Update `.env.example` to reflect every environment variable introduced or changed in this slice. Ask: was any value hard-coded that should be an env var? If yes, fix it before marking done.
5. Leave a manifest in `spec/slices/<slice-id>/manifest.md` with exactly three sections:
   - **What was done** — deviations from plan and why.
   - **Current state** — what is true about the system right now that the next slice inherits.
   - **Watch out** — open questions, fragile parts, assumptions made.
   - The test for a good manifest: can the next engineer start without asking anyone?
6. Leave a baseline fixture artifact in `spec/slices/<slice-id>/baseline-fixture-<window>.json` with:
   - Fixture window dates.
   - Source checksums (zip and csv sha256 per day).
   - Run 1 and Run 2 output fingerprints (row count, offsets, volume sums, hash per day).
   - `deterministic_match: true/false`.
   - Column key explaining every field in the fingerprint rows.
7. Leave run notes in `spec/slices/<slice-id>/run-notes.md` covering:
   - Date, scope, fixture window, runtime environment.
   - Any system changes made as a side effect of the proof run.
   - Known warnings and why they are acceptable or not.
   - Any guardrails that were deferred, explicitly named.
   - Confirmation that work plan checkboxes, version, changelog, and `.env.example` are all updated.
