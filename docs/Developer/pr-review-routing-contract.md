# PR Review Routing Contract

## Metadata
- Owner: Origo Engineering
- Last updated: 2026-03-30
- Slice/version reference: Slice 34 / `origo_control_plane v1.2.84`

## Purpose and scope
- This document freezes the pull-request routing contract for normal engineering changes in this repo.
- Scope:
  - PR authorship identity
  - required reviewer identity
  - conversation-resolution sequence
  - merge gate

## Inputs and outputs
- Inputs:
  - a branch with proposed code or documentation changes
  - a GitHub pull request opened from that branch
  - review feedback from `zero-bang`
- Outputs:
  - one PR whose final state has:
    - `zero-bang` requested as reviewer
    - all actionable conversations resolved
    - one final `zero-bang` approval before merge

## Contract
1. `zero-bang` is review authority only.
2. `zero-bang` must never be used as the git user, PR author, or implementation identity for normal engineering work.
3. Every PR must request review from `zero-bang`.
4. The author must wait for the `zero-bang` review outcome before merge.
5. If `zero-bang` leaves comments or requested changes, the author must:
   - address the feedback
   - resolve every conversation
   - re-request `zero-bang` review
6. Merge is allowed only after the final PR revision has one `zero-bang` approval.

## Operational workflow
1. Open PR as a non-`zero-bang` author.
2. Request `zero-bang` review.
3. Wait for the review outcome.
4. If comments arrive, fix the issues and resolve every conversation.
5. Re-request `zero-bang` review.
6. Merge only after final `zero-bang` approval.

## Failure modes and violations
- Forbidden author:
  - If a PR is opened as `zero-bang`, the approval path is invalid because the required reviewer and approver cannot also be the PR author.
- Missing review request:
  - Merge readiness is invalid until `zero-bang` has been explicitly requested.
- Unresolved conversations:
  - Merge readiness is invalid while actionable review conversations remain unresolved.
- Stale approval:
  - If code changes land after review comments were addressed, the author must re-request `zero-bang` review before merge.

## Determinism and replay notes
- The routing sequence is fixed:
  - author PR
  - request `zero-bang` review
  - resolve all conversations
  - re-request `zero-bang` review
  - merge after approval
- No alternate “operator memory” path is valid if it contradicts the sequence above.

## Environment variables and required config
- None.

## Minimal example
- Valid sequence:
  - PR author: `codex` or another non-`zero-bang` identity
  - reviewer requested: `zero-bang`
  - conversations: resolved
  - final approval: `zero-bang`
  - merge: allowed
- Invalid sequence:
  - PR author: `zero-bang`
  - reviewer/approver: `zero-bang`
  - merge: forbidden by contract
