# AGENTS.md

## Pull requests

Every PR closes exactly one OPEN issue with `Closes #N` (or Fixes/Resolves).
A `feat` PR's issue carries the `prd` label; other types use an ordinary issue or a voluntary PRD. PRDs stand alone. Issue titles need not match PR titles or follow Conventional Commits.

PR titles and every non-merge commit follow Conventional Commits v1.0.0.

Required checks preserve typing discipline, fail-loud behavior, Ruff, source-native runtime tests, CodeQL and the checked-in branch-protection snapshot. Typing and fail-loud budgets cannot be raised by the PR they gate.

Versions and CHANGELOG entries change during deliberate release preparation, not for every PR. Follow [Release policy](docs/Developer/Release-policy.md).

Use a branch and open a non-draft PR as soon as the change is ready for CI. Read failures, repair their cause and push. One approval and resolved review threads remain required. Branch protection does not require a branch update merely because `main` advanced; integrate target changes when needed to resolve conflicts or validate interacting changes. No direct push, force-push or deletion of `main`.

## Review work

When reviewing a PR, post each comment inline as a review comment. Do not confirm with the operator first.

When reviewing an issue, post comments as a new comment in the thread. Do not confirm with the operator first.

When reviewing comments left on an issue you are working on, always address the issues by editing the original body or by adding a new comment that clearly explains why something is not addressed. Do not confirm with the operator first

The opinion is the deliverable. Confirming before posting adds a round-trip and slows collaboration.

## Ingestion and performance

Before any work related to ingestion—including planning, review, debugging,
adapters, capture, backfills, reconciliation, projections or publication—read and
apply the [ingestion performance rules](docs/Developer/Ingestion-performance.md)
and read the [performance investigation log](docs/Developer/Performance-investigation-log.md).
Before any performance work elsewhere in the system, read both documents too.
Every performance task must append its evidence and positive and negative
learnings to the log before completion, including failed experiments and unchanged
results. State unmeasured or unknown outcomes explicitly. Preserve existing
entries; append corrections identifying the entries they supersede.

## Operator UX

Before drafting or implementing any PRD or task that adds a data source, read and apply the [source onboarding playbook](docs/Developer/Source-onboarding.md). This is a mandatory prerequisite for authors and reviewers; the PRD template requires it.

Use native Dagster jobs, assets, partitions, coverage, gap/failure selection and retry controls. The operator selects what to run through those controls. Never require the operator to type YAML, JSON, dates, identifiers, paths or configuration text into Dagit, or follow source-specific manual preparation instructions from a chat. Configuration, dependencies, preparation, automation and publication belong in versioned code. Deployment activates scheduled work and sensors for every enabled source; operators never enable them manually. Adding and backfilling a registered source automatically builds every declared projection and file output without separate operator launches. Apply this uniformly to full backfills, selected gaps, retries and other operator-facing operations across registered sources. A source PR is incomplete until this workflow is verified in the actual GUI with real data. Screenshots illustrate the required native behavior; they are not a request to build a custom interface.

## Monitoring

Before investigating any failure, read and follow [docs/Developer/Monitoring.md](docs/Developer/Monitoring.md): Dagit first, ClickHouse second, Docker third, the external collectors last. Every monitored fact has one store; Dagit is the investigation pane. The monitor worker is the sole detector and alert sender; its operational state is a cursor for notification and historical-import positions. The authorized /law exception is a read-only summary of the monitor's append-only observation tape, not a second truth database or detector. It is served at https://origo.vaquum.fi behind the Portal login. Do not add another dashboard, alert path or per-minute Dagster run in place of a worker receipt.

## Engineering stance

Validate the requested capability against its stated behavior, alongside the required checks:

**Radical simplicity.** The simplest code that meets the requirement wins. Complexity earns its place by naming the specific concern it addresses — not "robustness" or "future-proofing" in general.

**No defensive fog.** Agents are primed to produce defensible-looking code: `try/except` that swallows everything, fallbacks for cases that don't happen, docstrings that restate the signature, comments that narrate the line, parameters that might be useful someday. None of it belongs here. The fail-loud gate catches the AST-detectable forms mechanically; the rest is operator-caught at review.

**Minimal scope.** Touch only the files the task demands. Drive-by cleanups go in a separate task.

**No synthetic data.** Ever. Inventing data is not a shortcut — it corrupts everything downstream. If the real data isn't there, stop and ask.

**Validate against the stated expectation.** The question is never "did it run" — it's "did it return what the task promised."

**The smallest possible honest way always.** Task spec, code, communication, everything, let it be the smallest possible unit size that honestly delivers what is required.

## Test design

Use the same contract matrix for every data source. Additional source-specific tests require a distinct format, state transition or boundary failure; a larger suite is not evidence of better coverage.

Every new test must name the observable contract and the specific failure it detects.
Extend the existing owner of that contract. A separate case earns its place through a
different input, state transition or execution boundary; copied assertions do not.
Use genuine captured source data. Identify deliberate protocol faults explicitly.
Share immutable expensive preparation; keep mutable database, file and process state isolated.
Run one full-scale scenario per distinct resource bound and one end-to-end scenario per
distinct integration boundary. Check downstream rules at the cheapest layer that still
rejects their failure. Never invoke pytest from another test or copy a suite for another adapter.
Assert behavior and promised interfaces; incidental wording or code structure is not a contract.
For consolidation, map removed contracts to retained assertions and demonstrate rejection of
their relevant failures. Report test-code footprint and full-suite elapsed time before and after.

## When in doubt, stop

This is collaboration. If the requirement is unclear, if the scope is ambiguous, if a gate's meaning is unobvious, if the fix would require touching something that wasn't asked for — stop and ask the operator. Proceeding through doubt is where harm accumulates.
