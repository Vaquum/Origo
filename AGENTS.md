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

## Governance Hierarchy

- `AGENTS.md` is the only routing surface for governance.
- `contracts/governance/*.json` are the only normative governance contracts outside this file.
- These governance routes and contracts apply repo-wide to the whole Origo system and all future development, not only to the currently open slices.
- `spec/*.md` may describe governance, but they do not override routed machine contracts.
- `docs/Developer/*.md` may explain governance, but they are reference-only for governance.
- Historical slice docs are never governance authority.
- No task routing, no work. If a request cannot be routed safely from this file, stop immediately and report back to the operator before any code change, runtime action, PR action, deploy action, browser action, or data mutation.

## Universal Contract Routing

Load these contracts for every task as soon as the request is routed:
- `contracts/governance/governance-authority.json`
- `contracts/governance/master-doctrine.json`
- `contracts/governance/task-start-gate.json`
- `contracts/governance/task-end-gate.json`
- `contracts/governance/execution-doctrine.json`
- `contracts/governance/static-analysis.json`
- `contracts/governance/work-plan-discipline.json`
- `contracts/governance/documentation-contract.json`
- `contracts/governance/slice-closeout.json`
- `contracts/governance/pr-review-routing.json`
- `contracts/governance/contract-applicability.json`

## Master Doctrine

Load `contracts/governance/master-doctrine.json` for every task.

Fail-closed rules:
- The eight master principles in `contracts/governance/master-doctrine.json` govern every slice, step, move, task, and completion claim in Origo.
- `mechanical_enforcement_over_documentation` is the prime doctrine.
- The closed foundation set lives only in `contracts/governance/master-doctrine.json`.
- Anything outside the closed foundation set is deferrable until forced.
- Foundation expansion requires explicit operator approval before implementation starts.
- If any planned work contradicts the master doctrine, stop immediately and report back to the operator before doing any work.

## Universal Start Gate

Before any repo mutation, runtime action, browser action, deploy action, PR action, or data mutation, load `contracts/governance/task-start-gate.json` and answer every start-gate question in order.

Fail-closed rules:
- If any start-gate answer is `no` or `unknown`, stop immediately and report back to the operator before doing any work.
- No implementation, runtime mutation, repo mutation, PR work, deploy work, browser work, or data write may proceed before the start gate passes.

## Universal End Gate

Before any `done`, `ready`, `complete`, `mergeable`, `ready for review`, or similar terminal claim, load `contracts/governance/task-end-gate.json` and answer every end-gate question in order.

Fail-closed rules:
- If any end-gate answer is `no` or `unknown`, stop immediately and report back to the operator instead of making the claim.
- A task is not done if the claimed terminal state is only prepared rather than actually reached.
- A PR is not ready, complete, or mergeable until the required review contract has actually been satisfied.

## Before every slice

Read `spec/1-top-level-plan.md` and `spec/2-itemized-work-plan.md` in full. No exceptions.

## Task Admission

Before implementation starts, assign exactly one primary task type by loading:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

Fail-closed rules:
- Every request must match exactly one routed request family in `contracts/governance/request-task-coverage.json` before primary task-type assignment.
- If the request appears to be a genuinely new recurring work class not covered by the request-family catalog, stop immediately and report back to the operator so governance can be extended before implementation starts.
- The allowed primary task-type set is exhaustive for Origo engineering work. There is no catch-all type.
- If the request does not match a routed request family, stop immediately and report back to the operator.
- If the request is reference-sync, closeout-sync, or review-follow-up work without an explicit governing primary scope, stop immediately and report back to the operator.
- If the request does not map cleanly to exactly one primary task type, stop immediately and report back to the operator.
- If the request spans multiple task types and safe decomposition is not explicit, stop immediately and report back to the operator.
- If a task type exists but this file does not route the request to the needed contracts, stop immediately and report back to the operator.
- Follow any family-specific additional contracts from `contracts/governance/request-task-coverage.json` before work starts.
- After primary task-type assignment, load every activated shared-domain contract from `contracts/governance/contract-applicability.json` whose activation domains match the touched system surfaces.
- If the request touches any system surface without routed shared-domain contract coverage, stop immediately and report back to the operator before implementation starts.
- No implementation, runtime mutation, repo mutation, PR work, deploy work, browser work, or data write may proceed before routed contract coverage is established.

## Task-Type Routing

Every task type must load the task-admission contracts above and every activated shared-domain overlay below.

### `proof`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

### `governance`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`
- every impacted contract file in `contracts/governance/`

### `operator_surface`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

### `runtime_env`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

### `correctness`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

### `performance`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

### `capability`
Load:
- `contracts/governance/request-task-coverage.json`
- `contracts/governance/task-types.json`
- `contracts/governance/task-decomposition.json`
- `contracts/governance/task-admission.json`

## Shared-Domain Overlay Routing

Load the matching contract whenever the request touches the named system surface:
- `contracts/governance/dagster-authority.json` for Dagster/Dagit, partition truth, backfill, reconcile, reruns, write entrypoints, schedules, sensors, or Dagster-visible automation.
- `contracts/governance/ingest-throughput.json` for first-party daily-file ingestion, staged/vectorized ingest, worker concurrency, source pacing, or source timeouts.
- `contracts/governance/storage-sql-discipline.json` for ClickHouse schema, SQL query paths, migrations, migration ledger rules, or Arrow/Polars compatibility.
- `contracts/governance/serving-error-safety.json` for raw query/export serving semantics, status codes, warnings, strict-mode behavior, or historical request/response semantics.
- `contracts/governance/integrity-durability.json` for WAL/manifests/checksums, schema registry, integrity suites, append-only correction rules, or historical truth-source rules.
- `contracts/governance/rights-secrets.json` for source rights classification, legal-serving state, credentials, TLS/auth boundaries, or rights metadata exposure.
- `contracts/governance/runtime-recovery.json` for freshness policy, queue/concurrency limits, rollout method, quality gates, DR, audit retention, alerting, or quarantine policy.
- `contracts/governance/ingestion-guarantees.json` for exactly-once, no-miss, gap detection, quarantine, replay, recovery, or reconciliation.
- `contracts/governance/raw-fidelity.json` for `payload_raw`, `payload_sha256_raw`, raw-vs-json reproducibility, numeric precision, timestamps, or canonical payload-hash identity semantics.
- `contracts/governance/event-sourcing.json` for canonical event-log behavior, logical reset boundaries, projection-driven serving, projector checkpoints/watermarks, or aligned aggregate sink rules.
- `contracts/governance/historical-surface.json` for `native` / `aligned_1s`, historical HTTP/Python/query/export parameter contracts, response envelopes, or unbounded-default-window behavior.
- `contracts/governance/source-onboarding.json` for onboarding a new source into Origo with required mode/surface completion proof.
- `contracts/governance/source-migration.json` for migrating an existing source onto canonical writes and projection-driven reads.

## After every slice

Follow `contracts/governance/slice-closeout.json`. Do not report done before every required closeout step is complete.
