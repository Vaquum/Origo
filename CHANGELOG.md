# v3.6.1 on August 23, 2026
- Key every HuggingFace publish sensor run on both the materialized partition and its triggering run, so re-materializing a partition requests a fresh publish while repeated sensor evaluation of one materialization remains idempotent.

# v3.6.0 on August 20, 2026
- Add the `publish_btc_briefing_history` asset and its `btc_briefing_history/1` rolling file: one `btc_briefing_history.json` on the same `vaquum/btc_briefing_feed` dataset, republished on every daily partition, carrying 70 days of 15m bars and 61 days of daily bars rolled up from the 1m klines projection over the span that ends where the partition day begins — the two lookbacks the consuming briefing is computed over, so it reads its whole history from this dataset instead of re-fetching it from an exchange REST API on every run. The span's bar grid is anchored to its opening midnight and declared in UTC epoch seconds in the SQL itself, so every bar in the window sits on one midnight-aligned grid and none straddles a day boundary; the daily feed's completeness policy is applied across the whole span, so a span short by a bar, a bar built from fewer than its full complement of distinct 1m minutes, or a duplicated 1m source row raises instead of publishing a gap a consumer would read as real quiet. A new `publish_btc_briefing_history_sensor` fires on each daily feed materialization, so the history and that day's feed file always name the same day and compose without overlapping; a partition older than the published window is refused — before its rollup runs — rather than rolling the single file backwards.

# v3.5.1 on August 20, 2026
- Record `require_extra_approval_for_unattributed_changes` in the `Protect-Main` ruleset snapshot. GitHub added the field to the rulesets API, so the live ruleset began reporting it while `.github/rulesets/main.json` did not carry it, and `pr_checks_ruleset` failed every open PR on drift it had not caused. The snapshot now matches the live ruleset field for field, which is the only state in which that gate can tell a real out-of-band change from an API addition.

# v3.5.0 on August 18, 2026
- Publish the daily BTC briefing feed to HuggingFace on the daily cadence: `publish_btc_briefing_feed` becomes daily-partitioned and uploads each partition day's `btc_briefing/1` feed to the `vaquum/btc_briefing_feed` dataset as one JSON file per day plus a `latest.json` pointer and dataset card (reusing the existing `HF_TOKEN` credential; no new secret). A new `publish_btc_briefing_feed_sensor` fires after each daily spot klines materialization, so the first tick after deploy publishes the most recent complete UTC day and any past covered day is a manual partition launch.

# v3.4.0 on August 18, 2026
- Add the `publish_btc_briefing_feed` asset: builds and validates the daily BTC briefing feed (`btc_briefing/1`) for the last complete UTC day from the origo ClickHouse tables — 15m/1d OHLCV bars from the 1m klines projection, measured volume-at-price in exact integer satoshis split by taker side, and per-minute series, exact daily percentiles and 8h session aggregates of the depth20 1m book. Every time field is declared as UTC epoch seconds in the SQL itself, so the contract's time representation cannot drift with the server's Arrow serialization. An incomplete, short, or duplicated bar day raises instead of producing a corrupt feed. The asset computes and validates only; delivery to the consuming repository is deliberately a separate slice.

# v3.3.1 on July 21, 2026
- Add the MIT license (verbatim from Vaquum/Limen) as a root LICENSE file plus `[project]` license metadata in pyproject.toml, and rewrite README.md to the shared Vaquum module README structure: honest capability inventory backed by code on `main`, a docker-compose quickstart (table creation, one-day spot backfill, query-module read-back), and the standard boilerplate tail.

# v3.3.0 on July 10, 2026
- Make the daily spot/futures partition write atomic: the full-day row insert (millions of rows over minutes — the window a killed run left a partial day in) now builds and count-verifies a per-day staging table before the live table is touched, then promotes it with a synchronous day DELETE followed by an atomic `MOVE PARTITION` (metadata part-move, not a splittable INSERT..SELECT that can commit a partial day on cancellation). MOVE appends the day into the live month partition, preserving the month's other days. The only residual window (between DELETE and MOVE) leaves the day MISSING, never partial or duplicated, which the daily gap-repair schedule then heals (tracker #275 item 4). Unblocks run-level retries (item 21).

# v3.2.0 on July 10, 2026
- Enable Dagster run monitoring: runs stuck in STARTING fail after 300s, and any run is bounded at 26h (`max_runtime_seconds`; per-run override via the canonical `dagster/max_runtime` tag). DefaultRunLauncher has no worker health checks, so a dead STARTED worker turns red at the 26h bound, not sooner — an honest bound, not instant orphan detection. Gap repair additionally excludes partitions whose runs reached a terminal state within a 1h grace period, because a timed-out run is force-marked FAILED without confirmed worker exit (tracker #275 items 1 and 21). Run retries stay disabled until partition replacement is atomic.

# v3.1.0 on July 9, 2026
- Add hourly daily-gap-repair schedules for the spot and futures daily pipelines: ledger-absent days in a 14-day lookback (ending at today-2 to never race the regular daily ticks) whose Binance archive exists are re-requested as partition runs, keyed once per day per gap — the 2026-07-03 futures incident class now self-heals (tracker #275 item 17).

# v3.0.3 on July 6, 2026
- SECURITY: bind the deploy dagit webserver to 127.0.0.1 instead of all interfaces (it had no auth and was publicly reachable on :4000 since 2026-04-21); operator access is now via SSH tunnel. Add a tests/tools guard that fails any deploy-compose port not bound to loopback.

# v3.0.2 on July 6, 2026
- CI: sync the checked-in main ruleset snapshot with the `dismissal_restriction` field GitHub now returns on the live pull_request rule (disabled/default value; effective branch protection unchanged), unblocking the law-9 ruleset-drift gate.

# v3.0.1 on July 4, 2026
- Adopt the Origo identity in CI and deploy: repo renamed to Vaquum/Origo, deploy workflow variables `TDW_*` -> `ORIGO_*` (values unchanged; server names intentionally stay tdw), fresh `ORIGO_PRIVATE_KEY` deploy key, GHCR images `origo-dagster`/`origo-clickhouse` (legacy packages retained for rollback), slice-gate help text and ruleset fixtures updated.

# v3.0.0 on July 4, 2026
- BREAKING: rename the Python package `tdw_control_plane` to `origo` — module imports, Dagster workspace/code-location, packaging config, typing/fail-loud gate roots, and the deploy smoke test all move; the Dagster code location becomes `origo.definitions` (instigator state restarts via in-code RUNNING defaults).

# v2.1.0 on July 4, 2026
- Allow `package_root` to change in the typing and fail-loud budget-source ratchets only when the base root directory no longer exists in the head tree AND every ratchet total is identical to base (a totals-neutral rename); narrowing onto a subtree while the old root exists stays blocked, and no ratchet total can move in the same PR as a root change. Residual risk accepted and documented: a rename-shaped PR can still relocate files outside the scan surface — as any PR always could by moving files out of the root — and operator review remains the backstop for that.

# v2.0.1 on July 4, 2026
- Rebrand repo metadata to Origo: dist name, description, README, slice template prose, dev image tag, test fixtures dir and container prefix; remove the never-existing `quickstart_etl_tests` ghost path from pyproject, budgets, gates, and the lint contract; delete vestigial `dagster_cloud.yaml`.
- Pin `default_status=RUNNING` on all 12 HuggingFace asset sensors and add a regression test asserting every sensor defaults to RUNNING.

# v2.0.0 on July 2, 2026
- BREAKING: remove the legacy tdw warehouse pipeline — all tdw table/ingestion/summary assets, the tdw daily and monthly roll-forward schedules, their jobs, the tdw-only ClickHouse helpers in definitions, and the orphaned tdw utils.
- Remove the dead `query.get_binance_spot_klines` helper (read from `tdw.binance_trades_complete`) and the tdw module stub in the origo test fixture.
- Add a regression test that fails any PR reintroducing a tdw asset, job, schedule, or module.

# v1.20.1 on June 16, 2026
- Reconcile Binance spot depth20/depth200 live gaps across source history, ClickHouse projections, and Arrow chunks.

# v1.20.0 on June 15, 2026
- Auto-update Binance spot depth20 and depth200 Arrow snapshots as minute Arrow chunks after their source refresh jobs succeed.

# v1.19.0 on June 15, 2026
- Publish Binance spot depth20 and depth200 raw snapshots as mmap-ready Arrow IPC files under `/opt/arrow`.

# v1.18.1 on June 15, 2026
- Wire Binance spot depth200 service credentials into the deployment environment.

# v1.18.0 on June 15, 2026
- Add Binance spot depth200 source-native snapshots and 1-minute projection tables alongside the existing depth20 source.

# v1.17.2 on June 5, 2026
- Replace the legacy RFC issue template with a PRD issue form and keep the slice issue form loadable by removing its empty title field.

# v1.17.1 on June 5, 2026
- Fix the Arrow bar store writing multi-record-batch files for any series past polars' ~122k-row IPC batch default: a `memory_map=True` reader surfaced those batches as multiple chunks, breaking the single-batch zero-copy `ts` view the store exists to provide. Force one record batch via `record_batch_size`. Self-heals on deploy (the byte change yields a new content-hash version, so each series republishes once as a single batch).

# v1.17.0 on June 5, 2026
- Add a versioned, mmap-ready Arrow bar store: a run-status sensor rebuilds every series into a single-record-batch, uncompressed Arrow IPC file under LOCAL_ARROW_DIR (default /opt/arrow) whenever the Parquet mirror job succeeds. Measures are carried verbatim at full precision (no downcast), so the store stays bit-for-bit reproducible against the mirror; it is published with an atomic `latest` symlink swap, content-hash versioning, a monotonic freshness guard, and a few retained prior versions so in-flight mmap and pinned-version reads never break mid-swap.
- Run the Binance spot Parquet mirror every minute (was every 10 minutes) so the mirror — and the Arrow bar store it triggers on completion — track the 1-minute ClickHouse latest projections.

# v1.16.1 on June 4, 2026
- Fix Binance spot dollar-kline Hugging Face exports collapsing every timestamp to ~1970 under polars >=1.40 by emitting millisecond-precision DateTime64 so the Arrow round-trip preserves the real dates.

# v1.16.0 on June 4, 2026
- Add a 10-minute stateless job mirroring the 12 Binance spot kline series to monthly Parquet files on a local mount.

# v1.15.2 on June 4, 2026
- Route Hugging Face Binance spot time-kline exports through the Origo 1m kline projection.

# v1.15.1 on May 30, 2026
- Include latest Binance spot table creation in the scheduled latest data-source job.

# v1.15.0 on May 27, 2026
- Add rolling latest Binance spot trade, kline, and cut projections in Origo.

# v1.14.2 on May 22, 2026
- Require raw spot trades before replacing Binance spot dollar imbalance kline partitions.

# v1.14.1 on May 21, 2026
- Fix the Binance spot dollar kline base size to 1M and publish 1M, 15M, 30M, 60M, 120M, and 240M dollar snapshots.

# v1.14.0 on May 20, 2026
- Add Hugging Face publishers for BTCUSDT 100k, 2M, 4M, 8M, 16M, and 32M dollar spot kline snapshots from Origo dollar klines.

# v1.13.4 on May 20, 2026
- Add a manual Origo depth20 partition-state reconciliation job for existing ClickHouse rows.

# v1.13.3 on May 20, 2026
- Add manual Origo backfill jobs for Binance spot raw trades and depth20 snapshots plus 1m projection.

# v1.13.2 on May 20, 2026
- Decouple the Binance spot dollar klines backfill job from raw-trades ingestion and fail the dollar refresh when raw trades are absent.

# v1.13.1 on May 20, 2026
- Add a dedicated Binance spot dollar klines backfill job.

# v1.13.0 on May 20, 2026
- Add Binance spot dollar imbalance klines on Origo daily spot trades.

# v1.12.0 on May 20, 2026
- Add Binance spot tick klines on Origo daily spot trades.

# v1.11.0 on May 19, 2026
- Add Binance spot volume klines on Origo daily spot trades.

# v1.10.0 on May 19, 2026
- Add Hugging Face publishers for BTCUSDT 15-minute, 30-minute, and 2-hour spot kline snapshots from Origo daily spot trades.

# v1.9.0 on May 19, 2026
- Add Binance spot dollar klines on Origo daily spot trades.

# v1.8.0 on May 18, 2026
- Add Hugging Face publishers for BTCUSDT 1-hour and 4-hour spot kline snapshots from Origo daily spot trades.

# v1.7.1 on May 14, 2026
- Add Dagster table-creation jobs for the Binance spot depth20 source-native snapshots and 1-minute projection tables.

# v1.7.0 on May 14, 2026
- Add the Binance spot depth20 history service as an Origo source with source-native snapshots and a 1-minute source projection.

# v1.6.7 on May 1, 2026
- Add `audit_main_ruleset`, a privileged post-merge `main` workflow that audits full live parity of ruleset `5406599`, including `bypass_actors`, against `.github/rulesets/main.json`.
- Add `tools/privileged_ruleset_audit.py` and `tests/tools/test_privileged_ruleset_audit.py`, including the fail-loud contract for missing or underscoped visibility of `bypass_actors` and the live-payload snapshot on failure.
- Extend `pr_checks_ruleset` so the privileged-audit workflow and tool contract are mechanically protected by required CI before rollout.

# v1.6.6 on April 30, 2026
- Route the Hugging Face spot kline publisher through the Origo projection.

# v1.6.5 on April 28, 2026
- Move the default-running daily Binance spot Origo source schedule to `04:00 UTC` while leaving the futures schedule at `10:00 UTC`.

# v1.6.4 on April 28, 2026
- Move the default-running daily Binance Origo source schedules to `10:00 UTC` so routine automation runs after observed Binance archive publication.
- Add bounded hourly Dagster retries to the spot and futures daily archive ingest assets for late archive publication.

# v1.6.3 on April 25, 2026
- Replace the two daily Binance Origo source-template schedules with Dagster partitioned-job schedules that request the latest daily partition instead of launching non-partitioned empty-config runs.
- Start the daily spot and futures schedules enabled so Dagster owns routine daily automation while partition backfills stay on Dagster's built-in backfill path.

# v1.6.2 on April 23, 2026
- Rename the two Origo source-template schedules to `daily_binance_spot_pipeline_schedule` and `daily_binance_futures_pipeline_schedule` so both surfaces include the source prefix explicitly.
- Keep the existing spot and futures source-template jobs unchanged; this slice only renames the Dagster schedule definitions and their registration surface.

# v1.6.1 on April 23, 2026
- Add the repo-root `AGENTS.md` governance file with the operator-specified ten-law workflow contract and `zero-bang` approval authority.
- Add `tests/tools/test_agents_contract.py` and extend `pr_checks_ruleset` so the checked-in `AGENTS.md` file identity and workflow coverage are mechanically enforced in CI.

# v1.6.0 on April 23, 2026
- Complete the Binance futures Origo data-source template on top of `binance_daily_futures_trades` by adding the single-source `binance_futures_klines` projection and the shared `aligned_1m_exchange` futures path.
- Rename the generic spot schedule to `daily_spot_pipeline_schedule`, add `daily_futures_pipeline_schedule`, and wire `refresh_binance_futures_data_source_job` so spot and futures source-template automation follow the same naming law.
- Add checked-in real Binance futures daily fixtures for both the headerless and headered source shapes, plus fixture-backed futures row/schema/idempotency tests that prove `aligned_1m_exchange` can hold both `binance_spot` and `binance_futures`.

# v1.5.1 on April 23, 2026
- Correct the Origo Binance spot projection contract so `binance_spot_klines` and `aligned_1m_exchange` match the TDW 1-minute kline schema instead of the exchange-native 12-column shape.
- Replace the single-source and aligned refresh SQL so both tables materialize the TDW analytics columns (`mean`, `std`, `median`, `iqr`, maker/liquidity fields) from raw spot trades.
- Add a checked-in TDW contract fixture and replace the old exchange-native row tests with fixture-backed schema and row-parity tests for both projection tables.

# v1.5.0 on April 23, 2026
- Complete the first Binance spot Origo data-source template on top of `binance_daily_spot_trades` by adding the single-source `binance_spot_klines` projection and the shared `aligned_1m_exchange` projection layer.
- Replace the old raw-only daily Origo schedule target with `refresh_binance_spot_data_source_job`, which materializes the raw daily insert plus both projection layers for the same partition.
- Add end-to-end `tests/origo_source_native/test_origo_binance_spot_data_source_template.py` proofs for table-name contracts, exact schema, exact Binance-derived 1-minute rows, aligned dataset-source rows, and same-partition rerun idempotency.

# v1.4.1 on April 22, 2026
- Sync `tests/fixtures/github/ruleset_live_unexpected_field.json` to the current 9-context protected-check set on `main`, including `pr_checks_lint` and `pr_checks_tests`.
- Add `test_unexpected_field_fixture_preserves_required_contexts` so `pr_checks_ruleset` fails if that negative fixture ever drifts from the checked-in ruleset snapshot's required-status list.

# v1.4.0 on April 22, 2026
- Move the runtime image to Python `3.11.12` so Docker matches the package and CI interpreter contract.
- Replace the Origo path with a daily-source-native Binance spot trades template: idempotent `create_origo_database`, idempotent `create_binance_daily_spot_trades_table_origo`, and fail-loud `insert_daily_binance_spot_trades_to_origo`.
- Rename the Origo raw table surface to `binance_daily_spot_trades`, add the companion `binance_daily_spot_trades_ingestion` ledger, preserve source timestamps with `DateTime64(6)`, and record Dagster run metadata plus source checksums/counts per ingested daily file.
- Add `.github/workflows/pr_checks_tests.yml`, the ClickHouse-backed `tests/origo_source_native` suite, fixture-backed Binance daily archives plus `.CHECKSUM` files, and the checked-in ruleset snapshot change that requires `pr_checks_tests` on `main`.

# v1.3.3 on April 22, 2026
- Add `.github/workflows/pr_checks_lint.yml` so `tools` and `tests/tools` gain a required fail-loud Ruff gate on `main`, pinned to Ruff `0.15.11`.
- Extend `pr_checks_ruleset` with `tests/tools/test_lint_ci_contract.py` and a pinned Ruff install so the lint gate itself is mechanically protected by required CI.
- Remove dead Ruff ignores `ANN101` and `ANN102`, replace broad-exception handling in `tools/slice_gate.py` and `tools/typing_gate.py` with explicit fail-loud setup/read handling, and replace the remaining `RUF005` list concatenations in `tools/fail_loud_gate.py`.

# v1.3.2 on April 22, 2026
- Add `.github/rulesets/main.json`, `tools/ruleset_gate.py`, `pr_checks_ruleset`, and fixture-backed ruleset drift tests so `main` can ratchet its required PR-path contexts against a checked-in snapshot.
- Fix `tools/cc_gate.py` so linked-issue title lookup failures raise a hard setup error instead of silently skipping Conventional Commits validation.
- Remove the post-merge CHANGELOG automation workflow and `scripts/update_changelog.py` because `pr_checks_version` is now the authoritative pre-merge version/changelog gate.

# v1.3.1 on April 21, 2026
- Add `pr_checks_fail_loud` workflow and `tools/fail_loud_gate.py` ratcheting seven silent-fallback categories in the package: `bare_except`, `empty_pass`, `empty_ellipsis`, `empty_return_none`, `empty_continue_break`, `contextlib_suppress`, `errors_ignore_kwarg`. Base-vs-head protection so the budget cannot be weakened in the same PR that gates against it.
- Add `.github/fail_loud_budget.json` as the committed baseline oracle (`bare_except=4`, `empty_pass=6`, `empty_continue_break=1`, all others zero on 35 production files at introduction).
- Add `pr_checks_version` workflow and `tools/version_gate.py` enforcing six rules on every PR: pyproject.toml differs, `[project].version` advances by strict `MAJOR.MINOR.PATCH` (prerelease and build-metadata forms rejected outright, since the gate compares as integer triples and real semver precedence would be misrepresented), CHANGELOG.md differs, CHANGELOG's first `# v<X.Y.Z>` header matches the new version, the top version section has at least one non-empty non-header line of content before the next version header (so a header-only trail is rejected), and the bump level meets the minimum required by the PR's Conventional Commits type (`type!` → major, `feat` → minor, anything else → patch).
- `contextlib.suppress` detection resolves module-alias chains to a fixed point (`import contextlib as cl; mod = cl; sup = mod.suppress; sup2 = sup`) so any re-binding path to `contextlib.suppress` is counted. Same fixed-point technique already used by `typing_gate.py` for `typing.Any`.

# v1.3.0 on April 21, 2026
- Add `.github/ISSUE_TEMPLATE/slice.yml` — slice issue template. Eleven sections each carrying a blockquoted `> **Significance.**` paragraph that survives into the filed issue body.
- Add `pr_checks_slice` workflow and `tools/slice_gate.py` enforcing the PR↔slice-issue contract as eight deterministic rules: exactly one `Closes/Fixes/Resolves #N` reference, the reference resolves in the repo, resolves to an issue (not another PR), issue is OPEN, issue has the `slice` label, PR title byte-equals issue title, issue body contains every full multi-line Significance blockquote from the template verbatim (extracted at gate runtime so template and validator cannot drift apart), PR diff ⊆ issue `## Surfaces`, PR diff ∩ issue `## Out of Scope` = ∅.
- Add `pr_checks_slice_on_issue` workflow — stale-state recovery on `issues` events (edited, labeled, unlabeled, closed, reopened, deleted). Finds linked open PRs by scanning every open PR body with the same closing-keyword regex rule 1 uses, re-runs the slice gate against current state, and posts a fresh `pr_checks_slice` check-run to each PR's head SHA via the Checks API. Branch protection uses the latest check-run per name per SHA, so an issue change that breaks any rule invalidates the required check within seconds.
- Add `pr_checks_cc` workflow and `tools/cc_gate.py` enforcing Conventional Commits v1.0.0 on three surfaces: PR title, linked-issue title, and every non-merge commit in the PR range. Allowed types: `feat | fix | docs | style | refactor | perf | test | build | ci | chore | revert`.
- Use GitHub REST `pulls/:num/files` with `--paginate` for all PR file enumeration (previously `gh pr view --json files`, which caps at 100). Both slice workflows cross-check the enumerated count against the PR object's `changedFiles` field and hard-fail if they disagree, so scope rules cannot silently under-enforce on large PRs.
- Ensure branch-protection `Protect-Main` ruleset requires `pr_checks_slice` and `pr_checks_cc` in addition to `PR Checks CodeQL (python)` and `pr_checks_typing`.

# v1.2.1 on April 21, 2026
- Add `pr_checks_typing` workflow and `tools/typing_gate.py` enforcing typing discipline as a ratchet: pyright strict config audit, `pyrightconfig.json` ban, `pyright.include` identity check, regex escape-hatch ratchet, AST-based `typing.Any`-reference ratchet (covers bare `Any`, `typing.Any`, `t.Any`, aliased imports, and module-level assignment-alias chains), pyright total-error-count ratchet, `filesAnalyzed` ratchet, and a base-vs-head budget-source ratchet that blocks weakening of the oracle in the same PR it gates.
- Add `[tool.pyright]` strict configuration to `pyproject.toml` with the full `report*` matrix set to `error`.
- Add `[tool.ruff]` configuration to `pyproject.toml` selecting `E/F/I/UP/RUF/BLE/ANN`.
- Add `.github/typing_budget.json` as the committed baseline oracle (zero escape hatches, 1213 pyright-strict errors on 35 files at introduction).
- Bump `project.requires-python` to `>=3.11` to align with `tomllib` usage, `pyright.pythonVersion`, `ruff.target-version`, and CI.
- Bump `pr_checks_codeql.yml` Python from `3.10` to `3.11` to match the above.
