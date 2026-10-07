# Runtime test CI

`pr_checks_tests` executes every collected case once across two phases. Ordinary
cases use eight server workers (two on GitHub), with explicit `loadgroup` labels
and `--no-loadscope-reorder`. Independent scenarios run concurrently; diagnostic
server cases keep their shared native server together. Book HTTP captures and
verified sealed grids are prepared once per market behind a process lock in the
job's temporary directory; workers read them with the native seal/hash validator.
Every database, Dagster instance and mutable publication/spool directory stays
private. Placement labels are removed from reports using the exact assigned
marker; parameter identities remain intact.

Ordinary server temporary files use a 4GiB tmpfs within the existing 24GiB guest
RAM limit. It is unmounted before the fresh, serial resource process starts.
The `resource` marker identifies wall-clock/RSS and scheduling-sensitive HTTP/
catalog lookups; protocol/browser cases without those limits run in parallel.
The original resource workloads, measurement semantics and numerical limits
remain unchanged. Dynamic-configuration cases always build a fresh real catalog;
fixed-configuration protocol cases deep-copy genuine prepared catalogs/reports.

The original 1,052-case inventory and 50 acceptance selectors remain pinned in
`.github/tests_acceptance.json`, together with the six additional cases on merged
main (1,058 original identities total). Every retired identity maps to explicit
passing owners and a named contract; that retirement map is hash-pinned separately.
`tools/tests_ci.py` rejects missing originals/owners, incomplete execution, duplicate
identities, any skip/failure/error, contradictory aggregates and empty phase
reports. Historical contract coverage and actual executed cases are reported
separately. Daily-archive bundle verification and browser acceptance still run.
The source/dependency/fixture evidence and phase reports are retained as artifacts.
ShellCheck runs in the existing parallel lint job; CI contracts retain their
existing ruleset-job owner.

Same-repository PRs and workflow dispatch use the `origo-tests` self-hosted label.
Fork PRs run the same complete suite and verifier on GitHub with two workers.
Setting repository variable `ORIGO_TESTS_RUNNER=github` provides the complete
hosted route during a server outage; rerun pending work after setting it.
The workflow's manual dispatch also provides a `server`/`github` runner choice
for qualification of either complete execution path.
The required job name remains `pr_checks_tests`. Superseded runs are cancelled;
the current commit must pass. Reports and the 30 slowest test phases are retained
as GitHub artifacts for 14 days.
After Python setup, the workflow registers its shared-library directory with
the guest's dynamic loader. Deployment checks deliberately launch Python with
a cleared environment; they must work without inheriting `LD_LIBRARY_PATH`.

## Runner

The runner lives in the KVM domain `origo-tests` on `37.27.112.167`: 12 vCPUs,
24 GiB RAM, a 100 GiB private disk and Ubuntu 24.04. The host controller owns boot
and allows one instance. It has no host mounts, host Docker socket, production volumes or production
credentials. `deploy/tests-runner/isolation.xml` blocks guest connections to
private networks, link-local addresses, the host and its public aliases, and
IPv6. DNS, DHCP and replies to host-initiated administrative SSH are allowed.
The Docker daemon and all test containers are inside the guest.
Host-enforced limits cap aggregate QEMU CPU at 12 cores, memory plus swap at
28 GiB, and disk I/O at 100 MiB/s and 2,000 IOPS. One disposable overlay is bounded
by its 100 GiB virtual disk; one clean image is retained. The controller itself
is limited to one core and 256 MiB. Guest administrative access cannot raise
these ceilings.

Provisioning is versioned:

```sh
tools/provision_tests_runner.sh root@37.27.112.167 SSH_PUBLIC_KEY
```

The script requires the existing KVM/libvirt tooling and default NAT network.
It verifies the Ubuntu image checksum and the pinned GitHub runner archive
checksum. It does not recreate an existing VM. Cloud-init prepares Python 3.11,
Docker, Chromium's dependencies and the runner application. GitHub registration
uses a short-lived registration token; no GitHub PAT is installed in the guest.
The runner executes as `runner`, with guest-only administrative access for Docker
and browser dependency installation.

Activate the root-owned controller after provisioning and qualification:

```sh
deploy/tests-runner/activate.sh SERVER SSH_ADMIN_KEY APP_ID INSTALLATION_ID APP_PRIVATE_KEY
```

The repository-only `Vaquum Origo Tests Runner` App needs Administration write
and mandatory Metadata read. Its PEM key lives only at
`/etc/origo-tests-runner/app.pem` (0600, parent 0700). The controller caches an
installation token on the host and gives the guest its runner-registration token.
Registration is ephemeral: each listener accepts one job. A pre-job hook baked
into the clean image admits only Origo's `pr_checks_tests` job, with a
same-repository PR or workflow dispatch event. It rejects fork and
`pull_request_target` events before workflow steps, independently of PR-editable
routing. [GitHub documents the hook and its event payload](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/run-scripts).

After completion, failure, cancellation, listener loss, five minutes offline or
the 70-minute active-job deadline, the host destroys the guest and rebuilds its
overlay from the clean image. GitHub's job timeout is 60 minutes. Idle waiting
does not consume the active-job deadline. Cleanup runs outside the guest;
test code cannot preserve processes or filesystem state into the next job.
Stop the controller before refreshing the clean image through the versioned
activation script. Prepare the maintenance guest from the trusted clean image;
never promote a job's modified disk into it. Image conversion writes a separate
file and replaces the old clean image only after conversion succeeds, including
when the guest disk is an overlay backed by that old image.
The server operator owns image patching and refresh.
GitHub requests retry network timeouts and HTTP 5xx responses three times with
two- and four-second delays. Each failure is logged; permanent errors and
exhausted retries fail the controller. Installation-token cache writes close a
root-only temporary file before atomically replacing the cache, so interruption
cannot truncate the previous token.

## Regression evidence

| Successful GitHub run | Job elapsed | Full runtime suite |
| --- | ---: | ---: |
| [September 23](https://github.com/Vaquum/Origo/actions/runs/35856977062) | 11m29s | 580 passed in 8m58s |
| [September 28](https://github.com/Vaquum/Origo/actions/runs/36419523833) | about 25m | expanded monitoring, capture and cube coverage |
| [October 5](https://github.com/Vaquum/Origo/actions/runs/37313963929) | 55m56s | 1052 passed in 46m42s |

The October 5 job also spent approximately 7m45s executing acceptance subsets
which the full suite executes again. Dependency installation took 43s and browser
installation 24s. Most of the growth is test execution, not environment setup.
The largest observed recent step change was from 38m to 51m between revisions
`98b98df0f82b` and `642b600c2708`; this identifies a regression boundary, not a
proven cause. The latter changed vendor replay scratch placement and its tests.

A 30-second py-spy sample during the server baseline's book-law cases recorded
1,334 samples without profiler errors. Hourly replay appeared in 59.1% of sampled
stacks; gzip compression accounted for 23.9% of leaf samples, spool row decoding
16.2%, and ClickHouse tuple serialization 14.8%. These percentages describe that
sampled interval, not the complete suite. An earlier monitoring resource case
was CPU-bound while replaying approximately 2.5 million gate envelopes. Those
workloads and their assertions are retained.

The server baseline records every test phase with `--durations=0`, every outcome
in JUnit, the source SHA and installed dependencies. Compare that serial baseline
with the parallel run on the same guest and dependency environment before
claiming the measured speedup. Keep production-size workload and fault checks;
do not shrink recordings, sample rows, relax deadlines or skip unavailable data.

The pristine October 7 server baseline passed all 1,052 cases in 3,045.610 seconds
(50m46s). Python 3.11.17, pytest 9.1.1 and Polars 1.44.2 were held constant for
the paired run at SHA `485521924983e3d4fd7f7b7bf3985bc44a26edb3`.
The host ceilings were installed partway through the serial run; measured disk
use before installation was below their limits. The parallel run uses them
throughout. Later additions on main must pass in the actual migrated CI job.
The ordered six-worker run passed all 1,052 cases in 1,119.88 seconds (18m40s),
about 2.72 times faster. This is pytest elapsed time; actual GitHub job elapsed
time and assignment are verified separately. No original case, assertion,
workload size or timing/memory threshold was removed.
The first actual Actions job at `14010f3` ran all 1,058 current cases in
1,160.84 seconds: 1,057 passed and the nested deployment-environment check failed.
Its Actions-provided Python binary depended on `LD_LIBRARY_PATH`, which that
check clears. This job is retained as failed qualification; its runtime result
does not replace the passing paired benchmark or qualify the migration.

| Longest baseline file | Cases | Summed test phases |
| --- | ---: | ---: |
| `test_book_law.py` | 34 | 624.45s |
| `test_law_page.py` | 58 | 331.64s |
| `test_book_history.py` | 5 | 235.53s |
| `test_book_vendor.py` | 19 | 190.62s |
| `test_market_state_query.py` | 100 | 146.48s |

The first parallel attempt passed 589 cases before a paginated-history assertion
failed: all events were present but one catalog definition was absent. The
unchanged lookup deadline is 50ms; the same case passed in isolation. This is
evidence of scheduling sensitivity, not proof of a production-code defect.
The migration's exclusive module schedule preserved serial conditions for those checks.
No production-code path or assertion was edited.

The next run passed 1,050 cases before the notification resource case reported
2,006 MiB against its unchanged 1,024 MiB limit. That probe uses Linux
`ru_maxrss`, which also includes pre-exec inherited residency, unlike the
page probe's `/proc/self/status` measurement. The migration ran resource modules first to preserve their original serial ordering and prevents prior vendor-session
inputs from contaminating the subprocess's peak. Qualification must still pass
the original measurement and threshold; no measurement code is replaced.
The isolated diagnostic passed in 61.00s and measured 261.37 MiB, with the same
1,440 protocol frames and 711 genuine R1 frames; its reducer total was 0.849s
versus 0.886s in the failed reused worker. This distinguishes peak inheritance
from extra work in the replay itself.

Keep JUnit, complete collection, dependency freeze, source/fixture hashes and
per-file timing outside the disposable guest. The earlier migration compared complete passing reports with its original verifier and inventory:

```sh
python tools/tests_ci.py PARALLEL.xml COLLECTION.txt --baseline BASELINE.xml
```

It requires at least a twofold reduction in pytest wall time; it runs once during
qualification. CI retains actual job timing and assigned runner identity
separately, including checkout and dependency preparation.

## Consolidation and future additions

Each test owns an observable contract and names the failure it detects. Extend that owner for a new input, state transition or boundary; do not copy a suite for a source whose differences fit explicit parameters.

| Family | Original work | Retained work |
| --- | --- | --- |
| Daily source backfills | Four copied suites, 2,837 lines; repeated day preparation for publication, retry, refresh and direct render | One parameterized owner plus spot-only orchestration, 1,104 lines. All four genuine archives, unavailable days, backfill generation ownership and post-cutoff products remain. |
| Book reader laws | Six separate full-hour builds per market for readiness, provider delay, proofs, scheduling, migration and reconciliation | One full-hour build per market, with provider availability/deadline stages before it and the same observations and controlled proof/anchor faults after it. Provisional overlap, starvation and closing-input revision upgrades retain their distinct scenarios. |
| Provisional book input | Replay the genuine hour again only to prepare an overlapping minute | Copy the already verified immutable sealed grid into private test storage. Invalid tail replay remains independent. |
| Dashboard protocol | Rebuild a real minute and evaluator output for each protocol case | One genuine evaluator output per worker session, deep-copied per case; fixed-configuration catalog preparation is also shared. Database-damage scenarios still build and damage their own source state. |
| Dashboard scale | Three copies of the same 43,201-sample / 2,505,600-event replay | One fresh-process production-shape replay retains exact index bytes, RSS <256MiB, current read <1s and no limiting. The distinct 1,440-frame monitor replay and its budgets remain. |
| CI contract checks | Run the same contracts in the runtime and ruleset jobs; nested pytest in dashboard acceptance | Existing ruleset job owns those contracts; lint owns ShellCheck; runtime URL assertions stay in the runtime suite. |

`.github/tests_acceptance.json` retains the original 1,052-case inventory and 50 acceptance selectors. Each retired identity explicitly names its retained owner and contract. `tools/tests_ci.py` rejects an absent owner, an unmapped original, an incomplete collection, any skip/failure/error and duplicate outcomes. It reports actual passing case counts separately from historical contract coverage.

CI first runs ordinary tests with eight workers. Genuine book inputs are prepared once per market for all workers; independent scenarios run concurrently. Dedicated diagnostic-server cases retain their shared-server sequence. Marked wall-clock/RSS and deadline-sensitive cases then run in a fresh serial process. Placement labels do not change test identities. The two JUnit reports are merged and checked against the complete collection.

The merged runner migration baseline is 1,058 passing cases in 1,089.67 seconds (18m09s), with a complete job of 23m02s: [run 37673698503](https://github.com/Vaquum/Origo/actions/runs/37673698503). The dashboard module's first case includes 747 seconds waiting for the former module-wide resource lock; its body does not take that long.

The production host limits remain 12 vCPUs, 24GiB guest RAM, a 100GiB guest disk, 28GiB host memory/swap maximum, 100MiB/s and 2,000 IOPS. Qualification uses the same Python/dependency versions and genuine fixture tree as that baseline. The first consolidation passed 1,035 cases in 874.24 seconds (14m34s: ordinary 623.41s + resource 250.83s). RAM-backed temporary files and cost ordering reduced the same ordinary phase to 447.11s (7m27s). Sharing genuine book preparation and scheduling its independent scenarios passed all 46 book cases in 184.46s (3m04s).

The final ordinary phase passed 1,024 cases in 311.77s and the fresh serial resource phase passed nine in 102.93s. Their complete verified union passed 1,033 cases in **414.69s (6m55s), 2.63 times faster** than the merged baseline. These are summed pytest session wall times, not summed overlapping case durations or a measured Actions job. The resource phase was rerun after a configuration-case correction; its ordinary cases were unchanged. Current-head Actions must qualify the complete workflow in one job.

The pinned inventory protects all 1,058 pre-consolidation cases: 64 retired identities map to 39 parameterized/consolidated owners. Native Python test/support footprint is 36,019 → 34,369 lines (4.6% reduction); the backfill family alone is 2,837 → 1,104 (61.1%). A smaller case count alone is not evidence of preserved coverage.

Compare the real inventories separately during qualification:

```sh
python tools/tests_ci.py CURRENT.xml CURRENT-COLLECTION.txt \
  --baseline MERGED-BASELINE.xml --baseline-collection MERGED-COLLECTION.txt
```

The verifier requires both reports to pass their actual collections and all original contracts before accepting the twofold speedup. The retirement map and both original inventory sets have independent hash pins in the existing ruleset contract suite. Merge qualification also rejects a missing, truncated or empty phase, a cross-phase duplicate and a failed phase.

Temporary external pytest plugins qualify representative failure classes against original and consolidated owners. Making a missing native book component proof pass must fail both the original exact-proof case and the retained canary owner at their UNKNOWN assertion. Blocking an otherwise valid retry after interrupted publication must fail both original and retained source owners at their retry-success assertion. The plugins never enter the committed tree; genuine archives, original source rows and production code remain unchanged.

Qualification also retained three genuine failures caused by caching catalogs in tests that intentionally change configuration; those cases now call the real catalog builder. A separate unchanged monitor replay exceeded its one-second limit by 1ms. Three diagnostic replays measured 0.70–0.72s, and the subsequent complete resource phase passed. That variance remains visible in the saved evidence; no retry, cached measurement or relaxed limit is added to CI.

Book workloads still cost more than trade adapter protocols because their distinct boundaries require genuine depth reconstruction. The retained full-hour cases separately check normal vendor closure, legacy snapshot exchange-clock ordering, continued depth outside the initial seed, truncated-tail rejection, provisional replacement and prior closing-input revisions. Shared preparation removes their repeated input download and baseline replay; independent boundaries remain concurrent. A further collapse must name the fault it still rejects rather than infer equivalence from similar setup.
