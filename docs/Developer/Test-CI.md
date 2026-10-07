# Runtime test CI

`pr_checks_tests` runs the complete `tests/origo_source_native` suite once. Six
pytest-xdist workers on the server runner share files within one worker
(`--dist loadfile --no-loadscope-reorder`). Each worker's existing session fixture starts its own
ClickHouse container, credentials and loopback ports; each test still drops its
database before and after execution. No production database is used.
Docker allocates loopback ports atomically. Image builds send only the two
versioned ClickHouse configuration files, rather than copying the fixture corpus
into each worker's build context. The pinned deployment image and configuration
remain identical.
Law-page and notification-resource modules acquire exclusive process locks;
other modules acquire shared locks. Their unchanged timing and memory assertions
run without another test workload competing for the guest. Locks are scoped to
xdist's run UID. Resource modules are collected first, retaining the original
serial ordering before workers accumulate large session-scoped vendor inputs.

The acceptance selectors and parameter counts previously listed in the workflow
are preserved in `.github/tests_acceptance.json`. `tools/tests_ci.py` verifies
their actual outcomes in the complete run's JUnit report. All 1,052 baseline node
IDs are also pinned: execution must equal current collection and include every
baseline node. Missing selectors,
incorrect parameter counts, duplicate results, skips, errors and failures fail
the job. Assertions, real inputs, browser acceptance and performance thresholds
are unchanged. Daily-archive bundle verification still runs after the suite.

Same-repository PRs and workflow dispatch use the `origo-tests` self-hosted label.
Fork PRs run the same complete suite and verifier on GitHub with two workers.
Setting repository variable `ORIGO_TESTS_RUNNER=github` provides the complete
hosted route during a server outage; rerun pending work after setting it.
The workflow's manual dispatch also provides a `server`/`github` runner choice
for qualification of either complete execution path.
The required job name remains `pr_checks_tests`. Superseded runs are cancelled;
the current commit must pass. Reports and the 30 slowest test phases are retained
as GitHub artifacts for 14 days.

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
activation script. The server operator owns image patching and refresh.

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
The exclusive module schedule preserves serial conditions for those checks.
No production-code path or assertion was edited.

The next run passed 1,050 cases before the notification resource case reported
2,006 MiB against its unchanged 1,024 MiB limit. That probe uses Linux
`ru_maxrss`, which also includes pre-exec inherited residency, unlike the
page probe's `/proc/self/status` measurement. Running resource modules first
preserves their original serial ordering and prevents prior vendor-session
inputs from contaminating the subprocess's peak. Qualification must still pass
the original measurement and threshold; no measurement code is replaced.
The isolated diagnostic passed in 61.00s and measured 261.37 MiB, with the same
1,440 protocol frames and 711 genuine R1 frames; its reducer total was 0.849s
versus 0.886s in the failed reused worker. This distinguishes peak inheritance
from extra work in the replay itself.

Keep JUnit, complete collection, dependency freeze, source/fixture hashes and
per-file timing outside the disposable guest. Qualification compares complete
passing reports with:

```sh
python tools/tests_ci.py PARALLEL.xml COLLECTION.txt --baseline BASELINE.xml
```

It requires at least a twofold reduction in pytest wall time; it runs once during
qualification. CI retains actual job timing and assigned runner identity
separately, including checkout and dependency preparation.
