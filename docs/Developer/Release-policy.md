# Release policy

Prepare releases deliberately; ordinary PRs do not bump `project.version` or edit `CHANGELOG.md`.

1. Choose a new semantic version in `pyproject.toml` and add the matching first `# vX.Y.Z` section to `CHANGELOG.md`, summarizing changes since the previous release.
2. Review deployment pins in `[tool.origo.release]`. `native_jobs_only_version` intentionally preserves live collector/worker containers while it equals the project version. A new version expires that pin and selects the existing full rollout unless the release deliberately repins it. Verify the intended deployment before merging release preparation.
3. Merge the release preparation through the normal required checks and retain that PR number.
4. Open **Actions → Manual release → Run workflow**, select `main` and enter that merged preparation PR number. Review the workflow result and published release. The workflow is manual-only and serialized; ordinary merges never create releases.

The workflow verifies that preparation merged into `main`, pins its immutable merge commit, checks ancestry and reads both the version and matching first changelog section from that commit. Later ordinary merges cannot enter the release. An existing release or a tag at a different commit fails the run. If publication fails after creating its lightweight tag, rerun with the same preparation PR: the unchanged, unpublished tag is verified against that exact commit before publication resumes. Tags are never moved or replaced. No external release service or AI credential is required.

For the equivalent manual dispatch, set `RELEASE_PREPARATION_PR` to that merged PR number:

```sh
gh workflow run pr_post_release.yml --repo Vaquum/Origo --ref main -f preparation_pr="$RELEASE_PREPARATION_PR"
```

Deployments continue to follow merges to `main`; creating a GitHub release does not redeploy or publish a Python package. Never reuse a release version or move a published release tag.
