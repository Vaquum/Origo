# Release policy

Prepare releases deliberately; ordinary PRs do not bump `project.version` or edit `CHANGELOG.md`.

1. Choose a new semantic version in `pyproject.toml` and add the matching first `# vX.Y.Z` section to `CHANGELOG.md`, summarizing changes since the previous release.
2. Review deployment pins in `[tool.origo.release]`. `native_jobs_only_version` intentionally preserves live collector/worker containers while it equals the project version. A new version expires that pin and selects the existing full rollout unless the release deliberately repins it. Verify the intended deployment before merging release preparation.
3. Merge the release preparation through the normal required checks and retain that PR number.
4. Set `RELEASE_PREPARATION_PR` to that merged PR number. Fetch `origin/main` only to verify ancestry; derive the release target, version and notes from the preparation PR’s immutable merge commit. Stop on any failed command; an existing tag is an error.

```sh
git fetch origin main
RELEASE_COMMIT=$(gh pr view "$RELEASE_PREPARATION_PR" --repo Vaquum/Origo --json mergeCommit --jq '.mergeCommit.oid // empty')
test -n "$RELEASE_COMMIT"
git merge-base --is-ancestor "$RELEASE_COMMIT" origin/main
RELEASE_VERSION=$(git show "$RELEASE_COMMIT:pyproject.toml" | python3 -c 'import sys,tomllib; print(tomllib.loads(sys.stdin.read())["project"]["version"])')
gh api "repos/Vaquum/Origo/git/ref/tags/v$RELEASE_VERSION" # Must return 404; otherwise stop.
git show "$RELEASE_COMMIT:CHANGELOG.md" > release-changelog.md
# Put its matching version section into release-notes.md and review it.
gh release create "v$RELEASE_VERSION" --repo Vaquum/Origo --target "$RELEASE_COMMIT" --title "v$RELEASE_VERSION" --notes-file release-notes.md
```

Deployments continue to follow merges to `main`; creating a GitHub release does not redeploy or publish a Python package. Never reuse a release version or move a published release tag.
