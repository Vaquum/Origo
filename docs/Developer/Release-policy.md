# Release policy

Prepare releases deliberately; ordinary PRs do not bump `project.version` or edit `CHANGELOG.md`.

1. Choose a new semantic version in `pyproject.toml` and add the matching first `# vX.Y.Z` section to `CHANGELOG.md`, summarizing changes since the previous release.
2. Review deployment pins in `[tool.origo.release]`. `native_jobs_only_version` intentionally preserves live collector/worker containers while it equals the project version. A new version expires that pin and selects the existing full rollout unless the release deliberately repins it. Verify the intended deployment before merging release preparation.
3. Merge the release preparation through the normal required checks.
4. Fetch `origin/main` and verify that the release preparation is merged. Read its project version and matching first changelog section. Create the GitHub release at that exact merged commit using the commands below; an existing tag is an error.

```sh
git fetch origin main
RELEASE_COMMIT=$(git rev-parse origin/main)
RELEASE_VERSION=$(git show "$RELEASE_COMMIT:pyproject.toml" | python3 -c 'import sys,tomllib; print(tomllib.loads(sys.stdin.read())["project"]["version"])')
gh api "repos/Vaquum/Origo/git/ref/tags/v$RELEASE_VERSION" # Must return 404; otherwise stop.
# Put the matching CHANGELOG section into release-notes.md and review it.
gh release create "v$RELEASE_VERSION" --repo Vaquum/Origo --target "$RELEASE_COMMIT" --title "v$RELEASE_VERSION" --notes-file release-notes.md
```

Deployments continue to follow merges to `main`; creating a GitHub release does not redeploy or publish a Python package. Never reuse a release version or move a published release tag.
