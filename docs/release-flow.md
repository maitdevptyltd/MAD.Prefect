# Release Flow

## Summary
- `next` remains the prerelease branch. Pushes there can bump `rc` versions, update `CHANGELOG.md`, create prerelease tags/releases, and publish to PyPI.
- `main` remains PR-only. Pushes there read the already-committed stable version and changelog, create the stable tag/release, and publish to PyPI without pushing commits back to the branch.
- Stable releases are prepared by `.github/workflows/prepare-stable-release.yml`, which opens or updates a PR from a generated release branch into `main`.

## Usage
- Prerelease automation still happens on a normal push to `next`.
- Trigger a stable release prep run from GitHub Actions or with:

```bash
gh workflow run prepare-stable-release.yml --ref next
```

- Review the generated `release/stable-vX.Y.Z` PR into `main`.
- Merge that PR when the stable release is ready. The `publish` workflow on `main` will tag, create the GitHub release, build, and publish from the committed stable version.

## Operational Notes
- Stable release prep derives the final version from the current prerelease, for example `2.3.0rc15 -> 2.3.0`.
- The prep workflow uses `cz bump <stable-version> --changelog --files-only --yes`. In Commitizen `3.31.0`, `--files-only` updates `pyproject.toml` and `CHANGELOG.md` without creating the release commit or tag, and the older `--allow-no-commit` / `--version-files-only` flags are rejected.
- `CHANGELOG.md` remains the source of truth for published release notes.

## Progress
- [x] `publish.yml` keeps prerelease bumping on `next` and removes branch mutation from `main`.
- [x] `prepare-stable-release.yml` prepares a stable version/changelog PR into `main`.
- [x] Stable-release prep updated to match the Commitizen `3.31.0` CLI used by the repo.
- [x] Release-flow documentation added for developers operating the branch strategy.

## Next Steps
- Re-run `prepare-stable-release` on `next` after this fix lands to confirm the PR branch is created cleanly with the current Commitizen CLI.
- Consider adding `actionlint` to CI if workflow validation needs to become part of the automated test suite.

## Blockers & Risks
- Workflow behavior depends on `GITHUB_TOKEN` retaining permission to create tags/releases and to push the generated release-prep branch.
- No repository unit tests exercise GitHub Actions YAML directly, so workflow correctness is validated by review plus action syntax checks and the first live run.
