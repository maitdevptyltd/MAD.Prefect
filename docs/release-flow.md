# Release Flow

## Summary
- `next` remains the prerelease branch. Pushes there can bump `rc` versions, update `CHANGELOG.md`, create prerelease tags/releases, and publish to PyPI.
- `main` remains protected, but stable releases are finalized directly on `main` by Commitizen after changes have already landed there.
- Humans should continue to reach `main` via pull request. The release bot is the only actor that should bypass the branch rules to push the final bump commit and tag created by Commitizen.

## Usage
- Prerelease automation still happens on a normal push to `next`.
- Merge ready changes into `main`.
- The `publish` workflow on `main` will let Commitizen create the stable bump commit, tag, changelog update, and release before the package is built and published to PyPI.

## Operational Notes
- Stable release automation on `main` follows Commitizen's standard GitHub Actions model: checkout with a bot token, run `commitizen-action`, push the bump commit and tag, then create a GitHub release from the generated version.
- `main` should stay protected with a ruleset or branch protection rule that blocks force pushes and deletions, requires pull requests for humans, and allows only the release bot to bypass those requirements for the automated bump commit.
- The workflow should use a dedicated bot token such as `RELEASE_PAT` for checkout and Commitizen. This is required both to push back to a protected branch and to allow tags created by the automation to participate in downstream GitHub automation correctly.
- `CHANGELOG.md` remains the source of truth for published release notes.
- The repository is moving to Commitizen `4.x` so the stable bump path stays aligned with current upstream behavior.

## Progress
- [x] `publish.yml` continues to manage prerelease bumps on `next`.
- [x] `publish.yml` now finalizes stable releases on `main` using a protected-branch bypass token instead of a release-prep PR workflow.
- [x] Release-flow documentation now targets a protected-`main`, bot-bypass workflow aligned with Commitizen's standard GitHub Actions guidance.
- [x] Release-flow documentation added for developers operating the branch strategy.

## Next Steps
- Configure a protected-branch ruleset for `main` that grants bypass only to the release bot.
- Add `RELEASE_PAT` as an Actions secret and verify the bot has permission to push commits and tags to `main`.
- Re-run the `publish` workflow through a real merge to `main` and confirm that the stable changelog includes the prerelease history after the Commitizen `4.x` upgrade.
- Consider adding `actionlint` to CI if workflow validation needs to become part of the automated test suite.

## Blockers & Risks
- The stable-release path depends on repository settings that cannot be enforced from this repo alone: branch ruleset configuration, bypass actors, and the `RELEASE_PAT` secret.
- No repository unit tests exercise GitHub Actions YAML directly, so workflow correctness is validated by review, local dry runs, and the first live run.
