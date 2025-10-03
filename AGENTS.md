# MAD.Prefect Agent Guide

## Purpose

This guide explains responsibilities, handoff expectations, and how to record decisions while contributing to MAD.Prefect

## Agent Responsibilities

- Maintain developer-facing documentation in `/docs`.
  - Lead with front-facing code examples, showing how a developer is expected to use the feature.
  - Keep runnable examples and operational notes current.
- Capture assumptions, open questions, and blockers in documentation or inline code comments for the next developer.
- Ensure all tests are passing before handing off work.
- For every feature implemented, add or update focused unit tests that conclusively demonstrate the feature’s behavior. Tests should isolate the unit under change, avoid exercising external frameworks or third-party libraries, and stay scoped to the code you are responsible for. The corresponding documentation must be updated alongside the code so feature descriptions, examples, and operational guidance remain accurate.
- Treat unit tests as a first-class deliverable: when you add a module or behavior, create/extend the relevant tests in the same change so edge cases (happy path, failure modes, dry runs, etc.) are captured automatically.
- Drive the codebase toward full type safety: resolve Pylance (basic mode) diagnostics in the files you touch and note any pre-existing violations that could not be cleaned up.
- Prefer modelling request/response payloads and configuration data with Pydantic models (instead of raw dictionaries) so we benefit from validation, auto-complete, and richer static analysis.
- When validation or normalisation is required, add Pydantic field validators or custom types instead of rolling ad-hoc parsing logic inside engines/utilities. Keep business rules close to the model so downstream code stays lean.
- Push business logic into reusable engine modules or libraries; keep CLIs and thin wrappers focused on orchestration and argument parsing. Shared behaviours should live alongside the engine code so future agents inherit the patterns automatically.

## Workflow

1. Review existing documentation in `/docs` before making changes.
2. Before planning new feature work, draft or update the corresponding doc with the intended developer-facing API, examples, and summary of expected behavior.
3. Maintain **Progress**, **Next Steps**, and **Blockers & Risks** sections in the doc so the latest status is visible.
4. Use mock data and clear `TODO` comments when integrations are deferred.

We are driving toward broad unit test coverage—use judgment like a senior engineer:

- Aim for near-total coverage on new logic; only skip tests when there is a compelling reason.
- Design APIs and functions so they are straightforward to exercise in isolation.
- Keep tests fast, focused, and free of external dependencies by using fakes/mocks where needed.

## Testing & Tooling

- Use Poetry for environment management:

  ```bash
  poetry install
  poetry run pytest
  ```

* Manage dependencies with Poetry (`poetry add`, `poetry remove`) instead of editing `pyproject.toml` directly.
* Prefer well-supported libraries for common tasks (e.g. `python-dotenv` for `.env` loading) rather than rolling bespoke helpers.
* Keep `pytest.ini` in sync if the package layout changes.
* Run the full test suite and ensure all tests pass before handing off. When you touch features, maintain or extend the relevant unit tests and documentation so they accurately reflect the current behavior.
* After tests pass, execute Pylance in basic mode over each modified file (e.g. `poetry run pyright <paths>` note that `pyrightconfig.json` comes configured with setting `typeCheckingMode` to `basic`) and share any unavoidable diagnostics in the handoff notes.

## Handoff Checklist

- Document open tasks in **Next Steps** within `/docs`.
- Mention pending approvals or environment constraints.
- Highlight any tests or commands that should be rerun.
- Run type checking (Pylance in basic mode) and fix issues in files you touched.
- Run Pylance in basic mode for every file you modified before handover. If an existing file already fails validation, call out the pre-existing diagnostics in the docs and do not regress them further.
- Confirm every code change has matching unit tests (aligned with the responsibilities above) before handoff.
- Propose and include a Conventional Commit-style message summarizing the changes; every handoff must ship with the ready-to-use commit text.

## Code Style Preferences

- Favor simple, composable design with clear extension points.
- Add docstrings or concise comments when behavior is not obvious.
- Keep comments focused on why a decision was made, not what the code already shows.

## Communication Norms

- Prefer concise, actionable notes in docs rather than long chat transcripts.
- Flag blockers early in documentation so they can be addressed quickly.
- Record reasoning for deviations from the plan in `/docs` before wrapping up.
