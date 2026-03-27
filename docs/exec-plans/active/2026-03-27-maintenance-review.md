Goal

Review the repository for low-risk bugs, maintenance issues, and safe dependency
updates, then validate the changes locally before opening a PR and driving CI to
green.

Scope

- Keep runtime behavior backward compatible.
- Prefer targeted maintenance fixes over broad refactors.
- Update only dependencies that can be validated without breaking tests.

Plan

1. Inspect the current code, test suite, and dependency constraints for likely
   issues and upgrade candidates.
2. Implement backward-compatible maintenance fixes and any safe deprecations.
3. Update eligible dependencies and refresh the lockfile.
4. Run the fast verification loop locally, then open a PR and iterate on CI.

Progress

- Read automation memory, repo docs, dependency constraints, and recent git
  history.
- Ran a baseline `uv run pytest -q` to establish the current state.
- Identified three low-risk improvements:
  - warn on deprecated `motherduck_dbinstance_inactivity_ttl` input while
    preserving support
  - remove SQLAlchemy 2.0-deprecated test usage so upgrades stay clean
  - update stale dev-tool dependencies if the suite stays green

Surprises & Discoveries

- The worktree started detached from `HEAD`, so a maintenance branch was created
  before edits.
- The baseline test suite already passes, but it emits SQLAlchemy 2.0 deprecation
  warnings from test code.

Decision Log

- Deprecate the long MotherDuck TTL alias with warnings instead of removing it.
- Treat dev-tool updates as acceptable if they require no code changes beyond
  version bounds and keep validation green.

Validation

- Baseline: `uv run pytest -q` passed before edits.
- Planned after edits: `uv run pytest -q`, `uv run ty check duckdb_sqlalchemy/`,
  and `uv run pre-commit run --all-files`.

Outcomes & Retrospective

- In progress.
