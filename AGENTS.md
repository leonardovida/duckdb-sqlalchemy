# Repository Guidelines

## Project Structure & Module Organization
- `duckdb_sqlalchemy/` contains the dialect implementation and helpers (for example `datatypes.py`, `url.py`, `olap.py`, `config.py`).
- `duckdb_sqlalchemy/tests/` holds pytest suites. End-to-end tests against real databases live in `duckdb_sqlalchemy/tests/e2e/`.
- `examples/` provides runnable examples such as `examples/sqlalchemy_example.py`.
- Tooling and metadata are in `pyproject.toml`, `noxfile.py`, and `.pre-commit-config.yaml`.

## Build, Test, and Development Commands
- `pip install -e ".[dev]"` installs dev dependencies for local runs.
- `pip install -e ".[dev,devtools]"` also installs `pre-commit`.
- `nox -s tests` runs the full test matrix (multiple Python, DuckDB, and SQLAlchemy versions); expect this to be slow.
- `pytest` runs tests in the current environment (useful for quick checks).
- `nox -s ty` runs type checking with `ty`.
- `pre-commit run --all-files` applies formatting/lint checks via the configured hooks.

## Coding Style & Naming Conventions
- Python code uses 4-space indentation and standard naming: `snake_case` for functions/variables and `CamelCase` for classes.
- Linting and formatting are enforced with Ruff (`ruff` + `ruff-format`) via pre-commit.
- Type hints are expected in new code; `ty` is the canonical checker.

## Testing Guidelines
- Tests use `pytest` with plugins including `pytest-cov` and `pytest-remotedata`.
- Name tests `test_*.py` and keep them under `duckdb_sqlalchemy/tests/`.
- Prefer end-to-end tests in `duckdb_sqlalchemy/tests/e2e/`: real engines on real
  files, real SQL round trips, asserting stored rows and reflected schema. Use the
  `make_engine` and `db_path` fixtures there, which dispose engines for you.
- Add a unit test only for pure logic with many edge cases, MotherDuck paths that
  need credentials, or fault injection that a real engine cannot reproduce. Do not
  add tests that assert nothing, test the `duckdb` package alone, or pin mock calls
  or compiled SQL text when the statement can be executed instead.

## Release Notes and Breaking Changes
- Release notes live in `CHANGELOG.md` and the GitHub releases. `docs/releasing.md`
  describes the release process.
- `docs/breaking-changes.md` lists every breaking or behavior change by version,
  with who is affected and what to do. Read its "Unreleased" section and the
  entries for the area you are changing before you start.
- Any change users can notice (generated SQL, reflection output, defaults, return
  values, errors, supported versions) must add an entry under "Unreleased" in
  `docs/breaking-changes.md` and in `CHANGELOG.md` in the same PR.

## Commit & Pull Request Guidelines
- Recent history favors short, imperative subjects and occasionally uses Conventional Commit prefixes (for example `chore:`, `build:`).
- PRs should include a clear summary, test results, and linked issues when applicable.
- For user-facing changes, consider updating `CHANGELOG.md`.

## Security & Configuration Tips
- Never commit secrets. Use environment variables for MotherDuck tokens (`MOTHERDUCK_TOKEN` or `motherduck_token`).

# Personal guidelines

- Use `pre-commit` to run checks before committing.
- Always create a new branch for each feature or bug fix. After you completed the work, create a pull request with a concise title and description of the changes. Then wait for tests to pass and finally merge the changes into the main branch.
- Do not use semicolons in release titles or release notes.
