# Repository Guidelines

## Project Structure & Module Organization
- `duckdb_sqlalchemy/` contains the dialect implementation and helpers (for example `datatypes.py`, `url.py`, `olap.py`, `config.py`).
- `duckdb_sqlalchemy/tests/` holds pytest suites; snapshot fixtures live in `duckdb_sqlalchemy/tests/snapshots/`.
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
- Tests use `pytest` with plugins including `pytest-cov`, `pytest-remotedata`, and `pytest-snapshot`.
- Name tests `test_*.py` and keep them under `duckdb_sqlalchemy/tests/`.
- Snapshot assertions are stored under `duckdb_sqlalchemy/tests/snapshots/` and should be updated intentionally.

## Commit & Pull Request Guidelines
- Recent history favors short, imperative subjects and occasionally uses Conventional Commit prefixes (for example `chore:`, `build:`).
- PRs should include a clear summary, test results, and linked issues when applicable.
- For user-facing changes, consider updating `CHANGELOG.md`.

## Security & Configuration Tips
- Never commit secrets. Use environment variables for MotherDuck tokens (`MOTHERDUCK_TOKEN` or `motherduck_token`).

# Personal guidelines

- Use `pre-commit` to run checks before committing.
- Always create a new branch for each feature or bug fix. After you completed the work, create a pull request with a concise title and description of the changes. Then wait for tests to pass and finally merge the changes into the main branch.
