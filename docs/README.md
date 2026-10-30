# Documentation

This folder contains focused, task-oriented guides that keep the main README short. Start with the section closest to your workflow and follow the links as needed.

## Guides

- [overview.md](overview.md) - High-level overview and quick start
- [getting-started.md](getting-started.md) - Minimal install + setup walkthrough
- [migration-from-duckdb-engine.md](migration-from-duckdb-engine.md) - Migration guide from older dialects
- [connection-urls.md](connection-urls.md) - URL formats, helpers, and manual escaping
- [motherduck.md](motherduck.md) - Connection patterns and MotherDuck-specific options
- [dives.md](dives.md) - Building and deploying MotherDuck Dives from Python
- [configuration.md](configuration.md) - `connect_args`, extension preloads, and filesystem registration
- [olap.md](olap.md) - Table functions (`read_parquet`, `read_csv_auto`) and ATTACH examples
- [pandas-jupyter.md](pandas-jupyter.md) - DataFrame registration and notebook usage
- [types-and-caveats.md](types-and-caveats.md) - Supported types, parameter binding, and gotchas
- [alembic.md](alembic.md) - Alembic integration notes
- [performance.md](performance.md) - Exact values, bulk loading and profiling
- [adoption.md](adoption.md) - Ecosystem adoption and validation
- [breaking-changes.md](breaking-changes.md) - Breaking and behavior changes by version

## Project references

- [CHANGELOG.md](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/CHANGELOG.md) - Release notes

## Examples

The `examples/` directory contains runnable scripts, listed in
[examples/README.md](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/README.md).
Start with
[examples/local_quickstart.py](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/local_quickstart.py)
for DuckDB or
[examples/motherduck_quickstart.py](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_quickstart.py)
for MotherDuck.
