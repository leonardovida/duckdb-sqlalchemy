# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQLAlchemy dialect for DuckDB and MotherDuck. Provides first-class SQLAlchemy Core + ORM support with MotherDuck-specific helpers for connection URLs, pooling, and read scaling.

## Build and Development Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Install with pre-commit support
pip install -e ".[dev,devtools]"

# Run tests in current environment (quick)
pytest

# Run single test
pytest duckdb_sqlalchemy/tests/test_basic.py::test_name -v

# Run full test matrix (slow - multiple Python/DuckDB/SQLAlchemy versions)
nox -s tests

# Type checking
nox -s ty
# or directly
ty check duckdb_sqlalchemy/

# Lint and format
pre-commit run --all-files
```

## Architecture

The dialect implementation lives in `duckdb_sqlalchemy/__init__.py` and extends PostgreSQL's dialect (`PGDialect_psycopg2`) since DuckDB's SQL is PostgreSQL-compatible. Key components:

- **Dialect** (`__init__.py`): Main SQLAlchemy dialect class with connection handling, reflection, and DuckDB-specific execution context
- **ConnectionWrapper/CursorWrapper**: DBAPI-compliant wrappers around `duckdb.DuckDBPyConnection` with special handling for `register`, `commit`, and transaction isolation queries
- **url.py / motherduck.py**: URL builders (`URL`, `MotherDuckURL`) and helpers for connection string construction, token handling, and attach modes
- **datatypes.py**: Type mappings from DuckDB types to SQLAlchemy types (ISCHEMA_NAMES)
- **olap.py**: Table functions for `read_parquet`, `read_csv`, etc.
- **config.py**: DuckDB configuration handling and extension loading
- **bulk.py**: Bulk insert helpers (`copy_from_parquet`, `copy_from_csv`, `copy_from_rows`)

## Key Implementation Details

- The dialect uses `numeric_dollar` paramstyle for SQLAlchemy 2.x, `qmark` for 1.x
- Pool defaults: `NullPool` for file/MotherDuck, `SingletonThreadPool` for `:memory:`
- Bulk inserts over `duckdb_copy_threshold` (default 10k rows) use DataFrame/Arrow registration
- MotherDuck tokens are read from `MOTHERDUCK_TOKEN` environment variable automatically

## Testing

Tests are in `duckdb_sqlalchemy/tests/`. The nox matrix tests across:
- Python 3.10-3.13
- SQLAlchemy 1.3, 1.4, 2.0.45
- DuckDB 1.1.3 through 1.4.x

Snapshot fixtures are in `duckdb_sqlalchemy/tests/snapshots/`.

## Code Style

- 4-space indentation, `snake_case` functions, `CamelCase` classes
- Type hints expected in new code; `ty` is the type checker
- Ruff handles linting and formatting
