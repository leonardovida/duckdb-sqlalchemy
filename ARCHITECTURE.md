# Architecture

## Overview

`duckdb_sqlalchemy` is a small SQLAlchemy dialect package with a few clear
boundaries:

- `duckdb_sqlalchemy/__init__.py` contains the dialect, DBAPI shim, connection
  wrappers, and reflection helpers.
- `duckdb_sqlalchemy/motherduck.py` handles MotherDuck URL shaping, engine
  construction, and path-vs-config query partitioning.
- `duckdb_sqlalchemy/config.py` validates and renders DuckDB config settings.
- `duckdb_sqlalchemy/datatypes.py` implements custom DuckDB type support and
  SQL compilation helpers.
- `duckdb_sqlalchemy/bulk.py` provides COPY helpers for files and row streams.
- `duckdb_sqlalchemy/olap.py` wraps table functions for file scans.

## Invariants

- Keep URL query handling explicit: MotherDuck routing parameters must stay in
  the database string, while DuckDB config parameters remain in `query`.
- Validate identifiers before rendering SQL fragments.
- Preserve SQLAlchemy compatibility across the supported 1.3, 1.4, and 2.x
  lines without breaking older supported DuckDB releases.
- Prefer small wrappers and targeted helpers over broad abstractions.

## Operational Notes

- The test suite is the source of truth for compatibility behavior.
- Changes that affect version support, pooling defaults, or type rendering
  should update both tests and release notes.
