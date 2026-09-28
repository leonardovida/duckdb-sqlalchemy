# CLAUDE.md

Guidance for Claude Code in this repository. `AGENTS.md` is the canonical source
for commands, style, testing, and PR rules; read it first. This file only adds
architecture notes that are useful when changing the dialect.

## Project Overview

SQLAlchemy dialect for DuckDB and MotherDuck. Provides SQLAlchemy Core + ORM
support with MotherDuck-specific helpers for connection URLs, pooling, and read
scaling.

## Architecture

The dialect implementation lives in `duckdb_sqlalchemy/__init__.py` and extends
PostgreSQL's dialect (`PGDialect_psycopg2`) since DuckDB's SQL is
PostgreSQL-compatible. See `ARCHITECTURE.md` for the module map and invariants.

- **Dialect** (`__init__.py`): connection handling, reflection, and the DuckDB execution context
- **ConnectionWrapper/CursorWrapper**: DBAPI wrappers around `duckdb.DuckDBPyConnection` with special handling for `register`, `commit`, and transaction isolation queries
- **url.py / motherduck.py**: URL builders (`URL`, `MotherDuckURL`) and helpers for token handling, attach modes, and read scaling
- **_pool.py**: default pool selection and `pool=` overrides
- **datatypes.py**: DuckDB type mappings (`ISCHEMA_NAMES`)
- **olap.py**: table functions (`read_parquet`, `read_csv`, ...) and MotherDuck `md_*` helpers
- **config.py**: DuckDB configuration handling and extension loading
- **bulk.py**: COPY helpers (`copy_from_parquet`, `copy_from_csv`, `copy_from_rows`, `copy_to_parquet`)

## Key Implementation Details

- The DBAPI paramstyle is `numeric_dollar`.
- Pool defaults: `SingletonThreadPool` for `:memory:` and `duckdb://`,
  `QueuePool` for files and named `:memory:name` databases, `NullPool` for
  MotherDuck (`md:`/`motherduck:`).
- Bulk inserts over `duckdb_copy_threshold` (default 10k rows) register an Arrow table (pandas fallback) and run `INSERT ... SELECT`.
- MotherDuck tokens are read from `MOTHERDUCK_TOKEN` (or `motherduck_token`) for MotherDuck databases.

## Supported Versions

`noxfile.py` is the source of truth. It tests Python 3.10-3.14, SQLAlchemy
2.0.0 / 2.0.52 / 2.1.1, and DuckDB 1.3.0-1.5.x. Hosted CI runs the latest
SQLAlchemy and DuckDB on each Python version, SQLAlchemy 2.0 on Python 3.13 and
3.14, the minimum versions, and informational prerelease jobs.
