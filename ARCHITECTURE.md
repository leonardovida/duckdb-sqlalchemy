# Architecture

## Overview

`duckdb_sqlalchemy` is a small SQLAlchemy dialect package with a few clear
boundaries:

- `duckdb_sqlalchemy/__init__.py` contains the dialect, the DBAPI shim, the
  connection and cursor wrappers (shared-result buffering, DML rowcount), the
  statement and DDL compilers, the execution context, and reflection.
- `duckdb_sqlalchemy/motherduck.py` handles MotherDuck URL shaping, engine
  construction, token detection, and path-vs-config query partitioning.
- `duckdb_sqlalchemy/url.py` builds plain DuckDB URLs (`URL`, deprecated
  `make_url`).
- `duckdb_sqlalchemy/_pool.py` picks the default pool class and applies `pool=`
  overrides.
- `duckdb_sqlalchemy/config.py` validates and renders DuckDB config settings.
- `duckdb_sqlalchemy/datatypes.py` implements custom DuckDB type support and
  SQL compilation helpers.
- `duckdb_sqlalchemy/bulk.py` provides COPY helpers for files and row streams.
- `duckdb_sqlalchemy/olap.py` wraps DuckDB table functions (`read_parquet`,
  `read_csv`, `pragma_storage_info`, `quack_query`) and the MotherDuck `md_*`
  and `prompt_jev` SQL functions.
- Private helpers: `_statements.py` (idempotent-statement, transient-error, and
  disconnect detection for retries), `_bulk_insert.py` and `_row_shape.py`
  (Arrow/pandas data for the register bulk-insert path), `_arrow.py`
  (`duckdb_arrow` results), `_checkpoint.py` (`checkpoint()`), `_query.py` (URL
  query coercion), `_validation.py` (identifier checks), and `capabilities.py`
  / `_supports.py` (DuckDB version feature flags).

## Invariants

- Keep URL query handling explicit: MotherDuck routing parameters stay in
  `URL.query` so URLs round-trip, and the dialect moves them into the `md:`
  database string at connect time; DuckDB config parameters are applied as
  settings. Local files and `:memory:` never get routing parameters.
- Reflect from DuckDB's catalog functions (`duckdb_tables()`,
  `duckdb_constraints()`, ...), not the `pg_catalog` emulation, and resolve
  each unqualified name to one database and schema the way DuckDB binds it.
- Every cursor shares one DuckDB connection and its single open result, so
  buffer an unread result before another statement replaces it.
- Validate identifiers before rendering SQL fragments.
- Preserve compatibility with the supported SQLAlchemy 2.0 and 2.1 lines
  without breaking the DuckDB releases tested in `noxfile.py`.
- Prefer small wrappers and targeted helpers over broad abstractions.

## Operational Notes

- The test suite is the source of truth for compatibility behavior.
- Changes that affect version support, pooling defaults, or type rendering
  should update both tests and release notes.
