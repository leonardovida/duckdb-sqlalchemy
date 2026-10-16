---
layout: default
title: Breaking and behavior changes
---

# Breaking and behavior changes

This file lists every breaking or behavior-changing change shipped in `duckdb-sqlalchemy`, newest first, with who is affected and what to do. It is a focused index, not the full release notes. See [CHANGELOG.md](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/CHANGELOG.md) and the [GitHub releases](https://github.com/leonardovida/duckdb-sqlalchemy/releases) for everything else. Any pull request that changes behavior users can notice must add an entry under "Unreleased" in this file in the same pull request.

## How to use this file

- Search by version (for example `1.5.5.11`) or by API, option or URL key (for example `regexp_match`, `rowcount`, `session_name`, `QueuePool`) to find when and how it changed.
- Before changing code in an area listed here, read the "Unreleased" section and the newest entries for that area so you do not undo or contradict a recent change.
- Each entry says what changed, who is affected and what to do, and links to the pull request, commit or compare view that made the change.
- When upgrading across several versions, read every section between your current version and the target version, oldest first.
- Deprecated APIs that still work are summarized in the table at the end, with their replacements.

## Unreleased

- None yet.

## [1.5.6.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.6.2...v1.5.6.3) (2026-10-16)

- **STRUCT field conversion** - Fields now run their declared SQLAlchemy bind and result processors, including nested containers. Users of field `TypeDecorator`, `Enum`, JSON or Numeric conversion now get the declared conversion instead of raw values, insertion errors or incorrect Decimal storage. Existing rows are not rewritten. Review stored values if your application compensated for skipped processors. [#201](https://github.com/leonardovida/duckdb-sqlalchemy/pull/201)

- **`connection_timeout` is a MotherDuck startup parameter** - For `md:` and `motherduck:` databases, `connection_timeout` and `motherduck_connection_timeout` (URL query, `connect_args["config"]` or `create_motherduck_engine` keyword) now move into the `md:` database string as `connection_timeout` instead of reaching DuckDB as config. Connections that set it used to fail with an unrecognized configuration option and now connect with the timeout applied, and the value is part of the client instance cache key. Use a DuckDB interval string such as `30s` or a `datetime.timedelta`. It needs the MotherDuck extension for DuckDB 1.5.6, and older extensions ignore it. Local databases keep the key as ordinary DuckDB config. [#200](https://github.com/leonardovida/duckdb-sqlalchemy/pull/200)
- **`timedelta` values render as DuckDB intervals** - `datetime.timedelta` values in MotherDuck URL parameters and in settings the dialect applies with `SET` now render as interval strings such as `30s` or `1500ms` instead of Python's `0:00:30` form. [#200](https://github.com/leonardovida/duckdb-sqlalchemy/pull/200)
- **MotherDuck helper columns follow the DuckDB version** - `md_*` table-function helpers name only the columns that the MotherDuck extension for the installed DuckDB version returns. On DuckDB 1.5.5 and newer `md_list_flight_versions()` and `md_job_versions()` no longer expose `source_code` and `requirements_txt`, and `md_update_dive_metadata()`, `md_update_dive_status()`, `md_create_dive()` and `md_get_dive()` no longer expose `required_resources`. Those queries failed to bind before, so only code that built them without running them notices: `.c.<column>` now raises `AttributeError`. Use `md_get_flight_version()` for source columns, or pass `columns=`. [#200](https://github.com/leonardovida/duckdb-sqlalchemy/pull/200)
- **Server-side temporary tables are a session `SET`** - `motherduck_enable_server_side_temp_tables` now runs as `SET` on each new connection instead of joining the `md:` database string, where MotherDuck ignored it. Temporary tables created through such an engine now live on the server. Drop the option if you relied on local temporary tables. [#200](https://github.com/leonardovida/duckdb-sqlalchemy/pull/200)

## [1.5.6.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.6.1...v1.5.6.2) (2026-10-16)

- **MAP member conversion** - MAP keys and values now run their declared SQLAlchemy bind and result processors, including nested MAP/LIST members. Users of member `TypeDecorator`, `Enum`, JSON or Numeric conversion now get the declared conversion instead of raw values or insertion errors. Existing rows are not rewritten. Review stored values if an application previously compensated for skipped processors. [#198](https://github.com/leonardovida/duckdb-sqlalchemy/pull/198)

## [1.5.6.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.6...v1.5.6.1) (2026-10-11)

- **More keywords are quoted in generated SQL** - Identifiers that match DuckDB `type_function` keywords (for example `at`, `by`, `semi`, `anti`, `asof`, `glob`, `positional`, `map`, `struct`) are now double-quoted. Tables and columns with these names previously failed to create or query. Code that compares generated SQL text may see the new quotes. Nothing else to do. [#195](https://github.com/leonardovida/duckdb-sqlalchemy/pull/195)
- **4-byte float columns reflect as `REAL`** - `FLOAT`, `FLOAT4` and `REAL` columns now reflect as `sqlalchemy.REAL` instead of a generic `FLOAT`, so a reflected table created again keeps 4-byte floats instead of widening them to `DOUBLE`. Code that checks `type(column.type) is FLOAT` should check `isinstance(column.type, Float)` instead. Alembic no longer reports `Float(24)` or `REAL` models as changed against these columns. [#195](https://github.com/leonardovida/duckdb-sqlalchemy/pull/195)
- **Unnamed `Enum` compiles inline** - `Enum("a", "b")` without a name, and every reflected ENUM column, now renders as `ENUM('a', 'b')` instead of raising "ENUM type requires a name". DuckDB does not report which named ENUM type a column uses, so reflected ENUM columns still have no type name. Alembic compares such columns by their labels. [#195](https://github.com/leonardovida/duckdb-sqlalchemy/pull/195)
- **Alembic recreates indexed tables for blocked ALTERs** - DuckDB rejects `DROP COLUMN`, column renames, and type or nullability changes on a table that has an index. `op.drop_column()` and `op.alter_column()` now recreate such tables in batch mode, so these migrations and autogenerated downgrades run. Dropping a column also drops the indexes on it, as in PostgreSQL. Tables referenced by another table's foreign key still cannot be recreated. Renaming a table that has an index still fails. Drop its indexes first. [#195](https://github.com/leonardovida/duckdb-sqlalchemy/pull/195)
- **Alembic table renames keep their schema** - `op.rename_table()` and batch recreation emit an unqualified, quoted destination after `RENAME TO`, so renames in named schemas and attached catalogs work. Offline SQL scripts change accordingly. [#194](https://github.com/leonardovida/duckdb-sqlalchemy/pull/194)

## [1.5.5.15](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.14...v1.5.5.15) (2026-10-02)

- **Fixed-size arrays reflect as `FixedArray`** - DuckDB fixed-size array columns (for example `DOUBLE[384]`) now reflect as `duckdb_sqlalchemy.datatypes.FixedArray(item_type, size)` instead of a variable-length `ARRAY`. Code that checks reflected types with `isinstance(..., ARRAY)`, and Alembic autogenerate comparisons, may see a difference. Update type checks, and note that copies already created as LIST need an explicit migration to get a fixed size back. [#189](https://github.com/leonardovida/duckdb-sqlalchemy/pull/189)

## [1.5.5.14](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.13...v1.5.5.14) (2026-10-02)

- **Fewer statements eligible for transient retries** - When `nextval`, `setval` or an `md_*` function name is followed by a comment, the statement is no longer retried, even when the match is ambiguous. Users of the opt-in retry options may see the original error instead of a retry. Nothing to do unless you relied on those statements being retried. [#187](https://github.com/leonardovida/duckdb-sqlalchemy/pull/187)

## [1.5.5.13](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.12...v1.5.5.13) (2026-09-30)

- **`Float()` compiles to `DOUBLE`** - Generic `Float()` and `Float(precision)` above 24 now emit `DOUBLE` (64 bits) in DDL instead of a 32-bit type. Anyone creating tables or generating migrations with `Float` is affected. Use `REAL` or `Float(24)` to keep 32-bit columns, and migrate existing FLOAT columns explicitly if you want the extra precision. [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)
- **DML expressions no longer return a Count row** - Non-returning SQLAlchemy DML expressions (`insert()`, `update()`, `delete()`) no longer expose DuckDB's Count acknowledgement as result rows. Code that fetched that row should read `result.rowcount` instead. Textual SQL through `text()` still returns the Count row. [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)
- **`Identity()` options raise `CompileError`** - A plain `Identity()` falls back to a sequence, and Identity modifiers such as custom start or increment now raise a compilation error instead of being silently ignored. Use an explicit `Sequence` for custom starts and increments. [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)
- **Reflection and Alembic output** - Users of reflection and Alembic autogenerate may see different metadata or migration diffs. Review autogenerated migrations after upgrading. [#180](https://github.com/leonardovida/duckdb-sqlalchemy/pull/180) [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)
  - Columns with sequence defaults reflect with `autoincrement=True`.
  - `STRUCT`, `MAP`, `UNION` and ENUM columns reflect as native dialect types instead of `NullType`. Nested types with ENUM members still reflect as `NullType`.
  - Identity columns and constraint comments are no longer advertised as supported.
  - Alembic registers the DuckDB implementation automatically. A custom DuckDB implementation defined in `env.py` still takes precedence. Remove it to use the built-in one.
- **Numeric values** - Decimal results honor `Numeric.decimal_return_scale`, and bulk inserts keep large integers exact in mixed numeric batches and NaN distinct from NULL in the pandas fallback. Code that depended on the old rounding or NULL conversion may see different values. [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)
- **JSON expressions compile to DuckDB syntax** - JSON indexing, nested paths and scalar casts now compile to DuckDB-native SQL. Code that asserts on the compiled SQL text should update its expectations. [#183](https://github.com/leonardovida/duckdb-sqlalchemy/pull/183)

## [1.5.5.12](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.11...v1.5.5.12) (2026-09-28)

- **`regexp_match()` matches substrings** - `column.regexp_match(pattern)` now compiles to `regexp_matches()` and matches anywhere in the string, like PostgreSQL. It used DuckDB's `~` operator, which only matched the whole string. Anchor patterns that must match the whole string with `^...$`. [#176](https://github.com/leonardovida/duckdb-sqlalchemy/pull/176)
- **Real `rowcount` and ORM stale-row checks** - `CursorResult.rowcount` reports the rows an INSERT, UPDATE, DELETE or MERGE changed instead of `-1`, and the dialect sets `supports_sane_rowcount`. ORM mappings with `version_id_col` now raise `StaleDataError` on a version mismatch instead of skipping the check. Fix code that relied on `-1`, and handle `StaleDataError` where concurrent updates are possible. `rowcount` is still `-1` with RETURNING and for batched `executemany()` UPDATE and DELETE. [#176](https://github.com/leonardovida/duckdb-sqlalchemy/pull/176)
- **Reflected constraint names are DuckDB's names** - Reflected foreign key and check constraints are named like `child_parent_id_id_fkey` and `parent_n_check` instead of using the constraint SQL text. Update code, tests or Alembic comparisons that match on reflected constraint names. DuckDB ignores names given in DDL. [#176](https://github.com/leonardovida/duckdb-sqlalchemy/pull/176)
- **Constraint and temp-object reflection return more results** - `get_unique_constraints`, `get_temp_table_names` and `get_temp_view_names` return real results instead of empty lists, `has_schema` accepts `db.schema`, and foreign key, unique, check and comment reflection is scoped to the requested database and schema. Review Alembic autogenerate output after upgrading. [#176](https://github.com/leonardovida/duckdb-sqlalchemy/pull/176)
- **More HTTP errors are retried** - With `duckdb_retry_on_transient`, HTTP 429, 502, 503 and 504 errors in httpfs format (`... (HTTP 503)`) are now retried, and `504 Gateway Timeout` is no longer treated as a lost connection. Nothing to do unless you relied on those errors failing immediately. [#176](https://github.com/leonardovida/duckdb-sqlalchemy/pull/176)

## [1.5.5.11](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.10...v1.5.5.11) (2026-09-28)

This release changes defaults and minimum versions. Installs on Python 3.9 or with DuckDB pinned below 1.3.0 keep resolving 1.5.5.10. All entries come from [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175).

- **Minimum versions raised** - Python 3.10 and DuckDB 1.3.0 are now required by package metadata. Before this release the metadata allowed Python 3.9 and DuckDB 0.5.0. Upgrade Python and DuckDB, or stay on 1.5.5.10. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Reflection without a schema covers the current schema only** - `get_table_names()`, `has_table()`, `MetaData.reflect()` and `create_all()` without `schema=` look only at the current database and schema, not every schema and attached database. Code that found tables in other schemas or attached databases must pass `schema="name"` or `schema="db.schema"`. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **`duckdb://` uses `SingletonThreadPool`** - A URL with no database now uses `SingletonThreadPool`, like `:memory:`, so all connections share one in-memory database (it used `QueuePool` since 1.4.4.8). If you need a separate database per connection, pass `poolclass=QueuePool`. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **MotherDuck routing keys on local files raise** - MotherDuck path keys such as `access_mode` and `session_name` are only moved into `md:` and `motherduck:` database strings. For local files they are passed to DuckDB as config, so keys DuckDB does not know now raise "unrecognized configuration parameter". Remove MotherDuck-only keys from local URLs. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **`MotherDuckURL` keeps routing params in `URL.query`** - Routing parameters stay in `URL.query` (they still move into the `md:` string on connect), so `str(url)` round-trips. Code that read routing parameters from `url.database` should read `url.query`. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Only two isolation levels accepted** - `isolation_level` accepts only `AUTOCOMMIT` and `READ COMMITTED`. Other values raise `ArgumentError`. Remove other `isolation_level` values. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Numeric and Decimal values** - `Numeric`/`DECIMAL` values keep full precision instead of passing through float, and `Numeric(asdecimal=False)` returns floats as SQLAlchemy documents. Code that expected float rounding, or `Decimal` from `asdecimal=False`, should update. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Arrow results are read once** - Reading `.arrow` after fetching rows from a `duckdb_arrow` result raises `InvalidRequestError`. Read `.arrow` first, or fetch rows only. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **`duckdb_sqlalchemy.make_url` deprecated** - It now emits `DeprecationWarning`. Use `duckdb_sqlalchemy.URL` to build URLs or `sqlalchemy.engine.make_url` to parse URL strings. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Bulk insert fast path runs by default** - The `duckdb_copy_threshold` path (register an Arrow or pandas object and run `INSERT ... SELECT` above 10,000 rows) never ran with default engine settings and now does, preferring Arrow. Large `executemany()` inserts take a different execution path. Set `duckdb_copy_threshold` explicitly if you need different behavior. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **`MOTHERDUCK_TOKEN` injection narrowed** - The environment token is only added for MotherDuck databases that have no explicit credential, not for local files or connections that already pass one. Nothing to do unless you relied on the env token overriding an explicit credential. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Retries and connection errors** - `duckdb_retry_count` reports the original error instead of "Current transaction is aborted" and never retries `md_*` functions, `nextval` or `setval`. DuckDB I/O and HTTP errors no longer invalidate the connection. Update error handling that matched the old messages. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Interleaved results are buffered** - Running a statement while an earlier result is unread now buffers the rest of that result in memory instead of truncating it. Expect higher memory use in that pattern, and note that Arrow and DataFrame fetches are not available on a buffered result. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
- **Stricter helpers and validation** - Users of the COPY helpers, pool overrides and reflection may see new errors or warnings. [#175](https://github.com/leonardovida/duckdb-sqlalchemy/pull/175)
  - `copy_from_rows` writes UTF-8, keeps empty strings distinct from NULL, and raises on binary values.
  - COPY helpers reject non-finite floats and honor `schema_translate_map`.
  - Unknown pool override values warn instead of being silently ignored.
  - `get_check_constraints` raises `NoSuchTableError` for missing tables.
  - `duckdb_arraysize` sets the default `fetchmany()` size.

## [1.5.5.8](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.7...v1.5.5.8) (2026-09-18)

- **Some large bulk inserts skip the fast path** - Batches with SQL defaults, explicit VALUES expressions or aliased column keys now run as ordinary `executemany()` even above `duckdb_copy_threshold`, so they can be slower. Plain inserts keep the fast path. Nothing to do unless the slowdown matters, in which case simplify those insert shapes. [#166](https://github.com/leonardovida/duckdb-sqlalchemy/pull/166)

## [1.5.5.7](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.6...v1.5.5.7) (2026-09-14)

- **Fewer statements eligible for transient retries** - Statements with dollar-quoted strings, escape strings, several block comments, or `EXPLAIN ANALYZE` are no longer retried automatically. They still execute normally. Users of the opt-in retry options may see failures that were previously retried. [#163](https://github.com/leonardovida/duckdb-sqlalchemy/pull/163)
- **NULL MAP values on legacy DuckDB** - SQL NULL MAP values return `None` instead of an empty dict on legacy DuckDB versions. Check code that treated an empty dict as NULL. [#163](https://github.com/leonardovida/duckdb-sqlalchemy/pull/163)

## [1.5.5.6](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.5...v1.5.5.6) (2026-09-14)

- **Multi-statement SQL not retried after a mutation** - Opt-in transient retries no longer retry multi-statement SQL whose first statement is read-only but contains a later mutation. Single read-only statements are still retried. [#162](https://github.com/leonardovida/duckdb-sqlalchemy/pull/162)

## [1.5.5.4](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.3...v1.5.5.4) (2026-08-28)

- **View reflection returns empty metadata instead of raising** - Primary key, foreign key, unique, check constraint and index lookups on regular or temporary views return empty results instead of raising `NoSuchTableError` or `NotImplementedError`. Remove error handling that relied on those exceptions for views. [#156](https://github.com/leonardovida/duckdb-sqlalchemy/pull/156)

## [1.5.5.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.2...v1.5.5.3) (2026-08-26)

- **`md_get_flight_logs` result columns** - `md_get_flight_logs` returns MotherDuck's tabular `line_number`, `reported_at` and `line` columns. The deprecated `md_flight_logs` and `md_job_run_logs` keep a `logs` accessor as an alias for `line`. Read `line` in new code. [#155](https://github.com/leonardovida/duckdb-sqlalchemy/pull/155)
- **`pytz` is a runtime dependency on every Python version** - It is needed to materialize `TIMESTAMPTZ` values in minimal installations. Allow `pytz>=2024.2` in locked environments. [#155](https://github.com/leonardovida/duckdb-sqlalchemy/pull/155)

## [1.5.5.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.1...v1.5.5.2) (2026-08-21)

- **Unqualified reflection follows DuckDB name resolution** - A table reflected without a schema resolves like DuckDB binds an unqualified name: temp table first, then the current schema, then the search path. Autoload no longer merges columns and constraints from same-named tables in attached catalogs. Pass `schema=` if you meant another table. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5.1...v1.5.5.2)

## [1.5.5.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.5...v1.5.5.1) (2026-08-18)

- **Timezone-aware reflection** - `TIMESTAMPTZ` and `TIMETZ` columns reflect with `timezone=True`, so copied metadata recreates them with time zones. Review Alembic autogenerate diffs for those columns after upgrading. [#152](https://github.com/leonardovida/duckdb-sqlalchemy/pull/152)

## [1.5.4.4](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.4.3...v1.5.4.4) (2026-07-17)

- **Fewer errors treated as disconnects** - Only DuckDB operational errors are checked for disconnect messages. Binder and other programming errors that mention words such as `timeout` or `socket` no longer invalidate pooled connections, while DuckDB's `Connection already closed` error is recognized. Nothing to do unless you relied on those errors resetting the connection. [#140](https://github.com/leonardovida/duckdb-sqlalchemy/pull/140)

## [1.5.4.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.4.2...v1.5.4.3) (2026-07-10)

- **Flight helpers compile to verb-style function names** - `md_flights`, `md_flight_runs`, `md_flight_logs` and `md_flight_versions` are deprecated and compile to `md_list_flights`, `md_list_flight_runs`, `md_get_flight_logs` and `md_list_flight_versions`. The `md_*job*` helpers also compile to these. Switch to the verb-style helpers. [#138](https://github.com/leonardovida/duckdb-sqlalchemy/pull/138)

## [1.5.4.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.4.1...v1.5.4.2) (2026-07-03)

- **`copy_from_rows` header mode requires `columns`** - `copy_from_rows(..., include_header=True)` with sequence rows and no `columns` now raises before loading instead of silently treating the first data row as the header. Pass `columns=` when you use header mode with sequence rows. [#134](https://github.com/leonardovida/duckdb-sqlalchemy/pull/134)

## [1.5.4.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.4...v1.5.4.1) (2026-07-01)

- **Autoincrement primary keys use an implicit sequence** - Integer primary keys that SQLAlchemy treats as autoincrement render as `DEFAULT nextval('<table>_<column>_seq')` with a sequence created on `CREATE TABLE` and dropped on `DROP TABLE`, instead of PostgreSQL's unsupported `SERIAL`. Expect extra sequence objects in the database and in migrations. Declare an explicit `Sequence` only for a custom name or start value. [#130](https://github.com/leonardovida/duckdb-sqlalchemy/pull/130)

## [1.5.4](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.3...v1.5.4) (2026-06-26)

- **`md_*job*` helpers deprecated** - The MotherDuck job helpers added in 1.5.3 (`md_create_job`, `md_jobs`, `md_run_job` and others) emit `DeprecationWarning` and map to the Flight helpers. Use the `md_*flight*` helpers. [#116](https://github.com/leonardovida/duckdb-sqlalchemy/pull/116)
- **Flight config validated locally** - Flight helper calls with empty config keys, keys containing `=` or NUL bytes, non-string keys or values, or NUL-containing values raise `ValueError` before compilation. Fix the config map. [#121](https://github.com/leonardovida/duckdb-sqlalchemy/pull/121) [#124](https://github.com/leonardovida/duckdb-sqlalchemy/pull/124)
- **`application_name` moves into the user agent** - `application_name` in connect config is added to the generated `custom_user_agent` instead of being passed to DuckDB as a setting. Nothing to do unless you read it back as a DuckDB setting. [#115](https://github.com/leonardovida/duckdb-sqlalchemy/pull/115)

## [1.5.2.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.2.1...v1.5.2.2) (2026-05-11)

- **`MOTHERDUCK_TOKEN` wins over `motherduck_token`** - When both environment variables are set, the uppercase `MOTHERDUCK_TOKEN` is used. Remove the variable you do not want used. [#93](https://github.com/leonardovida/duckdb-sqlalchemy/pull/93)
- **MotherDuck database names with commas are rejected** - URL builders raise for MotherDuck database names that contain commas. Use MotherDuck database names without commas. [#97](https://github.com/leonardovida/duckdb-sqlalchemy/pull/97)
- **PostgreSQL session setup statements are ignored** - Common PostgreSQL session setup and transaction isolation statements emitted by PostgreSQL-oriented clients are now skipped instead of sent to DuckDB. Do not rely on those statements having an effect. [#98](https://github.com/leonardovida/duckdb-sqlalchemy/pull/98)

## [1.5.2.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.2...v1.5.2.1) (2026-04-25)

- **MotherDuck transport options are startup URL parameters** - `host`, `region_host`, `port`, `tls` and `grpc_local_subchannel_pool` move into the MotherDuck connection string instead of being applied as late config. Users with custom MotherDuck endpoints may see different client caching. Nothing to do for standard connections. [#86](https://github.com/leonardovida/duckdb-sqlalchemy/pull/86) [#87](https://github.com/leonardovida/duckdb-sqlalchemy/pull/87)
- **`motherduck_*` transport aliases deprecated** - `motherduck_host`, `motherduck_region_host`, `motherduck_port`, `motherduck_use_tls` and `motherduck_grpc_local_subchannel_pool` emit `DeprecationWarning` and normalize to the canonical keys. Use `host`, `region_host`, `port`, `tls` and `grpc_local_subchannel_pool`. [#89](https://github.com/leonardovida/duckdb-sqlalchemy/pull/89)

## [1.5.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.1.4...v1.5.2) (2026-04-17)

- **`session_name` replaces `session_hint`** - MotherDuck's `session_name` parameter is preferred. `session_hint`, `motherduck_session_hint` and `motherduck_session_name` emit `DeprecationWarning` and normalize to `session_name`. `stable_session_hint()` is deprecated in favor of `stable_session_name()`. Rename the parameter and helper. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.1.4...v1.5.2)
- **`GEOMETRY` reflects as a dialect type** - On DuckDB 1.5.2 and newer, `GEOMETRY` columns reflect as a concrete dialect type instead of `NullType`. Review reflection-based code and Alembic diffs. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.1.4...v1.5.2)

## [1.5.1.4](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.5.1...v1.5.1.4) (2026-04-10)

CHANGELOG.md lists 1.5.1.1, 1.5.1.2 and 1.5.1.3 separately, but those versions were never published to PyPI. Their changes first reached users in 1.5.1.4.

- **SQLAlchemy 1.x dropped** - Package metadata requires SQLAlchemy 2.0.45 or newer (it previously allowed 1.3.22). Projects on SQLAlchemy 1.x must upgrade SQLAlchemy or stay on 1.5.1. Since 1.5.4 the floor is SQLAlchemy 2.0.0 on Python below 3.14 and 2.0.45 on Python 3.14. [#57](https://github.com/leonardovida/duckdb-sqlalchemy/pull/57)
- **Local files default to `QueuePool`** - File-backed DuckDB engines use `QueuePool` instead of `NullPool`, so connections are kept open and reused. MotherDuck stays on `NullPool`. Pass `poolclass=NullPool` if you need a fresh connection each time, for example to release file locks. [#57](https://github.com/leonardovida/duckdb-sqlalchemy/pull/57)
- **More reads eligible for transient retries** - Read-only `WITH` statements and comment-prefixed queries are treated as idempotent by the opt-in retry options. Nothing to do unless such statements must never be retried. [#71](https://github.com/leonardovida/duckdb-sqlalchemy/pull/71)
- **Legacy MotherDuck aliases deprecated** - `motherduck_session_hint`, `motherduck_attach_mode`, `motherduck_saas_mode`, `cachebust` and `motherduck_dbinstance_inactivity_ttl` emit `DeprecationWarning` and normalize to `session_name` (via `session_hint` at the time), `attach_mode`, `saas_mode`, `cache_buster` and `dbinstance_inactivity_ttl`. Use the canonical keys. [#59](https://github.com/leonardovida/duckdb-sqlalchemy/pull/59) [#74](https://github.com/leonardovida/duckdb-sqlalchemy/pull/74)
- **`duckdb_insertmanyvalues_page_size` deprecated** - The execution option emits `DeprecationWarning`. Use SQLAlchemy's `insertmanyvalues_page_size`. [#63](https://github.com/leonardovida/duckdb-sqlalchemy/pull/63)

## [1.5.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.8...v1.5.1) (2026-03-26)

- **`VARIANT` reflects as a dialect type** - DuckDB `VARIANT` columns reflect as the new `VARIANT` type instead of `NullType`. Review reflection-based code and Alembic diffs. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.8...v1.5.1)

## [1.4.4.8](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.7...v1.4.4.8) (2026-03-24)

- **No isolation-level probe on connect** - Connection setup no longer queries the default isolation level, matching `duckdb_engine`. Nothing to do unless you read the dialect's default isolation level. [#53](https://github.com/leonardovida/duckdb-sqlalchemy/pull/53)
- **Empty and named in-memory URLs use `QueuePool`** - `duckdb://` and named in-memory URLs such as `:memory:analytics` switched from `SingletonThreadPool` to `QueuePool` for `duckdb_engine` compatibility. Exact `:memory:` kept `SingletonThreadPool`. This change is not in CHANGELOG.md, and 1.5.5.11 moved `duckdb://` back to `SingletonThreadPool`. [#52](https://github.com/leonardovida/duckdb-sqlalchemy/pull/52)

## [1.4.4.7](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.6...v1.4.4.7) (2026-03-22)

- **Reflection returns real types, keys and indexes** - `JSON` columns reflect as `JSON`, array and list columns as `ARRAY` instead of `NullType`, enum labels are populated, primary keys come from `duckdb_constraints()`, and indexes from `duckdb_indexes()` instead of an empty list. Review reflection-based code and Alembic autogenerate diffs. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.6...v1.4.4.7)

## [1.4.4.6](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.5...v1.4.4.6) (2026-03-22)

- **`DOUBLE` reflects as `DOUBLE`** - DuckDB `DOUBLE`/`FLOAT8` columns reflect as SQLAlchemy `DOUBLE` on SQLAlchemy 2.x. Update type checks on reflected columns. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.5...v1.4.4.6)

## [1.4.4.5](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.3...v1.4.4.5) (2026-03-22)

- **Empty MAP values return `{}`** - Empty `MAP` values are returned as empty dicts instead of `None`. Check code that tested for `None`. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.3...v1.4.4.5)

## [1.4.4.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.2...v1.4.4.3) (2026-02-21)

- **NULL MAP values return `None`** - SQL NULL `MAP` values are returned as `None` instead of an empty dict. Check code that tested for an empty dict. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4.2...v1.4.4.3)

## [1.4.4.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v1.4.4...v1.4.4.2) (2026-02-05)

- **Stricter identifier and option validation** - Config keys are validated before `SET`, preload extension names before `LOAD`, and COPY helper table, column and option identifiers reject SQL fragments. Inputs that relied on passing raw SQL through these values now raise. Pass plain identifiers and option names. [#20](https://github.com/leonardovida/duckdb-sqlalchemy/pull/20)

## [1.4.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.19.3...v1.4.3) (2026-01-15)

- **Version numbers follow DuckDB** - The package jumped from 0.19.3 to 1.4.3 so its version matches the supported DuckDB line. There are no functional changes, but constraints such as `~=0.19` or `<1` do not pick up this or later releases. Update version constraints to `>=1.4.3` or newer. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.19.3...v1.4.3)

## [0.19.2](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.19.0...v0.19.2) (2025-12-28)

- **Empty database URLs open `:memory:`** - A URL with no database connects to `:memory:` and uses `SingletonThreadPool` instead of `NullPool`. Nothing to do unless you relied on the earlier pool choice. [compare](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.19.0...v0.19.2)

## [0.18.0](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.17.0...v0.18.0) (2025-12-24)

- **Package renamed from `duckdb_engine`** - The distribution is `duckdb-sqlalchemy` and the module is `duckdb_sqlalchemy`. Uninstall `duckdb-engine`, install `duckdb-sqlalchemy`, and change imports to `duckdb_sqlalchemy`. `duckdb://` URLs keep working, and `duckdb+duckdb_sqlalchemy://` (since 1.5.5.11) selects this driver if both packages are installed. See the [migration guide](migration-from-duckdb-engine). [#2](https://github.com/leonardovida/duckdb-sqlalchemy/pull/2)

The sections below come from upstream [Mause/duckdb_engine](https://github.com/Mause/duckdb_engine) releases before the fork and are kept for context.

## [0.16.0](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.15.1...v0.16.0) (2025-03-29)

- **Python 3.8 dropped** - Python 3.9 became the minimum. [1acd720](https://github.com/leonardovida/duckdb-sqlalchemy/commit/1acd720ca91d1438405f848c80425d5cbc715762)

## [0.14.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.14.0...v0.14.1) (2025-01-10)

- **Division is no longer floor division** - The dialect sets `div_is_floordiv = False` to avoid bad casts, which changes the SQL emitted for `/`. Check integer division results. [dfbed6d](https://github.com/leonardovida/duckdb-sqlalchemy/commit/dfbed6df21470192551636cefa6ff7386d2cf6ae)

## [0.11.4](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.11.3...v0.11.4) (2024-04-09)

- **Python 3.7 dropped** - Upgrade to a supported Python. [9acdd66](https://github.com/leonardovida/duckdb-sqlalchemy/commit/9acdd660f9f50f120393128f06c12da7e89dfefd)

## [0.9.3](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.9.2...v0.9.3) (2023-12-05)

- **`numeric_dollar` paramstyle** - The dialect uses the `numeric_dollar` paramstyle (`$1`) where available. Code that builds raw DBAPI SQL with other placeholders should use SQLAlchemy named parameters. [f51ee7f](https://github.com/leonardovida/duckdb-sqlalchemy/commit/f51ee7fb481f6e41e9d6636356edb81ee5e424cd)

## [0.7.1](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.7.0...v0.7.1) (2023-05-09)

- **SQLAlchemy floor raised to 1.3.22** - Upgrade SQLAlchemy. [16cc7d8](https://github.com/leonardovida/duckdb-sqlalchemy/commit/16cc7d88f6982d572648c142a25a9e8681bb46f2)

## [0.6.5](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.6.4...v0.6.5) (2022-11-22)

- **Python 3.6 dropped** - Upgrade to a supported Python. [0d81998](https://github.com/leonardovida/duckdb-sqlalchemy/commit/0d81998caafee51d86f1692cd73867561b0ce576)

## [0.3.0](https://github.com/leonardovida/duckdb-sqlalchemy/compare/v0.2.0...v0.3.0) (2022-08-02)

- **`:memory:` uses `SingletonThreadPool`** - In-memory connections share one database per thread. [58ef77a](https://github.com/leonardovida/duckdb-sqlalchemy/commit/58ef77acb55f3bfbe8be511f79a151a25f54a374)

## Deprecated APIs still available

Each deprecated name below still works and emits `DeprecationWarning`. No removal has been announced for any of them.

| API | Deprecated in | Replacement | Removal plan |
| --- | --- | --- | --- |
| `duckdb_insertmanyvalues_page_size` execution option | 1.5.1.4 | `insertmanyvalues_page_size` | not scheduled |
| `motherduck_dbinstance_inactivity_ttl` URL or config key | 1.5.1.4 | `dbinstance_inactivity_ttl` | not scheduled |
| `motherduck_attach_mode` URL or config key | 1.5.1.4 | `attach_mode` | not scheduled |
| `motherduck_saas_mode` URL or config key | 1.5.1.4 | `saas_mode` | not scheduled |
| `cachebust` URL or config key | 1.5.1.4 | `cache_buster` | not scheduled |
| `motherduck_session_hint` URL or config key | 1.5.1.4 | `session_name` | not scheduled |
| `session_hint`, `motherduck_session_name` URL or config keys | 1.5.2 | `session_name` | not scheduled |
| `stable_session_hint()` | 1.5.2 | `stable_session_name()` | not scheduled |
| `motherduck_host`, `motherduck_region_host`, `motherduck_port`, `motherduck_use_tls`, `motherduck_grpc_local_subchannel_pool` | 1.5.2.1 | `host`, `region_host`, `port`, `tls`, `grpc_local_subchannel_pool` | not scheduled |
| `md_create_job`, `md_jobs`, `md_get_job`, `md_update_job`, `md_delete_job`, `md_run_job`, `md_cancel_job_run`, `md_job_runs`, `md_job_run_logs`, `md_job_versions`, `md_get_job_version` | 1.5.4 | `md_create_flight`, `md_list_flights`, `md_get_flight`, `md_update_flight`, `md_delete_flight`, `md_run_flight`, `md_cancel_flight_run`, `md_list_flight_runs`, `md_get_flight_logs`, `md_list_flight_versions`, `md_get_flight_version` | not scheduled |
| `md_flights`, `md_flight_runs`, `md_flight_logs`, `md_flight_versions` | 1.5.4.3 | `md_list_flights`, `md_list_flight_runs`, `md_get_flight_logs`, `md_list_flight_versions` | not scheduled |
| `duckdb_sqlalchemy.make_url` | 1.5.5.11 | `duckdb_sqlalchemy.URL` to build, `sqlalchemy.engine.make_url` to parse | not scheduled |
