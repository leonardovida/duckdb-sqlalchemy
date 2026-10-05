---
layout: default
title: Types and Caveats
---

# Types and Caveats

## Type support

Most SQLAlchemy core types map directly to DuckDB. DuckDB-specific helpers live in `duckdb_sqlalchemy.datatypes`.

| Type | DuckDB name | Notes |
| --- | --- | --- |
| `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UTinyInteger`, `USmallInteger`, `UInteger`, `UBigInteger` | UTINYINT, USMALLINT, UINTEGER, UBIGINT | Unsigned integers |
| `TinyInteger` | TINYINT | Signed 1-byte integer |
| `HugeInteger`, `UHugeInteger`, `VarInt` | HUGEINT, UHUGEINT, VARINT | 128-bit and variable-length integers |
| `Struct` | STRUCT | Nested fields via dict of SQLAlchemy types |
| `Map` | MAP | Key/value mapping |
| `Union` | UNION | Union types via dict of SQLAlchemy types |

### DuckDB-specific type examples

```python
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import Integer, String
from duckdb_sqlalchemy.datatypes import Map, Struct, Union, VarInt

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("payload", Struct({"user": String, "action": String})),
    Column("tags", Map(String, String)),
    Column("attributes", Union({"priority": Integer, "label": String})),
    Column("trace_id", VarInt),
)
```

## Parameter binding

Use named parameters in SQLAlchemy expressions and let the dialect pick the placeholder style:

```python
from sqlalchemy import text

stmt = text("SELECT * FROM events WHERE event_id = :event_id")
conn.execute(stmt, {"event_id": 123})
```

## Execution options

DuckDB-specific execution options are available on connections and statements:

```python
with engine.connect().execution_options(
    duckdb_arrow=True,
    duckdb_copy_threshold=10000,
    insertmanyvalues_page_size=1000,
) as conn:
    conn.execute(stmt)
```

- `duckdb_arrow`: return Arrow tables for SELECTs (`result.arrow` or `result.all()`) or bounded batch readers (`result.batches()`); requires `pyarrow`.
- `duckdb_copy_threshold`: for large INSERT executemany, register a pandas/Arrow object and run `INSERT INTO ... SELECT ...`.
- `insertmanyvalues_page_size`: canonical SQLAlchemy batch size for 2.x multi-row VALUES inserts.
- `duckdb_insertmanyvalues_page_size`: deprecated alias for `insertmanyvalues_page_size`; still supported for backward compatibility.
- `duckdb_arraysize`: cursor fetch size for `stream_results` / `fetchmany` workloads.

Arrow results consume the cursor; fetch rows or Arrow, not both. Reading
`.arrow` after fetching rows raises `InvalidRequestError`.

## Interleaved statements

All cursors of a SQLAlchemy connection share one DuckDB connection. If you run
another statement while an earlier result is still unread (for example an ORM
lazy load inside a loop), the dialect first buffers the rest of the earlier
result in memory so it is not truncated. Fetch errors propagate and preserve the pending cursor. Arrow and DataFrame fetches are not
available on a buffered result. Read large results fully, or use a separate
connection, to avoid the memory cost.

Arrow batch readers own their result until consumed or closed; another statement
on that connection raises `InvalidRequestError` while the reader remains active.
See [performance guidance](performance) for streaming and direct Arrow ingestion.

## Row counts and regular expressions

`result.rowcount` reports the rows changed by an INSERT, UPDATE, DELETE, or
MERGE. It is `-1` for statements with a RETURNING clause and when several
parameter sets run through DuckDB's `executemany()` (batched UPDATE and
DELETE; multi-row INSERTs are counted). `column.regexp_match(pattern)` compiles to
`regexp_matches()`, which matches anywhere in the string; `flags` are passed as
DuckDB regex options (for example `flags="i"`).

## Reflection

Reflection reads DuckDB's catalog functions (`duckdb_tables()`,
`duckdb_columns()`, `duckdb_constraints()`, `duckdb_indexes()`), not the
PostgreSQL `pg_catalog` emulation, so it sees DuckDB types and attached
databases.

### Which table a name refers to

- `schema=None` lists (`get_table_names()`, `get_view_names()`,
  `MetaData.reflect()`) cover the current database and schema only.
- A single table looked up without a schema (`get_columns("t")`,
  `Table("t", metadata, autoload_with=conn)`) resolves the way DuckDB binds an
  unqualified name: a temp table first, then the current schema, then the
  `search_path`. Its columns, keys, indexes, and comment all come from that one
  table, even when tables with the same name exist elsewhere.
- Pass `schema="db.schema"` to reach another attached database. A bare schema
  name is looked up in the current database first, then in the first attached
  database that has it.
- `get_temp_table_names()` and `get_temp_view_names()` list temporary
  relations, and `has_schema()` accepts `schema` or `db.schema`, matching the
  names `get_schema_names()` returns.
- Bulk reflection honors `kind` and `scope`: default objects and tables are
  selected by default; `ObjectKind.VIEW` returns views and
  `ObjectScope.TEMPORARY` returns temporary objects. Filtering precedes
  name resolution, so temporary shadows do not hide requested regular objects.
- Foreign keys from an unqualified lookup preserve the resolved target schema
  when it differs from the current schema.

```python
from sqlalchemy import inspect

inspector = inspect(conn)
inspector.get_table_names(schema="analytics.main")
inspector.get_foreign_keys("orders", schema="analytics.main")
inspector.has_schema("analytics.staging")
```

### Constraint names

DuckDB names every constraint itself (`child_parent_id_id_fkey`,
`parent_code_key`, `parent_n_check`) and ignores names given in DDL, so
reflected constraint names can differ from the names in your models. Foreign
keys cannot cross schemas in DuckDB, so a reflected foreign key always refers
to a table in the same schema.

### Nested types

`STRUCT`, `MAP`, and `UNION` columns reflect as `Struct`, `Map`, and `Union`
with their member types, so a reflected table recreates the same columns.
Lists reflect as SQLAlchemy `ARRAY`. Fixed-size DuckDB arrays reflect as
`duckdb_sqlalchemy.datatypes.FixedArray(item_type, size)`, preserving their
length when metadata is copied or Alembic generates a migration. Lists of
fixed arrays, fixed arrays of lists, and nested fixed arrays retain their shape.

```python
from sqlalchemy import Float
from duckdb_sqlalchemy.datatypes import FixedArray

embedding_type = FixedArray(Float(), 384)  # DOUBLE[384]
```

DuckDB enforces the size at insertion. `FixedArray` supports SQLAlchemy indexing
and item bind/result processors. `ARRAY(Float())` remains a variable-length list.

A nested type reflects as `NullType` when one of its members is an ENUM, because
DuckDB does not report the enum's type name inside a nested type, or when a
member type is not recognized. Declare those columns in your models.

### Enums

`inspector.get_enums()` lists the ENUM types created with `CREATE TYPE` in the
current database and schema, with their labels. Pass `schema="name"` or
`schema="db.schema"` for another schema, or `schema="*"` for all of them.

## Unsupported statements

DuckDB does not support these, so the statements SQLAlchemy emits for them fail:

- savepoints: `Connection.begin_nested()` and `Session.begin_nested()` emit
  `SAVEPOINT`, which DuckDB rejects with a parser error
- row locks: `select(...).with_for_update()` fails with "SELECT locking clause
  is not supported"
- adding or dropping UNIQUE, FOREIGN KEY, or CHECK constraints on an existing
  table (`ALTER TABLE ... ADD CONSTRAINT`); see
  [Alembic integration](alembic) for a batch-mode workaround

## Isolation levels

DuckDB has a single transaction isolation level. The dialect accepts
`isolation_level="AUTOCOMMIT"` (no `BEGIN` is issued) and the default
`"READ COMMITTED"`; other levels raise `ArgumentError`.

## Numeric precision

`Numeric`/`DECIMAL` values are bound and returned as `decimal.Decimal` without
passing through float. Use `Numeric(asdecimal=False)` or `Float` for floats.
Generic `Float()` and `Float(precision > 24)` compile to `DOUBLE` (64 bits);
`REAL` and `Float(24)` use 32 bits. Existing FLOAT columns retain their original
precision until explicitly migrated.

## Auto-increment columns

DuckDB does not support PostgreSQL `SERIAL`. For an integer primary key that
SQLAlchemy treats as autoincrement, the dialect creates an implicit
`<table>_<column>_seq` sequence with the table, uses it as the column default,
and drops it with the table:

```python
from sqlalchemy import Column, Integer, MetaData, Table

users = Table("users", MetaData(), Column("id", Integer, primary_key=True))
```

Declare an explicit `Sequence` only when you need a custom name or start value:

```python
from sqlalchemy import Column, Integer, Sequence, Table, MetaData

user_id_seq = Sequence("user_id_seq")
users = Table(
    "users",
    MetaData(),
    Column("id", Integer, user_id_seq, server_default=user_id_seq.next_value(), primary_key=True),
)
```

## Multiprocessing (fork)

DuckDB's Python bindings are not fork-safe. Creating a new connection in a
`multiprocessing` child process created with `fork` can raise runtime errors
(for example, `RuntimeError: thread::join failed: No such process`), especially
with MotherDuck or file-backed connections. Prefer `spawn` or `forkserver`, and
initialize engines/connections in the child process.

DuckDB canonicalizes `CHAR(n)` and `VARCHAR(n)` to unbounded `VARCHAR`; reflected string lengths are therefore `None`. It generates constraint names and supports table/column comments, but does not support constraint comments or identity columns. Use sequences for generated integer keys. A plain `Identity()` falls back to a sequence because SQLAlchemy ignores unsupported identity syntax; identity options raise `CompileError` so their requested behavior cannot silently change.
