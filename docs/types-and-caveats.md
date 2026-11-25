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
| `HugeInteger`, `UHugeInteger`, `VarInt` | HUGEINT, UHUGEINT, BIGNUM / VARINT | 128-bit and variable-length integers |
| `TimestampNs`, `TimeNs` | TIMESTAMP_NS, TIME_NS | Nanosecond-precision timestamps and times |
| `Struct` | STRUCT | Nested fields via dict of SQLAlchemy types |
| `Map` | MAP | Key/value mapping |
| `Union` | UNION | Union types via dict of SQLAlchemy types |
| `FixedArray` | `TYPE[N]` | Fixed-size array (`FixedArray(item_type, size)`) |
| `Variant` | VARIANT | Semi-structured variant values (DuckDB 1.5+) |
| `Geometry` | GEOMETRY | Spatial geometry values |

`Struct` fields and `Map` keys and values run their declared SQLAlchemy Python
bind and result processors, recursively through nested containers. This includes
`TypeDecorator`, non-native `Enum`, JSON and Numeric conversion. A whole SQL NULL
container remains `None`. STRUCT input dictionaries retain their keys and are
matched to declared fields using DuckDB's ASCII case-insensitive naming rules.

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

| Option | Default | Effect | Guide |
| --- | --- | --- | --- |
| `duckdb_arrow` | `False` | SELECT results are Arrow tables (`result.arrow`, `result.all()`) or bounded batch readers (`result.batches()`). Requires `pyarrow`. | [Arrow results](olap#arrow-results) |
| `duckdb_copy_threshold` | `10000` | Row count at which an `executemany` INSERT loads through a registered Arrow table (pandas as a fallback) and one `INSERT ... SELECT`. `0` disables it. | [Ordinary and bulk inserts](performance#ordinary-and-bulk-inserts) |
| `insertmanyvalues_page_size` | `1000` | SQLAlchemy's batch size for multi-row VALUES inserts below the copy threshold. | |
| `duckdb_arraysize` (or `arraysize`) | `1` | Rows `fetchmany()` returns without a size, for `stream_results` workloads. | [Streaming reads](olap#streaming-reads) |
| `duckdb_retry_on_transient` | `False` | Retries an idempotent statement once after a transient error. | [Transient retry](motherduck#transient-retry-opt-in) |
| `duckdb_retry_count` | `0` | Number of retries after a transient error. | [Transient retry](motherduck#transient-retry-opt-in) |
| `duckdb_retry_backoff` | none | Seconds to wait before each retry. | [Transient retry](motherduck#transient-retry-opt-in) |
| `duckdb_insertmanyvalues_page_size` | | Deprecated alias for `insertmanyvalues_page_size`. | |

`exact_decimal_division=True` is an engine argument, not an execution option.
Pass it to `create_engine()`. See [Decimal division](#decimal-division).

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
MERGE. Core `insert()`, `update()`, and `delete()` statements with `.returning(...)`
report the exact number of returned rows, while `text()` statements with
`RETURNING` report `-1`, as do statements executed through DuckDB's
`executemany()` (batched UPDATE and DELETE, and `text()` INSERTs). A Core `insert()` with several parameter sets is
counted in full when it fits in one `insertmanyvalues_page_size` page (1000
rows by default) or uses the register fast path (`duckdb_copy_threshold`, 10,000
rows by default). Between those sizes it runs in pages and `rowcount` reports
the last page only. `column.regexp_match(pattern)` compiles to
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

Named `Enum` types inside `Struct`, `Map`, `Union`, and `FixedArray` reflect as
unnamed `Enum` members with their labels because DuckDB does not report the
enum's type name inside a nested type. A nested type falls back to `NullType`
only when a member type is not recognized; declare those columns in your models.

`Struct` fields (`column["field"]`) and `Map` keys (`column[key]`) return values
with their declared types. `Map` indexing accepts both Python literals and SQL
expressions such as columns, `bindparam()`, and functions.

MAP keys and values honor their declared SQLAlchemy Python bind and result
processors, including `TypeDecorator` and nested MAP/LIST members. SQL NULL,
empty maps and NULL values inside a map remain distinct.

### Enums

`inspector.get_enums()` lists the ENUM types created with `CREATE TYPE` in the
current database and schema, with their labels. Pass `schema="name"` or
`schema="db.schema"` for another schema, or `schema="*"` for all of them.

## Unsupported statements

DuckDB does not support these, so the statements SQLAlchemy emits for them fail:

- savepoints: `Connection.begin_nested()` and `Session.begin_nested()` raise
  `ProgrammingError`. The dialect raises it without sending `SAVEPOINT`, so the
  outer transaction keeps its changes
- adding or dropping UNIQUE, FOREIGN KEY, or CHECK constraints on an existing
  table (`ALTER TABLE ... ADD CONSTRAINT`); see
  [Alembic integration](alembic) for a batch-mode workaround

## Statements the dialect adapts

- row locks: DuckDB has no `SELECT ... FOR UPDATE`, so `with_for_update()` is
  ignored, as on SQLite. DuckDB detects conflicting writes instead: a
  transaction that updates or deletes a row another open transaction changed
  fails with a `TransactionException`. Roll back and run the transaction again
- `select(...).fetch(n, percent=True)` compiles to `LIMIT n PERCENT`, and
  `fetch(n, with_ties=True)` raises `CompileError`
- `tablesample(table, n)` samples `n` percent, as in standard SQL, and
  compiles to `TABLESAMPLE system(n PERCENT)`. DuckDB samples whole vectors
  of 2048 rows with `system`, so use `tablesample(table, func.bernoulli(n))`
  for small tables
- `a.bitwise_xor(b)` compiles to `xor(a, b)`

## Isolation levels

DuckDB has a single transaction isolation level. The dialect accepts
`isolation_level="AUTOCOMMIT"` (no `BEGIN` is issued) and the default
`"READ COMMITTED"`; other levels raise `ArgumentError`.

A raw `COMMIT`, `END`, `ROLLBACK` or `ABORT` statement, with or without `WORK`
or `TRANSACTION`, sent through `exec_driver_sql()` or `text()` inside a
SQLAlchemy transaction commits or rolls back and begins the next DuckDB
transaction, as psycopg2 does. Later statements therefore stay in a
transaction that `commit()` or `rollback()` ends. When no transaction is open,
for example with `isolation_level="AUTOCOMMIT"`, these statements do nothing.

## Numeric precision

`Numeric`/`DECIMAL` values are bound and returned as `decimal.Decimal` without
passing through float. Use `Numeric(asdecimal=False)` or `Float` for floats.
DuckDB binds a `Decimal` parameter as `DOUBLE` when it needs more than 38
digits, which includes a value below 1 with 38 fractional digits, such as
`Decimal("0.00000000000000000000008706567643447616")`. Such a parameter loses
precision even when the target column is `DECIMAL(38, 38)`, and so does the same
number written as a SQL literal. Pass such a value as text inside a cast, for
example `cast(literal(format(value, "f")), Numeric(38, 38))`.
Generic `Float()` and `Float(precision > 24)` compile to `DOUBLE` (64 bits);
`REAL` and `Float(24)` use 32 bits. Existing FLOAT columns retain their original
precision until explicitly migrated.

### Decimal division

DuckDB divides `DECIMAL` values in `DOUBLE`, so `Numeric` division can come back
slightly off, for example `7.00000000000000088818` instead of `7`. Pass
`exact_decimal_division=True` to `create_engine()` to divide exactly:

```python
engine = create_engine("duckdb:///app.duckdb", exact_decimal_division=True)
```

With the option on, `a / b` is exact when both sides are columns or casts with a
declared `Numeric(precision, scale)` or integer type, and at least one side is
`Numeric`. The result is a `DECIMAL(38, R)` rounded half away from zero, where
`R` is the larger of the two scales, at least 16, and limited to what fits in 38
digits. Dividing by zero returns `NULL` instead of infinity or NaN.

Other divisions keep DuckDB's `DOUBLE` result: integer by integer, `Float`,
`Numeric` without precision and scale, bound literals and function results whose
type is unknown, and quotients that could need more than 22 integer digits
(`precision - scale` of the dividend plus the scale of the divisor). Exact
division uses 128-bit integer division and is much slower than `DOUBLE`. On 10
million `DECIMAL(18, 6)` rows it took about 2.5 seconds instead of 30
milliseconds, so turn it on where exact results matter more than speed.

### Floor division

`a // b` compiles to `FLOOR(a / b)`, including for two integer columns. DuckDB
computes `a / b` in `DOUBLE`, so the result is a `DOUBLE`, and it is inexact once
an operand or the quotient is larger than 2**53 (9,007,199,254,740,992). For
example `9007199254740993 // 1` returns `9007199254740992.0`. The result rounds
toward negative infinity like Python, so `-5 // 2` is `-3`.

For exact integer results on large values, use DuckDB's integer division
operator, which truncates toward zero (`-5 // 2` is `-2` there), through
`a.op("//")(b)`, or cast the operands to `Numeric` with
`exact_decimal_division=True`.

## Auto-increment columns

DuckDB does not support PostgreSQL `SERIAL`. For an integer primary key that
SQLAlchemy treats as autoincrement, the dialect creates an implicit
`<table>_<column>_seq` sequence with the table, uses it as the column default,
and drops it with the table:

```python
from sqlalchemy import Column, Integer, MetaData, Table

users = Table("users", MetaData(), Column("id", Integer, primary_key=True))
```

A plain `Identity()` on such a key is backed the same way.
To choose the sequence's name, pass `duckdb_sequence_name` to the column,
for example to match the `SEQUENCE NAME` of a PostgreSQL identity column that clients
call `nextval()` on:

```python
from sqlalchemy import BigInteger, Column, Identity, MetaData, Table

users = Table(
    "users",
    MetaData(),
    Column("id", BigInteger, Identity(), primary_key=True, duckdb_sequence_name="seq_users"),
)
```

The sequence is created in the table's schema and dropped with the table.
Unlike the implicit `<table>_<column>_seq`,
it is created without `IF NOT EXISTS`,
so creating the table fails if a sequence of that name already exists,
rather than adopting one another table may use.
`duckdb_sequence_name` on a column that is not an autoincrement integer primary key
raises `CompileError` before anything is created for that table.
The name is a plain sequence name, not a qualified one:
`"app.seq_users"` creates a sequence whose name contains a dot.
To place the sequence in a schema, set `schema` on the table.

DuckDB cannot make a table own a sequence reliably,
so the dialect drops it when SQLAlchemy drops the table,
from the column that names it.
Alembic's `op.drop_table` and a `drop_all` of reflected tables do not know the name,
so they leave the sequence behind, and creating the table again then fails.
Drop the sequence in the same migration:

```python
op.drop_table("users")
op.execute("DROP SEQUENCE seq_users")
```

A table reflected from DuckDB has a `nextval('seq_users')` default and no
`duckdb_sequence_name`,
so creating it in another database does not create the sequence.
Create the sequence first, or set `duckdb_sequence_name` on the reflected column
and remove its reflected default:

```python
column = reflected_table.c.id
column.server_default = None
column.dialect_options["duckdb"]["sequence_name"] = "seq_users"
```

Declare an explicit `Sequence` only when you need a start value:

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

## DuckDB 2.0

DuckDB 2.0 is supported for local databases on Python 3.11 and newer.

- MotherDuck does not accept DuckDB 2.x clients yet. Install
  `duckdb-sqlalchemy[motherduck]` for MotherDuck, as described in the
  [MotherDuck guide](motherduck#duckdb-versions).
- Statements executed with `duckdb_arrow=True` run as DuckDB relations. See
  [Arrow results on DuckDB 2.0](performance#arrow-results-on-duckdb-20) for
  what that changes.
- Arrow methods called on a DBAPI cursor without `duckdb_arrow=True`, such as
  `result.cursor.to_arrow_table()`, emit a DuckDB `UserWarning` about an extra
  copy of the rows.
- `begin_nested()` raises, as on DuckDB 1.x. DuckDB has no `SAVEPOINT`.
- DuckDB 2.0 can print a `nanobind: leaked ...` report to standard error when
  the interpreter exits, for example after an Arrow table or DataFrame was
  registered in a process that imported `sqlalchemy.orm`. The report comes from
  DuckDB's Python binding while the interpreter shuts down. It does not indicate
  lost data or an open connection, and the exit status is unaffected.
