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
| `HugeInteger`, `UHugeInteger`, `VarInt` | HUGEINT, UHUGEINT, VARINT | 128-bit and variable-length integers (VarInt requires DuckDB >= 1.0) |
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
    Column("trace_id", VarInt),  # requires DuckDB >= 1.0
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
    duckdb_insertmanyvalues_page_size=1000,
) as conn:
    conn.execute(stmt)
```

- `duckdb_arrow`: return Arrow tables for SELECTs (`result.arrow` or `result.all()`); requires `pyarrow`.
- `duckdb_copy_threshold`: for large INSERT executemany, register a pandas/Arrow object and run `INSERT INTO ... SELECT ...`.
- `duckdb_insertmanyvalues_page_size`: batch size for SQLAlchemy 2.x multi-row VALUES inserts.
- `duckdb_arraysize`: cursor fetch size for `stream_results` / `fetchmany` workloads.

Arrow results consume the cursor; fetch rows or Arrow, not both.

## Auto-increment columns

DuckDB does not support PostgreSQL `SERIAL`. Use a `Sequence` for auto-incrementing primary keys:

```python
from sqlalchemy import Column, Integer, Sequence, Table, MetaData

user_id_seq = Sequence("user_id_seq")
users = Table(
    "users",
    MetaData(),
    Column("id", Integer, user_id_seq, server_default=user_id_seq.next_value(), primary_key=True),
)
```

## Pandas chunksize

Older DuckDB versions (< 0.5.0) may have issues with `pandas.read_sql(..., chunksize=...)`. If you hit errors, use `chunksize=None` or upgrade DuckDB.
