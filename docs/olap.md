---
layout: default
title: OLAP workflows
---

# OLAP workflows

DuckDB exposes analytics-friendly table functions like `read_parquet` and `read_csv_auto`. The helpers in `duckdb_sqlalchemy.olap` make these easy to use with SQLAlchemy.

```python
from sqlalchemy import select
from duckdb_sqlalchemy import read_parquet, read_csv_auto

parquet = read_parquet("data/events.parquet", columns=["event_id", "ts"])
stmt = select(parquet.c.event_id, parquet.c.ts)

csv = read_csv_auto("data/events.csv", columns=["event_id", "ts"])
stmt = select(csv.c.event_id, csv.c.ts)
```

## Explicit CSV settings

Use `read_csv` when you need to control parsing options:

```python
from duckdb_sqlalchemy import read_csv

csv = read_csv(
    "data/events.csv",
    columns=["event_id", "ts"],
    header=True,
    delim="|",
)
stmt = select(csv.c.event_id, csv.c.ts)
```

## Other table functions

Use `table_function` for any DuckDB table function that does not have a helper:

```python
from duckdb_sqlalchemy import table_function

parquet = table_function(
    "read_parquet",
    "data/partitioned/events/*.parquet",
    columns=["event_id", "ts"],
    hive_partitioning=True,
)
stmt = select(parquet.c.event_id, parquet.c.ts)
```

## Arrow results

For large reads, you can request Arrow tables directly:

```python
from pyarrow import Table as ArrowTable
from sqlalchemy import select

with engine.connect().execution_options(duckdb_arrow=True) as conn:
    result = conn.execute(select(parquet.c.event_id, parquet.c.ts))
    table = result.arrow  # or result.all()
    assert isinstance(table, ArrowTable)
```

Notes:

- Arrow results consume the cursor; fetch rows or Arrow, not both.
- Requires `pyarrow` in your environment.

## Streaming reads

For large result sets, combine `stream_results` with a larger `arraysize`:

```python
with engine.connect().execution_options(stream_results=True, duckdb_arraysize=10_000) as conn:
    result = conn.execute(select(parquet.c.event_id, parquet.c.ts))
    for row in result:
        ...
```

`duckdb_arraysize` maps to the DBAPI cursor arraysize that `fetchmany()` uses.

## Bulk writes

For large `INSERT` executemany workloads, the dialect can register a pandas/Arrow
object and run `INSERT INTO ... SELECT ...` internally. Control the threshold
with `duckdb_copy_threshold`:

```python
rows = [{"event_id": 1, "ts": "2024-01-01"}, {"event_id": 2, "ts": "2024-01-02"}]
with engine.connect().execution_options(duckdb_copy_threshold=10000) as conn:
    conn.execute(events.insert(), rows)
```

If `pyarrow`/`pandas` are unavailable, the dialect falls back to regular
`executemany`. The bulk-register path is skipped when `RETURNING` or
`ON CONFLICT` is in use.

On SQLAlchemy 2.x you can also tune multi-row INSERT batching with
`duckdb_insertmanyvalues_page_size` (defaults to 1000).

## COPY helpers

Use COPY to load files directly into DuckDB without row-wise inserts:

```python
from duckdb_sqlalchemy import copy_from_parquet, copy_from_csv

with engine.begin() as conn:
    copy_from_parquet(conn, "events", "data/events.parquet")
    copy_from_csv(conn, "events", "data/events.csv", header=True)
```

For row iterables, you can stream to a temporary CSV in chunks:

```python
from duckdb_sqlalchemy import copy_from_rows

rows = ({"id": i, "name": f"user-{i}"} for i in range(1_000_000))
with engine.begin() as conn:
    copy_from_rows(conn, "users", rows, columns=["id", "name"], chunk_size=100_000)
```

## ATTACH for multi-database analytics

DuckDB can query across multiple databases in a single session:

```python
from sqlalchemy import create_engine, text

conn = create_engine("duckdb:///local.duckdb").connect()
conn.execute(text("ATTACH 'analytics.duckdb' AS analytics"))
rows = conn.execute(text("SELECT * FROM analytics.events LIMIT 10")).fetchall()
```

## Notes

- Column naming for table functions requires SQLAlchemy >= 1.4 (uses `table_valued`).
