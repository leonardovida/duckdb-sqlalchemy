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

## Storage metadata

Use `pragma_storage_info` to inspect DuckDB table storage through SQLAlchemy:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import pragma_storage_info

storage = pragma_storage_info("events")
stmt = select(storage.c.column_name, storage.c.segment_type, storage.c.compression)
```

The helper names DuckDB's released columns by default. On engines that support
the optional segment-info argument, pass `include_segment_info=True`.

## Quack remote queries

DuckDB 1.5.3 ships Quack as a core extension. Use `quack_query` for stateless
remote queries and name the returned columns for SQLAlchemy:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import quack_query

remote = quack_query(
    "quack:localhost",
    "SELECT 42 AS answer",
    columns=["answer"],
    token="MY_QUACK_TOKEN_01234567890ABCDEF",
)
stmt = select(remote.c.answer)
```

## MotherDuck metadata

MotherDuck exposes table functions for account and Dive metadata. The helpers
name the released columns so they are available through SQLAlchemy:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import md_access_tokens, md_list_dives, md_user_info

user_info = md_user_info()
user_stmt = select(
    user_info.c.user_id,
    user_info.c.username,
    user_info.c.org_id,
    user_info.c.org_name,
    user_info.c.org_type,
    user_info.c.region,
)

dives = md_list_dives()
dives_stmt = select(
    dives.c.id,
    dives.c.title,
    dives.c.status,
    dives.c.status_applies_to_version,
    dives.c.required_resources,
)

tokens = md_access_tokens()
tokens_stmt = select(tokens.c.token_name, tokens.c.token_type, tokens.c.expire_at)
```

## MotherDuck Dives as code

Dive helpers expose the SQL functions MotherDuck provides for managing Dives
from a SQL client or deployment script:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import (
    md_create_dive,
    md_get_dive,
    md_list_dive_versions,
    md_update_dive_content,
)

created = md_create_dive(
    title="Sales overview",
    content="export default function Dive() { return null }",
    api_version=1,
    required_resources=[{"url": "md:analytics", "alias": "analytics"}],
)
create_stmt = select(created.c.id, created.c.version_id)

dive = md_get_dive(id="00000000-0000-0000-0000-000000000000")
dive_stmt = select(dive.c.title, dive.c.status, dive.c.content)

updated = md_update_dive_content(
    id="00000000-0000-0000-0000-000000000000",
    content="export default function Dive() { return null }",
)
update_stmt = select(updated.c.version, updated.c.storage_url)

versions = md_list_dive_versions(id="00000000-0000-0000-0000-000000000000")
versions_stmt = select(versions.c.version, versions.c.created_at)
```

## MotherDuck Flights

MotherDuck also exposes preview table functions for Flight metadata. The
read-only helpers are useful for listing Flights, runs, logs, and versions:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import md_flight_runs, md_flight_versions, md_flights

flights = md_flights(limit=10)
flights_stmt = select(flights.c.flight_id, flights.c.flight_name, flights.c.status)

runs = md_flight_runs(flight_id="00000000-0000-0000-0000-000000000000", limit=10)
runs_stmt = select(runs.c.run_number, runs.c.status, runs.c.config, runs.c.started_at)

versions = md_flight_versions(flight_id="00000000-0000-0000-0000-000000000000")
versions_stmt = select(versions.c.flight_version, versions.c.requirements_txt)
```

Mutating Flight functions are available as helpers too:
`md_create_flight`, `md_update_flight`, `md_delete_flight`, `md_run_flight`,
and `md_cancel_flight_run`. They only execute when the SQLAlchemy statement is
run. `md_run_flight` accepts a `config` map to override a Flight's config for
that run only; run result helpers expose the effective `config` column. Config
map keys must be strings, non-empty, and cannot contain `=` or NUL bytes.
Config values must be strings or `None`, and cannot contain NUL bytes.

The older `md_*job*` helper names remain as deprecated compatibility aliases.
They compile through the current Flight functions while preserving legacy
`job_*` column access where possible.

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
`insertmanyvalues_page_size` (defaults to 1000). The older
`duckdb_insertmanyvalues_page_size` alias still works but is deprecated.

## COPY helpers

Use COPY to load files directly into DuckDB without row-wise inserts:

```python
from duckdb_sqlalchemy import copy_from_parquet, copy_from_csv

with engine.begin() as conn:
    copy_from_parquet(conn, "events", "data/events.parquet")
    copy_from_csv(conn, "events", "data/events.csv", header=True)
```

For safety, string table names, column names, and COPY option keys must be
identifiers. Dotted paths like `schema.events` are supported, but SQL
fragments are rejected.

If you need quoted or mixed-case identifiers, pass a SQLAlchemy `Table` object
instead of a plain string so SQLAlchemy handles quoting.

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

Quack remotes can be attached the same way when a Quack server is available:

```python
conn.execute(
    text(
        "ATTACH 'quack:localhost' AS remote_db "
        "(TOKEN 'MY_QUACK_TOKEN_01234567890ABCDEF')"
    )
)
rows = conn.execute(text("SELECT * FROM remote_db.events LIMIT 10")).fetchall()
```

## Notes

- Column naming for table functions requires SQLAlchemy >= 1.4 (uses `table_valued`).
