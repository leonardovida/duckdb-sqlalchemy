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

`md_user_info().c.region` is available with DuckDB 1.5.3 and newer. Older
MotherDuck clients expose the other five columns.

## MotherDuck Guides

MotherDuck [Guides](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/guides/)
are markdown documents that capture data context for agents. The Guide helpers
name each table function's released columns and bind named arguments through
SQLAlchemy:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import md_create_guide, md_get_guide, md_list_guides

guides = md_list_guides(topic="revenue", limit=10)
list_stmt = select(guides.c.id, guides.c.title, guides.c.current_version)

guide = md_get_guide(id="00000000-0000-0000-0000-000000000000")
read_stmt = select(guide.c.title, guide.c.content)

created = md_create_guide(title="Metric definitions", content="# Revenue\n...")
create_stmt = select(created.c.id, created.c.current_version)
```

The same module exposes `md_update_guide`, `md_update_guide_metadata`,
`md_set_guide_access`, `md_list_guide_versions`, and `md_delete_guide`.
Guide functions require a MotherDuck client version that exposes them; they are
available with DuckDB 1.5.5.

## MotherDuck AI classification

The [`prompt_jev`](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/ai-functions/prompt-jev/)
helper supports the function's positional input and instructions plus named
`choice`, `score`, `noul`, `questions`, and `batch_size` arguments:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import prompt_jev

stmt = select(
    prompt_jev(
        "I was charged twice",
        "Which team should handle this?",
        choice=["billing", "technical", "sales"],
    ).label("routing")
)
```

MotherDuck currently limits `prompt_jev` to the regions listed in its function
reference. The classification runs remotely and may incur usage charges.

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
    md_update_dive_status,
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

status = md_update_dive_status(
    id="00000000-0000-0000-0000-000000000000",
    status="endorsed",
    version=2,
)
status_stmt = select(status.c.id, status.c.status, status.c.status_set_by)

versions = md_list_dive_versions(id="00000000-0000-0000-0000-000000000000")
versions_stmt = select(versions.c.version, versions.c.created_at)
```

## MotherDuck Flights

MotherDuck also exposes preview table functions for Flight metadata. The
read-only helpers are useful for listing Flights, runs, logs, and versions:

```python
from sqlalchemy import select
from duckdb_sqlalchemy import (
    md_get_flight_logs,
    md_get_flight_run,
    md_list_flight_runs,
    md_list_flight_versions,
    md_list_flights,
)

flights = md_list_flights(limit=10)
flights_stmt = select(flights.c.flight_id, flights.c.flight_name, flights.c.status)

runs = md_list_flight_runs(
    flight_id="00000000-0000-0000-0000-000000000000",
    limit=10,
)
runs_stmt = select(runs.c.run_number, runs.c.status, runs.c.config, runs.c.started_at)

run = md_get_flight_run(
    flight_id="00000000-0000-0000-0000-000000000000",
    run_number=1,
)
run_stmt = select(run.c.run_number, run.c.status, run.c.config, run.c.started_at)

logs = md_get_flight_logs(
    flight_id="00000000-0000-0000-0000-000000000000",
    run_number=1,
)
logs_stmt = select(logs.c.line_number, logs.c.reported_at, logs.c.line)

versions = md_list_flight_versions(
    flight_id="00000000-0000-0000-0000-000000000000"
)
versions_stmt = select(
    versions.c.flight_version,
    versions.c.requirements_txt,
    versions.c.max_runtime_sec,
)
```

Mutating Flight functions are available as helpers too:
`md_create_flight`, `md_update_flight`, `md_delete_flight`, `md_run_flight`,
and `md_cancel_flight_run`. They only execute when the SQLAlchemy statement is
run. `md_run_flight` accepts a `config` map to override a Flight's config for
that run only; run result helpers expose the effective `config` column. Config
map keys must be strings, non-empty, and cannot contain `=` or NUL bytes.
Config values must be strings or `None`, and cannot contain NUL bytes.

The older noun-style `md_flights`, `md_flight_runs`, `md_flight_logs`, and
`md_flight_versions` helpers remain as deprecated compatibility aliases for the
verb-style functions above. The `md_*job*` helper names also remain as
deprecated compatibility aliases while preserving legacy `job_*` column access
where possible. The deprecated `md_flight_logs` and `md_job_run_logs` helpers
also preserve their historical `logs` column as an alias for the current
line-by-line `line` result.

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

## Export a query to Parquet

Use `copy_to_parquet` to export a SQLAlchemy `select()` directly through
DuckDB, without fetching the query's rows into Python:

```python
from pathlib import Path
from sqlalchemy import bindparam, select
from duckdb_sqlalchemy import copy_to_parquet

query = select(events.c.event_id, events.c.ts).where(
    events.c.event_id.in_(bindparam("event_ids", expanding=True))
)
with engine.connect() as conn:
    result = copy_to_parquet(
        conn,
        query,
        Path("selected-events.parquet"),
        parameters={"event_ids": [1, 2]},
        compression="zstd",
    )
    exported_rows = result.scalar_one()
```

The helper accepts SQLAlchemy `Select` and compound selects such as
`union_all()`. Pass one parameter mapping through `parameters`; normal typed
bind processing and expanding `IN` parameters are preserved. Raw SQL strings,
`text()` statements, and inserts/updates/deletes are not accepted. SQLAlchemy
expressions remain trusted application code.

COPY options such as `compression` are passed as keyword arguments. The format
is always Parquet and cannot be overridden. Paths may be strings or `Path`
objects and are escaped as SQL literals for compatibility with older DuckDB
versions. The helper is tested with DuckDB 1.3.0 and 1.5.5 and SQLAlchemy 2.0;
older DuckDB versions are not verified for this workflow.

File writes follow DuckDB COPY semantics: a single-file export replaces an
existing destination, and rolling back the SQL transaction does not undo the
file write. Use a distinct destination when retaining a previous export
matters. Multi-file options such as `partition_by` have their own DuckDB
append/overwrite behavior. The destination must be accessible to the executing
DuckDB instance; remote storage needs the relevant filesystem configuration.
This example is verified with local DuckDB, not MotherDuck remote storage.

## Export a partitioned snapshot

Use the existing COPY options to split a filtered query into a Hive partitioned
Parquet dataset. The destination is a directory; partition columns must appear
in the query's output:

```python
from pathlib import Path
from sqlalchemy import bindparam, select
from duckdb_sqlalchemy import copy_to_parquet, read_parquet

# Choose a new directory for each snapshot.
destination = Path("snapshots/events-2026-09-21")
query = select(events.c.event_id, events.c.region).where(
    events.c.event_id >= bindparam("minimum_id")
)
with engine.connect() as conn:
    count = copy_to_parquet(
        conn,
        query,
        destination,
        parameters={"minimum_id": 100},
        partition_by=["region"],
        compression="zstd",
    ).scalar_one()
    rows = []
    if count:
        snapshot = read_parquet(
            str(destination / "**" / "*.parquet"),
            columns=["event_id", "region"],
            hive_partitioning=True,
        )
        rows = conn.execute(select(snapshot.c.event_id, snapshot.c.region)).all()
```

Files live under directories such as `region=eu/`. Hive partitioning restores
the partition columns from those paths when scanning. An empty selection writes
no Parquet files, so check the COPY count before scanning the glob.

Run the self-contained [partitioned Parquet example](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/partitioned_parquet.py)
with `uv run python examples/partitioned_parquet.py` from the repository. It
creates temporary sample data, exports a bound query, and verifies the recovered
rows and partition directories. This local workflow is verified with DuckDB
1.3.0 / SQLAlchemy 2.0.0 and DuckDB 1.5.5 / SQLAlchemy 2.0.52.

By default, a second export to a nonempty destination fails. Prefer a distinct
directory per snapshot. `append=True` adds files; it does not update or
deduplicate rows, so repeating a batch can duplicate data. File writes survive
transaction rollback and are not an atomic dataset publication mechanism.
See DuckDB's [partitioned write documentation](https://duckdb.org/docs/current/data/partitioning/partitioned_writes.html)
for overwrite and append options. Remote storage is not tested by this recipe.

### Preserve string partition keys

Hive readers infer partition types from directory names. A string key such as
`"123"` or `"2026-09-23"` can return as an integer or date. Set `hive_types` when
reading a snapshot to preserve those identifiers as strings:

```python
snapshot = read_parquet(
    "snapshots/events/**/*.parquet",
    columns=["id", "region"],
    hive_partitioning=True,
    hive_types={"region": "VARCHAR"},
)
rows = conn.execute(select(snapshot.c.id, snapshot.c.region)).all()
```

`columns` names the SQLAlchemy columns. It does not set the types DuckDB infers.
Use DuckDB type names in `hive_types`. See
[Hive partition types](https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html#hive-types).

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
