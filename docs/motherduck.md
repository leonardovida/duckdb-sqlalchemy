---
layout: default
title: MotherDuck
---

# MotherDuck

MotherDuck connections use the `md:` database prefix.

```python
from sqlalchemy import create_engine

engine = create_engine("duckdb:///md:my_db")
```

## Quick start with config

DuckDB settings (threads, memory limits, etc.) can be passed via `connect_args`:

```python
engine = create_engine(
    "duckdb:///md:my_db",
    connect_args={"config": {"threads": 4, "memory_limit": "1GB"}},
)
```

## Tokens

Set `MOTHERDUCK_TOKEN` (or `motherduck_token`) in the environment and it will be picked up automatically when you connect to `md:` databases.

```bash
export MOTHERDUCK_TOKEN="..."
```

You can also pass the token in the URL or via `connect_args`:

```python
engine = create_engine(
    "duckdb:///md:my_db",
    connect_args={"config": {"motherduck_token": "..."}},
)
```

## Options

### Connection-string parameters (instance cache key)

MotherDuck (and DuckDB) cache client instances by database path/connection string. Parameters that affect
routing or instance identity must live in the database string so pooling and caching behave predictably.
You can still pass them via `connect_args["config"]`, but the dialect will move them into the database
string for MotherDuck connections.

Parameters that are treated as part of the database string:

- `user`
- `session_hint` (read-scaling affinity)
- `attach_mode` (`workspace` or `single`)
- `access_mode` (`read_only` for read-scaling tokens)
- `dbinstance_inactivity_ttl` (alias for `motherduck_dbinstance_inactivity_ttl`)
- `saas_mode`
- `cache_buster`

Example:

```
duckdb:///md:my_db?attach_mode=single&access_mode=read_only&session_hint=team-a
```

If you pass these in `connect_args["config"]`, the dialect will move them into the database string automatically.

### Config parameters

Other DuckDB settings can be passed as URL query params or via `connect_args["config"]`:

- Any DuckDB `SET`-table config option (for example `memory_limit`, `threads`)

## Recommended defaults for apps/BI

- Use `attach_mode=single` unless you need workspace-wide attach behavior.
- For read scaling tokens, add `access_mode=read_only` and a stable `session_hint`.

## Helpers

### MotherDuck URL builder

Use `MotherDuckURL` to ensure routing/instance-cache parameters live in the database string:

```python
from duckdb_sqlalchemy import MotherDuckURL

url = MotherDuckURL(
    database="md:my_db",
    attach_mode="single",
    access_mode="read_only",
    session_hint="team-a",
    query={"memory_limit": "1GB"},
)
```

### Explicit read-scaling engine

```python
from duckdb_sqlalchemy import create_motherduck_engine, stable_session_hint

engine = create_motherduck_engine(
    database="md:analytics",
    attach_mode="single",
    access_mode="read_only",
    session_hint=stable_session_hint("user-123", salt="org-1"),
    performance=True,
)
```

### Read scaling session hints

Use a stable hash to keep per-user affinity:

```python
from duckdb_sqlalchemy import stable_session_hint

session_hint = stable_session_hint("user-123", salt="org-1")
```

### Read scaling consistency notes

Read scaling routes queries to read replicas. If you need the freshest data,
use a non-read-scaling token or route those queries to a separate writer
engine. For per-user affinity, keep a stable `session_hint`; to refresh
routing, rotate the `session_hint` or recycle the connection/pool.

### Performance-first engine helper

`create_motherduck_engine(..., performance=True)` applies MotherDuck-friendly pooling defaults
(`QueuePool`, `pool_pre_ping=True`, `pool_recycle=23h`):

```python
from duckdb_sqlalchemy import create_motherduck_engine

engine = create_motherduck_engine(
    database="md:my_db",
    attach_mode="single",
    performance=True,
)
```

### Transient retry (opt-in)

For read-only statements you can opt-in to transient retries:

```python
from sqlalchemy import text

with engine.connect() as conn:
    conn = conn.execution_options(
        duckdb_retry_on_transient=True,
        duckdb_retry_count=2,
        duckdb_retry_backoff=0.5,
    )
    conn.execute(text("select 1"))
```

### Multiple client-side instances

To force distinct client instances, rotate across multiple database paths:

```python
from duckdb_sqlalchemy import create_engine_from_paths

engine = create_engine_from_paths(
    ["md:my_db?user=1", "md:my_db?user=2", "md:my_db?user=3"],
)
```
