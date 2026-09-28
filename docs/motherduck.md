---
layout: default
title: MotherDuck
---

# MotherDuck

MotherDuck connections use the `md:` database prefix.
Database names may not contain commas.

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
The environment token is not added when the URL or `connect_args` already carry a credential
(`token`, `motherduck_token`, `motherduck_oauth_token`, `oauth_token`, or `slt`), and it is never added
for local DuckDB files.
If you authenticate with an OAuth flow instead, pass `motherduck_oauth_token` through the
URL or `connect_args["config"]`.

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

## Multiprocessing (fork)

DuckDB's Python client is not fork-safe, so `multiprocessing` children created with
`fork` can fail when opening new connections (commonly observed with MotherDuck or
file-backed databases). Use the `spawn` or `forkserver` start methods and create
engines/connections inside the child process.

## Options

### Connection-string parameters (instance cache key)

MotherDuck (and DuckDB) cache client instances by database path/connection string. Parameters that affect
routing or instance identity must end up in the database string so pooling and caching behave predictably.
Pass them as URL query params or via `connect_args["config"]`; the dialect moves them into the database
string for MotherDuck connections.

Parameters that are treated as part of the database string:

- `user`
- `host`, `region_host`, `port`, `tls`, `grpc_local_subchannel_pool`
- `session_name` (read-scaling affinity)
- `attach_mode` (`workspace` or `single`)
- `access_mode` (`read_only` for read-scaling tokens)
- `dbinstance_inactivity_ttl` (preferred; `motherduck_dbinstance_inactivity_ttl`
  remains supported but is deprecated)
- `saas_mode` (`pgendpoint` is supported when you need PG endpoint-compatible routing)
- `motherduck_enable_server_side_temp_tables`
- `cache_buster`

For backward compatibility the dialect also accepts `session_hint`,
`motherduck_session_hint`, `motherduck_session_name`,
`motherduck_attach_mode`, `motherduck_saas_mode`, and `cachebust`, but it
normalizes them to the canonical keys above and emits a `DeprecationWarning`.
The same applies to the `motherduck_host`, `motherduck_region_host`,
`motherduck_port`, `motherduck_use_tls`, and
`motherduck_grpc_local_subchannel_pool` spellings.

Example:

```
duckdb:///md:my_db?attach_mode=single&access_mode=read_only&session_name=team-a
```

For local or staging routing, pass the endpoint override the same way:

```
duckdb:///md:my_db?host=localhost&port=1984&tls=off
```

If you pass these in `connect_args["config"]`, the dialect will move them into the database string automatically.

### Config parameters

Other DuckDB settings can be passed as URL query params or via `connect_args["config"]`:

- Any DuckDB `SET`-table config option (for example `memory_limit`, `threads`)
- MotherDuck startup auth keys such as `motherduck_token`, `token`, `motherduck_oauth_token`, and `oauth_token`

Example with a MotherDuck host override / PG endpoint-style routing:

```python
engine = create_engine(
    "duckdb:///md:my_db?host=custom.motherduck.com&port=443&tls=true&saas_mode=pgendpoint"
)
```

## Recommended defaults for apps/BI

- Use `attach_mode=single` unless you need workspace-wide attach behavior.
- For read scaling tokens, add `access_mode=read_only` and a stable `session_name`.

## Helpers

### MotherDuck URL builder

Use `MotherDuckURL` to validate routing/instance-cache parameters; the dialect moves them into the database string when it connects:

```python
from duckdb_sqlalchemy import MotherDuckURL

url = MotherDuckURL(
    database="md:my_db",
    attach_mode="single",
    access_mode="read_only",
    session_name="team-a",
    query={"memory_limit": "1GB"},
)
```

### Explicit read-scaling engine

```python
from duckdb_sqlalchemy import create_motherduck_engine, stable_session_name

engine = create_motherduck_engine(
    database="md:analytics",
    attach_mode="single",
    access_mode="read_only",
    session_name=stable_session_name("user-123", salt="org-1"),
    performance=True,
)
```

### Read scaling session names

Use a stable hash to keep per-user affinity:

```python
from duckdb_sqlalchemy import stable_session_name

session_name = stable_session_name("user-123", salt="org-1")
```

### Read scaling consistency notes

Read scaling routes queries to read replicas. If you need the freshest data,
use a non-read-scaling token or route those queries to a separate writer
engine. For per-user affinity, keep a stable `session_name`; to refresh
routing, rotate the `session_name` or recycle the connection/pool.

### Performance-first engine helper

`create_motherduck_engine(..., performance=True)` applies MotherDuck-friendly pooling defaults
(`QueuePool`, `pool_pre_ping=True`, `pool_recycle=23h`). Standard `create_engine`
keyword arguments such as `pool_size`, `max_overflow`, `echo`, or `connect_args` are passed
to `create_engine`; the remaining keyword arguments become URL params. Pool sizing
arguments without an explicit `poolclass` select `QueuePool`:

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

Retries only happen for idempotent statements, and only when no earlier
statement in the current transaction would be lost: either outside a
transaction, or when the failed statement was the first one in it (the dialect
rolls back and begins again). Otherwise the original error is raised. Queries
that call `md_*` table functions, `nextval`, or `setval` are never retried.

### Multiple client-side instances

To force distinct client instances, rotate across multiple database paths:

```python
from duckdb_sqlalchemy import create_engine_from_paths

engine = create_engine_from_paths(
    ["md:my_db?user=1", "md:my_db?user=2", "md:my_db?user=3"],
    pool_size=3,
    max_overflow=0,
)
```

Passing `pool_size` or `max_overflow` selects `QueuePool`, and each new pooled
connection rotates to the next path.
