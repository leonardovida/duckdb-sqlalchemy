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

## Choose a connection pattern

Each MotherDuck account runs its queries on its own Duckling, and every
connection made with that account's read-write token shares it. Pick the
pattern by who reads and who writes:

| Workload | Token | Engine | Example |
| --- | --- | --- | --- |
| Scripts, notebooks, ELT jobs | Read-write | `create_motherduck_engine(database="md:<db>", attach_mode="single")` | [`motherduck_quickstart.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_quickstart.py) |
| Several processes writing the same database | Read-write | One engine per process, retry the transaction on write conflicts | [`motherduck_concurrent_writers.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_concurrent_writers.py) |
| Dashboards with many concurrent users | Read scaling | One engine per user with a stable `session_name` | [`motherduck_read_scaling.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_read_scaling.py) |
| A backend serving anonymous reads | Read scaling | One engine whose pool rotates across session names | [`motherduck_read_scaling.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_read_scaling.py) |
| Customer-facing analytics with isolated tenants | One service account per tenant | One cached engine per tenant token | [Tenants](#one-duckling-per-tenant) |
| Data apps in MotherDuck | Read-write | Deploy Dives with the `md_*_dive` helpers | [`motherduck_dive.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_dive.py) |

Every example creates its own uniquely named database and drops it when it
finishes, so it can run against a real account.

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
- `attach_mode` (`workspace` or `single`. `single` attaches only the named
  database and starts faster, but needs a database name: `md:?attach_mode=single`
  fails to connect)
- `access_mode` (`read_only` is accepted, but a read-write token can still
  write through such a connection. Use a read scaling token to make readers
  read-only)
- `dbinstance_inactivity_ttl` (preferred; `motherduck_dbinstance_inactivity_ttl`
  remains supported but is deprecated)
- `connection_timeout` (bounds how long opening the MotherDuck connection may
  take. `motherduck_connection_timeout` is accepted too)
- `saas_mode` (`pgendpoint` is supported when you need PG endpoint-compatible routing)
- `cache_buster`

Interval parameters (`connection_timeout`, `dbinstance_inactivity_ttl`) accept
DuckDB interval strings such as `30s` or `15m`, or a `datetime.timedelta`.

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

### Connection timeout

By default the MotherDuck extension waits for as long as the service asks it
to, for example while a dedicated Duckling is still starting. Set
`connection_timeout` to fail fast instead:

```python
from datetime import timedelta

from duckdb_sqlalchemy import create_motherduck_engine

engine = create_motherduck_engine(
    database="md:analytics",
    connection_timeout=timedelta(seconds=30),
)
```

The timeout covers the token exchange and the initial attach. It is a soft
bound, so a connection can still succeed shortly after the deadline. When it
expires, connecting raises `sqlalchemy.exc.ProgrammingError` wrapping a DuckDB
`InvalidInputException` whose message says `Timed out connecting to
MotherDuck`. The setting needs a MotherDuck extension that supports it (the one
for DuckDB 1.5.6 does), and older extensions ignore it. The
`MOTHERDUCK_CONNECTION_TIMEOUT` environment variable sets the same timeout
without changing the URL.

### Server-side temporary tables

`motherduck_enable_server_side_temp_tables` is a per-connection MotherDuck
setting, not a startup parameter. The dialect runs
`SET motherduck_enable_server_side_temp_tables = 'true'` on every new pooled
connection when you pass it as a URL parameter, in `connect_args["config"]`,
or to `create_motherduck_engine`:

```python
engine = create_motherduck_engine(
    database="md:analytics",
    motherduck_enable_server_side_temp_tables=True,
)
```

## Recommended defaults for apps/BI

- Use `attach_mode=single` unless you need workspace-wide attach behavior.
- Serve dashboards and other read-heavy traffic with a read scaling token and a
  stable `session_name` per end user.
- Keep writers on a read-write token, and give heavy pipelines their own
  database or service account so they do not compete for one Duckling.

## Reader pools with read scaling

A [read scaling token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/read-scaling/)
connects to a pool of read-only Duckling replicas: 4 by default, configurable
up to 16. MotherDuck sends every connection with the same `session_name` to the
same replica, so the session name decides how readers spread across the pool.

Give each end user a stable session name to keep their queries on one replica
with a warm cache. `stable_session_name` hashes the user id so it does not
appear in MotherDuck logs. Cache one engine per user:

```python
import os
from functools import lru_cache

from duckdb_sqlalchemy import create_motherduck_engine, stable_session_name


@lru_cache(maxsize=1024)
def engine_for_user(user_id: str):
    return create_motherduck_engine(
        database="md:analytics",
        attach_mode="single",
        session_name=stable_session_name(user_id, salt=os.environ["SESSION_SALT"]),
        motherduck_token=os.environ["MOTHERDUCK_READ_SCALING_TOKEN"],
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
    )
```

A token passed to `create_motherduck_engine` goes into the DuckDB config
instead of the engine URL, and overrides `MOTHERDUCK_TOKEN` for that engine
only, so a process can hold a writer engine and reader engines side by side.

When requests are not tied to a user, one engine can spread its pooled
connections across the replicas instead. `create_engine_from_paths` opens each
new connection with the next path, so a pool as large as the replica pool
holds one connection per replica:

```python
from duckdb_sqlalchemy import create_engine_from_paths

readers = create_engine_from_paths(
    [f"md:analytics?attach_mode=single&session_name=reader-{i}" for i in range(4)],
    connect_args={"config": {"motherduck_token": os.environ["MOTHERDUCK_READ_SCALING_TOKEN"]}},
    pool_size=4,
    max_overflow=0,
    pool_pre_ping=True,
)
```

### Read your own writes

Replicas pick up new data about once a minute. When a reader has to see a
write right away, publish a snapshot from the writer after the write commits,
then refresh the database on the reader:

```python
from sqlalchemy import text

with writer.begin() as conn:
    conn.execute(text("INSERT INTO orders SELECT * FROM staging_orders"))

with writer.connect() as conn:
    conn.execute(text("CREATE SNAPSHOT OF analytics"))

with readers.connect() as conn:
    conn.execute(text("REFRESH DATABASE analytics"))
```

Run `CREATE SNAPSHOT` outside the write transaction: inside it, the
uncommitted changes of that transaction are discarded. Creating a snapshot
interrupts running queries on the database, and `REFRESH DATABASE` only works
on read scaling connections. To move a user to another replica, change their
`session_name` or dispose of their engine.

## Multiple writer processes

Processes that connect with the same read-write token share one Duckling, so
workers, orchestrator tasks and web servers can write to the same database at
the same time. DuckDB checks for conflicts when a transaction commits:

- Inserts from different processes do not conflict, and run side by side.
- When two transactions update or delete the same row, the first commit wins.
  The others raise `sqlalchemy.exc.OperationalError` wrapping a DuckDB
  `TransactionException` whose message contains "Conflict". The losing
  transaction is rolled back, and it can be retried as a whole.

```python
import random
import time

import duckdb
from sqlalchemy.exc import OperationalError


def run_with_conflict_retry(engine, work, attempts=10):
    for attempt in range(1, attempts + 1):
        try:
            with engine.begin() as conn:
                return work(conn)
        except OperationalError as exc:
            conflict = isinstance(exc.orig, duckdb.TransactionException) and (
                "conflict" in str(exc.orig).lower()
            )
            if not conflict or attempt == attempts:
                raise
            time.sleep(random.uniform(0.05, 0.2) * attempt)
```

Create the engine inside each process, and start processes with `spawn` or
`forkserver` (see [Multiprocessing](#multiprocessing-fork)). Concurrent DDL on
the same objects also conflicts, so create tables before starting the writers.
All writers share the Duckling's memory and CPU: give a heavy pipeline its own
database, or its own service account and therefore its own Duckling, rather
than adding more writer processes. The opt-in transient retry below does not
retry write conflicts, because only the application knows whether the
transaction can run again.

## One Duckling per tenant

For customer-facing analytics, MotherDuck recommends
[one service account per tenant](https://motherduck.com/docs/key-tasks/customer-facing-analytics/).
Each service account has its own Duckling, sized for that tenant, and its own
tokens. Keep one engine per tenant and choose it per request:

```python
@lru_cache(maxsize=256)
def engine_for_tenant(tenant: str):
    return create_motherduck_engine(
        database=f"md:{tenant}_analytics",
        attach_mode="single",
        motherduck_token=load_read_scaling_token(tenant),
        performance=True,
    )
```

`load_read_scaling_token` stands for your own secret store. Service accounts
and their tokens are created with the
[MotherDuck REST API](https://motherduck.com/docs/sql-reference/rest-api/users-create-service-account/).

## Dives

Dives are data apps stored in MotherDuck. Build the tables they read with
SQLAlchemy, and create, version and release the Dive itself with the
`md_*_dive` helpers. See [MotherDuck Dives](dives).

## Helpers

### MotherDuck URL builder

Use `MotherDuckURL` to validate routing/instance-cache parameters. The dialect moves them into the database string when it connects:

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

A token passed to `MotherDuckURL` stays in `URL.query`, so `str(url)` shows
it. `create_motherduck_engine` and `create_engine_from_paths` move tokens into
`connect_args["config"]` instead, so the engine URL and its `repr` do not
contain them. Prefer those helpers, or the `MOTHERDUCK_TOKEN` environment
variable, whenever the URL may be logged.
### Database string helpers

`append_query_to_database` adds routing params to an `md:` database string.
Keys you pass replace the same keys already in the string.
`validate_motherduck_database_name` raises `ValueError` for names DuckDB cannot
route, such as names that contain a comma, and does nothing for non-MotherDuck
databases:

```python
from duckdb_sqlalchemy import (
    append_query_to_database,
    validate_motherduck_database_name,
)

append_query_to_database("md:analytics", {"attach_mode": "single"})
# 'md:analytics?attach_mode=single'

validate_motherduck_database_name("md:analytics")
```

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

An error is transient when it reports HTTP 429, 502, 503, or 504, or says the
service is temporarily unavailable or rate limited. Retries only happen for
idempotent statements, and only when no earlier statement in the current transaction would be lost: either outside a
transaction, or when the failed statement was the first one in it (the dialect
rolls back and begins again). Otherwise the original error is raised. Queries
that call `md_*` or `ducklake_*` functions, `*_execute` functions such as
`postgres_execute`, `nextval`, or `setval` are never retried, and neither are
`EXPLAIN ANALYZE` statements. Only errors raised while the statement executes are retried; an error raised later while
rows are fetched from a streaming result is not.

Calls made through the raw driver connection count as earlier transaction work
too, so the dialect will not roll them back silently to retry a later read.
Opaque SQL functions (`query`, `quack_query`, `postgres_query`, `mysql_query`,
`sqlite_query`) are excluded because bound SQL can contain side effects that
the statement text does not reveal. Arbitrary user functions or views can also
have side effects: enable retries only for workloads you know are idempotent.

### Multiple client-side instances

DuckDB caches one client instance per database string, and every connection
from that instance shares its local state. To give pooled connections separate
client instances, rotate across database strings that differ only in a
parameter such as `user`. They still run on the same Duckling. To spread
connections across read scaling replicas, vary `session_name` instead (see
[reader pools](#reader-pools-with-read-scaling)).

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
