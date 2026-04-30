---
layout: default
title: Configuration
---

# Configuration

DuckDB configuration can be supplied via `connect_args` or URL query params.

## `connect_args` basics

```python
from sqlalchemy import create_engine

engine = create_engine(
    "duckdb:///:memory:",
    connect_args={
        "read_only": False,
        "config": {
            "memory_limit": "500MB",
            "threads": 4,
        },
    },
)
```

The supported keys are DuckDB configuration settings. See the DuckDB docs for the authoritative list.

## URL query configuration

You can also pass DuckDB settings in the connection URL:

```python
engine = create_engine("duckdb:///analytics.db?threads=4&memory_limit=1GB")
```

If you supply a setting in both the URL and `connect_args["config"]`, the URL value wins.

`read_only` is a top-level connect argument (not a `SET` option), so pass it in
`connect_args`:

```python
engine = create_engine("duckdb:///analytics.db", connect_args={"read_only": True})
```

## Validation and error behavior

- Config key names are identifier-validated before `SET` statements are emitted.
  SQL fragments or punctuation in key names are rejected with `ValueError`.
- Unknown but syntactically valid keys are forwarded to DuckDB and may fail with
  DuckDB's native "unrecognized configuration parameter" error.
- PostgreSQL compatibility aliases from MotherDuck's PG endpoint are normalized
  before `SET`: `compatibility_mode`, `nested_types_as`, and
  `ignore_nanoseconds` become their `pgcompat_*` setting names. Preload the
  relevant extension before using these settings.
- MotherDuck path-query options (`attach_mode`, `session_name`, `access_mode`,
  and related keys) are handled specially for `md:` URLs. See
  [motherduck.md](motherduck) and [connection-urls.md](connection-urls).

## Preload extensions

DuckDB can auto-install and auto-load extensions. You can preload extensions during connection:

```python
engine = create_engine(
    "duckdb:///:memory:",
    connect_args={
        "preload_extensions": ["https"],
        "config": {"s3_region": "ap-southeast-1"},
    },
)
```

For safety, extension names must be plain identifiers (`[A-Za-z0-9_]+`).
Values containing spaces, punctuation, or SQL fragments are rejected.

## Register filesystems

You can register filesystems via `fsspec`:

```python
from fsspec import filesystem
from sqlalchemy import create_engine

engine = create_engine(
    "duckdb:///:memory:",
    connect_args={
        "register_filesystems": [filesystem("gcs")],
    },
)
```

## Pool defaults and concurrency

- Exact `:memory:` uses `SingletonThreadPool`.
- Named in-memory URLs such as `:memory:analytics` and empty database URLs (`duckdb://`) use `QueuePool` for compatibility with `duckdb_engine`.
- Local file paths also use `QueuePool` so ORM and `Session` workloads can reuse a live DuckDB connection.
- MotherDuck defaults to `NullPool` to avoid keeping cloud sessions open unless you opt into pooling.

Override with `poolclass` if you need a different pooling strategy:

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

engine = create_engine("duckdb:///analytics.db", poolclass=NullPool)
```

You can also switch the dialect default pool class via URL or env var:

- URL: `duckdb_sqlalchemy_pool=queue` (alias: `pool=queue`)
- Env: `DUCKDB_SQLALCHEMY_POOL=queue`

For long-lived MotherDuck pools, set `pool_pre_ping=True` and consider
`pool_recycle=23*3600` to pick up backend upgrades.

## Checkpoint helper

DuckDB's `CHECKPOINT` statement can fail on a SQLAlchemy connection after
writes because SQLAlchemy 2.x keeps the connection inside an implicit
transaction until you commit or roll it back.

Use the explicit helper instead:

```python
from sqlalchemy import text

from duckdb_sqlalchemy import checkpoint

with engine.connect() as conn:
    conn.execute(text("insert into events values (1)"))
    checkpoint(conn)
```

`checkpoint(conn)` commits the current transaction before and after the
checkpoint so the connection does not remain in an aborted state. It also
works with raw DuckDB connections.
