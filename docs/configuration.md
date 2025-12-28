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

- `:memory:` uses `SingletonThreadPool` so connections share the same in-memory database per thread.
- File paths and MotherDuck default to `NullPool` to avoid multi-writer contention.

Override with `poolclass` if you need a different pooling strategy:

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine("duckdb:///analytics.db", poolclass=QueuePool, pool_size=5)
```

You can also switch the dialect default pool class via URL or env var:

- URL: `duckdb_sqlalchemy_pool=queue` (alias: `pool=queue`)
- Env: `DUCKDB_SQLALCHEMY_POOL=queue`

For long-lived MotherDuck pools, set `pool_pre_ping=True` and consider
`pool_recycle=23*3600` to pick up backend upgrades.
