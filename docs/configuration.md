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
