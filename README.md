# duckdb_engine

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/duckdb-engine)](https://pypi.org/project/duckdb-engine/) [![PyPI version](https://badge.fury.io/py/duckdb-engine.svg)](https://badge.fury.io/py/duckdb-engine) [![PyPI Downloads](https://img.shields.io/pypi/dm/duckdb-engine.svg)](https://pypi.org/project/duckdb-engine/) [![codecov](https://codecov.io/gh/Mause/duckdb_engine/graph/badge.svg)](https://codecov.io/gh/Mause/duckdb_engine)

Basic SQLAlchemy driver for [DuckDB](https://duckdb.org/)

<!--ts-->
- [duckdb\_engine](#duckdb_engine)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Usage in IPython/Jupyter](#usage-in-ipythonjupyter)
  - [Configuration](#configuration)
  - [How to register a pandas DataFrame](#how-to-register-a-pandas-dataframe)
  - [Things to keep in mind](#things-to-keep-in-mind)
    - [Auto-incrementing ID columns](#auto-incrementing-id-columns)
    - [Pandas `read_sql()` chunksize](#pandas-read_sql-chunksize)
    - [Unsigned integer support](#unsigned-integer-support)
  - [Alembic Integration](#alembic-integration)
  - [Preloading extensions (experimental)](#preloading-extensions-experimental)
  - [Registering Filesystems](#registering-filesystems)
  - [The name](#the-name)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->
<!-- Added by: me, at: Wed 20 Sep 2023 12:44:27 AWST -->

<!--te-->

## Installation
```sh
$ pip install duckdb-engine
```

`duckdb-engine` requires Python 3.10+ (Python 3.9 reached end-of-life in October 2025) and the DuckDB Python package `>=1.0.0,<2` (including DuckDB 1.4.3). Installing via pip will pull a compatible DuckDB version automatically.

DuckDB Engine also has a conda feedstock available, the instructions for the use of which are available in it's [repository](https://github.com/conda-forge/duckdb-engine-feedstock).

## Development (uv)

```sh
uv sync --all-extras
uv run pytest
uv run pre-commit run --all-files
```

## Usage

Once you've installed this package, you should be able to just use it, as SQLAlchemy does a python path search

```python
from sqlalchemy import Column, Integer, Sequence, String, create_engine
# SQLAlchemy 1.4+:
from sqlalchemy.orm import declarative_base
# SQLAlchemy 1.3 users can import from sqlalchemy.ext.declarative instead.
from sqlalchemy.orm.session import Session

Base = declarative_base()


class FakeModel(Base):  # type: ignore
    __tablename__ = "fake"

    id = Column(Integer, Sequence("fakemodel_id_sequence"), primary_key=True)
    name = Column(String)


eng = create_engine("duckdb:///:memory:")
Base.metadata.create_all(eng)
session = Session(bind=eng)

session.add(FakeModel(name="Frank"))
session.commit()

frank = session.query(FakeModel).one()

assert frank.name == "Frank"
```

## Usage in IPython/Jupyter

With IPython-SQL and DuckDB-Engine you can query DuckDB natively in your notebook! Check out [DuckDB's documentation](https://duckdb.org/docs/guides/python/jupyter) or
Alex Monahan's great demo of this on [his blog](https://alex-monahan.github.io/2021/08/22/Python_and_SQL_Better_Together.html#an-example-workflow-with-duckdb).

## Configuration

You can configure DuckDB by passing `connect_args` to the create_engine function
```python
create_engine(
    'duckdb:///:memory:',
    connect_args={
        'read_only': False,
        'config': {
            'memory_limit': '500mb'
        }
    }
)
```

The supported configuration parameters are listed in the [DuckDB docs](https://duckdb.org/docs/sql/configuration)

## Customer-Facing Analytics (MotherDuck)

For embedded / customer-facing analytics backends (high concurrency, predictable per-tenant isolation), MotherDuck recommends:

- Prefer persistent connections (connection pooling) to avoid per-request connect overhead.
- Use `attach_mode=single` so each end user is attached only to the database they should see.
- Use `session_hint=<user_id_or_hash>` on read-scaling tokens to keep an end user pinned to the same duckling for cache reuse and steadier latency.

Example:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "duckdb:///md:customer_db?attach_mode=single&session_hint=user-123",
    connect_args={"config": {"motherduck_token": "..."}},
)
```

## SQLAlchemy Engine setup for Customer-Facing Analytics (CFA)

In a typical 3-tier web app (browser → backend → MotherDuck), you want:

- **One long-lived `Engine` per tenant** (or per service-account token) rather than per request.
- **Connection pooling** so requests reuse existing connections.
- **Session affinity** (`session_hint`) for steadier latency on read-scaling.

### Recommended `create_engine()` settings

```python
from sqlalchemy import create_engine

engine = create_engine(
    # Per-tenant DB; attach only the one DB for isolation/predictability.
    "duckdb:///md:customer_db?attach_mode=single&session_hint=user-123",
    connect_args={
        "config": {
            "motherduck_token": "...",
            # Keep the underlying duckling warm briefly between connection churn.
            # (Useful if your backend has bursts of requests.)
            "dbinstance_inactivity_ttl": 60,
        }
    },
    # Pooling: reuse connections between requests.
    pool_pre_ping=True,
    # Tune these based on your backend concurrency *per tenant*.
    # Start small and increase if you see pool waits under load.
    pool_size=5,
    max_overflow=10,
)
```

Notes:

- `pool_size` is the steady-state number of open connections per tenant. Keep it low if you have many tenants.
- `max_overflow` controls burst capacity; set it based on your expected p95 concurrency bursts per tenant.
- For serverless / very short-lived runtimes where keeping warm pools is counterproductive, consider `poolclass=NullPool`.

### Per-tenant engine pattern (service-account-per-tenant)

If you follow the MotherDuck CFA recommendation of one service account (and token) per tenant, you generally want one cached `Engine` per tenant:

```python
from functools import lru_cache
from sqlalchemy import Engine, create_engine


@lru_cache(maxsize=256)
def engine_for_tenant(tenant_id: str, token: str, db: str) -> Engine:
    return create_engine(
        f"duckdb:///md:{db}?attach_mode=single",
        connect_args={"config": {"motherduck_token": token}},
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
```

This keeps connections warm per customer, prevents cross-tenant accidental attachments, and maps cleanly to per-tenant isolation.

## How to register a pandas DataFrame

```python
conn = create_engine("duckdb:///:memory:").connect()

# with SQLAlchemy 1.3
conn.execute("register", ("dataframe_name", pd.DataFrame(...)))

# with SQLAlchemy 1.4+
conn.execute(text("register(:name, :df)"), {"name": "test_df", "df": df})

conn.execute("select * from dataframe_name")
```

## Things to keep in mind
Duckdb's SQL parser is based on the PostgreSQL parser, but not all features in PostgreSQL are supported in duckdb. Because the `duckdb_engine` dialect is derived from the `postgresql` dialect, `SQLAlchemy` may try to use PostgreSQL-only features. Below are some caveats to look out for.

### Auto-incrementing ID columns
When defining an Integer column as a primary key, `SQLAlchemy` uses the `SERIAL` datatype for PostgreSQL. Duckdb does not yet support this datatype because it's a non-standard PostgreSQL legacy type, so a workaround is to use the `SQLAlchemy.Sequence()` object to auto-increment the key. For more information on sequences, you can find the [`SQLAlchemy Sequence` documentation here](https://docs.sqlalchemy.org/en/14/core/defaults.html#associating-a-sequence-as-the-server-side-default).

The following example demonstrates how to create an auto-incrementing ID column for a simple table:

```python
>>> import sqlalchemy
>>> engine = sqlalchemy.create_engine('duckdb:////path/to/duck.db')
>>> metadata = sqlalchemy.MetaData(engine)
>>> user_id_seq = sqlalchemy.Sequence('user_id_seq')
>>> users_table = sqlalchemy.Table(
...     'users',
...     metadata,
...     sqlalchemy.Column(
...         'id',
...         sqlalchemy.Integer,
...         user_id_seq,
...         server_default=user_id_seq.next_value(),
...         primary_key=True,
...     ),
... )
>>> metadata.create_all(bind=engine)
```

### Pandas `read_sql()` chunksize

**NOTE**: this is no longer an issue in DuckDB 1.x (including 1.4+).

The `pandas.read_sql()` method can read tables from `duckdb_engine` into DataFrames, but the `sqlalchemy.engine.result.ResultProxy` trips up when `fetchmany()` is called. Therefore, for now `chunksize=None` (default) is necessary when reading duckdb tables into DataFrames. For example:

```python
>>> import pandas as pd
>>> import sqlalchemy
>>> engine = sqlalchemy.create_engine('duckdb:////path/to/duck.db')
>>> df = pd.read_sql('users', engine)                ### Works as expected
>>> df = pd.read_sql('users', engine, chunksize=25)  ### Throws an exception
```

### Unsigned integer support

Unsigned integers are supported by DuckDB, and are available in [`duckdb_engine.datatypes`](duckdb_engine/datatypes.py).

## Alembic Integration

SQLAlchemy's companion library `alembic` can optionally be used to manage database migrations.

This support can be enabling by adding an Alembic implementation class for the `duckdb` dialect.

```python
from alembic.ddl.impl import DefaultImpl

class AlembicDuckDBImpl(DefaultImpl):
    """Alembic implementation for DuckDB."""

    __dialect__ = "duckdb"
```

After loading this class with your program, Alembic will no longer raise an error when generating or applying migrations.

## Preloading extensions (experimental)

> DuckDB includes builtin support for autoinstalling and autoloading extensions, see [the extension documentation](https://duckdb.org/docs/stable/extensions/overview#autoloadable-extensions) for more information.

Until the DuckDB python client allows you to natively preload extensions, I've added experimental support via a `connect_args` parameter

```python
from sqlalchemy import create_engine

create_engine(
    'duckdb:///:memory:',
    connect_args={
        'preload_extensions': ['https'],
        'config': {
            's3_region': 'ap-southeast-1'
        }
    }
)
```

## Registering Filesystems

> DuckDB allows registering filesystems from [fsspec](https://filesystem-spec.readthedocs.io/), see [documentation](https://duckdb.org/docs/guides/python/filesystems.html) for more information.

Support is provided under `connect_args` parameter

```python
from sqlalchemy import create_engine
from fsspec import filesystem

create_engine(
    'duckdb:///:memory:',
    connect_args={
        'register_filesystems': [filesystem('gcs')],
    }
)
```

## The name

Yes, I'm aware this package should be named `duckdb-driver` or something, I wasn't thinking when I named it and it's too hard to change the name now
