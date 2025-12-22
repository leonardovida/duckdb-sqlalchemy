# duckdb_engine

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/duckdb-engine)](https://pypi.org/project/duckdb-engine/) [![PyPI version](https://badge.fury.io/py/duckdb-engine.svg)](https://badge.fury.io/py/duckdb-engine) [![PyPI Downloads](https://img.shields.io/pypi/dm/duckdb-engine.svg)](https://pypi.org/project/duckdb-engine/) [![codecov](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy/graph/badge.svg)](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy)

SQLAlchemy driver for [DuckDB](https://duckdb.org/) and [MotherDuck](https://motherduck.com/).

<!--ts-->
- [duckdb\_engine](#duckdb_engine)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
  - [Connection URLs](#connection-urls)
    - [URL helper](#url-helper)
    - [MotherDuck](#motherduck)
  - [OLAP workflows](#olap-workflows)
    - [Parquet and CSV scans](#parquet-and-csv-scans)
    - [Multi-database analytics with ATTACH](#multi-database-analytics-with-attach)
  - [Usage in IPython/Jupyter](#usage-in-ipythonjupyter)
  - [Configuration](#configuration)
  - [How to register a pandas DataFrame](#how-to-register-a-pandas-dataframe)
  - [Type support](#type-support)
  - [Parameter binding](#parameter-binding)
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

DuckDB Engine also has a conda feedstock available, the instructions for the use of which are available in it's [repository](https://github.com/conda-forge/duckdb-engine-feedstock).

## Quickstart

```python
from sqlalchemy import Column, Integer, Sequence, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
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

See `examples/sqlalchemy_example.py` for a longer end-to-end example.

## Connection URLs

DuckDB URLs follow the standard SQLAlchemy format:

```
duckdb:///<database>?<config>
```

Examples:

```
duckdb:///:memory:
duckdb:///analytics.db
```

### URL helper

For programmatic construction (and automatic escaping), use the helper:

```python
import os
from sqlalchemy import create_engine
from duckdb_engine import URL

url = URL(
    database=":memory:",
    memory_limit="1GB",
)
engine = create_engine(url)

md_url = URL(
    database="md:my_db",
    motherduck_token=os.environ["MOTHERDUCK_TOKEN"],
)
md_engine = create_engine(md_url)
```

If you build URLs manually and your token contains special characters, escape it:

```python
from urllib.parse import quote_plus

escaped = quote_plus(os.environ["MOTHERDUCK_TOKEN"])
engine = create_engine(f"duckdb:///md:my_db?motherduck_token={escaped}")
```

### MotherDuck

MotherDuck connections use the `md:` database prefix and support connection-time
configuration options like:

- `motherduck_token` (can also be provided via `motherduck_token` or `MOTHERDUCK_TOKEN` env vars)
- `attach_mode` (`workspace` or `single`)
- `saas_mode` (`true`/`false`)
- `session_hint` (for read-scaling session affinity)
- `access_mode` (`read_only` for read scaling tokens)
- `dbinstance_inactivity_ttl` (alias for `motherduck_dbinstance_inactivity_ttl`)

If `motherduck_token` (lowercase) or `MOTHERDUCK_TOKEN` is set in the environment,
it will be used automatically when you connect to `md:` databases.

```
duckdb:///md:
duckdb:///md:my_db?attach_mode=single
duckdb:///md:my_db?session_hint=user123&access_mode=read_only
duckdb:///md:my_db?dbinstance_inactivity_ttl=1h
```

## OLAP workflows

### Parquet and CSV scans

DuckDB exposes analytics-friendly table functions like `read_parquet` and
`read_csv_auto`. The `duckdb_engine.olap` helpers make these easy to use with
SQLAlchemy:

```python
from sqlalchemy import select
from duckdb_engine import read_parquet, read_csv_auto

parquet = read_parquet("data/events.parquet", columns=["event_id", "ts"])
stmt = select(parquet.c.event_id, parquet.c.ts)

csv = read_csv_auto("data/events.csv", columns=["event_id", "ts"])
stmt = select(csv.c.event_id, csv.c.ts)
```

### Multi-database analytics with ATTACH

DuckDB can query across multiple databases in a single session:

```python
from sqlalchemy import text

conn = create_engine("duckdb:///local.duckdb").connect()
conn.execute(text("ATTACH 'analytics.duckdb' AS analytics"))
conn.execute(text("SELECT * FROM analytics.events LIMIT 10"))
```

## Usage in IPython/Jupyter

With IPython-SQL and DuckDB-Engine you can query DuckDB natively in your notebook! Check out [DuckDB's documentation](https://duckdb.org/docs/guides/python/jupyter) or
Alex Monahan's great demo of this on [his blog](https://alex-monahan.github.io/2021/08/22/Python_and_SQL_Better_Together.html#an-example-workflow-with-duckdb).

## Configuration

You can configure DuckDB by passing `connect_args` to the create_engine function:

```python
create_engine(
    "duckdb:///:memory:",
    connect_args={
        "read_only": False,
        "config": {
            "memory_limit": "500mb",
        },
    },
)
```

The supported configuration parameters are listed in the [DuckDB docs](https://duckdb.org/docs/sql/configuration).

## How to register a pandas DataFrame

```python
conn = create_engine("duckdb:///:memory:").connect()

# with SQLAlchemy 1.3
conn.execute("register", ("dataframe_name", pd.DataFrame(...)))

# with SQLAlchemy 1.4+
conn.execute(text("register(:name, :df)"), {"name": "test_df", "df": df})

conn.execute("select * from dataframe_name")
```

## Type support

Most SQLAlchemy core types map directly to DuckDB. DuckDB-specific helpers are
available in `duckdb_engine.datatypes`:

| Type | DuckDB name | Notes |
| --- | --- | --- |
| `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UTinyInteger`, `USmallInteger`, `UInteger`, `UBigInteger` | UTINYINT, USMALLINT, UINTEGER, UBIGINT | Unsigned integers |
| `TinyInteger` | TINYINT | Signed 1-byte integer |
| `HugeInteger`, `UHugeInteger`, `VarInt` | HUGEINT, UHUGEINT, VARINT | 128-bit and variable-length integers (VarInt requires DuckDB >= 1.0) |
| `Struct` | STRUCT | Nested fields via dict of SQLAlchemy types |
| `Map` | MAP | Key/value mapping |
| `Union` | UNION | Union types via dict of SQLAlchemy types |

## Parameter binding

SQLAlchemy parameter styles are handled by the dialect. Use named parameters in
SQLAlchemy expressions and let the engine handle the database-specific format:

```python
from sqlalchemy import text

stmt = text("SELECT * FROM events WHERE event_id = :event_id")
conn.execute(stmt, {"event_id": 123})
```

For SQLAlchemy 2.x, parameters compile to `$1`, `$2`, ...; SQLAlchemy 1.x uses
`?` placeholders.

## Things to keep in mind
Duckdb's SQL parser is based on the PostgreSQL parser, but not all features in PostgreSQL are supported in duckdb. Because the `duckdb_engine` dialect is derived from the `postgresql` dialect, `SQLAlchemy` may try to use PostgreSQL-only features. Below are some caveats to look out for.

### Auto-incrementing ID columns
When defining an Integer column as a primary key, `SQLAlchemy` uses the `SERIAL` datatype for PostgreSQL. Duckdb does not yet support this datatype because it's a non-standard PostgreSQL legacy type, so a workaround is to use the `SQLAlchemy.Sequence()` object to auto-increment the key. For more information on sequences, you can find the [`SQLAlchemy Sequence` documentation here](https://docs.sqlalchemy.org/en/14/core/defaults.html#associating-a-sequence-as-the-server-side-default).

The following example demonstrates how to create an auto-incrementing ID column for a simple table:

```python
>>> import sqlalchemy
>>> engine = sqlalchemy.create_engine("duckdb:////path/to/duck.db")
>>> metadata = sqlalchemy.MetaData(engine)
>>> user_id_seq = sqlalchemy.Sequence("user_id_seq")
>>> users_table = sqlalchemy.Table(
...     "users",
...     metadata,
...     sqlalchemy.Column(
...         "id",
...         sqlalchemy.Integer,
...         user_id_seq,
...         server_default=user_id_seq.next_value(),
...         primary_key=True,
...     ),
... )
>>> metadata.create_all(bind=engine)
```

### Pandas `read_sql()` chunksize

**NOTE**: this is no longer an issue in versions `>=0.5.0` of `duckdb`

The `pandas.read_sql()` method can read tables from `duckdb_engine` into DataFrames, but the `sqlalchemy.engine.result.ResultProxy` trips up when `fetchmany()` is called. Therefore, for now `chunksize=None` (default) is necessary when reading duckdb tables into DataFrames. For example:

```python
>>> import pandas as pd
>>> import sqlalchemy
>>> engine = sqlalchemy.create_engine("duckdb:////path/to/duck.db")
>>> df = pd.read_sql("users", engine)                ### Works as expected
>>> df = pd.read_sql("users", engine, chunksize=25)  ### Throws an exception
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

> DuckDB 0.9.0+ includes builtin support for autoinstalling and autoloading of extensions, see [the extension documentation](http://duckdb.org/docs/archive/0.9.0/extensions/overview#autoloadable-extensions) for more information.

Until the DuckDB python client allows you to natively preload extensions, I've added experimental support via a `connect_args` parameter

```python
from sqlalchemy import create_engine

create_engine(
    "duckdb:///:memory:",
    connect_args={
        "preload_extensions": ["https"],
        "config": {
            "s3_region": "ap-southeast-1",
        },
    },
)
```

## Registering Filesystems

> DuckDB allows registering filesystems from [fsspec](https://filesystem-spec.readthedocs.io/), see [documentation](https://duckdb.org/docs/guides/python/filesystems.html) for more information.

Support is provided under `connect_args` parameter

```python
from sqlalchemy import create_engine
from fsspec import filesystem

create_engine(
    "duckdb:///:memory:",
    connect_args={
        "register_filesystems": [filesystem("gcs")],
    },
)
```

## The name

Yes, I'm aware this package should be named `duckdb-driver` or something. The repository is named `duckdb-sqlalchemy`, but the package remains `duckdb-engine` for compatibility.
