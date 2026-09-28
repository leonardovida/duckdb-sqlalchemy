---
layout: default
title: Connection URLs
---

# Connection URLs

DuckDB URLs follow the standard SQLAlchemy shape:

```
duckdb:///<database>?<config>
```

## Common examples

```
duckdb:///:memory:
duckdb:///analytics.db
```

Use absolute paths when you need a specific location:

```
duckdb:////absolute/path/to/analytics.db
```

## Config in the query string

DuckDB settings from `duckdb_settings()` can be passed as URL query params:

```
duckdb:///analytics.db?threads=4&memory_limit=1GB
```

The `URL` helper will coerce booleans and sequences for you:

```python
from duckdb_sqlalchemy import URL

url = URL(database="analytics.db", threads=4, memory_limit="1GB")
```

## Dialect-only query params (pool overrides)

Override the default pool class in the URL when needed:

```
duckdb:///analytics.db?duckdb_sqlalchemy_pool=queue
duckdb:///analytics.db?pool=queue
```

Supported values: `queue` (`queuepool`), `null` (`nullpool`), `singleton`
(`singletonthreadpool`). Unknown values emit a `DuckDBEngineWarning` and fall
back to the default pool class.

## URL helper

Use the `URL` helper to build URLs safely (it handles booleans and sequences for you):

```python
from sqlalchemy import create_engine
from duckdb_sqlalchemy import URL

url = URL(database=":memory:", memory_limit="1GB")
engine = create_engine(url)
```

## MotherDuck URL helper

Use `MotherDuckURL` to build MotherDuck URLs with validated routing and
instance-cache params. All params are kept in the URL query, so `str(url)`
round-trips through `sqlalchemy.engine.make_url`; at connect time the dialect
moves routing params into the `md:` database string and applies the rest as
DuckDB config:

```python
from sqlalchemy import create_engine
from duckdb_sqlalchemy import MotherDuckURL

url = MotherDuckURL(
    database="md:my_db",
    attach_mode="single",
    access_mode="read_only",
    session_name="team-a",
    query={"memory_limit": "1GB"},
)
engine = create_engine(url)
```

Local or staging endpoint overrides belong in the same path-query bucket:

```python
url = MotherDuckURL(
    database="md:my_db",
    host="localhost",
    port=1984,
    tls="off",
)
```

## Manual escaping

If you build URLs manually and your token contains special characters, escape it:

```python
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine

escaped = quote_plus(os.environ["MOTHERDUCK_TOKEN"])
engine = create_engine(f"duckdb:///md:my_db?motherduck_token={escaped}")
```

See [motherduck.md](motherduck) for MotherDuck-specific examples and options.

## Quack remotes

DuckDB 1.5.3 ships Quack as a core extension for remote DuckDB access. Quack is
normally used from SQL with `ATTACH 'quack:host' AS name (...)` or the
`quack_query` table function; keep the SQLAlchemy engine connected to a local
DuckDB database and attach the remote from that session.

Routing params are only moved into the database string for `md:` and
`motherduck:` databases. For local files and `:memory:`, keys such as
`access_mode` stay DuckDB config, so `duckdb:///analytics.db?access_mode=read_only`
opens the existing file read-only.

`duckdb_sqlalchemy.make_url` is deprecated. Use `URL` to build URLs, or
`sqlalchemy.engine.make_url` to parse URL strings.

See [olap.md](olap) for `quack_query` and `ATTACH` examples.

## Pool defaults

Pooling behavior is described in [configuration.md](configuration)
(QueuePool for local files, NullPool for MotherDuck, SingletonThreadPool for `:memory:` and empty
`duckdb://` URLs) and can be
overridden with `poolclass`.
