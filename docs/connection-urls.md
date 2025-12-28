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

Supported values: `queue`, `null`, `singleton` (`singletonthreadpool`).

## URL helper

Use the `URL` helper to build URLs safely (it handles booleans and sequences for you):

```python
from sqlalchemy import create_engine
from duckdb_sqlalchemy import URL

url = URL(database=":memory:", memory_limit="1GB")
engine = create_engine(url)
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

See `motherduck.md` for MotherDuck-specific examples and options.

## Pool defaults

Pooling behavior is described in `configuration.md` (NullPool for file/MotherDuck, SingletonThreadPool for `:memory:`) and can be overridden with `poolclass`.
