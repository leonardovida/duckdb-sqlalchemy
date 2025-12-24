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

## URL helper

Use the `URL` helper to build URLs safely (it handles booleans and sequences for you):

```python
from sqlalchemy import create_engine
from duckdb_sqlalchemy import URL

url = URL(database=":memory:", read_only=False, memory_limit="1GB")
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
