---
layout: default
title: Pandas and Jupyter
---

# Pandas and Jupyter

These examples require pandas 2.2 or newer. Older pandas releases do not
support SQLAlchemy 2.x engines.

## Register a DataFrame

DuckDB can query a pandas DataFrame by registering it as a view.

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///:memory:")
df = pd.DataFrame({"id": [1, 2], "name": ["Ada", "Grace"]})

with engine.connect() as conn:
    conn.execute(text("register(:name, :df)"), {"name": "people", "df": df})
    rows = conn.execute(text("select * from people")).fetchall()
```

Registration lasts for the lifetime of the DuckDB connection. Inside a
transaction block:

```python
with engine.begin() as conn:
    conn.execute(text("register(:name, :df)"), {"name": "people", "df": df})
    rows = conn.execute(text("select * from people")).fetchall()
```

## read_sql / to_sql

Pandas works with the SQLAlchemy engine:

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("duckdb:///:memory:")

pd.DataFrame({"a": [1, 2]}).to_sql("t", engine, index=False, if_exists="replace")
result = pd.read_sql("select * from t", engine)
```

Parameters are supported. Wrap the SQL in `text()` so SQLAlchemy binds the
named parameters:

```python
from sqlalchemy import text

result = pd.read_sql_query(
    text("select * from t where a >= :min_a"),
    engine,
    params={"min_a": 1},
)
```

## Bulk writes from DataFrames

For large DataFrame writes, register the DataFrame and insert from it directly:

```python
from sqlalchemy import text

with engine.begin() as conn:
    conn.execute(text("register(:name, :df)"), {"name": "people_df", "df": df})
    conn.execute(text("INSERT INTO people SELECT * FROM people_df"))
```

If you already have rows in Python (dicts or tuples), you can enable the bulk
insert fast path via execution options:

```python
rows = df.to_dict(orient="records")
with engine.begin() as conn:
    conn = conn.execution_options(duckdb_copy_threshold=10000)
    conn.execute(people.insert(), rows)
```

## Jupyter (IPython SQL)

DuckDB works with `jupysql`/`ipython-sql`. Configure the SQLAlchemy engine and use the notebook extension to run SQL directly against DuckDB.
