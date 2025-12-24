# Pandas and Jupyter

## Register a DataFrame

DuckDB can query a pandas DataFrame by registering it as a view.

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///:memory:")
conn = engine.connect()

df = pd.DataFrame({"id": [1, 2], "name": ["Ada", "Grace"]})

# SQLAlchemy 1.3
conn.execute("register", ("people", df))

# SQLAlchemy 1.4+
# conn.execute(text("register(:name, :df)"), {"name": "people", "df": df})

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

## Jupyter (IPython SQL)

DuckDB works with `jupysql`/`ipython-sql`. Configure the SQLAlchemy engine and use the notebook extension to run SQL directly against DuckDB.
