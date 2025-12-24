# Alembic integration

SQLAlchemy's migration tool, Alembic, can be used with DuckDB by providing a dialect implementation class.

```python
from alembic.ddl.impl import DefaultImpl


class AlembicDuckDBImpl(DefaultImpl):
    __dialect__ = "duckdb"
```

Load this class in your Alembic environment to enable migration generation and application without dialect errors.
