# Alembic integration

SQLAlchemy's migration tool, Alembic, can be used with DuckDB by providing a dialect implementation class.

## Configure alembic.ini

Point Alembic at a DuckDB URL:

```
sqlalchemy.url = duckdb:///analytics.db
```

## env.py configuration

```python
from alembic import context
from alembic.ddl.impl import DefaultImpl
from sqlalchemy import engine_from_config, pool


class AlembicDuckDBImpl(DefaultImpl):
    __dialect__ = "duckdb"


def run_migrations_online() -> None:
    config = context.config
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()
```

Load this class in your Alembic environment to enable migration generation and application without dialect errors.

## Create migrations

```
alembic revision --autogenerate -m "create tables"
alembic upgrade head
```
