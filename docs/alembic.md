---
layout: default
title: Alembic integration
---

# Alembic integration

SQLAlchemy's migration tool, Alembic, works with DuckDB once you register an
Alembic implementation class for the `duckdb` dialect. Without it, Alembic
fails with `KeyError: 'duckdb'`.

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

Define the class in `env.py` (or a module it imports) so it is registered
before Alembic connects.

## Create migrations

```
alembic revision --autogenerate -m "create tables"
alembic upgrade head
```

Autogenerate compares your models with reflected tables, columns, primary and
foreign keys, unique and check constraints, indexes, and comments. Running
autogenerate again right after `upgrade head` produces an empty migration.

## Adding constraints to existing tables

DuckDB cannot add or drop UNIQUE, FOREIGN KEY, or CHECK constraints on an
existing table, so `op.create_unique_constraint()`,
`op.create_foreign_key()`, and `op.create_check_constraint()` fail with "No
support for that ALTER TABLE option yet". Primary keys, adding and dropping
columns, column type, nullability, and server default changes, column and table
renames, indexes, and table comments work with the regular operations.

Use batch mode with `recreate="always"`: Alembic creates a new table with the
constraint, copies the rows, drops the old table, and renames the new one.

```python
from alembic import op


def upgrade() -> None:
    with op.batch_alter_table("items", recreate="always") as batch_op:
        batch_op.create_unique_constraint("uq_items_sku", ["sku"])
        batch_op.create_check_constraint("ck_items_qty", "qty > 0")
```

DuckDB will not drop a table that another table's foreign key references, so
batch mode fails for such a table. Declare its constraints when you create it.

## Known limitations

- **Named unique constraints.** DuckDB generates its own constraint names
  (`items_sku_key`) and ignores the names in your DDL. Autogenerate compares
  unique constraints by name when your model names them, so
  `UniqueConstraint("sku", name="uq_items_sku")` shows up as a removed and an
  added constraint on every run. Declare unique constraints without a name
  (`Column("sku", String, unique=True)`), or delete those operations from the
  generated migration. Unnamed unique constraints and named foreign keys
  compare cleanly.
- **Dropping constraints by name.** Use the name DuckDB reports, for example
  from `inspect(conn).get_unique_constraints("items")`.
- **Column comments.** `op.alter_column(..., comment=...)` fails because the
  generic Alembic implementation cannot render the change for DuckDB. Run the
  statement directly:
  `op.execute("COMMENT ON COLUMN items.sku IS 'Stock keeping unit'")`.
  Table comments (`op.create_table_comment`) work.
- **Server default comparison.** With `compare_server_default=True`,
  autogenerate reports differences for the implicit `nextval(...)` defaults of
  autoincrement primary keys and for defaults written differently from how
  DuckDB stores them. Leave the option off, which is Alembic's default.
