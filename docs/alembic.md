---
layout: default
title: Alembic integration
---

# Alembic integration

SQLAlchemy's migration tool, Alembic, works with DuckDB without extra setup.
`duckdb_sqlalchemy` registers its Alembic implementation, `DuckDBImpl`, when
Alembic is loaded, so `env.py` needs no implementation class.

## Configure alembic.ini

Point Alembic at a DuckDB URL:

```
sqlalchemy.url = duckdb:///analytics.db
```

## env.py configuration

The `env.py` that `alembic init` creates works as is:

```python
from alembic import context
from sqlalchemy import engine_from_config, pool


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

An `AlembicDuckDBImpl(DefaultImpl)` class with `__dialect__ = "duckdb"` from
earlier versions of these docs keeps working and takes precedence over
`DuckDBImpl`. Remove it to get the DuckDB-specific comparison and rendering
below, or subclass `duckdb_sqlalchemy.alembic_impl.DuckDBImpl` instead. Code
that imports Alembic only after creating and connecting its engine can
register `DuckDBImpl` with `import duckdb_sqlalchemy.alembic_impl`.

## Create migrations

```
alembic revision --autogenerate -m "create tables"
alembic upgrade head
```

Autogenerate compares your models with reflected tables, columns, primary and
foreign keys, unique and check constraints, indexes, and comments. Running
autogenerate again right after `upgrade head` produces an empty migration.

`DuckDBImpl` also handles what the generic implementation gets wrong for
DuckDB:

- `Struct`, `Map`, and `Union` columns, and arrays of them, render as
  `duckdb_sqlalchemy.datatypes.Struct({...})` in the migration, and the file
  imports `duckdb_sqlalchemy.datatypes`.
- With `compare_type=True` (Alembic's default), a nested type whose fields
  changed is reported as a type change.
- A named unique constraint (`UniqueConstraint("sku", name="uq_items_sku")`)
  matches the constraint DuckDB created for the same columns. DuckDB replaces
  constraint names with its own (`items_sku_key`), so the generic comparison
  reported it as removed and added on every run.
- With `compare_server_default=True`, defaults are compared after undoing
  DuckDB's spelling (`'18'` for `18`, `CAST('t' AS BOOLEAN)` for `true`), and
  the implicit `nextval(...)` default of an autoincrement primary key is
  ignored.
- `op.alter_column(..., comment=...)` emits `COMMENT ON COLUMN`.

## Adding constraints to existing tables

DuckDB cannot add or drop UNIQUE, FOREIGN KEY, or CHECK constraints on an
existing table, so `op.create_unique_constraint()`,
`op.create_foreign_key()`, and `op.create_check_constraint()` fail with "No
support for that ALTER TABLE option yet". Primary keys, adding and dropping
columns, column type, nullability, and server default changes, column and table
renames, indexes, and table and column comments work with the regular
operations.

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

Batch recreation preserves an existing implicit primary-key sequence and its
position, both with reflection and with `copy_from=your_table`. Copying explicit
ids does not create a replacement sequence starting at 1. Offline batch scripts
with `copy_from` retain the model's existing implicit sequence name. Reflected
sequence-backed keys compile as INTEGER/BIGINT plus `nextval`, never SERIAL.

## Known limitations

- **Dropping constraints by name.** DuckDB ignores the names in your DDL, so
  drop a constraint by the name DuckDB reports, for example from
  `inspect(conn).get_unique_constraints("items")`.
- **Server default comparison** compares SQL text, ignoring case, spacing,
  casts, and enclosing parentheses outside string literals. An expression that
  DuckDB rewrites in some other way is still reported as changed. Write the
  default the way DuckDB stores it
  (`SELECT column_default FROM duckdb_columns()`), or leave
  `compare_server_default` off, which is Alembic's default.
