# duckdb-sqlalchemy: SQLAlchemy for DuckDB and MotherDuck

[![PyPI version](https://img.shields.io/pypi/v/duckdb-sqlalchemy)](https://pypi.org/project/duckdb-sqlalchemy/)
[![Python versions](https://img.shields.io/pypi/pyversions/duckdb-sqlalchemy)](https://pypi.org/project/duckdb-sqlalchemy/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/LICENSE.txt)

Use **DuckDB and MotherDuck with SQLAlchemy in Python**. `duckdb-sqlalchemy` is a SQLAlchemy dialect that connects SQLAlchemy Core, the ORM, and pandas workflows to local DuckDB databases and MotherDuck's cloud data warehouse.

Build queries with Python, reflect existing tables, and work with analytical data through SQLAlchemy's familiar `create_engine()` interface.

**[Read the documentation](https://leonardovida.github.io/duckdb-sqlalchemy/)** · [Getting started](https://leonardovida.github.io/duckdb-sqlalchemy/getting-started.html) · [Connect to MotherDuck](https://leonardovida.github.io/duckdb-sqlalchemy/motherduck.html) · [Release notes](https://github.com/leonardovida/duckdb-sqlalchemy/releases)

## What you can do

- **Query DuckDB with SQLAlchemy Core or ORM.** Use in-memory databases or persistent DuckDB files with Python query expressions, models, and sessions.
- **Connect to MotherDuck.** Configure authentication, database attachment, connection pooling, and read scaling through the same dialect.
- **Work with pandas and Apache Arrow.** Read SQL results into DataFrames or Arrow tables and load data with bulk insert and COPY helpers.
- **Query analytical files.** Compose SQLAlchemy queries over Parquet and CSV table functions, or query across attached databases.
- **Inspect existing schemas.** Reflect tables, views, columns, and constraints into SQLAlchemy metadata. See the [type support and caveats](https://leonardovida.github.io/duckdb-sqlalchemy/types-and-caveats.html) for limitations.

## Install

```sh
pip install duckdb-sqlalchemy
```

This installs DuckDB and SQLAlchemy as dependencies. Install `pandas` or `pyarrow` separately if you use their integrations.

## Quick start: query DuckDB from Python

Run a query against an in-memory DuckDB database:

```python
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///:memory:")

with engine.connect() as conn:
    total = conn.execute(
        text("SELECT sum(i) FROM range(1, 6) AS numbers(i)")
    ).scalar_one()
    print(total)  # 15
```

For a persistent local database, use `create_engine("duckdb:///analytics.duckdb")`.

### Use the SQLAlchemy ORM

Create a table, insert a row, and retrieve it through a session:

```python
from sqlalchemy import Column, Integer, String, create_engine, select
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String)


engine = create_engine("duckdb:///:memory:")
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Ada"))
    session.commit()
    user = session.scalars(select(User)).one()
    print(user.name)  # Ada
```

Continue with the [DuckDB setup guide](https://leonardovida.github.io/duckdb-sqlalchemy/getting-started.html) or [runnable examples](https://github.com/leonardovida/duckdb-sqlalchemy/tree/main/examples).

## Connect SQLAlchemy to MotherDuck

You need a MotherDuck account and access token. Set the token in your environment:

```sh
export MOTHERDUCK_TOKEN="your-access-token"
```

Then connect to your MotherDuck workspace:

```python
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///md:")

with engine.connect() as conn:
    print(conn.execute(text("SELECT 1")).scalar_one())  # 1
```

To connect to a specific existing database, use `duckdb:///md:my_db`. The dialect reads `MOTHERDUCK_TOKEN` or `motherduck_token` from the environment automatically.

See the [MotherDuck connection guide](https://leonardovida.github.io/duckdb-sqlalchemy/motherduck.html) for authentication, attach modes, pooling, read scaling, and opt-in transient retries.

## Documentation: find your next step

| I want to… | Guide |
| --- | --- |
| Install the dialect and run my first query | [Getting started](https://leonardovida.github.io/duckdb-sqlalchemy/getting-started.html) |
| Connect to a DuckDB file or MotherDuck database | [Connection URLs](https://leonardovida.github.io/duckdb-sqlalchemy/connection-urls.html) |
| Set up MotherDuck authentication and read scaling | [MotherDuck](https://leonardovida.github.io/duckdb-sqlalchemy/motherduck.html) |
| Configure pooling, extensions, or filesystems | [Configuration](https://leonardovida.github.io/duckdb-sqlalchemy/configuration.html) |
| Query Parquet/CSV, load rows, or attach databases | [Analytical queries and COPY helpers](https://leonardovida.github.io/duckdb-sqlalchemy/olap.html) |
| Use pandas DataFrames or Jupyter notebooks | [Pandas and Jupyter](https://leonardovida.github.io/duckdb-sqlalchemy/pandas-jupyter.html) |
| Check data types and known limitations | [Types and caveats](https://leonardovida.github.io/duckdb-sqlalchemy/types-and-caveats.html) |
| Configure schema migrations | [Alembic integration](https://leonardovida.github.io/duckdb-sqlalchemy/alembic.html) |
| Move from duckdb-engine | [Migration guide](https://leonardovida.github.io/duckdb-sqlalchemy/migration-from-duckdb-engine.html) |

Browse the **[full documentation](https://leonardovida.github.io/duckdb-sqlalchemy/)** or its [Markdown source](https://github.com/leonardovida/duckdb-sqlalchemy/tree/main/docs).

## Connection URL examples

| Database | SQLAlchemy URL |
| --- | --- |
| In-memory DuckDB | `duckdb:///:memory:` |
| Local DuckDB file | `duckdb:///analytics.duckdb` |
| Absolute file path on Unix/macOS | `duckdb:////absolute/path/to/analytics.duckdb` |
| MotherDuck workspace | `duckdb:///md:` |
| Existing MotherDuck database | `duckdb:///md:my_db` |

Use `URL` or `MotherDuckURL` when building connection URLs in Python. See [connection URL options](https://leonardovida.github.io/duckdb-sqlalchemy/connection-urls.html) for parameters and examples.

## Compatibility

| Component | Requirement |
| --- | --- |
| Python | 3.9+; the regular CI matrix covers 3.10–3.14 |
| SQLAlchemy | 2.0.0+ on Python below 3.14; 2.0.45+ on Python 3.14+; SQLAlchemy 2.1 requires Python 3.11+ |
| DuckDB | 0.5.0+; the configured compatibility matrix spans 1.3.0–1.5.5 |

The configured SQLAlchemy matrix includes 2.0.0, 2.0.52, and 2.1.0rc2. See [test configuration](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/noxfile.py), [CI runs](https://github.com/leonardovida/duckdb-sqlalchemy/actions), and [known caveats](https://leonardovida.github.io/duckdb-sqlalchemy/types-and-caveats.html) for the scope of compatibility checks. MotherDuck connections also require a DuckDB version supported by the service.

## Migrating from duckdb-engine

This project is a fork of [Mause/duckdb_engine](https://github.com/Mause/duckdb_engine). The Python package is `duckdb-sqlalchemy`, the import module is `duckdb_sqlalchemy`, and the SQLAlchemy driver name remains `duckdb`.

When migrating, replace the installed `duckdb-engine` distribution with `duckdb-sqlalchemy` and update explicit imports from `duckdb_engine` to `duckdb_sqlalchemy`. Both packages register the `duckdb://` dialect name, so avoid installing both in the same environment. Review the [migration guide](https://leonardovida.github.io/duckdb-sqlalchemy/migration-from-duckdb-engine.html) and test your application before switching.

## Examples and contributing

Explore [example scripts](https://github.com/leonardovida/duckdb-sqlalchemy/tree/main/examples) for local SQLAlchemy usage, MotherDuck read scaling, connection pools, Arrow reads, and attach modes.

To work on the dialect, clone the repository and install the development dependencies:

```sh
pip install -e ".[dev,devtools]"
pytest
nox -s ty
pre-commit run --all-files
```

Run `nox -s tests` for the full compatibility matrix; this is slower than the local test suite. See the [repository guidelines](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/AGENTS.md) before contributing.

[Report a bug](https://github.com/leonardovida/duckdb-sqlalchemy/issues) · [Read the changelog](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/CHANGELOG.md) · [View the roadmap](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/ROADMAP.md)

## License

[MIT](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/LICENSE.txt). The [changelog](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/CHANGELOG.md) preserves the project's upstream history.
