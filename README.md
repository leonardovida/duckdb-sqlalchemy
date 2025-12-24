# duckdb-sqlalchemy

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/duckdb-sqlalchemy)](https://pypi.org/project/duckdb-sqlalchemy/) [![PyPI version](https://badge.fury.io/py/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy) [![PyPI Downloads](https://img.shields.io/pypi/dm/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy/) [![codecov](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy/graph/badge.svg)](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy)

SQLAlchemy dialect for DuckDB and MotherDuck. Use it to run DuckDB locally or connect to MotherDuck with the standard SQLAlchemy APIs.

## Install

```sh
pip install duckdb-sqlalchemy
```

Conda packages are available via the conda-forge feedstock: https://github.com/conda-forge/duckdb-sqlalchemy-feedstock.

## Quick start

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, Session

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
    assert session.query(User).one().name == "Ada"
```

## Documentation

Start here for focused, task-based docs:

- `docs/README.md` - Docs index
- `docs/connection-urls.md` - URL formats and helpers
- `docs/motherduck.md` - MotherDuck setup and options
- `docs/configuration.md` - Connection configuration, extensions, filesystems
- `docs/olap.md` - Parquet/CSV scans and ATTACH workflows
- `docs/pandas-jupyter.md` - DataFrame registration and notebook usage
- `docs/types-and-caveats.md` - Type support and known caveats
- `docs/alembic.md` - Alembic integration

## Examples

- `examples/sqlalchemy_example.py` - end-to-end example
- `examples/motherduck_read_scaling_per_user.py` - per-user read scaling pattern
- `examples/motherduck_queuepool_high_concurrency.py` - QueuePool tuning
- `examples/motherduck_multi_instance_pool.py` - multi-instance pool rotation
- `examples/motherduck_arrow_reads.py` - Arrow results + streaming
- `examples/motherduck_attach_modes.py` - workspace vs single attach mode

## Contributing

See `AGENTS.md` for repo-specific workflow and PR expectations.
