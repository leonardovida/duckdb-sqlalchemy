# duckdb-sqlalchemy

[![PyPI version](https://badge.fury.io/py/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy)
[![PyPI Downloads](https://img.shields.io/pypi/dm/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy/)
[![codecov](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy/graph/badge.svg)](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy)

The production-grade SQLAlchemy dialect for DuckDB and MotherDuck. Use the full SQLAlchemy Core and ORM APIs with DuckDB's analytical engine, locally or in the cloud via MotherDuck.

This dialect handles connection pooling, bulk inserts, type mappings, and cloud-specific configuration so you can focus on queries instead of driver quirks.

## Why this dialect

- **Full SQLAlchemy compatibility**: Core, ORM, Alembic migrations, and reflection work out of the box.
- **MotherDuck support**: Automatic token handling, attach modes, session hints, and read scaling helpers.
- **Production defaults**: Sensible pooling, transient retry for reads, and bulk insert optimization via Arrow/DataFrame registration.
- **Actively maintained**: Tracks current DuckDB releases with long-term support commitment.

## Compatibility

| Component | Supported versions |
| --- | --- |
| Python | 3.9+ |
| SQLAlchemy | 1.3.22+ (2.x recommended) |
| DuckDB | 1.3.0+ (1.4.3 recommended) |

## Install

```sh
pip install duckdb-sqlalchemy
```

## Quick start (DuckDB)

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

## Quick start (MotherDuck)

```bash
export MOTHERDUCK_TOKEN="..."
```

```python
from sqlalchemy import create_engine

engine = create_engine("duckdb:///md:my_db")
```

MotherDuck uses the `md:` database prefix. Tokens are picked up from `MOTHERDUCK_TOKEN` (or `motherduck_token`) automatically. If your token has special characters, URL-escape it or pass it via `connect_args`.

## Connection URLs

DuckDB URLs follow the standard SQLAlchemy shape:

```
duckdb:///<database>?<config>
```

Examples:

```
duckdb:///:memory:
duckdb:///analytics.db
duckdb:////absolute/path/to/analytics.db
duckdb:///md:my_db?attach_mode=single&access_mode=read_only&session_hint=team-a
```

Use the URL helpers to build connection strings safely:

```python
from duckdb_sqlalchemy import URL, MotherDuckURL

local_url = URL(database=":memory:", read_only=False)
md_url = MotherDuckURL(database="md:my_db", attach_mode="single")
```

## Configuration and pooling

This dialect ships with sensible defaults (NullPool for file/MotherDuck connections, SingletonThreadPool for `:memory:`) and lets you override pooling explicitly. For production services, use the MotherDuck performance helper or configure `QueuePool`, `pool_pre_ping`, and `pool_recycle`.

See `docs/configuration.md` and `docs/motherduck.md` for detailed guidance.

## Documentation

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

## Release and support policy

- Long-term maintenance: this project is intended to remain supported indefinitely.
- Compatibility: we track current DuckDB and SQLAlchemy releases while preserving SQLAlchemy semantics.
- Breaking changes: only in major/minor releases with explicit notes in `CHANGELOG.md`.
- Security: please open an issue with details; we will prioritize fixes.

## Changelog and roadmap

- `CHANGELOG.md` - release notes
- `ROADMAP.md` - upcoming work and priorities

## Contributing

See `AGENTS.md` for repo-specific workflow, tooling, and PR expectations. We welcome issues, bug reports, and high-quality pull requests.

## License

MIT. See `LICENSE.txt`.
