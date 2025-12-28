# duckdb-sqlalchemy

[![PyPI version](https://badge.fury.io/py/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy)
[![PyPI Downloads](https://img.shields.io/pypi/dm/duckdb-sqlalchemy.svg)](https://pypi.org/project/duckdb-sqlalchemy/)
[![codecov](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy/graph/badge.svg)](https://codecov.io/gh/leonardovida/duckdb-sqlalchemy)

duckdb-sqlalchemy is a DuckDB SQLAlchemy dialect for DuckDB and MotherDuck. It supports SQLAlchemy Core and ORM APIs for local DuckDB and MotherDuck connections.

The dialect handles pooling defaults, bulk inserts, type mappings, and cloud-specific configuration.

## Why this dialect

- **SQLAlchemy compatibility**: Core, ORM, Alembic, and reflection.
- **MotherDuck support**: Token handling, attach modes, session hints, and read scaling helpers.
- **Operational defaults**: Pooling defaults, transient retry for reads, and bulk insert optimization via Arrow/DataFrame registration.
- **Maintained**: Tracks current DuckDB releases with a long-term support posture.

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

local_url = URL(database=":memory:", memory_limit="1GB")
md_url = MotherDuckURL(database="md:my_db", attach_mode="single")
```

## Configuration and pooling

This dialect defaults to `NullPool` for file/MotherDuck connections and `SingletonThreadPool` for `:memory:`. You can override pooling explicitly. For long-lived MotherDuck pools, use the performance helper or configure `QueuePool`, `pool_pre_ping`, and `pool_recycle`.

See `docs/configuration.md` and `docs/motherduck.md` for detailed guidance.

## Documentation

- `docs/index.md` - GitHub Pages entrypoint
- `docs/README.md` - Docs index
- `docs/overview.md` - Overview and quick start
- `docs/getting-started.md` - Minimal install + setup walkthrough
- `docs/migration-from-duckdb-engine.md` - Migration guide from older dialects
- `docs/connection-urls.md` - URL formats and helpers
- `docs/motherduck.md` - MotherDuck setup and options
- `docs/configuration.md` - Connection configuration, extensions, filesystems
- `docs/olap.md` - Parquet/CSV scans and ATTACH workflows
- `docs/pandas-jupyter.md` - DataFrame registration and notebook usage
- `docs/types-and-caveats.md` - Type support and known caveats
- `docs/alembic.md` - Alembic integration

Docs site (GitHub Pages):

```
https://leonardovida.github.io/duckdb-sqlalchemy/
```

## Examples

- `examples/sqlalchemy_example.py` - end-to-end example
- `examples/motherduck_read_scaling_per_user.py` - per-user read scaling pattern
- `examples/motherduck_queuepool_high_concurrency.py` - QueuePool tuning
- `examples/motherduck_multi_instance_pool.py` - multi-instance pool rotation
- `examples/motherduck_arrow_reads.py` - Arrow results + streaming
- `examples/motherduck_attach_modes.py` - workspace vs single attach mode

## Release and support policy

- Long-term maintenance: intended to remain supported.
- Compatibility: track current DuckDB and SQLAlchemy releases while preserving SQLAlchemy semantics.
- Breaking changes: only in major/minor releases with explicit notes in `CHANGELOG.md`.
- Security: open an issue with details; fixes are prioritized.

## Changelog and roadmap

- `CHANGELOG.md` - release notes
- `ROADMAP.md` - upcoming work and priorities

## Contributing

See `AGENTS.md` for repo-specific workflow, tooling, and PR expectations. We welcome issues, bug reports, and high-quality pull requests.

## License

MIT. See `LICENSE.txt`.
