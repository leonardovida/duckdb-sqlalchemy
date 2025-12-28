---
layout: default
title: Getting Started
---

# Getting Started

This guide is a compact walkthrough for the DuckDB SQLAlchemy dialect. It focuses on the most common local and MotherDuck setups and points you to deeper docs when you need them.

## Install

```sh
pip install duckdb-sqlalchemy
```

## Local DuckDB

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)

engine = create_engine("duckdb:///local.duckdb")
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Ada"))
    session.commit()
```

## In-memory DuckDB

```python
from sqlalchemy import create_engine

engine = create_engine("duckdb:///:memory:")
```

## MotherDuck

```bash
export MOTHERDUCK_TOKEN="..."
```

```python
from sqlalchemy import create_engine

engine = create_engine("duckdb:///md:my_db")
```

If your token includes special characters, URL-escape it or pass it via `connect_args`.

## Connection URLs

DuckDB URLs follow the SQLAlchemy pattern:

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

## Next steps

- Connection strings: `docs/connection-urls.md`
- MotherDuck options: `docs/motherduck.md`
- Configuration and pooling: `docs/configuration.md`
- OLAP helpers: `docs/olap.md`
