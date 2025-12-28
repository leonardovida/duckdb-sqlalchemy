---
layout: default
title: DuckDB SQLAlchemy dialect
---

# DuckDB SQLAlchemy dialect

This project provides a production-ready SQLAlchemy dialect for DuckDB and MotherDuck. Use SQLAlchemy Core and ORM APIs with DuckDB locally or in MotherDuck without manual driver workarounds.

## What you get

- SQLAlchemy Core, ORM, Alembic, and reflection support.
- MotherDuck helpers for attach modes, session hints, and read scaling.
- Sensible pooling defaults and optional performance tuning.
- Bulk insert helpers via Arrow or DataFrame registration.

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
```

## Next steps

- Connection strings: `docs/connection-urls.md`
- MotherDuck usage: `docs/motherduck.md`
- Configuration and pooling: `docs/configuration.md`
- OLAP helpers: `docs/olap.md`
