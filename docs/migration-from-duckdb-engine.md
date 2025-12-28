---
layout: default
title: Migration from duckdb_engine
---

# Migration from duckdb_engine

This project is the actively maintained DuckDB SQLAlchemy dialect. If you are coming from the older `duckdb_engine` package, migrate as follows:

## Package and import rename

Install the new package:

```sh
pip install duckdb-sqlalchemy
```

Update imports:

```python
from duckdb_sqlalchemy import Dialect, URL, MotherDuckURL
```

SQLAlchemy URLs use the `duckdb://` driver name in both packages. Existing URLs will continue to work.

## Notes

- The package name is now `duckdb-sqlalchemy` and the module is `duckdb_sqlalchemy`.
- The dialect remains registered as `duckdb` for SQLAlchemy.
- See `docs/motherduck.md` for MotherDuck-specific behavior.
