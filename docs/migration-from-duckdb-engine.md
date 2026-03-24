---
layout: default
title: Migration from duckdb_engine
---

# Migration from duckdb_engine

`duckdb-sqlalchemy` is the recommended package name for new work in this repository. If you are coming from `duckdb_engine`, migrate as follows:

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
- Connection setup no longer probes the default isolation level, matching `duckdb_engine` lifecycle behavior.
- See [motherduck.md](motherduck) for MotherDuck-specific behavior.
- See [README.md](../README.md) for project lineage, release policy, and
  roadmap links.
