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
- Reflection without a schema (`inspect(engine).get_table_names()`, `has_table`,
  `MetaData.create_all`) is limited to the current database and schema.
  `duckdb_engine` looked across every schema and attached database. Pass
  `schema=` to reflect other schemas.
- `duckdb://` with no database uses `SingletonThreadPool`, like `:memory:`.
- See [motherduck.md](motherduck) for MotherDuck-specific behavior.
- See the [README](https://github.com/leonardovida/duckdb-sqlalchemy#readme)
  for project lineage and the
  [roadmap](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/ROADMAP.md).
