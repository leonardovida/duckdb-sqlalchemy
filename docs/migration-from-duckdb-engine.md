---
layout: default
title: Migration from duckdb_engine
---

# Migration from duckdb_engine

`duckdb-sqlalchemy` is the recommended package name for new work in this repository. If you are coming from `duckdb_engine`, migrate as follows:

## Package and import rename

Replace the existing dialect distribution:

```sh
pip uninstall duckdb-engine
pip install duckdb-sqlalchemy
```

Update imports:

```python
from duckdb_sqlalchemy import Dialect, URL, MotherDuckURL
```

SQLAlchemy URLs use the `duckdb://` driver name in both packages. Existing URLs continue to work. Both distributions register that name, so install one per environment. The explicit URL `duckdb+duckdb_sqlalchemy:///:memory:` selects this driver when resolving a registration collision.

## Notes

- The package name is now `duckdb-sqlalchemy` and the module is `duckdb_sqlalchemy`.
- The dialect remains registered as `duckdb` for SQLAlchemy.
- Connection setup no longer probes the default isolation level, matching `duckdb_engine` lifecycle behavior.
- Reflection without a schema (`inspect(engine).get_table_names()`, `has_table`,
  `MetaData.reflect`) lists the current database and schema. Single-object lookup
  and existence checks follow DuckDB's unqualified name resolution: temporary
  objects, the current schema, then the search path.
  `duckdb_engine` looked across every schema and attached database. Pass
  `schema=` to reflect other schemas.
- `duckdb://` with no database uses `SingletonThreadPool`, like `:memory:`.
- Before switching, validate application results for exact decimals/large integers,
  NULL versus NaN, timezone values, nested data, and generated keys. Check
  query counts, memory and transaction rollback on representative workloads.
- Use [performance guidance](performance) for bulk/Arrow APIs and
  [adoption evidence](adoption) for downstream integration proposals.
- See [motherduck.md](motherduck) for MotherDuck-specific behavior.
- See the [README](https://github.com/leonardovida/duckdb-sqlalchemy#readme)
  for project lineage and the
  [roadmap](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/ROADMAP.md).
