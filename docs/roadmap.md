# Roadmap: SQLAlchemy 2.x + OLAP-first

This doc turns the strategy plan into concrete, bite-sized PR checklists. Each
section calls out the primary files to edit so reviews stay focused.

## PR 1: Capabilities + statement cache + insertmanyvalues hooks

- [x] Add a capabilities model in `duckdb_sqlalchemy/capabilities.py`
- [x] Wire `Dialect.initialize()` to store capabilities in `duckdb_sqlalchemy/__init__.py`
- [x] Enable statement caching (`supports_statement_cache`) in `duckdb_sqlalchemy/__init__.py`
- [x] Enable insertmanyvalues + page size mapping in `duckdb_sqlalchemy/__init__.py`
- [x] Add execution options for `duckdb_insertmanyvalues_page_size` and `duckdb_copy_threshold`
- [x] Update docs: `docs/olap.md`
- [x] Update docs: `docs/types-and-caveats.md`
- [x] Mark custom types cacheable + normalize Struct/Union cache keys
- [x] Accept dict-style params in the `register()` execution path
- [x] Defer comment support detection to `Dialect.initialize()`

## PR 2: Pool defaults + thread guardrails

- [x] Default to `NullPool` for file + MotherDuck URLs in `duckdb_sqlalchemy/__init__.py`
- [x] Keep `SingletonThreadPool` for `:memory:` connections
- [x] Add docs to `docs/configuration.md` and `docs/connection-urls.md`
- [ ] Add a threading/pool regression test in `duckdb_sqlalchemy/tests/`

## PR 3: Arrow reads + streaming defaults

- [x] Add `duckdb_arrow` execution option and Arrow result wrapper
- [x] Add `duckdb_arraysize` execution option for streaming reads
- [x] Document `stream_results` + `arraysize` in `docs/olap.md`
- [ ] Add perf regression tests (optional) in `duckdb_sqlalchemy/tests/`

## PR 4: Bulk write fast path (register/COPY/insertmanyvalues)

- [x] Implement the bulk-insert routing logic in `duckdb_sqlalchemy/__init__.py`
- [x] Add docs to `docs/olap.md`
- [x] Add docs to `docs/pandas-jupyter.md`
- [x] Add COPY helpers in `duckdb_sqlalchemy/bulk.py`
- [ ] Add perf tests for the three write paths

## PR 5: Reflection completeness

- [ ] Implement `get_indexes`, `get_unique_constraints`, `get_check_constraints`
- [ ] Add inspector tests in `duckdb_sqlalchemy/tests/`

## PR 6: Async wrapper + Alembic module

- [ ] Add async dialect wrapper (threadpool) with docs
- [ ] Promote Alembic impl to `duckdb_sqlalchemy/alembic_impl.py`

## PR 7: SQLAlchemy suite integration

- [x] Add `duckdb_sqlalchemy/requirements.py` for SQLAlchemy suite
- [x] Add optional pytest suite harness under `duckdb_sqlalchemy/tests/sqlalchemy_suite/`
- [x] Add `test.cfg` to configure suite DB URL and requirements class

## Code stubs (starter templates)

### Dialect.initialize() capabilities hook

```python
from sqlalchemy.engine.default import DefaultDialect
from .capabilities import get_capabilities


class Dialect(PGDialect_psycopg2):
    def initialize(self, connection):
        DefaultDialect.initialize(self, connection)
        self._capabilities = get_capabilities(duckdb.__version__)
```

### insertmanyvalues hook (compiler + executemany)

```python
class DuckDBCompiler(PGCompiler):
    def visit_insert(self, insert_stmt, **kw):
        # TODO: enable insertmanyvalues for supported types
        return super().visit_insert(insert_stmt, **kw)


class Dialect(PGDialect_psycopg2):
    insertmanyvalues_page_size = 1000

    def do_executemany(self, cursor, statement, parameters, context=None):
        # TODO: route INSERT executemany to insertmanyvalues when enabled
        return super().do_executemany(cursor, statement, parameters, context)
```

### Arrow result wrapper (minimal)

```python
class ArrowResult:
    def __init__(self, table):
        self._table = table

    @property
    def arrow(self):
        return self._table

    def all(self):
        return self._table
```
