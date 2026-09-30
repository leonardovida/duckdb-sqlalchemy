---
layout: default
title: Ecosystem adoption
---

# Ecosystem adoption

The package registers the `duckdb://` dialect for SQLAlchemy Core, ORM,
pandas/Jupyter and MotherDuck. Wider adoption requires published releases,
reproducible compatibility evidence and acceptance by downstream maintainers.

## Reviewable integration recipe

```sh
python -m pip uninstall duckdb-engine
python -m pip install duckdb-sqlalchemy
```

```python
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///:memory:")
with engine.connect() as conn:
    assert conn.execute(text("SELECT 42")).scalar_one() == 42
```

Both distributions register `duckdb://`. Install one dialect distribution per
environment. Applications that must coexist with another registration can use
`duckdb+duckdb_sqlalchemy:///:memory:` to select this driver explicitly.
See [migration guidance](migration-from-duckdb-engine) for imports, reflection,
transactions and data validation.

## Evidence for documentation and integration proposals

A proposal to DuckDB, MotherDuck or another downstream project should include:

- The released package version and supported Python/SQLAlchemy/DuckDB versions.
- An executable connection example and a link to [CI](https://github.com/leonardovida/duckdb-sqlalchemy/actions).
- Upstream SQLAlchemy suite results and explicitly unsupported capabilities.
- Wheel/sdist installation checks with neither optional library, Arrow only
  and pandas only; Windows/macOS tests.
- Real MotherDuck integration results for a service-supported DuckDB version.
- [Reproducible performance measurements](performance), with source shape,
  versions, native memory limits and validation checks stated.
- A tested rollback/migration recipe for users of `duckdb-engine`.

This repository does not imply endorsement by DuckDB or MotherDuck.
Track acceptance and publication separately from code readiness.

## Real MotherDuck validation

The `MotherDuck integration` workflow runs manually and weekly when the
repository secret `MOTHERDUCK_TOKEN` is configured. Optional repository
variables `MOTHERDUCK_TEST_DATABASE` and `MOTHERDUCK_DUCKDB_VERSION`
choose an existing accessible database and a service-supported DuckDB version.
Without credentials the workflow explicitly reports that it did not run.

Locally:

```sh
MOTHERDUCK_TOKEN=... pytest -q duckdb_sqlalchemy/tests/test_integration.py -k motherduck --remote-data -rs
```

The test checks connection, query execution, reflection, batch Arrow reads and
pooled reuse. It does not create or change persistent user tables.
