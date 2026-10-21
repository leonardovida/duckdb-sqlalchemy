# Snapshot schema evolution recipe

## Goal

Let SQLAlchemy users query disjoint Parquet export batches after adding an
optional column, retaining live joins and bound report filters. Use the existing
`copy_to_parquet` and `read_parquet` primitives.

The live-join recipe stops at one dataset. A two-file probe on DuckDB 1.5.6
shows that selecting an added column fails with the default reader, while
`union_by_name=True` fills the older rows with NULL. A scalar/list conflict
still fails, so this is additive schema composition rather than validation.
DuckDB's public [schema combination reference](https://duckdb.org/docs/1.5/data/multiple_files/combining_schemas.html)
documents name matching, missing-column NULLs and increased memory use.

## Acceptance

- Run a self-contained example against real temporary database and Parquet files.
- Compare the combined snapshot query to the equivalent source query, including
  added/reordered fields, NULL Hive keys, string identifiers, expanding bound
  filters, empty report selections and a dimension update after export.
- Check both input file orders and negative cases: no name union, and scalar/list
  type conflicts. Use DuckDB 1.3.0 / SQLAlchemy 2.0.0 as well as current versions.
- Run the full local suite, pre-commit, canonical ty and hosted stable gates.

## Non-goals

No dataset manager, schema registry, reader/export API, dependency or version
change. No deduplication, latest-snapshot selection, remote-storage guarantee,
atomic publication or performance claim. No dialect runtime behavior changes,
so no behavior-change migration entry is needed.

## Result

The example combines two disjoint partitioned batches, skips a third empty
export and matches the source query after updating a live dimension. Missing
optional fields become NULL and string partition and join keys retain their
values. Repeated expanding binds include an empty selection. Four additional
end-to-end cases exercise both file orders, default-reader failure and
incompatible scalar/list types. No runtime implementation change was needed.

Local validation: full locked Python 3.14 / DuckDB 1.5.6 / SQLAlchemy 2.0.52
suite passed (768 tests, 5 expected skips). The snapshot/export tests and local
examples passed on Python 3.10 / DuckDB 1.3.0 / SQLAlchemy 2.0.0 (21 tests).
All-file pre-commit, canonical ty and lock consistency checks passed.
Credentialed MotherDuck, remote storage and the full local nox matrix are
outside this local-file recipe's coverage. Hosted gates and delivery are
recorded on the accompanying pull request.
