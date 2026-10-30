# Repository sweep, October 2026

## Goal

Find and fix bugs, usability problems and stale documentation across the dialect, helpers, docs and tooling.

## Scope

Five parallel reviews covered the connection and cursor layer, reflection and Alembic, bulk and datatypes, MotherDuck and OLAP helpers, and docs, examples, packaging and CI. Every reported bug was reproduced with a script against DuckDB 1.5.6 and SQLAlchemy 2.0.52 before it was fixed.

## Plan

1. Review each area and reproduce findings.
2. Fix connection setup and MotherDuck helpers.
3. Fix the result and transaction lifecycle.
4. Fix bulk, COPY and typed binds in a separate worktree.
5. Fix reflection, DDL and Alembic comparison in a separate worktree.
6. Fix docs, CI and packaging.
7. Merge, run both suites and pre-commit, and record user-visible changes.

## Progress

All steps are complete. 43 findings were reported. 40 are fixed, 1 is partly fixed and 2 are deferred.

## Surprises & Discoveries

- `test_exact_decimal_division_matches_python_decimal` fails on `main` for `Decimal("8.706567643447616E-23")` cast to `DECIMAL(38,38)`. DuckDB binds a Decimal with 38 fractional digits as DOUBLE and loses the last digit. This is a binder limitation, not a regression.
- DuckDB binds `Decimal("1E+2")` as 1 in every untyped position. Typed integer, UNION, Numeric and Float binds now rewrite such values. Untyped `text()` parameters still do not, because a per-parameter scan would cost every statement.
- The scheduled MotherDuck workflow has never run its test: the `MOTHERDUCK_TOKEN` secret is not configured, and the job reports success after skipping.

## Decision Log

- Raw Arrow readers keep the pending-cursor slot but are not buffered, so `batches()` protection still raises while plain readers no longer block later statements.
- A commit that fails while buffering rolls back the DuckDB transaction, since SQLAlchemy treats the transaction as finished and skips the reset rollback.
- Retries buffer other cursors before the retry loop, so a fetch error of an earlier result is raised, not retried.
- Implicit sequence naming (`table_column_seq` collisions) is unchanged. Renaming would orphan sequences in existing databases.
- Treating a bare attached catalog name as `catalog.main` in reflection is deferred.

## Validation

- Unit and e2e suite: 875 passed, 5 skipped, 1 known pre-existing Hypothesis failure.
- SQLAlchemy upstream suite (`DUCKDB_SQLA_SUITE=1`, SQLAlchemy 2.0.52): 1331 passed, 249 skipped.
- `pre-commit run --all-files` passes, including `ty`.
- New regression tests fail on the pre-sweep code and pass after it.
- MotherDuck paths were checked without credentials, by capturing `duckdb.connect` arguments.

## Outcomes & Retrospective

The sweep fixed token exposure in connection errors, several pool-poisoning lifecycle bugs, silent data changes in bulk paths, and reflection output that could not be recreated. Follow-ups: configure `MOTHERDUCK_TOKEN` for the scheduled workflow, decide on the bare-catalog reflection fallback, and decide whether implicit sequence names should change in a major release.
