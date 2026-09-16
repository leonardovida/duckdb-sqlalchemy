# Goal

Export a SQLAlchemy select directly to Parquet while preserving parameter and
type processing, without materializing the query result in Python.

# Scope

One additive `copy_to_parquet` helper. Reuse COPY option rendering. No new
dependencies, release, remote storage setup, or changes to existing imports.

# Plan

1. Implement a private SQLAlchemy executable wrapper and public helper.
2. Exercise typed and expanding binds, compound selects, COPY results, paths,
   and invalid input with real DuckDB.
3. Document usage and file side effects, then run local and compatibility checks.
4. Open a PR and merge after hosted checks pass.

# Progress

- Resumed the clean experiment branch from current default branch `bcfce71`.
- Prior prototype established compiler visitation preserves SQLAlchemy binds.
- Implemented public export helper, documentation and nine integration cases.
- Local and compatibility checks passed. PR delivery follows these checks.

# Surprises & Discoveries

- DuckDB 1.3.0 rejects bound COPY destinations. Escape the destination literal
  while retaining bound query values.
- Raw `exec_driver_sql` with manually compiled parameters skips custom bind
  processors. Keep normal `Connection.execute` handling.
- COPY results describe exported counts, not query columns. Query result
  processors must not be applied to those counts.
- File writes survive rollback and replace existing single-file destinations.

# Decision Log

- Accept Select and CompoundSelect, with one explicit parameter mapping.
- Fix the format to Parquet and reject format overrides.
- Verify the active compatibility floor and document older versions as untested.

# Validation

- Full local pytest: 271 passed, 41 skipped (DuckDB 1.5.5 / SQLAlchemy 2.0.52).
- Export integration cases: 9 passed on DuckDB 1.3.0 / SQLAlchemy 2.0.0.
- Pre-commit, including Ruff and ty, passed. Lock check and package build passed.
- Isolated wheel exported a bound query and read the Parquet rows back without
  pandas or PyArrow installed.
- Hosted Python/SQLAlchemy/DuckDB checks remain the merge gate.

# Outcomes & Retrospective

The implementation preserves query bind processing while isolating COPY result
metadata from query result converters. Rejecting executemany parameter lists
prevents accidental repeated exports to one path. No version bump or release
requested. PR checks and the verified merge commit are recorded in the PR and
external automation record.
