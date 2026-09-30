# Repository correctness and performance sweep

## Goal and scope
Resolve the nine review findings, improve reflection and data movement, and establish executable compatibility and performance evidence for SQLAlchemy/DuckDB adoption.

## Completed work
- Reproduced all nine findings against unchanged main at `6b41e798d7525a94176ada4e6a7ac1cb3d648df3`: 14 failed, 484 passed, 2 skipped.
- Fixed fetch-error propagation, cursor/result ownership, literal-aware Alembic comparisons, exact numeric ingestion, explicit Numeric return scale, CSV dialect serialization, commented/CTE DML counts, reflection kind/scope and foreign-key target schemas.
- Reused prepared bulk conversions and reduced positional Arrow copies. Added direct typed Arrow ingestion and owned bounded SELECT batches.
- Enabled SQLAlchemy's upstream dialect suites and fixed the defects they exposed: Float precision, non-returning DML contracts, exact RETURNING counts, sequence autoincrement metadata, native JSON paths/scalar casts, inspector caches, scoped sequence metadata, backslash escaping, single-table options and temporary sequence lifecycle.
- Added 25 randomized signed-64-bit nullable integer/text cases per ingestion path, plus decimal, binary, JSON, timezone, NaN/NULL, quoted identifiers, Arrow ownership, rollback and cleanup regressions.
- Added platform, dependency-floor, wheel/sdist, optional-dependency and benchmark CI. Added real MotherDuck checks gated on configured credentials.
- Documented performance, profiling, query tagging, migration, native limitations and upstream adoption requirements.

## Validation evidence
At implementation commit `4a8e2c00426322c4e6e688aa816c3a6d04e2acd4`:
- Python 3.11 regular suite: 545 passed, 2 skipped; all Python 3.10–3.14 and SQLAlchemy 2.0 jobs passed.
- Dependency floor (SQLAlchemy 2.0.0, DuckDB 1.3.0), macOS, Windows and upstream prerelease jobs passed.
- SQLAlchemy 2.0 upstream suite: 1333 passed, 250 skipped.
- SQLAlchemy 2.1 upstream suite: 1531 passed, 275 skipped.
- Six isolated wheel/sdist checks passed: no optional dependency, Arrow only, pandas only.
- Benchmark correctness and reflection query-budget checks passed. Core insert speed remained unchanged; 101-name existence reflection dropped from 201 to 2 queries (0.214 s to 0.013 s in this sample).
- Type checking passed. Final formatting changes are derived directly from pre-commit output and rechecked by required CI on the final PR head.

[Executable CI evidence](https://github.com/leonardovida/duckdb-sqlalchemy/actions/runs/36757147809)

## Decisions and discoveries
- The managed executor failed to start; GitHub Actions supplied all executable validation.
- Native limitations are represented by explicit requirements or targeted assertions: DuckDB canonicalizes string lengths, regenerates constraint names, lacks constraint comments/identity modifiers, and restricts foreign-key changes within one transaction. CTE and join fixtures still test their behavior using valid DuckDB data setup.
- The existing qualified catalog.schema listing API is preserved and tested.
- Native RETURNING row counts are unavailable, so structured writes materialize their returned rows for reliable ORM version checks; SELECT batches remain bounded.
- Temporary tables need temporary implicit sequences to avoid orphaned persistent metadata.
- Faster conversion is declined when it changes numeric values or NaN/NULL semantics; pandas safety checks carry a measured cost.
- MotherDuck service access remains unverified here because credentials were not supplied. Official ecosystem acceptance and release publication require separate external outcomes.

## Outcomes and retrospective
All review findings are fixed with observed failure evidence. The suite now checks supported upstream dialect behavior instead of skipping it, catching several additional correctness problems before merge. The final PR requires green tests, pre-commit and type checks. No finite repository sweep can certify the absence of all future bugs.
