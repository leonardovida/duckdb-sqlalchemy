# Repository correctness and performance sweep

## Goal
Make DuckDB's SQLAlchemy integration preserve values, expose failures, reflect the correct objects, and validate supported workflows and performance.

## Scope
The nine findings from the repository review; batched reflection, reduced ingestion copies, direct Arrow ingestion and bounded Arrow reads; SQLAlchemy, optional dependency, platform, distribution and MotherDuck validation; reproducible benchmarks; accurate migration and adoption guidance.

## Plan
1. Add regression tests on the reviewed base commit and observe their failures in GitHub Actions.
2. Fix result ownership, DML counts, numeric conversion, bulk conversion, CSV serialization and Alembic comparisons.
3. Apply reflection kind/scope consistently, preserve FK target schemas, and batch existence checks.
4. Add Arrow APIs, semantic differential tests and benchmarks.
5. Strengthen CI, packaging and documentation, then run required checks and review the complete diff.

## Progress
- Reviewed main at 6b41e798d7525a94176ada4e6a7ac1cb3d648df3.
- Created a dedicated working branch.
- Reproduced all nine review findings on the unchanged runtime and implemented regression-backed fixes.
- Added Arrow ingestion/batch APIs, batched reflection and conversion reuse.
- Expanded packaging, platform, upstream and minimum-version CI; resolving final conformance differences.

## Surprises & Discoveries
- The managed executor cannot start. GitHub Actions is the executable validation environment.
- Existing buffering tests required suppressed fetch errors; updated them to enforce error propagation.
- The upstream SQLAlchemy suite is skipped and the MotherDuck test targets an unsupported DuckDB version.
- The pandas method matrix does not forward its method argument.

## Decision Log
- Keep all work on a new branch and use GitHub Actions for executable checks.
- Require regression evidence and green required checks before merging.
- Keep real MotherDuck tests opt-in and report missing credentials; do not invent service credentials.
- Official upstream adoption requires maintainers' acceptance; add concrete migration/adoption materials without claiming official status.

## Validation
Pending baseline regressions, pytest, pre-commit/Ruff/ty, upstream suite, platform and package tests, benchmark smoke runs and complete PR CI.

## Outcomes & Retrospective
In progress.

### Reproduction
- GitHub Actions Python 3.11: 14 failed, 484 passed, 2 skipped with the unmodified runtime. All nine findings reproduced, including integer corruption in the stored database value.
- Prepared fixes preserve fetch errors, exact numeric fallback, SQL literal identity, numeric return scale, COPY CSV options, and commented/CTE DML row counts.
- Added batch reflection scope/kind handling and batched existence checks; added owned bounded Arrow readers.

### Expanded validation
- All nine original regressions now pass; remaining regular-suite failures identify CSV sniffing with custom dialects. Typed Arrow, decimals, bytes, JSON, timezone, nullable integers and NaN differential tests pass.
- Six isolated wheel/sdist checks pass with neither optional library, Arrow only and pandas only. Initial benchmark smoke checks pass, including exact aggregate checksums and two-query batch existence.
- Windows exposed case-insensitive environment alias assumptions in tests; fixed the fixtures with explicit mappings.
- Upstream suite setup required a profile path and its standard pytest markers. Enable and resolve actual compatibility tests next.

### Conformance and performance follow-through
- Ordinary inserts and bulk inserts have no material regression in the same-runner smoke benchmark after replacing the common-path SQL lexer with a guarded fast path.
- Batch existence reflection: 201 baseline queries versus 2; one CI sample improved from 0.217 s to 0.016 s.
- Generic Float now compiles to DOUBLE unless single precision is explicitly requested. Non-returning structured DML hides DuckDB's Count acknowledgment; raw SQL preserves its established Count-row contract.
- Fixed upstream harness provisioning for schemas and temporary tables. File-backed tests use independent connections. Requirements expose supported capabilities and explicitly close unsupported identity, cascades and named constraint behavior.
- Preserve actual native string normalization and Boolean default expressions in targeted suite assertions. CTE fixtures use an integer hierarchy without DuckDB's unsupported self-referencing insert constraint.
- Upstream checks additionally found sequence defaults incorrectly reflected as non-autoincrement and inherited unsupported constraint-comment capability; fixed both.
- Exact RETURNING row counts preserve stale-version detection on the SQLAlchemy 2.0.0 floor.
- Added 25 randomized nullable signed-64-bit integer/text round trips for each Arrow, pandas and ordinary path.
