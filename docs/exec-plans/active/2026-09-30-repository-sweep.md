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
- Preparing regressions and CI-based validation.

## Surprises & Discoveries
- The managed executor cannot start. GitHub Actions is the executable validation environment.
- Existing buffering tests require suppressed fetch errors.
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
