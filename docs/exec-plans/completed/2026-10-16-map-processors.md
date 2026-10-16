# MAP member processors

## Goal
Preserve declared SQLAlchemy key/value conversion when binding and reading MAPs.

## Scope
`Map.bind_processor`, `Map.result_processor`, real database regression tests,
and user-visible behavior notes. No changes to register eligibility, retries,
optional dependencies, or version floors.

## Plan
1. Compare current MAP, nested bulk inference, and exact division candidates.
2. Reproduce the selected defect before editing implementation.
3. Apply dialect-adapted member processors and verify stored and returned values.
4. Run focused, full, floor/stable, lint, type and distribution checks.
5. Review, push, merge after hosted gates, then use the maintenance release workflow
   if confirmed against the published distribution.

## Progress
- Inspected origin/main 880ce96, current docs/configuration and issue discussion.
- PR194 and PR196 are merged, release1.5.6.1 published. No open PR or changed issue.
- Canonical checkout retained with unrelated .claude files.

## Surprises & Discoveries
- MAPs already decline automatic registered bulk insertion, preserving ordinary
  bind processing even above the threshold. That path still omits member hooks.

## Candidate comparison

| Responsibility and code path | Evidence and impact | Scope and overlap |
| --- | --- | --- |
| MAP `datatypes.py` processors, ordinary/threshold INSERT and typed SELECT callers | Declared decorators are skipped silently, Enum insertion fails, Numeric/JSON reads bypass conversion. Six real-database cases fail on baseline. | Selected bounded conversion fix. No open PR or active schedule overlaps. |
| Bulk `_bulk_insert.py::build_bulk_insert_dataframe` via automatic register fallback | Nested list/struct NaN becomes NULL when registered, unlike ordinary binding. Top-level guard does not inspect nested values. | Independent follow-up. Do not widen this MAP patch. Reproduce end-to-end without Arrow before implementing. |
| New exact division compiler in `__init__.py` (not present last run) | 10 existing cases plus 3,202 bounded random exact cases match Python Decimal. Literal overflow also fails with option off. No supported-path regression. | Rejected with evidence, no patch or feature work. |

## Decision Log
- Use real file databases and raw SQL to distinguish stored conversion from
  result conversion. Preserve the existing legacy input-shape branch.

## Validation
- Baseline main: all six new execution cases fail. PyPI 1.5.6.1 also skips
  decorator storage conversion in a fresh isolated install outside the source.
- Fixed: 84 focused passed / 2 skipped, 620 full passed / 5 skipped.
- Minimum Python3.10 / DuckDB1.3 / SQLAlchemy2.0: 82 focused passed / 4 skipped.
- SQLAlchemy2.1.4: 84 focused passed / 2 skipped.
- Remote datatype suite: 49 passed. Credentialed MotherDuck not relevant/not run.
- All-file pre-commit, canonical ty, uv lock and diff checks pass. Formatting
  rerun only. Wheel/sdist strict Twine and fresh wheel smoke pass without pandas
  or Arrow. Full local nox matrix not run. All hosted stable/platform/distribution gates passed.

## Outcomes & Retrospective
PR198 merged as 068be9f20d874196ef6a5ec83b766fc2b679a266 after all
required stable gates and automated review passed. Tested and merged trees match.
The informational DuckDB2.0.0.dev2610151406 job has 12 Arrow/retry failures,
all reproduced on unchanged baseline (602 passed), with all six MAP cases passing
in the hosted head. No retry or weakened checks. Maintenance release1.5.6.2 is
prepared through a separate checked PR and the automatic registry/hash/smoke
workflow. The automation continuity record tracks its terminal publication state.
No performance claim or unrelated feature changes.
