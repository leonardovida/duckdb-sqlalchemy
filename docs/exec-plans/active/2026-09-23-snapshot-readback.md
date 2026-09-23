# Snapshot identifier preservation

## Goal
Preserve string partition identifiers when SQLAlchemy users read exported snapshots.

## Scope
Document and test existing Hive reader options. Fix the repeated-export defect
uncovered by the experiment. No new public API, dependency, or release.

## Plan
Compare inferred and explicit partition types, verify repeated snapshots remain
independent, then run export compatibility tests, full tests, and pre-commit.

## Progress
Inspected main 8628f289, prior strategy and maintenance records, and current
issues/PRs. No default-branch delta or overlapping PR. Implemented readback
coverage and a narrow COPY cache opt-out.

## Surprises & Discoveries
DuckDB 1.5.5 preserves `001` by default, but infers `123` as an integer and
`2026-09-23` as a date. Explicit VARCHAR preserves all three strings.
Repeated structurally identical exports reused the first literal destination
and COPY options. A real regression test reproduced silent overwrite of the
first file with the second export's rows.

## Decision Log
Existing hive_types options cover typed readback. Document them instead of
adding an API. Fix cache behavior as necessary support for repeated snapshots.
inherit_cache=False does not disable a class's own _traverse_internals cache
key. Explicitly return no compilation cache key, preserving bind traversal.
No performance claim. Each export compiles its own COPY statement.

## Validation
Baseline: 318 passed, 8 skipped. Regression fails without the cache opt-out,
returning row 2 from the first file instead of row 1. Final full suite: 322 passed, 8 skipped. All 16 export tests pass on DuckDB
1.3.0 / SQLAlchemy 2.0.0 and the current DuckDB 1.5.5 / SQLAlchemy 2.0.52
suite passes. The partitioned example, pre-commit including ty/Ruff, lock
check, and diff check pass. Formatting corrections were rerun clean.

## Outcomes & Retrospective
The typed readback experiment established no new API need and exposed a
correctness blocker to repeated snapshots. Hosted checks remain the merge gate.
