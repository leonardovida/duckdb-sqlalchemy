# Partitioned Parquet snapshot recipe

## Goal
Let analytics developers export filtered SQLAlchemy queries into partitioned
snapshots and query the recovered partition columns using existing public APIs.

## Scope
One executable local recipe, user documentation, and integration coverage. No
new API, dependency, remote-storage claim, or performance claim.

## Plan
Validate native COPY partition options, demonstrate a bound-query round trip,
cover empty exports and destination reuse, then check compatibility and CI.

## Progress
- Inspected main `3258c9a`, strategy memory, maintenance/refactor records, and open
  issues/PRs. Export shipped in v1.5.5.8; no overlapping PR or new demand signal.
- Confirmed the existing option renderer supports partitioned exports.
- Added example, docs, and three regression cases.

## Surprises & Discoveries
An empty partitioned export returns zero but writes no Parquet files; scanning
its glob raises an error. A nonempty destination is refused by default, leaving
existing files intact. Exported files survive SQL rollback.

## Decision Log
Continue the export initiative with a runnable workflow, not another wrapper.
DuckDB's current partitioned-write documentation and local probes establish the
behavior; demand remains a hypothesis, not an issue-backed conclusion.

## Validation
Baseline full suite: 315 passed, 8 skipped. Independent DuckDB 1.3.0 / SQLAlchemy
2.0.0 probes passed partition readback, empty export, and byte preservation after
refused reuse. DuckDB 1.5.5 / SQLAlchemy 2.0.52 empty-export probe also passed.
Final full suite: 318 passed, 8 skipped. All 12 export tests and the executable
recipe pass on both dependency pairs. Pre-commit (including ty/Ruff), lock check,
and diff check pass after automatic formatting corrections. Hosted CI is the
remaining delivery gate.

## Outcomes & Retrospective
Existing APIs cover the workflow; no runtime implementation change is needed.
Prefer new snapshot destinations and handle zero rows before scanning.
