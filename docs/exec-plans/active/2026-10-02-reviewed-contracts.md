# Reviewed dialect contracts

Baseline: released main `2e18a806be6f9d8b2ef8c30eee9b0d5d06ce15e9`.
Opus 5.5 high completed cycle one. Leo stopped Opus during cycle two and
authorized implementation, PR and normal delivery of the validated findings.

## Scope

- Preserve SQL-side bind expressions and MAP values above the bulk threshold.
- Prevent retries from losing raw-driver writes or replaying bound dynamic SQL.
- Repair reflected autoincrement DDL and both Alembic batch recreation paths.
- Preserve quoted search-path names and fixed-size array DDL contracts.
- Repair CSV temporary-file failure ownership and offer strict mapping validation.
- Avoid redundant Arrow RETURNING tuples and measure numeric-scan optimization.
- Add bounded Arrow conflict handling, optional dependency extras, and focused docs.

No native appender, automatic tuning, asynchronous driver, export redesign or
duplicated PR #181 snapshot work. Missing optional-dependency CI was disproven
by the existing six distribution lanes. Sparse mapping compatibility remains
the default. The broad PK-default-removal hypothesis needs semantics beyond
the observed implicit sequence exemption and is not an unconditional defect.

## Evidence and acceptance

Baseline execution confirms uppercase values bypass lower() at threshold 1,
MAP conversion failure only on Arrow, raw-driver INSERT discarded by a retry,
bound query() advancing a sequence twice, reflected batch DDL emitting SERIAL,
model batch recreation resetting the sequence, quoted attached-catalog lookup
failure, fixed INTEGER[3] becoming INTEGER[], and a leaked CSV header tempfile.
Regression tests must cover actual execution and data, dependency floors,
ordinary/Arrow/pandas parity, nested type/cache identity, and failure cleanup.

Before push: focused and full pytest, all-file pre-commit, canonical ty, lock,
distribution build/Twine, installed smokes and comparable performance samples.
Then review, required hosted gates, normal squash merge and exact-main checks.
A confirmed public bugfix qualifies for the established patch release flow.

## Ownership and progress

Main agent owns integration and final validation. One Luna delegate owns only
CSV/row-shape implementation and its tests. Opus is stopped and will not be run
again in this task. Worktree is isolated and the canonical checkout is preserved.
