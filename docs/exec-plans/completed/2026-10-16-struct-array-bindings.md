# Preserve STRUCT ndarray binding

An extra post-merge probe exposed a regression in the STRUCT field conversion
fix after 1.5.6.3 reached PyPI. Standard ARRAY/FixedArray floating item processors
turn integer ndarrays into lists of unsupported NumPy integer scalars. The
earlier guard covered only arrays whose item type needed no conversion.

Use ndarray `tolist()` before standard array item conversion when hooks are
needed. Keep the no-op native path and custom array processors unchanged.
Exercise ordinary, Arrow and pandas fallback, fixed/variable arrays and
multidimensional input with real file reopen and raw stored values.

Plan: prove the regression, correct the conversion boundary, run full/focused,
floor/SQLAlchemy 2.1, lint/type gates and independent review, then merge and
publish the next fourth-component maintenance version. Preserve the original
release artifacts and worktree. Do not withdraw or overwrite PyPI 1.5.6.3.

## Outcome

[PR203](https://github.com/leonardovida/duckdb-sqlalchemy/pull/203) merged as
`da23d6d9ffa3adaba34ba5338dcf8caa49e26dc5` after all required hosted gates
passed. Tested and merged trees match. Independent review found no remaining
issue, including custom array overrides and distinct field closures.

Six of nine new cases fail on unchanged 1.5.6.3 and three Arrow controls pass.
All 18 STRUCT cases pass fixed. Full suite: 716 passed, five skipped. Floor
focused: 49 passed. SQLAlchemy 2.1 STRUCT: 18 passed. Types, lock consistency
and all-file pre-commit pass. Informational nightly retains the unchanged
12 Arrow/retry failures, with all new cases passing. The fresh immutable
public 1.5.6.3 wheel independently reproduces the ndarray failure.

The separate 1.5.6.4 maintenance release and exact publication verification
are tracked in this automation's continuity record.
